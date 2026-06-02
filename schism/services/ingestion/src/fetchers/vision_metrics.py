"""
vision_metrics.py — Binance open interest + long/short ratio fetcher.

Two fetch modes:
  fetch_historical()  — daily metrics zips from data.binance.vision
                        Path: /data/futures/um/daily/metrics/{symbol}/{date}.zip
                        Each zip: 5-min snapshots of OI and LSR for one day (288 rows).
                        Resampled to 1h by taking the first reading of each hour —
                        this HH:00:00 snapshot aligns with klines open_time.

  fetch_live()        — Binance Futures REST:
                          /fapi/v1/openInterest                        (weight 1) → OI
                          /futures/data/globalLongShortAccountRatio    (weight ?) → LSR

Vision daily metrics CSV columns (subset used):
  create_time                  YYYY-MM-DD HH:MM:SS UTC, 5-min intervals
  sum_open_interest            BTC-denominated OI level → oi
  count_long_short_ratio       global all-user L/S positions ratio → lsr
  (others dropped)

Note: data.binance.vision does NOT have monthly openInterest/ or longShortRatio/
paths. The daily/metrics/ path is the only bulk-download source for OI and LSR.

COMMON_FLOOR: daily metrics start 2020-09-01.
"""
from __future__ import annotations

import asyncio
import io
import logging
import math
import random
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

log = logging.getLogger(__name__)

_VISION_BASE = "https://data.binance.vision/data/futures/um/daily/metrics"
_MAX_RETRIES = 3

_OI_COL  = "sum_open_interest"
_LSR_COL = "count_long_short_ratio"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _metrics_url(symbol: str, d: date) -> str:
    fname = f"{symbol}-metrics-{d.strftime('%Y-%m-%d')}.zip"
    return f"{_VISION_BASE}/{symbol}/{fname}"


async def _download_one(
    client: httpx.AsyncClient,
    url: str,
    sem: asyncio.Semaphore,
    jitter: tuple[float, float],
) -> bytes | None:
    """
    Download one daily metrics zip under the semaphore.

    Retries on SSL EOF with exponential back-off. Jitter is held before releasing
    the semaphore slot to throttle the batch rate against the Vision CDN.
    """
    async with sem:
        result: bytes | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await client.get(url, follow_redirects=True)
                if resp.status_code == 404:
                    log.debug("404 (not yet available): %s", url)
                    break
                resp.raise_for_status()
                result = resp.content
                break
            except (httpx.RemoteProtocolError, httpx.ReadError) as exc:
                if attempt == _MAX_RETRIES:
                    log.warning("Vision download failed after %d attempts %s: %s", _MAX_RETRIES, url, exc)
                    break
                wait = 2 ** attempt + random.uniform(*jitter)
                log.debug("SSL EOF retry %d/%d for %s in %.1fs", attempt, _MAX_RETRIES, url, wait)
                await asyncio.sleep(wait)

        await asyncio.sleep(random.uniform(*jitter))

    return result


def _parse_zip(raw: bytes, d: date) -> pd.DataFrame:
    """
    Parse one daily metrics zip.

    Resamples from 5-min to 1h by taking the first reading of each hour window.
    The HH:00:00 snapshot aligns with klines open_time.
    Returns columns: timestamp (UTC, hourly), oi, lsr.
    """
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            df = pd.read_csv(f, dtype=str)

    df.columns = [c.strip() for c in df.columns]

    missing = {"create_time", _OI_COL, _LSR_COL} - set(df.columns)
    if missing:
        raise ValueError(f"Metrics {d}: missing columns {missing}, got {list(df.columns)}")

    df["create_time"] = pd.to_datetime(df["create_time"], utc=True)
    df[_OI_COL]       = pd.to_numeric(df[_OI_COL],  errors="raise")
    df[_LSR_COL]      = pd.to_numeric(df[_LSR_COL], errors="raise")

    # Resample 5-min → 1h: take first value (= HH:00:00 snapshot per bar).
    df["timestamp"] = df["create_time"].dt.floor("h")
    hourly = (
        df.groupby("timestamp", sort=True)[[_OI_COL, _LSR_COL]]
        .first()
        .reset_index()
        .rename(columns={_OI_COL: "oi", _LSR_COL: "lsr"})
    )

    nan_count = hourly[["oi", "lsr"]].isna().sum()
    if nan_count.any():
        log.warning("Metrics %s: NaN oi=%d lsr=%d — dropping", d, nan_count["oi"], nan_count["lsr"])
    hourly = hourly.dropna(subset=["oi", "lsr"])

    if (hourly["oi"] <= 0).any():
        log.warning("Metrics %s: %d non-positive OI values", d, (hourly["oi"] <= 0).sum())
    if (hourly["lsr"] <= 0).any():
        log.warning("Metrics %s: %d non-positive LSR values", d, (hourly["lsr"] <= 0).sum())

    return hourly


def _day_list(start: date, end: date) -> list[date]:
    days: list[date] = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_historical(
    start: date,
    end: date,
    raw_dir: Path,
    symbol: str = "BTCUSDT",
    interval: str = "1h",  # unused — kept for API parity with klines/funding fetchers
    semaphore_limit: int = 10,  # daily files are ~11 KB; higher concurrency is safe
    jitter: tuple[float, float] = (0.5, 1.0),
) -> pd.DataFrame:
    """
    Download daily metrics zips for [start, end], resample to 1h, and return.

    Saves to raw_dir/{symbol}_vision_metrics.parquet (merge-on-existing, dedup on timestamp).
    """
    days = _day_list(start, end)
    log.info(
        "Vision metrics: %d daily zips for %s [%s → %s]",
        len(days), symbol, start, end,
    )

    sem = asyncio.Semaphore(semaphore_limit)
    async with httpx.AsyncClient(timeout=60.0) as client:
        payloads = await asyncio.gather(
            *[_download_one(client, _metrics_url(symbol, d), sem, jitter) for d in days]
        )

    frames: list[pd.DataFrame] = []
    for d, raw in zip(days, payloads):
        if raw is None:
            continue
        try:
            frames.append(_parse_zip(raw, d))
        except Exception as exc:
            log.warning("Metrics parse error %s: %s", d, exc)

    if not frames:
        raise RuntimeError(
            f"No metrics data fetched for {symbol} [{start}, {end}]. "
            "Check that start >= 2020-09-01 (COMMON_FLOOR)."
        )

    fresh = (
        pd.concat(frames, ignore_index=True)
        .sort_values("timestamp")
        .drop_duplicates("timestamp")
        .reset_index(drop=True)
    )

    out = raw_dir / f"{symbol}_vision_metrics.parquet"
    if out.exists():
        existing = pd.read_parquet(out)
        fresh = (
            pd.concat([existing, fresh], ignore_index=True)
            .sort_values("timestamp")
            .drop_duplicates("timestamp")
            .reset_index(drop=True)
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    fresh.to_parquet(out, index=False)
    log.info("Saved %d 1h bars → %s", len(fresh), out)
    return fresh


def _check_weight(headers: httpx.Headers) -> None:
    used = headers.get("X-MBX-USED-WEIGHT-1M")
    if used is not None and int(used) > 5_000:
        log.warning("Binance REST weight %s/6000 — approaching limit", used)


async def fetch_live(
    client: httpx.AsyncClient,
    base_url: str,
    symbol: str = "BTCUSDT",
    period: str = "1h",
) -> dict[str, Any]:
    """
    Fetch the latest OI and LSR from Binance REST.

    OI:  GET {base_url}/fapi/v1/openInterest                           weight 1
    LSR: GET {base_url}/futures/data/globalLongShortAccountRatio       weight ?
         LSR endpoint is under /futures/data/, not /fapi/v1/.

    Raises RuntimeError on 429/418 per Binance escalation policy.
    """
    oi_resp, lsr_resp = await asyncio.gather(
        client.get(f"{base_url}/fapi/v1/openInterest", params={"symbol": symbol}),
        client.get(
            f"{base_url}/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": 1},
        ),
    )

    for resp in (oi_resp, lsr_resp):
        _check_weight(resp.headers)
        if resp.status_code == 429:
            raise RuntimeError("Binance 429 — back off immediately, do not retry")
        if resp.status_code == 418:
            retry_after = resp.headers.get("Retry-After", "?")
            raise RuntimeError(f"Binance 418 (IP ban) — Retry-After: {retry_after}s")
        resp.raise_for_status()

    oi       = float(oi_resp.json()["openInterest"])
    lsr_data = lsr_resp.json()

    if not lsr_data:
        raise RuntimeError(f"Empty LSR response from Binance for {symbol} period={period}")

    lsr = float(lsr_data[0]["longShortRatio"])

    if not math.isfinite(oi) or oi <= 0:
        raise ValueError(f"Invalid OI from REST: {oi}")
    if not math.isfinite(lsr) or lsr <= 0:
        raise ValueError(f"Invalid LSR from REST: {lsr}")

    return {"oi": oi, "lsr": lsr}
