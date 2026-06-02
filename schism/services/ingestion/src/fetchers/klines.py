"""
klines.py — Binance BTCUSDT perpetual klines fetcher.

Two fetch modes:
  fetch_historical()  — bulk monthly zips from data.binance.vision
                        S3-style static hosting; no documented rate limit but SSL EOF
                        occurs at high concurrency. Safe: Semaphore(5) + jitter.
  fetch_live()        — single bar from Binance Futures REST (/fapi/v1/klines)
                        Weight: 1 per call (limit=2). Reads X-MBX-USED-WEIGHT-1M.
                        On 429 → raise immediately (caller backs off).
                        On 418 → raise with Retry-After from header (IP ban).

Binance klines column layout (12 columns, 0-indexed):
  0   open_time                  ms UTC
  1   open
  2   high
  3   low
  4   close
  5   volume
  6   close_time                 (dropped)
  7   quote_asset_volume         (dropped)
  8   number_of_trades           (dropped)
  9   taker_buy_base_volume      O5 / CVD source — klines col 9
  10  taker_buy_quote_volume     (dropped)
  11  ignore                     (dropped)
"""
from __future__ import annotations

import asyncio
import io
import logging
import random
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

log = logging.getLogger(__name__)

_VISION_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
_COLS_IDX  = [0, 1, 2, 3, 4, 5, 9]
_COLS_NAME = ["open_time", "open", "high", "low", "close", "volume", "taker_buy_base_volume"]
_MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _vision_url(symbol: str, interval: str, year: int, month: int) -> str:
    fname = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    return f"{_VISION_BASE}/{symbol}/{interval}/{fname}"


async def _download_one(
    client: httpx.AsyncClient,
    url: str,
    sem: asyncio.Semaphore,
    jitter: tuple[float, float],
) -> bytes | None:
    """
    Download one Vision zip under the semaphore.

    Retries on SSL EOF (RemoteProtocolError/ReadError) with exponential backoff —
    this is the typical failure mode on concurrent Vision downloads, not a rate limit.
    Jitter is applied before releasing the semaphore slot to throttle batch rate.
    """
    async with sem:
        result: bytes | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await client.get(url, follow_redirects=True)
                if resp.status_code == 404:
                    # Future months don't exist yet — not an error.
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

        # Throttle: hold the semaphore slot a bit longer before the next task acquires it.
        await asyncio.sleep(random.uniform(*jitter))

    return result


def _parse_zip(raw: bytes, year: int, month: int) -> pd.DataFrame:
    """Extract the CSV from zip bytes and return a typed DataFrame."""
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            raw_text = f.read().decode()

    # Klines CSVs have no header before 2022-01; newer files ship with one.
    # Detect by checking whether the first field is numeric (a timestamp in ms).
    first_line = raw_text.splitlines()[0]
    has_header = not first_line.split(",")[0].strip().isdigit()

    df = pd.read_csv(
        io.StringIO(raw_text),
        header=0 if has_header else None,
        usecols=_COLS_IDX,
        names=_COLS_NAME if not has_header else None,
        dtype=str,
    )
    # Normalise column names regardless of whether header was present.
    df.columns = _COLS_NAME

    # open_time is ms UTC → tz-aware datetime
    df["timestamp"] = pd.to_datetime(pd.to_numeric(df["open_time"]), unit="ms", utc=True)
    df = df.drop(columns=["open_time"])

    for col in ["open", "high", "low", "close", "volume", "taker_buy_base_volume"]:
        df[col] = pd.to_numeric(df[col], errors="raise")

    # Sanity: taker_buy_base_volume must be in [0, volume]
    bad = (df["taker_buy_base_volume"] < 0) | (df["taker_buy_base_volume"] > df["volume"])
    if bad.any():
        log.warning("%04d-%02d: %d bars with taker_buy_base_volume outside [0, volume]", year, month, bad.sum())

    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_historical(
    start: date,
    end: date,
    raw_dir: Path,
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    semaphore_limit: int = 5,
    jitter: tuple[float, float] = (0.5, 1.0),
) -> pd.DataFrame:
    """
    Download monthly Vision zips for [start, end] and return a combined DataFrame.

    Merge-writes to raw_dir/{symbol}_{interval}_klines.parquet — existing bars are
    kept so partial re-runs don't re-download already-saved months (dedup on timestamp).
    The parquet file is the canonical raw store; never overwrite individual rows.
    """
    # Build (year, month) list covering [start, end] inclusive.
    months: list[tuple[int, int]] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1

    log.info(
        "Vision klines: %d monthly zips for %s %s [%04d-%02d → %04d-%02d]",
        len(months), symbol, interval, months[0][0], months[0][1], months[-1][0], months[-1][1],
    )

    sem = asyncio.Semaphore(semaphore_limit)
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = [
            _download_one(client, _vision_url(symbol, interval, y, m), sem, jitter)
            for y, m in months
        ]
        payloads = await asyncio.gather(*tasks)

    frames: list[pd.DataFrame] = []
    for (y, m), raw in zip(months, payloads):
        if raw is None:
            continue
        try:
            frames.append(_parse_zip(raw, y, m))
        except Exception as exc:
            log.warning("Parse error %04d-%02d: %s", y, m, exc)

    if not frames:
        raise RuntimeError(
            f"No klines data fetched for {symbol} {interval} [{start}, {end}]. "
            "Check that COMMON_FLOOR >= 2020-09-01 and end <= current month."
        )

    fresh = (
        pd.concat(frames, ignore_index=True)
        .sort_values("timestamp")
        .drop_duplicates("timestamp")
        .reset_index(drop=True)
    )

    out = raw_dir / f"{symbol}_{interval}_klines.parquet"
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
    log.info("Saved %d bars → %s", len(fresh), out)
    return fresh


def _check_weight(headers: httpx.Headers) -> None:
    """Warn if REST weight usage is approaching the 6,000/min limit."""
    used = headers.get("X-MBX-USED-WEIGHT-1M")
    if used is not None and int(used) > 5_000:
        log.warning("Binance REST weight %s/6000 — approaching limit", used)


async def fetch_live(
    client: httpx.AsyncClient,
    base_url: str,
    symbol: str = "BTCUSDT",
    interval: str = "1h",
) -> dict[str, Any]:
    """
    Pull the last fully-closed 1h bar from Binance Futures REST.

    limit=2: index 0 = last closed bar, index 1 = current open bar (discarded).
    Weight cost: 1. Reads X-MBX-USED-WEIGHT-1M after every call.

    Raises RuntimeError on 429 (rate limit) or 418 (IP ban).
    Caller (scheduler.py) is responsible for back-off on 429 and
    honouring Retry-After on 418 — do not retry inside this function.
    """
    resp = await client.get(
        f"{base_url}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": 2},
    )
    _check_weight(resp.headers)

    if resp.status_code == 429:
        raise RuntimeError("Binance 429 — back off immediately, do not retry")
    if resp.status_code == 418:
        retry_after = resp.headers.get("Retry-After", "?")
        raise RuntimeError(f"Binance 418 (IP ban) — Retry-After: {retry_after}s")

    resp.raise_for_status()

    row = resp.json()[0]  # index 0 = last closed bar
    return {
        "timestamp":              datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
        "open":                   float(row[1]),
        "high":                   float(row[2]),
        "low":                    float(row[3]),
        "close":                  float(row[4]),
        "volume":                 float(row[5]),
        "taker_buy_base_volume":  float(row[9]),
    }
