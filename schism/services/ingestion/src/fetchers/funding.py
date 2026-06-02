"""
funding.py — Binance BTCUSDT perpetual funding rate fetcher.

Two fetch modes:
  fetch_historical()  — bulk monthly zips from data.binance.vision
                        Vision funding CSVs are sparse: only settlement bars present.
                        Non-settlement bars must be filled with funding_rate=0 by the
                        publisher before assembling RawBar.
  fetch_live()        — Binance Futures REST:
                          /fapi/v1/fundingRate   (weight 1) → latest settlement rate
                          /fapi/v1/fundingInfo   (weight 0) → fundingIntervalHours

Vision CSV columns (3-column, header varies by file age):
  0   calcTime            ms UTC of settlement
  1   fundingIntervalHours
  2   fundingRate

Design note — why NOT ΔFR:
  fundingIntervalHours makes FR zero on (N-1) bars out of every N.
  ΔFR would be a deterministic schedule artifact, not a signal.
  U3 = EWMA(FR_t) uses the level; U4 = 1[settlement bar] uses a binary flag.
  Both are computed in the feature service, not here. This fetcher only
  provides the raw FR value and the interval so the feature service can
  derive U4 without hardcoding 8h.
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

_VISION_BASE = "https://data.binance.vision/data/futures/um/monthly/fundingRate"
_MAX_RETRIES = 3

# Vision CSV sometimes ships with a header row, sometimes without.
# We read with header=None and rename by position, then coerce types.
_COLS_IDX  = [0, 1, 2]
_COLS_NAME = ["calc_time_ms", "funding_interval_hours", "funding_rate"]


# ---------------------------------------------------------------------------
# Internal helpers (same download pattern as klines.py)
# ---------------------------------------------------------------------------

def _vision_url(symbol: str, year: int, month: int) -> str:
    fname = f"{symbol}-fundingRate-{year}-{month:02d}.zip"
    return f"{_VISION_BASE}/{symbol}/{fname}"


async def _download_one(
    client: httpx.AsyncClient,
    url: str,
    sem: asyncio.Semaphore,
    jitter: tuple[float, float],
) -> bytes | None:
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


def _parse_zip(raw: bytes, year: int, month: int) -> pd.DataFrame:
    """
    Parse one Vision funding zip.

    Vision CSVs vary: some have a header row (calcTime,fundingIntervalHours,fundingRate),
    some don't. We detect by checking whether the first row is numeric.
    """
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            raw_text = f.read().decode()

    first_line = raw_text.splitlines()[0]
    has_header = not first_line.split(",")[0].strip().isdigit()

    df = pd.read_csv(
        io.StringIO(raw_text),
        header=0 if has_header else None,
        names=_COLS_NAME if not has_header else None,
        usecols=_COLS_IDX,
        dtype=str,
    )
    # Normalise column names regardless of whether header was present.
    df.columns = _COLS_NAME

    df["timestamp"]             = pd.to_datetime(pd.to_numeric(df["calc_time_ms"]), unit="ms", utc=True)
    df["funding_rate"]          = pd.to_numeric(df["funding_rate"], errors="raise")
    df["funding_interval_hours"] = pd.to_numeric(df["funding_interval_hours"], errors="raise")

    # Sanity: funding rate typically in [-0.03, 0.03]; log outliers, don't drop
    extreme = df["funding_rate"].abs() > 0.03
    if extreme.any():
        log.warning(
            "%04d-%02d: %d bars with |funding_rate| > 0.03 (max %.5f)",
            year, month, extreme.sum(), df["funding_rate"].abs().max(),
        )

    return df[["timestamp", "funding_rate", "funding_interval_hours"]]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_historical(
    start: date,
    end: date,
    raw_dir: Path,
    symbol: str = "BTCUSDT",
    semaphore_limit: int = 5,
    jitter: tuple[float, float] = (0.5, 1.0),
) -> pd.DataFrame:
    """
    Download monthly Vision funding zips for [start, end].

    Returns a sparse DataFrame — only settlement bars are present.
    Columns: timestamp, funding_rate, funding_interval_hours.

    The publisher must left-join this onto the klines timestamp index and
    fill non-settlement bars with funding_rate=0 before building RawBar.
    Saves to raw_dir/{symbol}_funding.parquet (merge-on-existing, dedup on timestamp).
    """
    months: list[tuple[int, int]] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1

    log.info(
        "Vision funding: %d monthly zips for %s [%04d-%02d → %04d-%02d]",
        len(months), symbol, months[0][0], months[0][1], months[-1][0], months[-1][1],
    )

    sem = asyncio.Semaphore(semaphore_limit)
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = [_download_one(client, _vision_url(symbol, y, m), sem, jitter) for y, m in months]
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
        raise RuntimeError(f"No funding data fetched for {symbol} [{start}, {end}]")

    fresh = (
        pd.concat(frames, ignore_index=True)
        .sort_values("timestamp")
        .drop_duplicates("timestamp")
        .reset_index(drop=True)
    )

    out = raw_dir / f"{symbol}_funding.parquet"
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
    log.info("Saved %d settlement bars → %s", len(fresh), out)
    return fresh


def _check_weight(headers: httpx.Headers) -> None:
    used = headers.get("X-MBX-USED-WEIGHT-1M")
    if used is not None and int(used) > 5_000:
        log.warning("Binance REST weight %s/6000 — approaching limit", used)


async def _fetch_funding_interval_hours(
    client: httpx.AsyncClient,
    base_url: str,
    symbol: str,
) -> float:
    """
    GET /fapi/v1/fundingInfo — returns fundingIntervalHours for the symbol.
    Weight: 0. Changes rarely; caller should cache the result.
    """
    resp = await client.get(f"{base_url}/fapi/v1/fundingInfo")
    _check_weight(resp.headers)
    resp.raise_for_status()

    for entry in resp.json():
        if entry["symbol"] == symbol:
            return float(entry["fundingIntervalHours"])

    raise ValueError(f"Symbol {symbol} not found in /fapi/v1/fundingInfo response")


async def fetch_live(
    client: httpx.AsyncClient,
    base_url: str,
    bar_timestamp: datetime,
    symbol: str = "BTCUSDT",
    _funding_interval_cache: dict[str, float] | None = None,
) -> dict[str, Any]:
    """
    Return funding_rate and funding_interval_hours for the given bar.

    If bar_timestamp falls on a settlement time (bar.hour % funding_interval_hours == 0),
    fetches the actual rate from /fapi/v1/fundingRate. Otherwise returns funding_rate=0
    (no settlement occurred this bar).

    _funding_interval_cache: pass a dict to avoid re-calling /fapi/v1/fundingInfo on
    every bar. The caller (scheduler) should maintain this dict across invocations.

    Raises RuntimeError on 429/418. See klines.fetch_live for escalation policy.
    """
    cache = _funding_interval_cache if _funding_interval_cache is not None else {}

    if symbol not in cache:
        cache[symbol] = await _fetch_funding_interval_hours(client, base_url, symbol)

    interval_h = cache[symbol]

    # Determine if this bar is a settlement bar without hardcoding 8h.
    is_settlement = (bar_timestamp.hour % int(interval_h)) == 0

    if not is_settlement:
        return {
            "funding_rate":           0.0,
            "funding_interval_hours": interval_h,
        }

    # Settlement bar — pull the actual rate.
    resp = await client.get(
        f"{base_url}/fapi/v1/fundingRate",
        params={"symbol": symbol, "limit": 1},
    )
    _check_weight(resp.headers)

    if resp.status_code == 429:
        raise RuntimeError("Binance 429 — back off immediately, do not retry")
    if resp.status_code == 418:
        retry_after = resp.headers.get("Retry-After", "?")
        raise RuntimeError(f"Binance 418 (IP ban) — Retry-After: {retry_after}s")

    resp.raise_for_status()

    data = resp.json()
    if not data:
        log.warning("Empty /fapi/v1/fundingRate response for settlement bar %s", bar_timestamp)
        return {"funding_rate": 0.0, "funding_interval_hours": interval_h}

    return {
        "funding_rate":           float(data[0]["fundingRate"]),
        "funding_interval_hours": interval_h,
    }
