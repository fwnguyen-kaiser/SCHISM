"""
publishers.py — merge fetcher outputs and XADD RawBar events to Redis Streams.

Merge strategy (klines is the spine, left join):
  klines          dense 1h — every bar present
  funding         sparse   — only settlement bars; all others filled funding_rate=0.0
  vision_metrics  dense 1h — every bar; gaps forward-filled (≤2 bars) with warning

NaN policy (applied before building RawBar):
  funding_rate            → fill 0.0   (no settlement occurred this bar)
  funding_interval_hours  → ffill      (interval is constant; carry from nearest settlement)
  oi                      → ffill ≤2, then drop + warn
  lsr                     → ffill ≤2, then drop + warn

Any bar that still contains NaN after fills is dropped and logged — never published.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from schism_shared import RawBar, RedisStreamClient
from schism_shared.constants import STREAM_NAMES

log = logging.getLogger(__name__)

_RAW_BAR_STREAM = STREAM_NAMES["raw_bar"]

# Maximum consecutive bars we'll forward-fill OI/LSR before dropping the bar.
_MAX_FFILL_BARS = 2


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------

def _merge_historical(
    klines: pd.DataFrame,
    funding: pd.DataFrame,
    vision: pd.DataFrame,
) -> pd.DataFrame:
    """
    Left-join klines ← funding ← vision on timestamp.

    Returns a clean DataFrame ready to iterate into RawBar objects.
    Bars that cannot be repaired (NaN OI/LSR beyond ffill limit) are dropped.
    """
    df = klines.copy()

    # --- funding (sparse) ---
    # funding DataFrame only has settlement bars.
    # Left join → non-settlement rows get NaN in funding columns.
    df = df.merge(
        funding[["timestamp", "funding_rate", "funding_interval_hours"]],
        on="timestamp",
        how="left",
    )
    nan_fr = df["funding_rate"].isna().sum()
    if nan_fr:
        log.debug("Filling %d non-settlement bars with funding_rate=0.0", nan_fr)
    df["funding_rate"] = df["funding_rate"].fillna(0.0)

    # funding_interval_hours: forward-fill (interval constant between settlements)
    df["funding_interval_hours"] = (
        df["funding_interval_hours"]
        .ffill()
        .bfill()  # bfill covers bars before the first settlement in the window
    )
    still_nan = df["funding_interval_hours"].isna().sum()
    if still_nan:
        log.warning(
            "%d bars have no funding_interval_hours even after ffill/bfill — "
            "check that funding data covers the same date range as klines",
            still_nan,
        )

    # --- vision metrics (dense) ---
    df = df.merge(
        vision[["timestamp", "oi", "lsr"]],
        on="timestamp",
        how="left",
    )

    # OI=0 is a Vision data error — Binance never reports zero open interest.
    # Treat as NaN so the same ffill policy applies, rather than publishing
    # phantom O7=0 / U1=-1 values that would inject false regime signal.
    zero_oi = (df["oi"] <= 0).sum()
    if zero_oi:
        log.warning("Replacing %d OI<=0 bars with NaN (Vision data error)", zero_oi)
        df.loc[df["oi"] <= 0, "oi"] = float("nan")

    for col in ("oi", "lsr"):
        gap_count = df[col].isna().sum()
        if gap_count:
            log.warning(
                "Vision gap: %d bars missing %s — forward-filling up to %d bars",
                gap_count, col, _MAX_FFILL_BARS,
            )
        df[col] = df[col].ffill(limit=_MAX_FFILL_BARS)

    # Drop bars that still have NaN after fills
    required = ["open", "high", "low", "close", "volume", "taker_buy_base_volume",
                "funding_rate", "funding_interval_hours", "oi", "lsr"]
    before = len(df)
    df = df.dropna(subset=required)
    dropped = before - len(df)
    if dropped:
        log.warning("Dropped %d bars with unresolvable NaN after merge", dropped)

    return df.reset_index(drop=True)


def _validate_live(fields: dict[str, Any]) -> None:
    """
    Validate a single live bar dict before building RawBar.
    Raises ValueError on any non-finite or out-of-range value.
    """
    checks: list[tuple[str, float, float]] = [
        ("open",                     1e-6, 1e9),
        ("high",                     1e-6, 1e9),
        ("low",                      1e-6, 1e9),
        ("close",                    1e-6, 1e9),
        ("volume",                   1e-6, 1e15),   # zero-vol live bar = API glitch
        ("taker_buy_base_volume",    0.0,  1e15),   # 0 is valid (all sells)
        ("oi",                       1e-6, 1e15),   # OI=0 is a Vision data error
        ("lsr",                      1e-6, 1e6),
        ("funding_interval_hours",   1.0,  24.0),
    ]
    for name, lo, hi in checks:
        v = fields.get(name)
        if v is None or not math.isfinite(float(v)):
            raise ValueError(f"Live bar field '{name}' is None or non-finite: {v!r}")
        if not (lo <= float(v) <= hi):
            raise ValueError(f"Live bar field '{name}'={v} outside expected range [{lo}, {hi}]")

    # funding_rate is allowed to be 0.0 (non-settlement) or any finite value
    fr = fields.get("funding_rate")
    if fr is None or not math.isfinite(float(fr)):
        raise ValueError(f"Live bar 'funding_rate' is None or non-finite: {fr!r}")


def _row_to_rawbar(row: pd.Series | dict[str, Any]) -> RawBar:
    return RawBar(
        timestamp=row["timestamp"],
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row["volume"]),
        taker_buy_base_volume=float(row["taker_buy_base_volume"]),
        oi=float(row["oi"]),
        lsr=float(row["lsr"]),
        funding_rate=float(row["funding_rate"]),
        funding_interval_hours=float(row["funding_interval_hours"]),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def publish_historical(
    raw_dir: Path,
    redis: RedisStreamClient,
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    batch_log_every: int = 1_000,
) -> int:
    """
    Read parquet files from raw_dir, merge, and XADD every bar to raw_bar stream.

    Returns the number of bars published. Designed for one-time backfill runs;
    does not deduplicate against already-published bars (Redis Streams are append-only).
    Call this once per historical load, not on every restart.
    """
    klines  = pd.read_parquet(raw_dir / f"{symbol}_{interval}_klines.parquet")
    funding = pd.read_parquet(raw_dir / f"{symbol}_funding.parquet")
    vision  = pd.read_parquet(raw_dir / f"{symbol}_vision_metrics.parquet")

    df = _merge_historical(klines, funding, vision)
    log.info("Publishing %d historical bars to stream '%s'", len(df), _RAW_BAR_STREAM)

    published = 0
    for i, (_, row) in enumerate(df.iterrows()):
        bar = _row_to_rawbar(row)
        await redis.publish(_RAW_BAR_STREAM, bar)
        published += 1
        if published % batch_log_every == 0:
            log.info("Published %d / %d bars", published, len(df))

    log.info("Historical publish complete: %d bars → '%s'", published, _RAW_BAR_STREAM)
    return published


async def publish_live(
    klines_fields: dict[str, Any],
    funding_fields: dict[str, Any],
    vision_fields: dict[str, Any],
    redis: RedisStreamClient,
) -> str:
    """
    Merge one live bar from the three fetcher dicts and XADD to raw_bar stream.

    klines_fields : output of klines.fetch_live()
    funding_fields: output of funding.fetch_live()
    vision_fields : output of vision_metrics.fetch_live()

    Returns the Redis entry ID of the published event.
    Raises ValueError if any field fails validation (bar is not published).
    """
    merged: dict[str, Any] = {
        **klines_fields,
        **funding_fields,
        **vision_fields,
    }

    # Explicit NaN/missing guard — all numeric fields must be finite before publish.
    _validate_live(merged)

    bar = _row_to_rawbar(merged)
    entry_id = await redis.publish(_RAW_BAR_STREAM, bar)
    log.info("Published live bar ts=%s entry_id=%s", bar.timestamp.isoformat(), entry_id)
    return entry_id
