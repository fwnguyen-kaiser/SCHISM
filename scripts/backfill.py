"""
Standalone backfill script — pulls klines, funding, and Vision metrics from
data.binance.vision and saves to schism/data/raw/ as parquet files.

Usage:
    python scripts/backfill.py [--start 2020-09-01] [--end today]

Runs from the monorepo root. No Docker required.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

# Resolve paths relative to repo root (parent of this script's directory).
_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT / "schism/services/ingestion/src"))

from fetchers import funding, klines, vision_metrics  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("backfill")

_RAW_DIR = _REPO_ROOT / "schism/data/raw"
_COMMON_FLOOR = date(2020, 9, 1)
_SYMBOL   = "BTCUSDT"
_INTERVAL = "1h"


async def run(start: date, end: date) -> None:
    _RAW_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Backfill [%s → %s]  raw_dir=%s", start, end, _RAW_DIR)

    # Sequential: klines first (largest download), then funding, then vision.
    klines_df  = await klines.fetch_historical(start, end, _RAW_DIR, _SYMBOL, _INTERVAL)
    funding_df = await funding.fetch_historical(start, end, _RAW_DIR, _SYMBOL)
    vision_df  = await vision_metrics.fetch_historical(start, end, _RAW_DIR, _SYMBOL, _INTERVAL)

    _check(klines_df, funding_df, vision_df)


def _check(kl, fr, vm) -> None:
    log.info("=" * 60)
    log.info("DATA CHECK")
    log.info("=" * 60)

    # ── klines ──────────────────────────────────────────────────────────────
    log.info("KLINES  bars=%d  range=[%s, %s]", len(kl), kl["timestamp"].min(), kl["timestamp"].max())
    kl_nans = kl[["open","high","low","close","volume","taker_buy_base_volume"]].isna().sum()
    log.info("  NaN counts: %s", kl_nans.to_dict())
    bad_tbv = (kl["taker_buy_base_volume"] < 0) | (kl["taker_buy_base_volume"] > kl["volume"])
    log.info("  taker_buy_base_volume outside [0, volume]: %d", bad_tbv.sum())
    log.info("  volume   min=%.4g  max=%.4g", kl["volume"].min(), kl["volume"].max())
    log.info("  close    min=%.2f  max=%.2f", kl["close"].min(), kl["close"].max())

    # ── funding ─────────────────────────────────────────────────────────────
    log.info("FUNDING  bars=%d  range=[%s, %s]", len(fr), fr["timestamp"].min(), fr["timestamp"].max())
    fr_nans = fr[["funding_rate","funding_interval_hours"]].isna().sum()
    log.info("  NaN counts: %s", fr_nans.to_dict())
    log.info("  funding_rate     min=%.6f  max=%.6f", fr["funding_rate"].min(), fr["funding_rate"].max())
    log.info("  funding_interval unique values: %s", sorted(fr["funding_interval_hours"].unique().tolist()))
    extreme_fr = (fr["funding_rate"].abs() > 0.03).sum()
    log.info("  |funding_rate| > 0.03: %d settlement bars", extreme_fr)

    # ── vision metrics ───────────────────────────────────────────────────────
    log.info("VISION   bars=%d  range=[%s, %s]", len(vm), vm["timestamp"].min(), vm["timestamp"].max())
    vm_nans = vm[["oi","lsr"]].isna().sum()
    log.info("  NaN counts: %s", vm_nans.to_dict())
    log.info("  OI   min=%.4g  max=%.4g  (non-positive: %d)", vm["oi"].min(), vm["oi"].max(), (vm["oi"] <= 0).sum())
    log.info("  LSR  min=%.4f  max=%.4f  (non-positive: %d)", vm["lsr"].min(), vm["lsr"].max(), (vm["lsr"] <= 0).sum())

    log.info("=" * 60)
    log.info("Expected bar count from %s to %s: ~%d", _COMMON_FLOOR, date.today(),
             int((date.today() - _COMMON_FLOOR).days * 24))
    log.info("=" * 60)


def _parse_args() -> tuple[date, date]:
    p = argparse.ArgumentParser(description="SCHISM historical backfill")
    p.add_argument("--start", default=str(_COMMON_FLOOR), help="ISO date (default: COMMON_FLOOR)")
    p.add_argument("--end",   default=str(date.today()),  help="ISO date (default: today)")
    args = p.parse_args()
    return date.fromisoformat(args.start), date.fromisoformat(args.end)


if __name__ == "__main__":
    start, end = _parse_args()
    asyncio.run(run(start, end))
