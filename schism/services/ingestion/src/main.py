"""
Ingestion service entry point.

Environment variables (all required unless noted):
  REDIS_URL           Redis connection string, e.g. redis://redis:6379
  BINANCE_BASE_URL    Binance Futures REST root (default: https://fapi.binance.com)
  RAW_DIR             Host-mounted path for raw parquet files (default: /app/data/raw)
  SYMBOL              Trading pair (default: BTCUSDT)
  INTERVAL            Bar cadence (default: 1h)
  RUN_BACKFILL        Set to "1" to run historical backfill before starting live loop
  BACKFILL_START      ISO date for backfill start (default: 2020-09-01 = COMMON_FLOOR)
  BACKFILL_END        ISO date for backfill end (default: today)
  LOG_LEVEL           Logging level (default: INFO)
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import date
from pathlib import Path

from scheduler import IngestionScheduler


def _setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


async def main() -> None:
    _setup_logging()
    log = logging.getLogger(__name__)

    redis_url        = os.environ["REDIS_URL"]
    binance_base_url = os.environ.get("BINANCE_BASE_URL", "https://fapi.binance.com")
    raw_dir          = Path(os.environ.get("RAW_DIR", "/app/data/raw"))
    symbol           = os.environ.get("SYMBOL", "BTCUSDT")
    interval         = os.environ.get("INTERVAL", "1h")
    run_backfill     = os.environ.get("RUN_BACKFILL", "0") == "1"
    backfill_start   = os.environ.get("BACKFILL_START", "2020-09-01")
    backfill_end     = os.environ.get("BACKFILL_END", date.today().isoformat())

    sched = IngestionScheduler(
        redis_url=redis_url,
        binance_base_url=binance_base_url,
        raw_dir=raw_dir,
        symbol=symbol,
        interval=interval,
    )

    # Graceful shutdown on SIGTERM (Docker stop).
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(sched.stop()))

    if run_backfill:
        log.info("RUN_BACKFILL=1 — running historical backfill before live loop")
        await sched.run_backfill(backfill_start, backfill_end)

    log.info("Starting live ingestion loop for %s %s", symbol, interval)
    await sched.start()


if __name__ == "__main__":
    asyncio.run(main())
