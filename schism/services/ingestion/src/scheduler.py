"""
scheduler.py — APScheduler-based 1h bar ingestion loop.

Fires 5 seconds after each hour close (HH:00:05 UTC) to ensure Binance has
finalised the bar. Fetches klines, funding, and vision metrics in parallel,
then publishes a RawBar event to the raw_bar Redis stream.

Backoff policy (Binance escalation):
  429 (rate limit) → log warning, skip bar, continue next hour.
                     Do NOT retry immediately — live inference tolerates one
                     missing bar without breaking EWMA continuity.
  418 (IP ban)     → parse Retry-After from error message, set _banned_until,
                     skip all ticks until the ban expires.

The funding_interval_hours cache persists across ticks so /fapi/v1/fundingInfo
is only called once (on first settlement bar) rather than every hour.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from schism_shared import RedisStreamClient
from schism_shared.constants import STREAM_NAMES

from fetchers import funding, klines, vision_metrics
from publishers import publish_historical, publish_live

log = logging.getLogger(__name__)

# Seconds after bar close before fetching — small buffer for Binance finalisation.
_BAR_CLOSE_OFFSET_S = 5

# Regex to extract seconds from "Retry-After: Xs" in the RuntimeError message.
_RETRY_AFTER_RE = re.compile(r"Retry-After:\s*(\d+)s")


class IngestionScheduler:
    """
    Manages the live 1h ingestion loop and optional historical backfill.

    Usage:
        sched = IngestionScheduler(...)
        await sched.run_backfill(start, end)   # one-time historical load
        await sched.start()                    # begin live loop (blocks until stop)
    """

    def __init__(
        self,
        redis_url: str,
        binance_base_url: str,
        raw_dir: Path,
        symbol: str = "BTCUSDT",
        interval: str = "1h",
    ) -> None:
        self.redis_url        = redis_url
        self.binance_base_url = binance_base_url
        self.raw_dir          = raw_dir
        self.symbol           = symbol
        self.interval         = interval

        self._http: httpx.AsyncClient | None = None
        self._redis: RedisStreamClient | None = None
        self._funding_interval_cache: dict[str, float] = {}
        self._banned_until: datetime | None = None

        self._scheduler = AsyncIOScheduler(timezone="UTC")

    # ------------------------------------------------------------------
    # Client lifecycle
    # ------------------------------------------------------------------

    async def _ensure_clients(self) -> None:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30.0)
        if self._redis is None:
            self._redis = RedisStreamClient.from_url(self.redis_url)
            # Ingestion is a producer; XADD creates the stream on first publish.
            # Consumer groups are the responsibility of each consuming service.

    async def _close_clients(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    # ------------------------------------------------------------------
    # Scheduler tick
    # ------------------------------------------------------------------

    async def _tick(self) -> None:
        """Invoked at HH:00:05 UTC for each hour close."""
        now = datetime.now(timezone.utc)

        # IP ban guard — skip silently until ban expires.
        if self._banned_until is not None:
            if now < self._banned_until:
                log.warning(
                    "IP ban active until %s — skipping bar at %s",
                    self._banned_until.isoformat(), now.isoformat(),
                )
                return
            self._banned_until = None
            log.info("IP ban expired — resuming ingestion")

        await self._ensure_clients()
        assert self._http is not None and self._redis is not None  # type narrowing

        # Bar timestamp: open_time of the 1h bar that just closed.
        # The tick fires at HH:00:05; the closed bar opened at (H-1):00:00.
        # Subtracting 1h keeps bar_ts aligned with klines open_time and with
        # the funding settlement convention (settlement at 08:00 belongs to the
        # bar that *opens* at 08:00, i.e. the bar fetched at 09:00:05).
        bar_ts = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

        log.info("Ingestion tick: bar_ts=%s", bar_ts.isoformat())

        try:
            klines_data, funding_data, vision_data = await asyncio.gather(
                klines.fetch_live(self._http, self.binance_base_url, self.symbol, self.interval),
                funding.fetch_live(
                    self._http, self.binance_base_url, bar_ts, self.symbol,
                    self._funding_interval_cache,
                ),
                vision_metrics.fetch_live(self._http, self.binance_base_url, self.symbol),
            )
        except RuntimeError as exc:
            self._handle_rest_error(exc, bar_ts)
            return
        except Exception as exc:
            log.error("Unexpected fetch error at bar_ts=%s: %s", bar_ts.isoformat(), exc, exc_info=True)
            return

        # Verify klines bar timestamp matches our expected bar_ts.
        fetched_ts: datetime = klines_data["timestamp"]
        if fetched_ts != bar_ts:
            log.warning(
                "Bar timestamp mismatch: expected %s, klines returned %s — "
                "publishing with klines timestamp",
                bar_ts.isoformat(), fetched_ts.isoformat(),
            )

        try:
            entry_id = await publish_live(klines_data, funding_data, vision_data, self._redis)
            log.debug("Published bar %s → entry_id=%s", fetched_ts.isoformat(), entry_id)
        except ValueError as exc:
            log.error("Validation error, bar not published (bar_ts=%s): %s", bar_ts.isoformat(), exc)

    def _handle_rest_error(self, exc: RuntimeError, bar_ts: datetime) -> None:
        msg = str(exc)
        if "429" in msg:
            log.warning("Binance 429 — skipping bar %s (will retry next hour)", bar_ts.isoformat())
            return
        if "418" in msg:
            match = _RETRY_AFTER_RE.search(msg)
            ban_seconds = int(match.group(1)) if match else 3600  # default 1h if header missing
            self._banned_until = datetime.now(timezone.utc) + timedelta(seconds=ban_seconds)
            log.error(
                "Binance 418 IP ban — pausing ingestion until %s (%ds)",
                self._banned_until.isoformat(), ban_seconds,
            )
            return
        # Unknown RuntimeError — re-raise so it surfaces in logs.
        log.error("REST error at bar_ts=%s: %s", bar_ts.isoformat(), exc, exc_info=True)

    # ------------------------------------------------------------------
    # Historical backfill
    # ------------------------------------------------------------------

    async def run_backfill(
        self,
        start_date: str,
        end_date: str,
        semaphore_limit: int = 5,
        jitter: tuple[float, float] = (0.5, 1.0),
    ) -> None:
        """
        One-time historical load: fetch Vision zips and publish all bars to Redis.

        start_date / end_date: ISO date strings, e.g. "2020-09-01" / "2024-12-31".
        Do not call this on every restart — it is a one-shot backfill operation.
        Bars already in the parquet files are deduplicated automatically (see publishers).
        """
        from datetime import date as date_t

        start = date_t.fromisoformat(start_date)
        end   = date_t.fromisoformat(end_date)

        log.info("Starting historical backfill %s → %s for %s", start, end, self.symbol)
        await self._ensure_clients()
        assert self._redis is not None

        log.info("Fetching klines from Vision...")
        await klines.fetch_historical(start, end, self.raw_dir, self.symbol, self.interval,
                                      semaphore_limit, jitter)

        log.info("Fetching funding from Vision...")
        await funding.fetch_historical(start, end, self.raw_dir, self.symbol,
                                       semaphore_limit, jitter)

        log.info("Fetching OI/LSR from Vision...")
        await vision_metrics.fetch_historical(start, end, self.raw_dir, self.symbol, self.interval,
                                               semaphore_limit, jitter)

        log.info("Merging and publishing to Redis...")
        count = await publish_historical(self.raw_dir, self._redis, self.symbol, self.interval)
        log.info("Backfill complete: %d bars published", count)

    # ------------------------------------------------------------------
    # Start / stop
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the live 1h ingestion loop. Runs until stop() is called."""
        await self._ensure_clients()
        self._scheduler.add_job(
            self._tick,
            trigger="cron",
            minute=0,
            second=_BAR_CLOSE_OFFSET_S,
            id="ingestion_tick",
            max_instances=1,     # never queue a second tick if previous is still running
            coalesce=True,       # if multiple missed ticks accumulated, run only the latest
        )
        self._scheduler.start()
        log.info(
            "Ingestion scheduler started — firing at HH:00:%02ds UTC for %s %s",
            _BAR_CLOSE_OFFSET_S, self.symbol, self.interval,
        )

        # Keep the coroutine alive until cancelled (e.g. KeyboardInterrupt / SIGTERM).
        try:
            while True:
                await asyncio.sleep(60)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        await self._close_clients()
        log.info("Ingestion scheduler stopped")
