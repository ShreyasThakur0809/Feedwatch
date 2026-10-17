from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from decimal import Decimal

import httpx

from .book import DepthDiff, OrderBook, Snapshot, bridge_index
from .config import Config
from .errors import SequenceGapError
from .feed import BinanceDepthFeed, Connected, Disconnected, FeedEvent
from .metrics import Metrics
from .quoter import Quoter
from .staleness import StalenessTracker

log = logging.getLogger("feedwatch")


class Watcher:
    """Consumes a depth feed, keeps a synced order book, and tracks the
    periods during which a market maker would have been quoting."""

    def __init__(
        self, config: Config, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._cfg = config
        self._clock = clock
        self._book = OrderBook(max_depth=config.book_depth)
        self._quoter = Quoter(config.spread_bps)
        self._staleness = StalenessTracker(config.stale_ms / 1000, clock)
        self._metrics = Metrics(clock)
        self._buffered: deque[DepthDiff] = deque(maxlen=config.buffer_max)
        self._connected = False
        self._ever_connected = False
        self._quoting = False
        self._stop = asyncio.Event()
        self._http: httpx.AsyncClient | None = None
        self._snapshot_task: asyncio.Task[Snapshot] | None = None

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        cfg = self._cfg
        feed = BinanceDepthFeed(
            cfg.ws_base,
            cfg.symbol,
            cfg.depth_stream,
            cfg.backoff_base_s,
            cfg.backoff_max_s,
        )
        watchdog = asyncio.create_task(self._watchdog_loop())
        reporter = asyncio.create_task(self._report_loop())
        try:
            async with httpx.AsyncClient(
                base_url=cfg.rest_base, timeout=10
            ) as http:
                self._http = http
                async for event in feed.events():
                    if self._stop.is_set():
                        break
                    await self._handle(event)
        finally:
            if self._snapshot_task is not None:
                self._snapshot_task.cancel()
            watchdog.cancel()
            reporter.cancel()
            self._metrics.close()
            log.info("stopped: %s", self._summary())

    async def _handle(self, event: FeedEvent) -> None:
        if isinstance(event, Connected):
            if self._ever_connected:
                self._metrics.reconnects += 1
            self._ever_connected = True
            self._connected = True
            log.info("connected to %s", event.url)
            self._update_quotes()
        elif isinstance(event, Disconnected):
            self._connected = False
            if not self._staleness.stale:
                self._metrics.stale_episodes += 1
            self._staleness.on_disconnect()
            self._update_quotes()
            log.warning(
                "disconnected: %s, retrying in %.1fs",
                event.reason,
                event.retry_in_s,
            )
        elif isinstance(event, DepthDiff):
            self._metrics.messages += 1
            self._staleness.on_message()
            if self._book.synced:
                try:
                    self._book.apply(event)
                    self._metrics.diffs_applied += 1
                except SequenceGapError as exc:
                    log.warning("sequence gap: %s, resyncing book", exc)
                    self._metrics.resyncs += 1
                    self._book.synced = False
            if not self._book.synced:
                self._buffered.append(event)
                self._sync_step()
            self._update_quotes()

    def _sync_step(self) -> None:
        """Drive one step of snapshot resync without blocking event intake.

        The snapshot fetch runs as a task while diffs keep buffering, so by
        the time it returns the buffer spans its last_update_id and a bridge
        diff exists. Blocking the consume loop on the fetch instead starves
        the buffer and the bridge never appears.
        """
        if self._snapshot_task is None:
            self._snapshot_task = asyncio.create_task(self._fetch_snapshot())
            return
        if not self._snapshot_task.done():
            return
        task, self._snapshot_task = self._snapshot_task, None
        try:
            snapshot = task.result()
        except Exception as exc:
            log.warning("snapshot fetch failed: %s, refetching", exc)
            self._snapshot_task = asyncio.create_task(self._fetch_snapshot())
            return
        self._metrics.snapshots += 1
        idx = bridge_index(snapshot.last_update_id, self._buffered)
        if idx is None:
            log.warning(
                "no bridge for snapshot %d, refetching while buffering",
                snapshot.last_update_id,
            )
            self._snapshot_task = asyncio.create_task(self._fetch_snapshot())
            return
        try:
            self._book.load_snapshot(
                snapshot.last_update_id, snapshot.bids, snapshot.asks
            )
            buffered = list(self._buffered)
            self._book.apply(buffered[idx], check_chain=False)
            for diff in buffered[idx + 1 :]:
                self._book.apply(diff)
                self._metrics.diffs_applied += 1
        except SequenceGapError as exc:
            log.warning("resync chain broke: %s, resyncing again", exc)
            self._book.synced = False
            self._snapshot_task = asyncio.create_task(self._fetch_snapshot())
            return
        self._buffered.clear()
        log.info(
            "book synced at update %d, %d bids / %d asks",
            self._book.last_update_id,
            len(self._book.bids),
            len(self._book.asks),
        )

    async def _fetch_snapshot(self) -> Snapshot:
        assert self._http is not None
        response = await self._http.get(
            "/api/v3/depth",
            params={
                "symbol": self._cfg.symbol.upper(),
                "limit": self._cfg.snapshot_limit,
            },
        )
        response.raise_for_status()
        data = response.json()
        return Snapshot(
            last_update_id=int(data["lastUpdateId"]),
            bids=tuple((Decimal(p), Decimal(q)) for p, q in data["bids"]),
            asks=tuple((Decimal(p), Decimal(q)) for p, q in data["asks"]),
        )

    def _update_quotes(self) -> None:
        if not self._connected:
            reason = "disconnected"
        elif not self._book.synced:
            reason = "order book syncing"
        elif self._staleness.stale:
            reason = "stale feed"
        elif self._quoter.desired(self._book) is None:
            reason = "empty or crossed book"
        else:
            reason = None
        want = reason is None
        if want == self._quoting:
            return
        self._quoting = want
        self._metrics.quoting_changed(want)
        if want:
            quote = self._quoter.desired(self._book)
            log.info(
                "quoting bid=%s ask=%s mid=%s",
                quote.bid,
                quote.ask,
                self._book.mid(),
            )
        else:
            log.warning("quotes pulled: %s", reason)

    async def _watchdog_loop(self) -> None:
        interval = self._cfg.watchdog_ms / 1000
        while True:
            await asyncio.sleep(interval)
            was_stale = self._staleness.stale
            if self._staleness.poll() and not was_stale:
                self._metrics.stale_episodes += 1
                log.warning(
                    "feed stale: no depth message for >%dms",
                    self._cfg.stale_ms,
                )
            elif was_stale and not self._staleness.stale:
                log.info("feed fresh again")
            self._update_quotes()

    async def _report_loop(self) -> None:
        while True:
            await asyncio.sleep(self._cfg.report_every_s)
            log.info("report: %s", self._summary())

    def _summary(self) -> str:
        m = self._metrics
        return (
            f"uptime={self._clock() - m.started_at:.0f}s "
            f"msgs={m.messages} diffs={m.diffs_applied} "
            f"resyncs={m.resyncs} reconnects={m.reconnects} "
            f"stale_episodes={m.stale_episodes} gap_episodes={m.gap_episodes} "
            f"liveliness={m.liveliness_pct():.1f}% mid={self._book.mid()}"
        )
