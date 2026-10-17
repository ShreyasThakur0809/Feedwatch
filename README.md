# feedwatch

A market data feed liveliness monitor for a crypto order book. It consumes
Binance's public depth WebSocket and reproduces, at toy scale, the failure
modes production market-making bots defend against: disconnects, stale
feeds, sequence gaps and resync storms.

It answers one question continuously: **for what fraction of uptime would a
market maker actually have quotes resting on the book right now?** That
fraction is `liveliness`, the same signal that shows up as gaps on a
monitoring dashboard when a production bot stops quoting.

Public endpoints only, no API keys, no real orders.

## What it does

```
Binance depth@100ms WebSocket ──► BinanceDepthFeed ──► Watcher
                                     │                    │
                            reconnect with               ├─ OrderBook (snapshot + diffs)
                            backoff + jitter             ├─ StalenessTracker
                                                         ├─ Quoter (mid ± spread_bps/2)
                              REST /api/v3/depth ◄───────┤  Metrics (liveliness %)
                              snapshot on gap/resync ◄───┘
```

- **Synced state**: every diff must chain to the book's `last_update_id`
  (`pu == last`, or `U <= last + 1` when the stream omits `pu`). A broken
  chain raises `SequenceGapError` and the book falls out of sync.
- **Unsynced state**: diffs are buffered while a REST snapshot is fetched
  concurrently, then spliced in at the bridge diff
  (`U <= lastUpdateId + 1 <= u`).
- **Staleness**: no message for `stale_ms` marks the feed stale. Quotes are
  pulled until the feed is fresh again.
- **Quoting**: a synthetic quote is computed at mid ± half spread. Quotes
  are pulled whenever the feed is stale, the book is syncing, the socket is
  down, or the book is empty/crossed. Every transition counts as a
  liveliness gap episode.

## Why the snapshot fetch runs concurrently with buffering

The first implementation fetched the snapshot inside the consume loop.
While the ~500ms HTTP call was in flight, incoming diffs piled up in the
websocket's receive queue but never reached the buffer. Every snapshot then
returned *ahead* of everything buffered, so no bridge diff existed, the
resync loop refetched forever, and the book never synced.

Fix: the fetch runs as a task while the consume loop keeps buffering. By
the time the snapshot returns, the buffer spans its `last_update_id` and a
bridge is guaranteed to exist. Same reason production systems decouple
their feed reader from event processing with a queue.

## Run it

```bash
python3.13 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m feedwatch --symbol btcusdt --report-every 5
```

Sample output:

```
feedwatch - INFO - connected to wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
feedwatch - INFO - book synced at update 101551476019, 63 bids / 59 asks
feedwatch - INFO - quoting bid=81681.98 ask=81763.70 mid=81722.84
feedwatch - INFO - report: uptime=30s msgs=292 diffs=288 resyncs=0 \
    reconnects=0 stale_episodes=0 gap_episodes=0 liveliness=95.8% mid=81714.00
```

Flags: `--symbol`, `--spread-bps`, `--stale-ms`, `--book-depth`,
`--report-every`.

## Tests

```bash
.venv/bin/pytest
```

Unit tests cover diff chaining (with and without `pu`), snapshot bridging,
staleness transitions on an injected clock, quoter edge cases and backoff
bounds. The watcher itself is verified by the live run.

## How this maps to production incidents

| Production incident | Mechanism reproduced here |
|---|---|
| WS disconnects, gaps in liveliness | `Disconnected` events, backoff reconnect, gap accounting |
| Stale market data feed, bot stops quoting | `StalenessTracker` marks stale, quotes pulled, resume on recovery |
| Order book desync | `SequenceGapError` -> buffered snapshot resync |
| "Irregular orders, cancelling everything" | quoting state reset to a known-good state before resuming |
| HTTP 429s and REST vs WS tradeoff | REST used only for snapshots, not the hot path |
| Blocking call starving the pipeline | snapshot fetch moved off the consume loop |

## Deliberate design choices

- `Decimal` for prices and quantities, never float. Floats drift; money
  must not.
- No message for `stale_ms` means stale, which is right for a 100ms depth
  stream but wrong for illiquid pairs. Those need a heartbeat/ping based
  check instead.
- The ws library's ping/pong covers transport liveness; the staleness
  tracker covers *data* liveness. Both exist because a healthy TCP
  connection can still deliver garbage or nothing.
- Liveliness, not error count, is the north-star metric. A bot that is
  "up" but not quoting is indistinguishable from down.

## Known limits

- Single symbol, top-of-book quoting only, no inventory or hedging.
- The recv queue grows unboundedly if the consumer is slower than the
  stream for a long time; production would shed load or widen the buffer
  deliberately.
- Binance-specific message shapes. A second connector is the obvious next
  step: `feed.py` already isolates the transport from the event types.
