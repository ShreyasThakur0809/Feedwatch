from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    symbol: str = "btcusdt"
    ws_base: str = "wss://stream.binance.com:9443/ws"
    rest_base: str = "https://api.binance.com"
    depth_stream: str = "depth@100ms"
    snapshot_limit: int = 1000
    book_depth: int = 50           # levels kept per side
    stale_ms: int = 1500           # no message for this long -> feed is stale
    watchdog_ms: int = 500         # how often staleness is polled
    spread_bps: float = 10.0       # full bid/ask spread in basis points
    backoff_base_s: float = 0.5
    backoff_max_s: float = 30.0
    report_every_s: float = 30.0
    buffer_max: int = 5000         # bound on diffs buffered while unsynced
