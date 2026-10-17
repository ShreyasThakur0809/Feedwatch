import argparse
import asyncio
import logging
import signal

from .config import Config
from .watcher import Watcher


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="feedwatch",
        description="Watch a Binance depth feed and track market-making liveliness.",
    )
    parser.add_argument("--symbol", default="btcusdt", help="e.g. btcusdt")
    parser.add_argument("--spread-bps", type=float, default=10.0)
    parser.add_argument(
        "--stale-ms",
        type=int,
        default=1500,
        help="silence longer than this marks the feed stale",
    )
    parser.add_argument("--report-every", type=float, default=30.0)
    parser.add_argument("--book-depth", type=int, default=50)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    cfg = Config(
        symbol=args.symbol,
        spread_bps=args.spread_bps,
        stale_ms=args.stale_ms,
        report_every_s=args.report_every,
        book_depth=args.book_depth,
    )
    watcher = Watcher(cfg)

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, watcher.stop)
        await watcher.run()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
