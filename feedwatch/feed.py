from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Union

import websockets

from .book import DepthDiff


@dataclass(frozen=True)
class Connected:
    url: str


@dataclass(frozen=True)
class Disconnected:
    reason: str
    retry_in_s: float


FeedEvent = Union[Connected, Disconnected, DepthDiff]


def backoff_delay(attempt: int, base_s: float, cap_s: float) -> float:
    """Exponential backoff with full jitter."""
    return min(cap_s, base_s * 2**attempt) * random.uniform(0.5, 1.5)


class BinanceDepthFeed:
    """Async iterator over a Binance depth stream that reconnects forever.

    Yields Connected and Disconnected events around the DepthDiff stream so
    consumers can react to transport state instead of inferring it.
    """

    def __init__(
        self,
        ws_base: str,
        symbol: str,
        stream: str,
        backoff_base_s: float,
        backoff_cap_s: float,
    ) -> None:
        self._url = f"{ws_base}/{symbol.lower()}@{stream}"
        self._base_s = backoff_base_s
        self._cap_s = backoff_cap_s

    async def events(self) -> AsyncIterator[FeedEvent]:
        attempt = 0
        while True:
            try:
                async with websockets.connect(
                    self._url, ping_interval=20, ping_timeout=10
                ) as ws:
                    attempt = 0
                    yield Connected(url=self._url)
                    async for raw in ws:
                        yield DepthDiff.from_message(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except (OSError, websockets.WebSocketException) as exc:
                delay = backoff_delay(attempt, self._base_s, self._cap_s)
                yield Disconnected(
                    reason=f"{type(exc).__name__}: {exc}", retry_in_s=delay
                )
                attempt += 1
                await asyncio.sleep(delay)
