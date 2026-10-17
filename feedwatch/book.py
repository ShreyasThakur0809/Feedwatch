from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from .errors import SequenceGapError

PriceQty = tuple[Decimal, Decimal]


@dataclass(frozen=True)
class DepthDiff:
    """One Binance @depth event, valid for update ids (U, u].

    prev_update_id (pu) is present on newer streams; when absent the chain
    check falls back to requiring first_update_id <= last_update_id + 1.
    """

    first_update_id: int
    final_update_id: int
    prev_update_id: int | None
    bids: tuple[PriceQty, ...]
    asks: tuple[PriceQty, ...]

    @classmethod
    def from_message(cls, data: dict) -> "DepthDiff":
        return cls(
            first_update_id=int(data["U"]),
            final_update_id=int(data["u"]),
            prev_update_id=int(data["pu"]) if data.get("pu") is not None else None,
            bids=tuple((Decimal(p), Decimal(q)) for p, q in data["b"]),
            asks=tuple((Decimal(p), Decimal(q)) for p, q in data["a"]),
        )


@dataclass(frozen=True)
class Snapshot:
    last_update_id: int
    bids: tuple[PriceQty, ...]
    asks: tuple[PriceQty, ...]


def bridge_index(last_update_id: int, buffered: Iterable[DepthDiff]) -> int | None:
    """Index of the first buffered diff that continues a snapshot.

    Binance requires the first applied diff to satisfy
    U <= last_update_id + 1 <= u. Diffs fully covered by the snapshot are
    skipped. Anything else means the stream moved ahead of the snapshot and
    it must be refetched.
    """
    for i, diff in enumerate(buffered):
        if diff.final_update_id <= last_update_id:
            continue
        if diff.first_update_id <= last_update_id + 1 <= diff.final_update_id:
            return i
        return None
    return None


def _apply_side(side: dict[Decimal, Decimal], levels: Iterable[PriceQty]) -> None:
    for price, qty in levels:
        if qty == 0:
            side.pop(price, None)
        else:
            side[price] = qty


class OrderBook:
    def __init__(self, max_depth: int = 50) -> None:
        self.max_depth = max_depth
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_update_id = 0
        self.synced = False

    def load_snapshot(
        self, last_update_id: int, bids: Iterable[PriceQty], asks: Iterable[PriceQty]
    ) -> None:
        self.bids = {p: q for p, q in bids if q != 0}
        self.asks = {p: q for p, q in asks if q != 0}
        self.last_update_id = last_update_id
        self.synced = True
        self._trim()

    def apply(self, diff: DepthDiff, *, check_chain: bool = True) -> None:
        if check_chain and not self._chains(diff):
            raise SequenceGapError(
                f"diff U={diff.first_update_id} pu={diff.prev_update_id} does "
                f"not chain to last_update_id={self.last_update_id}"
            )
        _apply_side(self.bids, diff.bids)
        _apply_side(self.asks, diff.asks)
        self.last_update_id = diff.final_update_id
        self._trim()

    def best_bid(self) -> Decimal | None:
        return max(self.bids, default=None)

    def best_ask(self) -> Decimal | None:
        return min(self.asks, default=None)

    def mid(self) -> Decimal | None:
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2

    def crossed(self) -> bool:
        bid, ask = self.best_bid(), self.best_ask()
        return bid is not None and ask is not None and bid >= ask

    def _chains(self, diff: DepthDiff) -> bool:
        if diff.prev_update_id is not None:
            return diff.prev_update_id == self.last_update_id
        return diff.first_update_id <= self.last_update_id + 1

    def _trim(self) -> None:
        if len(self.bids) > self.max_depth * 2:
            keep = set(sorted(self.bids, reverse=True)[: self.max_depth])
            self.bids = {p: q for p, q in self.bids.items() if p in keep}
        if len(self.asks) > self.max_depth * 2:
            keep = set(sorted(self.asks)[: self.max_depth])
            self.asks = {p: q for p, q in self.asks.items() if p in keep}
