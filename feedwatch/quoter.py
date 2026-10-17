from dataclasses import dataclass
from decimal import Decimal

from .book import OrderBook


@dataclass(frozen=True)
class Quote:
    bid: Decimal
    ask: Decimal


class Quoter:
    """Computes the quote a market maker would rest around mid.

    spread_bps is the full bid/ask spread in basis points, with half
    applied on each side of mid.
    """

    def __init__(self, spread_bps: float) -> None:
        self._half_spread = Decimal(str(spread_bps)) / Decimal(20_000)

    def desired(self, book: OrderBook) -> Quote | None:
        mid = book.mid()
        if mid is None or book.crossed():
            return None
        return Quote(
            bid=mid * (1 - self._half_spread),
            ask=mid * (1 + self._half_spread),
        )
