from decimal import Decimal

from feedwatch.book import OrderBook
from feedwatch.quoter import Quoter

D = Decimal


def book(bids, asks) -> OrderBook:
    b = OrderBook()
    b.load_snapshot(1, bids=[(D(p), D(q)) for p, q in bids],
                    asks=[(D(p), D(q)) for p, q in asks])
    return b


def test_quote_straddles_mid_at_half_spread():
    quoter = Quoter(spread_bps=10)  # 5 bps each side
    quote = quoter.desired(book([("99", "1")], [("101", "1")]))
    assert quote is not None
    assert quote.bid == D("100") * (1 - D("0.0005"))
    assert quote.ask == D("100") * (1 + D("0.0005"))


def test_no_quote_on_empty_book():
    assert Quoter(10).desired(OrderBook()) is None


def test_no_quote_on_crossed_book():
    assert Quoter(10).desired(book([("101", "1")], [("99", "1")])) is None
