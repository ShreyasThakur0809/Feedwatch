from decimal import Decimal

import pytest

from feedwatch.book import DepthDiff, OrderBook, bridge_index
from feedwatch.errors import SequenceGapError

D = Decimal


def diff(first: int, final: int, prev: int, bids=(), asks=()) -> DepthDiff:
    return DepthDiff(
        first_update_id=first,
        final_update_id=final,
        prev_update_id=prev,
        bids=tuple((D(p), D(q)) for p, q in bids),
        asks=tuple((D(p), D(q)) for p, q in asks),
    )


def test_snapshot_loads_and_mid_is_computed():
    book = OrderBook()
    book.load_snapshot(
        100,
        bids=[(D("99"), D("1"))],
        asks=[(D("101"), D("2"))],
    )
    assert book.synced
    assert book.last_update_id == 100
    assert book.mid() == D("100")


def test_apply_updates_inserts_and_deletes_levels():
    book = OrderBook()
    book.load_snapshot(
        100,
        bids=[(D("99"), D("1")), (D("98"), D("1"))],
        asks=[(D("101"), D("1"))],
    )
    book.apply(
        diff(101, 110, 100, bids=[("99", "0"), ("97", "5")], asks=[("101", "2")])
    )
    assert D("99") not in book.bids
    assert book.bids[D("97")] == D("5")
    assert book.asks[D("101")] == D("2")
    assert book.last_update_id == 110


def test_apply_with_wrong_prev_id_raises_sequence_gap():
    book = OrderBook()
    book.load_snapshot(100, bids=[(D("99"), D("1"))], asks=[(D("101"), D("1"))])
    with pytest.raises(SequenceGapError):
        book.apply(diff(120, 130, 119))


def test_apply_without_pu_chains_on_first_update_id():
    book = OrderBook()
    book.load_snapshot(100, bids=[(D("99"), D("1"))], asks=[(D("101"), D("1"))])
    book.apply(diff(101, 110, None, asks=[("102", "3")]))  # U == last + 1
    assert book.last_update_id == 110


def test_apply_without_pu_raises_on_gap():
    book = OrderBook()
    book.load_snapshot(100, bids=[(D("99"), D("1"))], asks=[(D("101"), D("1"))])
    with pytest.raises(SequenceGapError):
        book.apply(diff(105, 110, None))  # U > last + 1


def test_parse_handles_missing_pu_field():
    parsed = DepthDiff.from_message(
        {"U": 5, "u": 9, "b": [["1.0", "2.0"]], "a": []}
    )
    assert parsed.prev_update_id is None
    assert parsed.bids == ((D("1.0"), D("2.0")),)


def test_bridge_diff_applies_without_chain_check():
    book = OrderBook()
    book.load_snapshot(100, bids=[(D("99"), D("1"))], asks=[(D("101"), D("1"))])
    book.apply(diff(95, 105, 90, asks=[("102", "3")]), check_chain=False)
    assert book.last_update_id == 105
    assert book.asks[D("102")] == D("3")


def test_crossed_book_detected():
    book = OrderBook()
    book.load_snapshot(1, bids=[(D("101"), D("1"))], asks=[(D("99"), D("1"))])
    assert book.crossed()


def test_empty_side_gives_no_mid():
    book = OrderBook()
    book.load_snapshot(1, bids=[], asks=[(D("101"), D("1"))])
    assert book.mid() is None


def test_book_trims_to_max_depth_keeping_best_levels():
    book = OrderBook(max_depth=10)
    book.load_snapshot(
        1,
        bids=[(D(str(100 - i)), D("1")) for i in range(25)],  # 100 down to 76
        asks=[(D("200"), D("1"))],
    )
    assert len(book.bids) == 10
    assert book.best_bid() == D("100")  # top of book survives trimming


def test_bridge_index_finds_continuing_diff():
    buffered = [
        diff(50, 60, 49),
        diff(61, 70, 60),
        diff(69, 80, 70),
    ]
    assert bridge_index(70, buffered) == 2  # 69 <= 71 <= 80
    assert bridge_index(75, buffered) == 2  # 69 <= 76 <= 80


def test_bridge_index_returns_none_when_stream_ahead():
    buffered = [diff(70, 80, 69)]
    assert bridge_index(60, buffered) is None  # gap: 70 > 61


def test_bridge_index_returns_none_when_snapshot_ahead():
    buffered = [diff(50, 60, 49)]
    assert bridge_index(60, buffered) is None
