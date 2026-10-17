from feedwatch.feed import backoff_delay


def test_backoff_stays_within_jittered_bounds():
    for attempt in range(6):
        for _ in range(100):
            delay = backoff_delay(attempt, base_s=0.5, cap_s=30.0)
            expected = min(30.0, 0.5 * 2**attempt)
            assert expected * 0.5 <= delay <= expected * 1.5


def test_backoff_is_capped():
    for _ in range(100):
        assert backoff_delay(50, base_s=0.5, cap_s=30.0) <= 45.0
