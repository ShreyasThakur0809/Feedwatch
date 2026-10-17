from feedwatch.staleness import StalenessTracker


def test_starts_stale_before_first_message():
    tracker = StalenessTracker(max_age_s=1.5)
    assert tracker.poll() is True


def test_message_marks_fresh_then_ages_out():
    now = [100.0]
    tracker = StalenessTracker(max_age_s=1.5, clock=lambda: now[0])
    tracker.on_message()
    assert tracker.poll() is False
    now[0] += 1.0
    assert tracker.poll() is False
    now[0] += 0.6
    assert tracker.poll() is True


def test_new_message_recovers_freshness():
    now = [0.0]
    tracker = StalenessTracker(max_age_s=1.0, clock=lambda: now[0])
    tracker.on_message()
    now[0] += 5.0
    assert tracker.poll() is True
    tracker.on_message()
    assert tracker.poll() is False


def test_disconnect_forces_stale():
    tracker = StalenessTracker(max_age_s=60.0)
    tracker.on_message()
    assert tracker.poll() is False
    tracker.on_disconnect()
    assert tracker.poll() is True
