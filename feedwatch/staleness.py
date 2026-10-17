import time
from collections.abc import Callable


class StalenessTracker:
    """Marks the feed stale when no message arrives within max_age_s.

    The clock is injectable so tests can advance time deterministically.
    """

    def __init__(
        self, max_age_s: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._max_age_s = max_age_s
        self._clock = clock
        self._last_message_at: float | None = None
        self.stale = True  # nothing received yet counts as stale

    def on_message(self) -> None:
        self._last_message_at = self._clock()
        self.stale = False

    def on_disconnect(self) -> None:
        self.stale = True

    def poll(self) -> bool:
        if self._last_message_at is None:
            self.stale = True
        elif self._clock() - self._last_message_at > self._max_age_s:
            self.stale = True
        return self.stale
