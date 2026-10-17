import time
from collections.abc import Callable


class Metrics:
    """In-memory counters plus liveliness accounting.

    Liveliness is the fraction of uptime during which the bot would have
    had quotes resting on the book. The gap episodes counter mirrors what
    shows up as liveliness gaps on a Grafana dashboard.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self.started_at = clock()
        self.messages = 0
        self.diffs_applied = 0
        self.snapshots = 0
        self.resyncs = 0
        self.reconnects = 0
        self.stale_episodes = 0
        self.gap_episodes = 0
        self._quoting_since: float | None = None
        self._quote_time_s = 0.0

    def quoting_changed(self, quoting: bool) -> None:
        now = self._clock()
        if quoting and self._quoting_since is None:
            self._quoting_since = now
        elif not quoting and self._quoting_since is not None:
            self._quote_time_s += now - self._quoting_since
            self._quoting_since = None
            self.gap_episodes += 1

    def close(self) -> None:
        """Stop the clock on quote time without counting a gap episode."""
        if self._quoting_since is not None:
            self._quote_time_s += self._clock() - self._quoting_since
            self._quoting_since = None

    def quote_time_s(self) -> float:
        total = self._quote_time_s
        if self._quoting_since is not None:
            total += self._clock() - self._quoting_since
        return total

    def liveliness_pct(self) -> float:
        elapsed = self._clock() - self.started_at
        if elapsed <= 0:
            return 0.0
        return 100.0 * self.quote_time_s() / elapsed
