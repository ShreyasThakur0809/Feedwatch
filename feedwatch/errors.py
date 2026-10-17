class FeedWatchError(Exception):
    """Base class for feedwatch errors."""


class SequenceGapError(FeedWatchError):
    """A depth diff did not chain to the book's last update id."""


class SnapshotSyncError(FeedWatchError):
    """Could not align buffered diffs with a REST snapshot."""
