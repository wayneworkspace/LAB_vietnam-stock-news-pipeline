"""Run-overlap guard: only one pipeline step may touch the databases at a time.

DuckDB cannot open one file read-write and read-only at the same time, so an overlapping
/build-gold and /digest would fail. The guard is a non-blocking OS file lock (flock): a second caller
gets PipelineBusy at once instead of waiting, and the OS releases the lock if the holder dies, so there
is no stale lock to clean up. It protects one host (Linux / the Docker container), not a cluster.
"""
import fcntl
import os
from contextlib import contextmanager

from config import PIPELINE_LOCK_FILE


class PipelineBusy(RuntimeError):
    """Another pipeline step already holds the lock."""


@contextmanager
def pipeline_lock(path: str = None):
    path = path or PIPELINE_LOCK_FILE
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise PipelineBusy("Another pipeline step is already running")
        yield
    finally:
        os.close(fd)      # closing the descriptor releases the lock
