"""Run-overlap guard: only one pipeline step may touch the databases at a time.

Why: DuckDB cannot open one file read-write and read-only at the same time, and two rebuilds
writing at once would clash. n8n retries and manual curl calls make overlaps realistic.

How: a non-blocking lock. A second caller gets PipelineBusy at once instead of waiting.
- A threading.Lock covers the API's worker threads (one uvicorn process serves several threads).
- An OS file lock (flock) covers other processes, for example `docker exec` runs. The OS drops it
  automatically if the holder dies, so there is no stale-lock cleanup to maintain.
The file lock is skipped where fcntl does not exist (native Windows); the thread lock still works there.

Scope: this protects one host, it is not a distributed lock.
"""
import json
import os
import threading
from datetime import datetime
from typing import Optional

from config import PIPELINE_LOCK_FILE, VN_TZ

try:
    import fcntl
except ImportError:      # native Windows
    fcntl = None

_thread_lock = threading.Lock()
_holder: Optional[dict] = None     # who holds the lock inside this process


class PipelineBusy(RuntimeError):
    """Raised when another step already holds the lock. `holder` says which one."""

    def __init__(self, holder: Optional[dict] = None):
        self.holder = holder or {}
        super().__init__(f"Another pipeline step is already running: {self.holder.get('step', 'unknown')}")


def _read_holder(fd: int) -> dict:
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        return json.loads(os.read(fd, 4096).decode("utf-8") or "{}")
    except (OSError, ValueError):
        return {}


class PipelineLock:
    """Non-blocking lock. Use as a context manager, or call acquire()/release() explicitly
    (a background run acquires in the request thread and releases from its worker thread)."""

    def __init__(self, step: str, path: Optional[str] = None):
        self.step = step
        self.path = path or PIPELINE_LOCK_FILE
        self._fd: Optional[int] = None
        self._held = False

    def acquire(self) -> "PipelineLock":
        global _holder
        if not _thread_lock.acquire(blocking=False):
            raise PipelineBusy(_holder)
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
            try:
                if fcntl is not None:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError:
                        raise PipelineBusy(_read_holder(fd))
                info = {"step": self.step, "pid": os.getpid(),
                        "started_at": datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")}
                os.ftruncate(fd, 0)
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, json.dumps(info).encode("utf-8"))
            except BaseException:
                os.close(fd)
                raise
        except BaseException:
            _thread_lock.release()
            raise
        self._fd, self._held = fd, True
        _holder = info
        return self

    def release(self) -> None:
        global _holder
        if not self._held:
            return
        self._held = False
        _holder = None
        try:
            if fcntl is not None:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None
            _thread_lock.release()

    def __enter__(self) -> "PipelineLock":
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()


def pipeline_lock(step: str, path: Optional[str] = None) -> PipelineLock:
    return PipelineLock(step, path)


def running_step() -> Optional[dict]:
    """Who holds the lock in this process right now (None when idle)."""
    return _holder
