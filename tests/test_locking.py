import os
import subprocess
import sys
import textwrap

import pytest

from serving import locking
from serving.locking import PipelineBusy, PipelineLock

PYTHON_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python")


def test_second_acquire_is_refused_with_the_holder_then_allowed_after_release(tmp_path):
    path = str(tmp_path / "x.lock")
    first = PipelineLock("build-gold", path).acquire()
    assert locking.running_step()["step"] == "build-gold"
    with pytest.raises(PipelineBusy) as busy:
        PipelineLock("digest", path).acquire()
    assert busy.value.holder["step"] == "build-gold"
    first.release()
    assert locking.running_step() is None
    PipelineLock("digest", path).acquire().release()


def test_context_manager_releases_even_when_the_body_fails(tmp_path):
    path = str(tmp_path / "x.lock")
    with pytest.raises(ZeroDivisionError):
        with PipelineLock("build-silver", path):
            1 / 0
    PipelineLock("digest", path).acquire().release()


def test_release_is_idempotent(tmp_path):
    lock = PipelineLock("x", str(tmp_path / "x.lock")).acquire()
    lock.release()
    lock.release()
    PipelineLock("y", str(tmp_path / "x.lock")).acquire().release()


@pytest.mark.skipif(locking.fcntl is None, reason="OS file locks need fcntl (POSIX)")
def test_a_lock_held_by_another_process_is_respected_and_freed_when_it_dies(tmp_path):
    path = str(tmp_path / "x.lock")
    code = textwrap.dedent("""
        import sys, time
        from serving.locking import PipelineLock
        PipelineLock("other-process", sys.argv[1]).acquire()
        print("ready", flush=True)
        time.sleep(60)
    """)
    env = dict(os.environ, PYTHONPATH=PYTHON_DIR)
    proc = subprocess.Popen([sys.executable, "-c", code, path], stdout=subprocess.PIPE, text=True, env=env)
    try:
        assert proc.stdout.readline().strip() == "ready"
        with pytest.raises(PipelineBusy) as busy:
            PipelineLock("mine", path).acquire()
        assert busy.value.holder["step"] == "other-process"
    finally:
        proc.kill()
        proc.wait()
    # The OS released the file lock when the process died: no stale lock to clean up.
    PipelineLock("mine", path).acquire().release()
