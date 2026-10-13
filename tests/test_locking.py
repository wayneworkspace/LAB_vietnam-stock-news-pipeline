import os
import subprocess
import sys
import textwrap

import pytest

from serving.locking import PipelineBusy, pipeline_lock

PYTHON_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python")


def test_second_acquire_is_refused_then_allowed_after_release(tmp_path):
    path = str(tmp_path / "x.lock")
    with pipeline_lock(path):
        with pytest.raises(PipelineBusy):
            with pipeline_lock(path):
                pass
    with pipeline_lock(path):
        pass


def test_lock_is_released_even_when_the_body_fails(tmp_path):
    path = str(tmp_path / "x.lock")
    with pytest.raises(ZeroDivisionError):
        with pipeline_lock(path):
            1 / 0
    with pipeline_lock(path):
        pass


def test_a_lock_held_by_another_process_is_respected_and_freed_when_it_dies(tmp_path):
    path = str(tmp_path / "x.lock")
    code = textwrap.dedent("""
        import sys, time
        from serving.locking import pipeline_lock
        with pipeline_lock(sys.argv[1]):
            print("ready", flush=True)
            time.sleep(60)
    """)
    proc = subprocess.Popen([sys.executable, "-c", code, path], stdout=subprocess.PIPE, text=True,
                            env=dict(os.environ, PYTHONPATH=PYTHON_DIR))
    try:
        assert proc.stdout.readline().strip() == "ready"
        with pytest.raises(PipelineBusy):
            with pipeline_lock(path):
                pass
    finally:
        proc.kill()
        proc.wait()
    with pipeline_lock(path):      # the OS released the lock when the process died: nothing stale to clean up
        pass
