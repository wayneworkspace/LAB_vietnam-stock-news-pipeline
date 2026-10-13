"""Test setup. The environment is configured BEFORE any pipeline module is imported, because
config.py reads it once at import time. Every test works inside one throwaway data folder that is
emptied before each test."""
import os
import shutil
import tempfile

import pytest

DATA = tempfile.mkdtemp(prefix="stock-tests-")
os.environ["DATA_DIR"] = DATA
os.environ["RAW_DIR"] = os.path.join(DATA, "bronze", "json")
os.environ["PIPELINE_LOCK_FILE"] = os.path.join(DATA, "pipeline.lock")
os.environ["PIPELINE_STATUS_FILE"] = os.path.join(DATA, "pipeline_status.json")
os.environ["GOLD_PARQUET_DIR"] = os.path.join(DATA, "gold", "parquet")
for var in ("ENABLE_DEBUG_ENDPOINTS", "EXPORT_PARQUET", "ACTIVE_PROMPT_VERSION", "MARKET_CLOSE_TIME",
            "PRICE_LOOKBACK_DAYS"):
    os.environ.pop(var, None)


@pytest.fixture(autouse=True)
def clean_data():
    for name in os.listdir(DATA):
        path = os.path.join(DATA, name)
        shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
    yield


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(DATA, ignore_errors=True)
