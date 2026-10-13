import os
import sqlite3
from datetime import date

import pytest
from fastapi.testclient import TestClient

import config
from serving import api
from serving.locking import pipeline_lock
from storage import backup, bronze

from helpers import add_price, seed_watchlist


def seed_bronze(days=1):
    seed_watchlist("FPT")
    con = bronze.connect()
    for i in range(days):
        add_price(con, "FPT", date(2026, 10, 1 + i))
    con.commit()
    con.close()


def count(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("SELECT count(*) FROM raw_data").fetchone()[0]
    finally:
        con.close()


def test_backup_is_a_complete_copy_named_after_the_day():
    seed_bronze(days=3)
    out = backup.backup_bronze(today=date(2026, 10, 14))
    assert os.path.basename(out["file"]) == "bronze-2026-10-14.sqlite"
    assert count(out["file"]) == count(config.BRONZE_DB) == 3
    assert not any(n.endswith(".tmp") for n in os.listdir(config.BACKUP_DIR))


def test_running_twice_on_the_same_day_replaces_the_file():
    seed_bronze()
    backup.backup_bronze(today=date(2026, 10, 14))
    seed_bronze(days=4)
    out = backup.backup_bronze(today=date(2026, 10, 14))
    assert count(out["file"]) == 4
    assert len(os.listdir(config.BACKUP_DIR)) == 1


def test_only_the_newest_backups_are_kept():
    seed_bronze()
    for day in range(10, 16):
        out = backup.backup_bronze(today=date(2026, 10, day), keep=3)
    assert sorted(os.listdir(config.BACKUP_DIR)) == ["bronze-2026-10-13.sqlite", "bronze-2026-10-14.sqlite",
                                                     "bronze-2026-10-15.sqlite"]
    assert out["removed"] == ["bronze-2026-10-12.sqlite"]


def test_without_a_bronze_database_there_is_nothing_to_back_up():
    with pytest.raises(RuntimeError, match="No Bronze"):
        backup.backup_bronze()


def test_backup_endpoint():
    client = TestClient(api.app)
    assert client.post("/backup").status_code == 409                 # nothing to back up yet
    seed_bronze()
    r = client.post("/backup")
    assert r.status_code == 200 and r.json()["file"].endswith(".sqlite")
    with pipeline_lock():
        assert client.post("/backup").status_code == 409             # a pipeline step is running
