"""Backup of Bronze, the only layer that cannot be rebuilt (Silver and Gold are rebuilt from it).

Uses SQLite's online backup API, so the copy is consistent even if a write happens meanwhile.
One file per day (bronze-YYYY-MM-DD.sqlite); a second run the same day replaces it, and only the
newest KEEP_BACKUPS files are kept. The copy is built as .tmp and then renamed, so a failed
backup never replaces a good one.

Note: the backups live next to the data, so they protect against corruption and mistakes, not against
losing the disk. Copy data/backups/ to another machine or drive too.
"""
import glob
import os
import sqlite3
from datetime import date, datetime
from typing import Optional

from config import BACKUP_DIR, BRONZE_DB, VN_TZ

KEEP_BACKUPS = 7


def backup_bronze(dest_dir: str = BACKUP_DIR, keep: int = KEEP_BACKUPS, today: Optional[date] = None) -> dict:
    if not os.path.exists(BRONZE_DB):
        raise RuntimeError("No Bronze database to back up yet")
    today = today or datetime.now(VN_TZ).date()
    os.makedirs(dest_dir, exist_ok=True)
    final = os.path.join(dest_dir, f"bronze-{today.isoformat()}.sqlite")
    tmp = final + ".tmp"
    src, dst = sqlite3.connect(BRONZE_DB), sqlite3.connect(tmp)
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()
    os.replace(tmp, final)
    files = sorted(glob.glob(os.path.join(dest_dir, "bronze-????-??-??.sqlite")), reverse=True)
    removed = [os.path.basename(f) for f in files[keep:]]
    for f in files[keep:]:
        os.remove(f)
    return {"file": final, "removed": removed}
