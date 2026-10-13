"""Shared configuration."""
import os
import tempfile
from datetime import timedelta, timezone

VN_TZ = timezone(timedelta(hours=7))
DATA_DIR = os.getenv("DATA_DIR", "/data")
BRONZE_DB = os.path.join(DATA_DIR, "bronze", "bronze.sqlite")   # SQLite, OLTP
SILVER_DB = os.path.join(DATA_DIR, "silver", "silver.duckdb")   # DuckDB, OLAP
GOLD_DB = os.path.join(DATA_DIR, "gold", "gold.duckdb")         # DuckDB, OLAP
GOLD_PARQUET_DIR = os.path.join(DATA_DIR, "gold", "parquet")    # Gold exported as Parquet
RAW_DIR = os.getenv("RAW_DIR", os.path.join(DATA_DIR, "bronze", "json"))
LEGACY_DB = os.getenv("DUCKDB_PATH", os.path.join(DATA_DIR, "stock.duckdb"))  # old DB, kept only for migration

# The /debug/* endpoints write files and call the source site: off unless explicitly enabled.
ENABLE_DEBUG_ENDPOINTS = os.getenv("ENABLE_DEBUG_ENDPOINTS", "false").strip().lower() in {"1", "true", "yes"}

# Incremental price loads re-fetch (days since the last stored trade date) + this many days.
PRICE_LOOKBACK_DAYS = 3

# News published at or after this time (VN time) counts for the NEXT trading session.
# HOSE: the ATC closing auction ends at 14:45, when the closing price is set (put-through trading
# continues to 15:00 but no longer moves it). Not verified for HNX / UPCOM.
# Source: https://prime.kisvn.vn/support/trading-regulations/hose-transaction-guidance
MARKET_CLOSE_TIME = "14:45"

# Lock file for the run-overlap guard. It lives outside the ./data bind mount because file locks
# are unreliable on Windows-mounted folders.
PIPELINE_LOCK_FILE = os.path.join(tempfile.gettempdir(), "stock-pipeline.lock")
