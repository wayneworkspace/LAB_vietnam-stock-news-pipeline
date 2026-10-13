"""Shared configuration: paths, timezone and feature flags. Everything can be overridden with environment variables."""
import os
import re
import tempfile
from datetime import timedelta, timezone


def env_flag(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable (1 / true / yes / on)."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


VN_TZ = timezone(timedelta(hours=7))

# --- Paths -----------------------------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", "/data")
BRONZE_DB = os.path.join(DATA_DIR, "bronze", "bronze.sqlite")   # SQLite, OLTP
SILVER_DB = os.path.join(DATA_DIR, "silver", "silver.duckdb")   # DuckDB, OLAP
GOLD_DB = os.path.join(DATA_DIR, "gold", "gold.duckdb")         # DuckDB, OLAP
GOLD_PARQUET_DIR = os.getenv("GOLD_PARQUET_DIR", os.path.join(DATA_DIR, "gold", "parquet"))
RAW_DIR = os.getenv("RAW_DIR", os.path.join(DATA_DIR, "bronze", "json"))
LEGACY_DB = os.getenv("DUCKDB_PATH", os.path.join(DATA_DIR, "stock.duckdb"))  # old DB, kept only for migration

# --- Feature flags ---------------------------------------------------------------------------
# /debug/* endpoints write files and call the source site; keep them off outside development.
ENABLE_DEBUG_ENDPOINTS = env_flag("ENABLE_DEBUG_ENDPOINTS", False)
# Export the Gold tables to Parquet (fact tables partitioned by date) after every /build-gold.
EXPORT_PARQUET = env_flag("EXPORT_PARQUET", True)

# --- Price ingestion -------------------------------------------------------------------------
# Incremental loads re-fetch (days since the last stored trade date) + this many days.
PRICE_LOOKBACK_DAYS = int(os.getenv("PRICE_LOOKBACK_DAYS", "3"))

# --- Market session --------------------------------------------------------------------------
# News published at or after this time (VN time) counts for the NEXT trading session.
# HOSE: continuous matching ends 14:30 and the ATC closing auction ends 14:45, when the closing
# price is set (put-through trading continues to 15:00 but no longer moves the close).
# Source: https://prime.kisvn.vn/support/trading-regulations/hose-transaction-guidance
# HNX and UPCOM are assumed to behave the same way; this has not been verified for them.
MARKET_CLOSE_TIME = os.getenv("MARKET_CLOSE_TIME", "14:45")
if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", MARKET_CLOSE_TIME):
    raise ValueError(f"MARKET_CLOSE_TIME must look like HH:MM, got {MARKET_CLOSE_TIME!r}")

# --- Run-overlap protection ------------------------------------------------------------------
# The lock lives on the container's own filesystem (not the ./data bind mount) because file locks
# are unreliable on Windows-mounted folders. The status file is plain JSON, so it can live in DATA_DIR.
PIPELINE_LOCK_FILE = os.getenv("PIPELINE_LOCK_FILE", os.path.join(tempfile.gettempdir(), "stock-pipeline.lock"))
PIPELINE_STATUS_FILE = os.getenv("PIPELINE_STATUS_FILE", os.path.join(DATA_DIR, "pipeline_status.json"))
