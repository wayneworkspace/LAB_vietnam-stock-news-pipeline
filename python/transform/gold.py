"""Gold (DuckDB, OLAP): star schema for analytics.
dim_ticker, dim_date, fact_price_daily, fact_news; rebuilt completely from Silver and the watchlist on every run.
The tables are also exported to Parquet (fact tables partitioned by date) unless EXPORT_PARQUET is off."""
import os
import shutil
from datetime import datetime

import duckdb
import pandas as pd

from config import EXPORT_PARQUET, GOLD_DB, GOLD_PARQUET_DIR, SILVER_DB, VN_TZ
from storage import bronze
from transform import trading_calendar
from transform.prompts import ACTIVE_PROMPT_VERSION

DATA_BOUNDS_SQL = """
WITH dates AS (
    SELECT trade_date AS d FROM silver.prices_daily
    UNION ALL SELECT published_date FROM silver.news WHERE published_date IS NOT NULL
)
SELECT min(d), max(d) FROM dates
"""

# The calendar itself is static (see transform/trading_calendar.py); this joins the days that
# actually have a valid price so that a missing price on an expected trading day is visible.
DIM_DATE_SQL = """
CREATE OR REPLACE TABLE dim_date AS
WITH observed AS (
    SELECT DISTINCT trade_date AS date_key FROM silver.prices_daily WHERE is_valid
)
SELECT CAST(c.date_key AS DATE) AS date_key, c.year, c.month, c.day, c.iso_week, c.day_of_week,
       c.is_weekend, c.is_holiday, c.holiday_name, c.holiday_data_available, c.is_trading_day,
       o.date_key IS NOT NULL AS has_price_data
FROM cal c LEFT JOIN observed o ON CAST(c.date_key AS DATE) = o.date_key
ORDER BY 1
"""

FACT_PRICE_SQL = """
CREATE OR REPLACE TABLE fact_price_daily AS
SELECT ma, trade_date AS date_key, open, high, low, close, adj_close, volume, pct_change, is_adjusted
FROM silver.prices_daily WHERE is_valid
"""

# Only results from the ACTIVE prompt version are used, so a half-finished rollout of a new version
# never mixes two versions in the digest. Unscored news simply has NULL sentiment.
FACT_NEWS_SQL = """
CREATE OR REPLACE TABLE fact_news AS
WITH e AS (
    SELECT * FROM silver.news_enriched
    WHERE prompt_version = ?
    QUALIFY row_number() OVER (PARTITION BY news_id, ma ORDER BY enriched_at DESC) = 1
)
SELECT n.news_id, n.ma, n.published_at, n.published_date AS date_key, n.effective_trade_date,
       n.news_type, n.time_known, n.title, n.url, e.summary, e.sentiment, e.prompt_version
FROM silver.news n LEFT JOIN e ON e.news_id = n.news_id AND e.ma = n.ma
"""

# table -> partition column (None = a single file). Daily partitions are small on purpose: this
# demonstrates partition pruning. For long histories prefer coarser partitions (year / month).
PARQUET_TABLES = {"dim_ticker": None, "dim_date": None,
                  "fact_price_daily": "date_key", "fact_news": "date_key"}


def _sql_str(value: str) -> str:
    return value.replace("\\", "/").replace("'", "''")


def export_parquet(con, out_dir: str) -> dict:
    """Write every Gold table under out_dir. Built in a temporary folder and swapped in at the end,
    so a failed export never leaves a half-written folder behind."""
    tmp = out_dir.rstrip("/\\") + ".tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    try:
        for table, part in PARQUET_TABLES.items():
            if part:
                con.execute(f"COPY {table} TO '{_sql_str(os.path.join(tmp, table))}' "
                            f"(FORMAT PARQUET, PARTITION_BY ({part}))")
            else:
                con.execute(f"COPY {table} TO '{_sql_str(os.path.join(tmp, table + '.parquet'))}' (FORMAT PARQUET)")
        shutil.rmtree(out_dir, ignore_errors=True)
        os.replace(tmp, out_dir)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    files = {}
    for table in PARQUET_TABLES:
        base = os.path.join(out_dir, table)
        if os.path.isdir(base):
            files[table] = sum(len(names) for _, _, names in os.walk(base))
        else:
            files[table] = int(os.path.exists(base + ".parquet"))
    return {"dir": out_dir, "files": files}


def build_gold() -> dict:
    bronze.init_bronze()      # the watchlist table must exist even if /init-db was never called
    b = bronze.connect()
    try:
        rows = b.execute("SELECT ma, ten, san, active, ghi_chu FROM watchlist ORDER BY ma").fetchall()
    finally:
        b.close()
    wl = pd.DataFrame(rows, columns=["ma", "ten", "san", "active", "ghi_chu"]).astype(
        {"ma": "object", "ten": "object", "san": "object", "active": "int64", "ghi_chu": "object"})
    os.makedirs(os.path.dirname(GOLD_DB), exist_ok=True)
    con = duckdb.connect(GOLD_DB)
    try:
        try:
            con.execute(f"ATTACH '{SILVER_DB}' AS silver (READ_ONLY)")
        except duckdb.IOException as e:
            raise RuntimeError(f"No Silver data yet, run /build-silver first ({e})")
        try:
            con.register("wl", wl)
            con.execute("CREATE OR REPLACE TABLE dim_ticker AS "
                        "SELECT ma, ten, san, active = 1 AS active, ghi_chu FROM wl")

            holidays = trading_calendar.load_holidays()
            data_min, data_max = con.execute(DATA_BOUNDS_SQL).fetchone()
            today = datetime.now(VN_TZ).date()
            latest = max(data_max, today) if data_max else today
            start, end = trading_calendar.calendar_bounds(data_min, latest, holidays)
            con.register("cal", trading_calendar.build_calendar(start, end, holidays))
            con.execute(DIM_DATE_SQL)

            con.execute(FACT_PRICE_SQL)
            con.execute(FACT_NEWS_SQL, [ACTIVE_PROMPT_VERSION])
        except duckdb.CatalogException as e:
            raise RuntimeError(f"No Silver data yet, run /build-silver first ({e})")
        q = lambda s: con.execute(s).fetchone()[0]
        out = {t: q(f"SELECT count(*) FROM {t}")
               for t in ["dim_ticker", "dim_date", "fact_price_daily", "fact_news"]}
        out["fact_news_scored"] = q("SELECT count(*) FROM fact_news WHERE sentiment IS NOT NULL")
        out["active_prompt_version"] = ACTIVE_PROMPT_VERSION
        # True means the holiday file does not cover the current year: add its rows to
        # python/reference/vn_market_holidays.csv (until then only weekends are treated as closed).
        out["holiday_calendar_stale"] = trading_calendar.holiday_calendar_stale(latest, holidays)
        if EXPORT_PARQUET:
            out["parquet"] = export_parquet(con, GOLD_PARQUET_DIR)
        return out
    finally:
        con.close()
