"""Gold (DuckDB, OLAP): star schema for analytics.
dim_ticker, dim_date, fact_price_daily, fact_news; rebuilt completely from Silver and the watchlist on every run.
The tables are also exported to Parquet (fact tables partitioned by date)."""
import os
import shutil
from datetime import datetime

import duckdb
import pandas as pd

from config import GOLD_DB, GOLD_PARQUET_DIR, SILVER_DB, VN_TZ
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

# dim_date comes from the static calendar (weekends + holidays), not from the days that have data,
# so a day with missing data is no longer indistinguishable from a day the market was closed.
DIM_DATE_SQL = """
CREATE OR REPLACE TABLE dim_date AS
SELECT CAST(date_key AS DATE) AS date_key, year, month, day, iso_week, day_of_week,
       is_weekend, is_holiday, holiday_name, is_trading_day
FROM cal ORDER BY 1
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


def export_parquet(con, out_dir: str) -> None:
    """Write every Gold table under out_dir, replacing the previous export."""
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir)
    for table, part in PARQUET_TABLES.items():
        if part:
            con.execute(f"COPY {table} TO '{os.path.join(out_dir, table)}' "
                        f"(FORMAT PARQUET, PARTITION_BY ({part}))")
        else:
            con.execute(f"COPY {table} TO '{os.path.join(out_dir, table + '.parquet')}' (FORMAT PARQUET)")


def build_gold() -> dict:
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
        con.execute(f"ATTACH '{SILVER_DB}' AS silver (READ_ONLY)")
        try:
            con.register("wl", wl)
            con.execute("CREATE OR REPLACE TABLE dim_ticker AS "
                        "SELECT ma, ten, san, active = 1 AS active, ghi_chu FROM wl")

            data_min, data_max = con.execute(DATA_BOUNDS_SQL).fetchone()
            today = datetime.now(VN_TZ).date()
            con.register("cal", trading_calendar.build_calendar(data_min or today, max(data_max or today, today)))
            con.execute(DIM_DATE_SQL)

            con.execute(FACT_PRICE_SQL)
            con.execute(FACT_NEWS_SQL, [ACTIVE_PROMPT_VERSION])
        except duckdb.CatalogException as e:
            raise RuntimeError(f"No Silver data yet, run /build-silver first ({e})")
        q = lambda s: con.execute(s).fetchone()[0]
        out = {t: q(f"SELECT count(*) FROM {t}")
               for t in ["dim_ticker", "dim_date", "fact_price_daily", "fact_news"]}
        export_parquet(con, GOLD_PARQUET_DIR)
        return out
    finally:
        con.close()
