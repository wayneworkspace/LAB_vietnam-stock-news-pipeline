"""Silver (DuckDB, OLAP): clean and standardise the data coming from Bronze.
prices_daily and news are rebuilt completely from Bronze on every run (re-running creates no duplicates).
news_enriched is rebuilt from Bronze as well (AI results are stored in Bronze, the most durable layer).
Prices keep the unit used by the source (thousand VND)."""
import os

import duckdb
import pandas as pd

from config import MARKET_CLOSE_TIME, SILVER_DB
from storage import bronze

PRICES_SQL = r"""
CREATE OR REPLACE TABLE prices_daily AS
WITH parsed AS (
    SELECT CAST(raw_id AS BIGINT) AS raw_id, ma,
           CAST(try_strptime(json_extract_string(CAST(payload AS VARCHAR), '$.Ngay'), '%d/%m/%Y') AS DATE) AS trade_date,
           TRY_CAST(json_extract_string(CAST(payload AS VARCHAR), '$.GiaMoCua') AS DOUBLE) AS open,
           TRY_CAST(json_extract_string(CAST(payload AS VARCHAR), '$.GiaCaoNhat') AS DOUBLE) AS high,
           TRY_CAST(json_extract_string(CAST(payload AS VARCHAR), '$.GiaThapNhat') AS DOUBLE) AS low,
           TRY_CAST(json_extract_string(CAST(payload AS VARCHAR), '$.GiaDongCua') AS DOUBLE) AS close,
           TRY_CAST(json_extract_string(CAST(payload AS VARCHAR), '$.GiaDieuChinh') AS DOUBLE) AS adj_close,
           CAST(TRY_CAST(json_extract_string(CAST(payload AS VARCHAR), '$.KhoiLuongKhopLenh') AS DOUBLE) AS BIGINT) AS volume
    FROM raw_price
),
latest AS (
    SELECT * FROM parsed WHERE trade_date IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY ma, trade_date ORDER BY raw_id DESC) = 1
)
SELECT ma, trade_date, open, high, low, close, adj_close, volume,
       adj_close / lag(adj_close) OVER (PARTITION BY ma ORDER BY trade_date) - 1 AS pct_change,
       abs(adj_close - close) > 0.005 AS is_adjusted,
       coalesce(high >= low AND high >= open AND high >= close AND low <= open AND low <= close
                AND close > 0 AND volume >= 0, false) AS is_valid,
       raw_id AS source_raw_id
FROM latest
ORDER BY ma, trade_date
"""

NEWS_SQL = r"""
CREATE OR REPLACE TABLE news AS
WITH parsed AS (
    SELECT CAST(raw_id AS BIGINT) AS raw_id, ma, left(CAST(payload_hash AS VARCHAR), 16) AS news_id,
           regexp_replace(trim(json_extract_string(CAST(payload AS VARCHAR), '$.title')), '\s+', ' ', 'g') AS title,
           json_extract_string(CAST(payload AS VARCHAR), '$.url') AS url,
           try_strptime(json_extract_string(CAST(payload AS VARCHAR), '$.published_at'), '%Y-%m-%d %H:%M:%S') AS published_at
    FROM raw_news
),
latest AS (
    SELECT * FROM parsed
    QUALIFY row_number() OVER (PARTITION BY ma, news_id ORDER BY raw_id DESC) = 1
),
base AS (
    SELECT news_id, ma, title, url, published_at, CAST(published_at AS DATE) AS published_date,
           CASE WHEN title LIKE ma || ':%' THEN 'cong_bo' ELSE 'bai_bao' END AS news_type,
           coalesce(NOT (hour(published_at) = 0 AND minute(published_at) <= 1), false) AS time_known,
           raw_id
    FROM latest
)
SELECT news_id, ma, title, url, published_at, published_date, news_type, time_known,
       CASE WHEN published_at IS NULL THEN NULL
            WHEN time_known AND CAST(published_at AS TIME) >= CAST('{close_time}' AS TIME)
                 THEN (SELECT min(trade_date) FROM calendar c WHERE c.trade_date > b.published_date)
            ELSE (SELECT min(trade_date) FROM calendar c WHERE c.trade_date >= b.published_date)
       END AS effective_trade_date,
       raw_id AS source_raw_id
FROM base b
ORDER BY ma, published_at
"""


def connect_silver():
    os.makedirs(os.path.dirname(SILVER_DB), exist_ok=True)
    return duckdb.connect(SILVER_DB)


ENRICHED_SQL = """
CREATE OR REPLACE TABLE news_enriched AS
SELECT news_id, ma, summary, CAST(sentiment AS TINYINT) AS sentiment, model, prompt_version,
       CAST(enriched_at AS TIMESTAMP) AS enriched_at
FROM raw_enriched
"""

ENRICHED_COLS = ["news_id", "ma", "summary", "sentiment", "model", "prompt_version", "enriched_at"]


def _preserve_old_enrichment(con, b):
    """An earlier version kept news_enriched inside Silver. Move those rows to Bronze (skipping rows
    already there) before rebuilding, so AI results that were already paid for are not lost."""
    has = con.execute("SELECT count(*) FROM information_schema.tables "
                      "WHERE table_name = 'news_enriched'").fetchone()[0]
    if not has:
        return 0
    rows = con.execute("SELECT news_id, ma, summary, sentiment, model, prompt_version, "
                       "strftime(enriched_at, '%Y-%m-%d %H:%M:%S') FROM news_enriched").fetchall()
    cur = b.executemany(
        "INSERT OR IGNORE INTO news_enriched (news_id, ma, summary, sentiment, model, prompt_version, "
        "enriched_at) VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    b.commit()
    return len(rows)


def _raw(b, data_type: str) -> pd.DataFrame:
    rows = b.execute("SELECT raw_id, ma, payload, payload_hash FROM raw_data "
                     "WHERE source = 'cafef' AND data_type = ? ORDER BY raw_id", [data_type]).fetchall()
    return pd.DataFrame(rows, columns=["raw_id", "ma", "payload", "payload_hash"]).astype(
        {"raw_id": "int64", "ma": "object", "payload": "object", "payload_hash": "object"})


def build_silver() -> dict:
    con = connect_silver()
    b = bronze.connect()
    try:
        bronze.init_bronze()
        _preserve_old_enrichment(con, b)
        raw_news, raw_price = _raw(b, "news"), _raw(b, "price")
        enr = pd.DataFrame(b.execute(f"SELECT {', '.join(ENRICHED_COLS)} FROM news_enriched").fetchall(),
                           columns=ENRICHED_COLS).astype(
            {c: "object" for c in ENRICHED_COLS if c != "sentiment"})
        enr["sentiment"] = enr["sentiment"].astype("float64")
    finally:
        b.close()
    try:
        con.register("raw_news", raw_news)
        con.register("raw_price", raw_price)
        con.register("raw_enriched", enr)
        con.execute(PRICES_SQL)
        con.execute("CREATE OR REPLACE TEMP TABLE calendar AS "
                    "SELECT DISTINCT trade_date FROM prices_daily WHERE is_valid")
        con.execute(NEWS_SQL.replace("{close_time}", MARKET_CLOSE_TIME))
        con.execute(ENRICHED_SQL)
        q = lambda s: con.execute(s).fetchone()[0]
        return {
            "prices_daily": q("SELECT count(*) FROM prices_daily"),
            "prices_invalid": q("SELECT count(*) FROM prices_daily WHERE NOT is_valid"),
            "news": q("SELECT count(*) FROM news"),
            "news_without_effective_date": q("SELECT count(*) FROM news WHERE effective_trade_date IS NULL"),
            "news_enriched": q("SELECT count(*) FROM news_enriched"),
        }
    finally:
        con.close()
