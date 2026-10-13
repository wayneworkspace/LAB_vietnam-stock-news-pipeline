import os
from datetime import date

import duckdb
import pytest

import config
from storage import bronze
from transform import gold, silver

from helpers import add_enriched, add_news, add_price, seed_watchlist, weekdays

FIRST_PRICE_DAY, LAST_PRICE_DAY = date(2026, 10, 5), date(2026, 10, 16)    # Monday .. Friday


@pytest.fixture
def world():
    """Prices for two trading weeks and five news items around the close, plus AI scores in two prompt versions."""
    seed_watchlist("FPT")
    bronze.init_bronze()
    con = bronze.connect()
    for day in weekdays(FIRST_PRICE_DAY, LAST_PRICE_DAY):
        add_price(con, "FPT", day)
    ids = {
        "before_close": add_news(con, "FPT", "FPT: before the close", "https://x/1", "2026-10-14 14:44:00"),
        "at_close": add_news(con, "FPT", "FPT: at the close", "https://x/2", "2026-10-14 14:45:00"),
        "friday_evening": add_news(con, "FPT", "Friday evening", "https://x/3", "2026-10-16 15:30:00"),
        "saturday": add_news(con, "FPT", "Saturday", "https://x/4", "2026-10-17 10:00:00"),
        "time_unknown": add_news(con, "FPT", "Date only", "https://x/5", "2026-10-14 00:00:00"),
    }
    add_enriched(con, ids["before_close"], "FPT", "v1", 1)
    add_enriched(con, ids["at_close"], "FPT", "v1", -1)
    add_enriched(con, ids["before_close"], "FPT", "v2", 2)      # v2 has scored only one item so far
    con.commit()
    con.close()
    return ids


def query(db, sql, params=None):
    con = duckdb.connect(db, read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def effective_dates():
    rows = query(config.SILVER_DB, "SELECT title, effective_trade_date FROM news")
    return {title: str(day) for title, day in rows}


# --- Silver: session close -----------------------------------------------------

def test_news_at_or_after_the_hose_close_counts_for_the_next_session(world):
    silver.build_silver()
    eff = effective_dates()
    assert eff["FPT: before the close"] == "2026-10-14"      # 14:44, the session is still open
    assert eff["FPT: at the close"] == "2026-10-15"          # 14:45, the closing price is already set
    assert eff["Date only"] == "2026-10-14"                  # no time of day: treated as the same day


def test_close_time_comes_from_config(world, monkeypatch):
    monkeypatch.setattr(silver, "MARKET_CLOSE_TIME", "14:30")
    silver.build_silver()
    assert effective_dates()["FPT: before the close"] == "2026-10-15"   # 14:44 is now after the close


# --- Gold: dim_date ----------------------------------------------------------------------------

def test_dim_date_is_a_static_calendar(world):
    silver.build_silver()
    gold.build_gold()
    rows = {str(r[0]): r[1:] for r in query(
        config.GOLD_DB, "SELECT date_key, is_trading_day, is_holiday, holiday_name FROM dim_date")}
    assert len(rows) >= 365                                         # the whole year, not just the data range
    assert rows["2026-01-01"][1] is True                            # before the first data day
    assert rows["2026-02-17"][:2] == (False, True)                  # Tet: closed
    assert rows["2026-09-30"][:2] == (True, False)                  # no price that day, but the market was open
    assert rows["2026-10-17"][0] is False                           # Saturday


# --- Gold: prompt version pinning ----------------------------------------------------------------

def news_scores():
    return {title: (sentiment, version) for title, sentiment, version in query(
        config.GOLD_DB, "SELECT title, sentiment, prompt_version FROM fact_news")}


def test_gold_reads_only_the_active_prompt_version(world):
    silver.build_silver()
    gold.build_gold()
    scores = news_scores()
    assert scores["FPT: before the close"] == (1, "v1")       # not the v2 score of 2
    assert scores["FPT: at the close"] == (-1, "v1")


def test_promoting_to_v2_switches_versions_without_mixing_them(world, monkeypatch):
    silver.build_silver()
    monkeypatch.setattr(gold, "ACTIVE_PROMPT_VERSION", "v2")
    gold.build_gold()
    scores = news_scores()
    assert scores["FPT: before the close"] == (2, "v2")
    assert scores["FPT: at the close"] == (None, None)        # not scored by v2 yet: shown as unscored, never as v1


# --- Gold: Parquet export ------------------------------------------------------------------------

def test_gold_is_exported_to_parquet_partitioned_by_date(world):
    silver.build_silver()
    out = gold.build_gold()
    base = config.GOLD_PARQUET_DIR
    assert os.path.isdir(os.path.join(base, "fact_price_daily", "date_key=2026-10-14"))
    assert os.path.exists(os.path.join(base, "dim_ticker.parquet"))

    con = duckdb.connect()
    glob = os.path.join(base, "fact_price_daily", "*", "*.parquet").replace("\\", "/")
    total = con.execute(f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true)").fetchone()[0]
    one_day = con.execute(f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true) "
                          "WHERE date_key = DATE '2026-10-14'").fetchone()[0]
    con.close()
    assert total == out["fact_price_daily"]
    assert one_day == 1


def test_rebuilding_gold_replaces_the_parquet_export(world):
    silver.build_silver()
    gold.build_gold()
    gold.build_gold()      # a second export into an existing folder must not fail or duplicate files
    files = [f for _, _, names in os.walk(os.path.join(config.GOLD_PARQUET_DIR, "fact_price_daily")) for f in names]
    assert len(files) == len(weekdays(FIRST_PRICE_DAY, LAST_PRICE_DAY))
