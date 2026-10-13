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


# --- Silver: session close and calendar -----------------------------------------------------

def test_news_at_or_after_the_hose_close_counts_for_the_next_session(world):
    silver.build_silver()
    eff = effective_dates()
    assert eff["FPT: before the close"] == "2026-10-14"      # 14:44, the session is still open
    assert eff["FPT: at the close"] == "2026-10-15"          # 14:45, the closing price is already set
    assert eff["Date only"] == "2026-10-14"                  # no time of day: treated as the same day


def test_next_trading_day_comes_from_the_calendar_when_prices_do_not_exist_yet(world):
    result = silver.build_silver()
    eff = effective_dates()
    assert eff["Friday evening"] == "2026-10-19"              # Monday, although no price row exists for it yet
    assert eff["Saturday"] == "2026-10-19"
    assert result["news_without_effective_date"] == 0


def test_close_time_is_configurable(world, monkeypatch):
    monkeypatch.setattr(silver, "MARKET_CLOSE_TIME", "14:30")
    silver.build_silver()
    assert effective_dates()["FPT: before the close"] == "2026-10-15"   # 14:44 is now after the close


def test_market_close_time_must_look_like_hh_mm(monkeypatch):
    import importlib
    monkeypatch.setenv("MARKET_CLOSE_TIME", "3pm")
    with pytest.raises(ValueError):
        importlib.reload(config)
    monkeypatch.delenv("MARKET_CLOSE_TIME")
    importlib.reload(config)


def test_a_holiday_is_skipped_when_mapping_news(world):
    con = bronze.connect()
    add_news(con, "FPT", "Before Tet", "https://x/9", "2026-02-13 16:00:00")     # Friday after the close
    con.commit()
    con.close()
    silver.build_silver()
    assert effective_dates()["Before Tet"] == "2026-02-23"       # Feb 16-20 is Tet, Feb 21-22 a weekend


# --- Gold: dim_date ----------------------------------------------------------------------------

def test_dim_date_is_a_static_calendar_that_separates_holidays_from_missing_data(world):
    silver.build_silver()
    gold.build_gold()
    rows = {str(r[0]): r for r in query(
        config.GOLD_DB, "SELECT date_key, is_trading_day, has_price_data, is_holiday, holiday_name FROM dim_date")}
    assert len(rows) >= 365                                               # the whole year, not just the data range
    assert rows["2026-01-01"][3] is True                                  # before the first data day
    assert rows["2026-02-17"][1:4] == (False, False, True)                # Tet: closed, no data, a holiday
    assert rows["2026-09-30"][1:4] == (True, False, False)                # a data GAP: expected trading day, no price
    assert rows["2026-10-14"][1:3] == (True, True)
    assert rows["2026-10-17"][1] is False                                 # Saturday


def test_gold_reports_whether_the_holiday_file_is_stale(world):
    silver.build_silver()
    out = gold.build_gold()
    assert isinstance(out["holiday_calendar_stale"], bool)


# --- Gold: prompt version pinning ----------------------------------------------------------------

def news_scores():
    return {title: (sentiment, version) for title, sentiment, version in query(
        config.GOLD_DB, "SELECT title, sentiment, prompt_version FROM fact_news")}


def test_gold_reads_only_the_active_prompt_version(world):
    silver.build_silver()
    out = gold.build_gold()
    scores = news_scores()
    assert out["active_prompt_version"] == "v1"
    assert scores["FPT: before the close"] == (1, "v1")       # not the v2 score of 2
    assert scores["FPT: at the close"] == (-1, "v1")
    assert out["fact_news_scored"] == 2


def test_promoting_to_v2_switches_versions_without_mixing_them(world, monkeypatch):
    silver.build_silver()
    monkeypatch.setattr(gold, "ACTIVE_PROMPT_VERSION", "v2")
    out = gold.build_gold()
    scores = news_scores()
    assert scores["FPT: before the close"] == (2, "v2")
    assert scores["FPT: at the close"] == (None, None)        # not scored by v2 yet: shown as unscored, never as v1
    assert out["fact_news_scored"] == 1


# --- Gold: Parquet export ------------------------------------------------------------------------

def test_gold_is_exported_to_parquet_partitioned_by_date(world):
    silver.build_silver()
    out = gold.build_gold()
    base = out["parquet"]["dir"]
    assert os.path.isdir(os.path.join(base, "fact_price_daily", "date_key=2026-10-14"))
    assert os.path.exists(os.path.join(base, "dim_ticker.parquet"))
    assert out["parquet"]["files"]["fact_price_daily"] == out["fact_price_daily"]    # one file per trading day

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
    again = gold.build_gold()
    assert again["parquet"]["files"]["fact_price_daily"] == again["fact_price_daily"]
    assert not os.path.exists(config.GOLD_PARQUET_DIR + ".tmp")


def test_parquet_export_can_be_switched_off(world, monkeypatch):
    silver.build_silver()
    monkeypatch.setattr(gold, "EXPORT_PARQUET", False)
    out = gold.build_gold()
    assert "parquet" not in out
    assert not os.path.exists(config.GOLD_PARQUET_DIR)


def test_gold_without_silver_asks_for_build_silver_first():
    seed_watchlist("FPT")
    with pytest.raises(RuntimeError, match="build-silver"):
        gold.build_gold()
