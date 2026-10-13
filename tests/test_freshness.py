from datetime import date, datetime

import pytest

from serving import api, freshness
from storage import bronze

from helpers import add_price, seed_watchlist
from config import VN_TZ


def at(y, m, d, hh=7, mm=30):
    return datetime(y, m, d, hh, mm, tzinfo=VN_TZ)


def test_expected_day_is_the_last_trading_day_before_today():
    assert freshness.expected_latest_trading_day(at(2026, 10, 14)) == date(2026, 10, 13)      # Wed -> Tue
    assert freshness.expected_latest_trading_day(at(2026, 10, 19)) == date(2026, 10, 16)      # Mon -> Fri
    assert freshness.expected_latest_trading_day(at(2026, 10, 13, 20, 0)) == date(2026, 10, 12)   # today is never required
    assert freshness.expected_latest_trading_day(at(2026, 2, 23)) == date(2026, 2, 13)        # after Tet: Feb 16-20 are closed


def seed_prices(ma, last_day):
    seed_watchlist(ma)
    con = bronze.connect()
    add_price(con, ma, last_day)
    con.commit()
    con.close()


def test_fresh_data_passes():
    seed_prices("FPT", date(2026, 10, 13))
    assert freshness.check_freshness(at(2026, 10, 14)) == {"expected": "2026-10-13", "checked": 1}


def test_a_stale_ticker_fails_with_its_last_date_and_the_expected_one():
    seed_prices("FPT", date(2026, 10, 9))
    with pytest.raises(RuntimeError) as e:
        freshness.check_freshness(at(2026, 10, 14))
    assert "FPT" in str(e.value) and "2026-10-09" in str(e.value) and "2026-10-13" in str(e.value)


def test_a_ticker_without_any_price_is_stale():
    seed_watchlist("FPT")
    with pytest.raises(RuntimeError, match="FPT.*no price"):
        freshness.check_freshness(at(2026, 10, 14))


def test_an_empty_watchlist_has_nothing_to_check():
    bronze.init_bronze()
    assert freshness.check_freshness(at(2026, 10, 14))["checked"] == 0


def test_the_check_runs_right_before_the_digest():
    names = [name for name, _ in api._pipeline_steps(30, False)]
    assert names[-2:] == ["check-freshness", "digest"]
