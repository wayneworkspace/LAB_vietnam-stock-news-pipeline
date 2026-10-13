from datetime import date

import pytest

from ingestion import fetch_prices
from source import cafef_price
from storage import bronze

from helpers import make_provider, seed_watchlist

TODAY = date(2026, 10, 14)      # a Wednesday


def run(today, calls, **kwargs):
    return fetch_prices.run_fetch_prices(days=30, delay_s=0, provider=make_provider(today, calls),
                                         today=today, **kwargs)


def test_plan_window_modes():
    plan = fetch_prices.plan_window
    assert plan(None, TODAY, 30, 3) == (30, "bootstrap")
    assert plan(date(2026, 10, 13), TODAY, 30, 3) == (4, "incremental")     # 1 day gap + 3 lookback
    assert plan(date(2026, 10, 14), TODAY, 30, 3) == (3, "incremental")     # already up to date: lookback only
    assert plan(date(2026, 8, 1), TODAY, 30, 3) == (74 + 3, "incremental")  # a 74-day outage is still covered
    assert plan(date(2026, 10, 20), TODAY, 30, 3) == (3, "incremental")     # a watermark in the future must not go negative
    assert plan(date(2026, 10, 13), TODAY, 30, 3, full=True) == (30, "full")


def test_first_run_loads_the_full_window_then_runs_are_incremental():
    seed_watchlist("FPT")
    calls = []

    first = run(TODAY, calls)
    assert first["tickers"]["FPT"]["mode"] == "bootstrap"
    assert calls[-1]["days"] == 30
    assert first["tickers"]["FPT"]["watermark"] is None
    stored = first["tickers"]["FPT"]["new"]
    assert stored > 15

    con = bronze.connect()
    assert bronze.latest_price_date(con, "FPT") == TODAY
    con.close()

    # The next day only the gap (1 day) plus the 3-day lookback is requested, and only the new day is new.
    second = run(date(2026, 10, 15), calls)
    assert second["tickers"]["FPT"]["mode"] == "incremental"
    assert second["tickers"]["FPT"]["window_days"] == 4
    assert second["tickers"]["FPT"]["watermark"] == "2026-10-14"
    assert calls[-1]["days"] == 4
    assert second["tickers"]["FPT"]["new"] == 1

    # Running the same day again adds nothing (idempotent).
    third = run(date(2026, 10, 15), calls)
    assert third["tickers"]["FPT"]["new"] == 0


def test_full_flag_forces_the_full_window():
    seed_watchlist("FPT")
    calls = []
    run(TODAY, calls)
    result = run(TODAY, calls, full=True)
    assert result["tickers"]["FPT"]["mode"] == "full"
    assert calls[-1]["days"] == 30
    assert result["tickers"]["FPT"]["new"] == 0


def test_a_new_ticker_bootstraps_while_an_existing_one_stays_incremental():
    seed_watchlist("FPT")
    calls = []
    run(TODAY, calls)
    seed_watchlist("VNM")
    result = run(date(2026, 10, 15), calls)
    assert result["tickers"]["FPT"]["mode"] == "incremental"
    assert result["tickers"]["VNM"]["mode"] == "bootstrap"


def test_each_run_keeps_its_own_json_snapshot():
    import os
    seed_watchlist("FPT")
    calls = []
    run(TODAY, calls)
    names = os.listdir(os.environ["RAW_DIR"])
    assert any(n.startswith("cafef_price_FPT_") and n.endswith(".json") for n in names)


def test_truncated_result_raises_instead_of_returning_partial_data():
    def endless(ma, exchange, days, page):
        return {"Data": {"Data": [{"Ngay": "01/10/2026"}], "TotalCount": 100}}

    with pytest.raises(RuntimeError, match="max_pages"):
        cafef_price.fetch_all_records("FPT", provider=endless, delay_s=0, max_pages=3)


def test_truncation_is_reported_per_ticker_in_the_run_summary():
    seed_watchlist("FPT")

    def endless(ma, exchange, days, page):
        return {"Data": {"Data": [{"Ngay": "01/10/2026"}], "TotalCount": 10_000}}

    result = fetch_prices.run_fetch_prices(delay_s=0, provider=endless, today=TODAY)
    assert "max_pages" in result["errors"]["FPT"]
    assert result["status"] == "error"


def test_latest_price_date_ignores_malformed_dates():
    seed_watchlist("FPT")
    con = bronze.connect()
    for payload, h in [('{"Ngay": "garbage"}', "h1"), ('{"Ngay": "99/99/2026"}', "h2"), ("{}", "h3")]:
        bronze.insert_raw(con, "seed", "cafef", "price", "FPT", payload, h, "2026-10-14 08:00:00", "2026-10-14")
    con.commit()
    assert bronze.latest_price_date(con, "FPT") is None     # nothing usable, so callers fall back to a full load
    con.close()
