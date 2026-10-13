from datetime import date

import pandas as pd

from transform import trading_calendar as tc


def row(cal, day):
    return cal[cal["date_key"] == pd.Timestamp(day)].iloc[0]


def test_tet_2026_weekdays_are_holidays_and_weekends_are_not_listed_holidays():
    cal = tc.build_calendar(date(2026, 2, 13), date(2026, 2, 23))
    assert bool(row(cal, "2026-02-13").is_trading_day)               # Friday before Tet
    assert not bool(row(cal, "2026-02-14").is_trading_day)           # Saturday
    assert not bool(row(cal, "2026-02-14").is_holiday)
    for day in ["2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20"]:
        assert bool(row(cal, day).is_holiday) and not bool(row(cal, day).is_trading_day)
    assert bool(row(cal, "2026-02-23").is_trading_day)               # first Monday after Tet


def test_calendar_covers_whole_calendar_years():
    cal = tc.build_calendar(date(2026, 6, 10), date(2027, 3, 1))
    assert cal["date_key"].min() == pd.Timestamp("2026-01-01")
    assert cal["date_key"].max() == pd.Timestamp("2027-12-31")
    assert bool(row(cal, "2027-01-01").is_trading_day)               # no 2027 rows in the file yet: weekday rule only


def test_calendar_columns_and_weekday_numbers():
    cal = tc.build_calendar(date(2026, 10, 12), date(2026, 10, 18))
    assert list(cal.columns) == ["date_key", "year", "month", "day", "iso_week", "day_of_week",
                                 "is_weekend", "is_holiday", "holiday_name", "is_trading_day"]
    assert row(cal, "2026-10-12").day_of_week == 1 and row(cal, "2026-10-18").day_of_week == 7
    assert row(cal, "2026-10-12").iso_week == 42


def test_holiday_file_lists_weekdays_only():
    h = tc.load_holidays()
    assert h["date"].is_unique
    assert (h["date"].dt.weekday < 5).all()
