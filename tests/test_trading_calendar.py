from datetime import date

import pandas as pd
import pytest

from transform import trading_calendar as tc


def row(cal, day):
    return cal[cal["date_key"] == pd.Timestamp(day)].iloc[0]


def test_tet_2026_weekdays_are_holidays_and_weekends_are_not_counted_twice():
    cal = tc.build_calendar(date(2026, 2, 13), date(2026, 2, 23))
    assert bool(row(cal, "2026-02-13").is_trading_day)               # Friday before Tet
    assert not bool(row(cal, "2026-02-14").is_trading_day)           # Saturday
    assert not bool(row(cal, "2026-02-14").is_holiday)               # weekend, but not a listed holiday
    for day in ["2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20"]:
        assert bool(row(cal, day).is_holiday) and not bool(row(cal, day).is_trading_day)
        assert row(cal, day).holiday_name == "Lunar New Year (Tet)"
    assert bool(row(cal, "2026-02-23").is_trading_day)               # first Monday after Tet


def test_calendar_columns_match_the_dim_date_contract():
    cal = tc.build_calendar(date(2026, 10, 12), date(2026, 10, 18))
    assert list(cal.columns) == ["date_key", "year", "month", "day", "iso_week", "day_of_week", "is_weekend",
                                 "is_holiday", "holiday_name", "holiday_data_available", "is_trading_day"]
    assert row(cal, "2026-10-12").day_of_week == 1                   # Monday
    assert row(cal, "2026-10-18").day_of_week == 7                   # Sunday
    assert row(cal, "2026-10-12").iso_week == 42
    assert row(cal, "2026-10-12").holiday_name is None


def test_a_year_without_holiday_rows_is_flagged_and_falls_back_to_weekdays():
    cal = tc.build_calendar(date(2027, 1, 1), date(2027, 1, 4))
    assert not cal["holiday_data_available"].any()
    assert bool(row(cal, "2027-01-01").is_trading_day)               # 2027 New Year is unknown, so it counts as a weekday
    assert tc.holiday_calendar_stale(date(2027, 1, 4))
    assert not tc.holiday_calendar_stale(date(2026, 10, 14))


def test_bounds_cover_the_covered_years_and_stretch_for_data_outside_them():
    assert tc.calendar_bounds(None, None) == (date(2026, 1, 1), date(2026, 12, 31))
    assert tc.calendar_bounds(date(2025, 6, 1), date(2027, 3, 1)) == (date(2025, 6, 1), date(2027, 3, 1))
    assert tc.calendar_bounds(date(2026, 3, 1), date(2026, 4, 1)) == (date(2026, 1, 1), date(2026, 12, 31))


def test_holiday_file_is_well_formed():
    h = tc.load_holidays()
    assert h["date"].is_unique
    assert (h["date"].dt.weekday < 5).all(), "the file should list weekdays only (weekends are implicit)"
    assert tc.covered_years(h) == {2026}


def test_an_empty_holiday_file_is_rejected(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("date,name\n")
    with pytest.raises(ValueError):
        tc.calendar_bounds(None, None, tc.load_holidays(str(path)))
