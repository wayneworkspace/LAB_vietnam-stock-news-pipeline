"""Static trading calendar for the Vietnamese stock market.

A day is an expected trading day when it is a weekday and is not listed in
python/reference/vn_market_holidays.csv. The file is maintained by hand once a year, so every
calendar row also carries `holiday_data_available`: False means the year is not covered and only the
weekday rule was applied (best effort).

Observed price data always wins over this calendar. Silver unions the two, and Gold exposes both
`is_trading_day` (expected) and `has_price_data` (observed), so a missing price on an expected
trading day shows up as a data gap instead of silently looking like a holiday.
"""
import os
from datetime import date
from typing import Optional

import pandas as pd

HOLIDAYS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reference",
                            "vn_market_holidays.csv")


def load_holidays(path: Optional[str] = None) -> pd.DataFrame:
    """Read the holiday file: one row per closed weekday (columns: date, name)."""
    df = pd.read_csv(path or HOLIDAYS_CSV, comment="#", dtype=str, skipinitialspace=True)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    return df[["date", "name"]]


def covered_years(holidays: pd.DataFrame) -> set:
    """Years that have at least one row in the holiday file."""
    return {int(y) for y in holidays["date"].dt.year.unique()}


def _holidays(holidays: Optional[pd.DataFrame]) -> pd.DataFrame:
    h = load_holidays() if holidays is None else holidays
    if h.empty:
        raise ValueError("The holiday file has no rows; add at least one holiday")
    return h


def calendar_bounds(data_min: Optional[date], data_max: Optional[date],
                    holidays: Optional[pd.DataFrame] = None) -> tuple:
    """(start, end) of the calendar: Jan 1 of the first covered year to Dec 31 of the last one,
    stretched to include any data that falls outside that range."""
    years = covered_years(_holidays(holidays))
    start, end = date(min(years), 1, 1), date(max(years), 12, 31)
    if data_min is not None and data_min < start:
        start = data_min
    if data_max is not None and data_max > end:
        end = data_max
    return start, end


def holiday_calendar_stale(latest_day: date, holidays: Optional[pd.DataFrame] = None) -> bool:
    """True when `latest_day` falls in a year the holiday file does not cover."""
    return latest_day.year not in covered_years(_holidays(holidays))


def build_calendar(start: date, end: date, holidays: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """One row per calendar day from start to end (inclusive)."""
    h = _holidays(holidays)
    days = pd.date_range(start=start, end=end, freq="D")
    names = dict(zip(h["date"], h["name"]))
    cal = pd.DataFrame({"date_key": days})
    cal["year"] = days.year
    cal["month"] = days.month
    cal["day"] = days.day
    cal["iso_week"] = days.isocalendar().week.to_numpy(dtype="int64")
    cal["day_of_week"] = days.dayofweek + 1            # ISO: Monday = 1 ... Sunday = 7
    cal["is_weekend"] = cal["day_of_week"] >= 6
    holiday_name = cal["date_key"].map(names)
    cal["is_holiday"] = holiday_name.notna()
    cal["holiday_name"] = holiday_name.astype("object").where(holiday_name.notna(), None)
    cal["holiday_data_available"] = cal["year"].isin(covered_years(h))
    cal["is_trading_day"] = ~cal["is_weekend"] & ~cal["is_holiday"]
    return cal
