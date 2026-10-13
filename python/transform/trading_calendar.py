"""Static trading calendar for the Vietnamese stock market.

A day is an expected trading day when it is a weekday and is not listed in
python/reference/vn_market_holidays.csv. The file is maintained by hand once a year; a year with no
rows in it is treated as having weekends only.
"""
import os
from datetime import date

import pandas as pd

HOLIDAYS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reference",
                            "vn_market_holidays.csv")


def load_holidays() -> pd.DataFrame:
    """Read the holiday file: one row per closed weekday (columns: date, name)."""
    df = pd.read_csv(HOLIDAYS_CSV, comment="#", dtype=str, skipinitialspace=True)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    return df[["date", "name"]]


def build_calendar(first_day: date, last_day: date) -> pd.DataFrame:
    """One row per day, from Jan 1 of first_day's year to Dec 31 of last_day's year."""
    h = load_holidays()
    days = pd.date_range(start=date(first_day.year, 1, 1), end=date(last_day.year, 12, 31), freq="D")
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
    cal["is_trading_day"] = ~cal["is_weekend"] & ~cal["is_holiday"]
    return cal
