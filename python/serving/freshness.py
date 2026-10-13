"""Data freshness check: is the newest stored price as recent as the static trading calendar says it should be?

Today's price is never required (a run at 07:30 cannot have it), so the expected day is the last
trading day BEFORE today. A ticker whose newest price is older than that, or that has no price at all,
fails the check. The calendar is the weekend + holiday file (transform/trading_calendar.py): a holiday
missing from the file shows up here as a permanent "stale" until the file is fixed.
"""
from datetime import date, datetime, timedelta
from typing import Optional

from config import VN_TZ
from storage import bronze, watchlist
from transform import trading_calendar

LOOKBACK_DAYS = 30      # longer than any market closure (Tet is at most 9 days with weekends)


def expected_latest_trading_day(now: Optional[datetime] = None) -> date:
    today = (now or datetime.now(VN_TZ)).date()
    cal = trading_calendar.build_calendar(today - timedelta(days=LOOKBACK_DAYS), today)
    days = cal[cal["is_trading_day"] & (cal["date_key"].dt.date < today)]["date_key"]
    return days.max().date()


def check_freshness(now: Optional[datetime] = None) -> dict:
    """Raise RuntimeError listing every stale ticker; otherwise return what was checked."""
    expected = expected_latest_trading_day(now)
    con = bronze.connect()
    try:
        tickers = [ma for ma, _ in watchlist.active_tickers(con)]
        last = {ma: bronze.latest_price_date(con, ma) for ma in tickers}
    finally:
        con.close()
    stale = [f"{ma}: {'no price stored' if d is None else 'newest price ' + d.isoformat()}"
             for ma, d in last.items() if d is None or d < expected]
    if stale:
        raise RuntimeError(f"Stale prices (expected at least {expected.isoformat()}): " + "; ".join(stale))
    return {"expected": expected.isoformat(), "checked": len(tickers)}
