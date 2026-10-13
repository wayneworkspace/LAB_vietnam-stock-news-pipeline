"""Seed helpers shared by the tests."""
import json
from datetime import date, timedelta

from ingestion.fetch_news import sha
from storage import bronze, watchlist

NOW = "2026-10-14 08:00:00"


def weekdays(start: date, end: date) -> list:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5]


def price_payload(day: date, close: float = 100.0) -> dict:
    """One CafeF price record (dates are DD/MM/YYYY). The numbers form a valid OHLC bar."""
    return {"Ngay": day.strftime("%d/%m/%Y"), "GiaMoCua": close - 1, "GiaCaoNhat": close + 1,
            "GiaThapNhat": close - 2, "GiaDongCua": close, "GiaDieuChinh": close,
            "KhoiLuongKhopLenh": 1000}


def make_provider(today: date, calls: list):
    """A fake price source: every weekday inside the requested window, recording each call."""
    def provider(ma, exchange, days, page):
        calls.append({"ma": ma, "days": days, "page": page})
        rows = [price_payload(d) for d in weekdays(today - timedelta(days=days), today)]
        return {"Data": {"Data": rows, "TotalCount": len(rows)}}
    return provider


def seed_watchlist(*tickers: str) -> None:
    bronze.init_bronze()
    for ma in tickers:
        watchlist.upsert_watchlist(ma, san="HOSE")


def add_price(con, ma: str, day: date, close: float = 100.0) -> None:
    payload = json.dumps(price_payload(day, close), sort_keys=True)
    bronze.insert_raw(con, "seed", "cafef", "price", ma, payload, sha(f"{ma}|{day.isoformat()}|{payload}"),
                      NOW, NOW[:10])


def add_news(con, ma: str, title: str, url: str, published_at: str) -> str:
    """Insert one news item and return its news_id (the first 16 characters of the url hash)."""
    payload = json.dumps({"title": title, "url": url, "published_at": published_at}, ensure_ascii=False)
    bronze.insert_raw(con, "seed", "cafef", "news", ma, payload, sha(url), NOW, NOW[:10],
                      url=url, published_at=published_at)
    return sha(url)[:16]


def add_enriched(con, news_id: str, ma: str, version: str, sentiment: int) -> None:
    con.execute("INSERT INTO news_enriched (news_id, ma, summary, sentiment, model, prompt_version, enriched_at) "
                "VALUES (?, ?, ?, ?, 'test-model', ?, ?)",
                [news_id, ma, f"summary {version}", sentiment, version, NOW])
