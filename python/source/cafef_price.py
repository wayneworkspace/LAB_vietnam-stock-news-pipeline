"""CafeF price source: trading history by ticker (JSON). No database access.
Note: the date parameters are sent as MM/DD/YYYY, while the dates in the response are DD/MM/YYYY."""
import json
import os
import time
from datetime import datetime, timedelta

import requests

from config import RAW_DIR, VN_TZ

URL = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx"
HEADERS = {"User-Agent": "StockLearningBot/0.1 (personal research; 1 request/ticker/day)"}
DEBUG_DIR = os.path.join(RAW_DIR, "..", "debug")


def fetch_price_json(ma: str, exchange: str = "HOSE", days: int = 30,
                     page: int = 1, size: int = 50) -> dict:
    end = datetime.now(VN_TZ).date()
    start = end - timedelta(days=days)
    params = {
        "ExchangeType": exchange, "Symbol": ma.upper(),
        "StartDate": start.strftime("%m/%d/%Y"), "EndDate": end.strftime("%m/%d/%Y"),
        "PageIndex": page, "PageSize": size,
    }
    r = requests.get(URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def save_debug_json(ma: str, exchange: str = "HOSE") -> dict:
    data = fetch_price_json(ma, exchange)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    path = os.path.normpath(os.path.join(DEBUG_DIR, f"cafef_price_{ma.upper()}.json"))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return {"file": path, "bytes": os.path.getsize(path)}


def fetch_all_records(ma: str, exchange: str = "HOSE", days: int = 30,
                      delay_s: float = 1.0, provider=None, max_pages: int = 20) -> list[dict]:
    """Fetch page by page (the server returns at most 20 rows per page) until TotalCount is reached.
    Raises instead of returning a silently truncated result when max_pages runs out first."""
    get = provider or (lambda ma, ex, days, page: fetch_price_json(ma, ex, days, page))
    records, total = [], None
    for page in range(1, max_pages + 1):
        if page > 1:
            time.sleep(delay_s)
        d = get(ma, exchange, days, page)["Data"]
        rows = d.get("Data") or []
        total = d.get("TotalCount", total)
        records.extend(rows)
        if not rows or (total is not None and len(records) >= total):
            break
    else:
        raise RuntimeError(f"{ma}: stopped after max_pages={max_pages} with {len(records)} of "
                           f"{total} rows; use a shorter window or raise max_pages")
    return records


def parse_prices(records: list[dict]) -> list[dict]:
    out = []
    for r in records:
        try:
            d, m, y = map(int, r["Ngay"].split("/"))        # the response uses DD/MM/YYYY
            out.append({
                "trade_date": datetime(y, m, d).date().isoformat(),
                "open": r.get("GiaMoCua"), "high": r.get("GiaCaoNhat"),
                "low": r.get("GiaThapNhat"), "close": r.get("GiaDongCua"),
                "adj_close": r.get("GiaDieuChinh"),
                "volume": r.get("KhoiLuongKhopLenh"),        # matched orders only, negotiated trades not included
                "raw": r,
            })
        except (KeyError, ValueError, AttributeError):
            continue
    return out
