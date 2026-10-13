"""CafeF news source: fetch a company's news page by ticker and parse the HTML. No database access.
CafeF's robots.txt currently allows this; only one page per ticker per run is requested."""
import os
import re
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

from config import RAW_DIR

BASE = "https://cafef.vn/du-lieu/tin-doanh-nghiep/{ma}/event.chn"
HEADERS = {"User-Agent": "StockLearningBot/0.1 (personal research; 1 request/ticker/day)"}
ADAPTER_VERSION = "cafef-event-v1"
DEBUG_DIR = os.path.join(RAW_DIR, "..", "debug")


def fetch_html(ma: str) -> str:
    r = requests.get(BASE.format(ma=ma.lower()), headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def save_debug_html(ma: str) -> dict:
    html = fetch_html(ma)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    path = os.path.normpath(os.path.join(DEBUG_DIR, f"cafef_{ma.upper()}.html"))
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return {"file": path, "bytes": len(html)}


def _clean_url(href: str) -> str:
    """Drop the ?utm_source=... part so the same article always yields the same url."""
    if href.startswith("/"):
        href = "https://cafef.vn" + href
    p = urlsplit(href)
    return urlunsplit((p.scheme, p.netloc, p.path, "", ""))


def parse_news(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    box = soup.find(id="divEvents")
    items = []
    if not box:
        return items
    for li in box.find_all("li"):
        a = li.find("a", class_="docnhanhTitle")
        t = li.find("span", class_="timeTitle")
        if not a or not a.get("href"):
            continue
        published = None
        if t:
            m = re.search(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2})", t.get_text())
            if m:
                d, mo, y, h, mi = map(int, m.groups())
                published = datetime(y, mo, d, h, mi).isoformat(sep=" ")  # Vietnam time
        items.append({
            "title": a.get_text(strip=True),
            "url": _clean_url(a["href"]),
            "published_at": published,
        })
    return items
