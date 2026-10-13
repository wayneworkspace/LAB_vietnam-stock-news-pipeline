"""Ingestion: fetch news for the active tickers and write to Bronze only (raw_data + run_log + a daily JSON file).
Re-running creates no duplicate rows. Turning the raw payloads into the news table is Silver's job."""
import hashlib
import json
import os
import time
import uuid
from datetime import datetime

from config import RAW_DIR, VN_TZ
from source import cafef
from storage import bronze, watchlist

ADAPTER_VERSION = cafef.ADAPTER_VERSION


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def run_fetch_news(delay_s: float = 2.0, html_provider=None) -> dict:
    get_html = html_provider or cafef.fetch_html
    started = datetime.now(VN_TZ)
    now = started.strftime("%Y-%m-%d %H:%M:%S")
    run_id = f"fetch-news-{started:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    os.makedirs(RAW_DIR, exist_ok=True)
    bronze.init_bronze()
    con = bronze.connect()
    summary = {"run_id": run_id, "tickers": {}, "errors": {}}
    try:
        bronze.log_start(con, run_id, "fetch-news")
        tickers = [ma for ma, _ in watchlist.active_tickers(con)]
        total_in = total_new = 0
        for i, ma in enumerate(tickers):
            if i:
                time.sleep(delay_s)
            try:
                items = cafef.parse_news(get_html(ma))
            except Exception as e:
                summary["errors"][ma] = str(e)
                continue
            new = 0
            for it in items:
                if bronze.insert_raw(
                        con, run_id, "cafef", "news", ma,
                        json.dumps(it, ensure_ascii=False), sha(it["url"]), now, now[:10],
                        request_params=json.dumps({"page": "event.chn"}), url=it["url"],
                        published_at=it["published_at"], adapter_version=ADAPTER_VERSION):
                    new += 1
            con.commit()
            summary["tickers"][ma] = {"fetched": len(items), "new": new}
            total_in += len(items)
            total_new += new
            fn = os.path.join(RAW_DIR, f"cafef_news_{ma}_{now[:10]}.json")
            with open(fn, "w", encoding="utf-8") as f:
                json.dump({"ma": ma, "fetched_at": now, "items": items}, f, ensure_ascii=False, indent=2)
            summary["tickers"][ma]["file"] = os.path.basename(fn)
        status = "error" if summary["errors"] and not summary["tickers"] else "ok"
        bronze.log_end(con, run_id, status, total_in, total_new,
                       json.dumps(summary["errors"], ensure_ascii=False) if summary["errors"] else None)
        summary["status"] = status
        return summary
    finally:
        con.close()
