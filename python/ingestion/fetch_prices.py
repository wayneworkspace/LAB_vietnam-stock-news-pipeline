"""Ingestion: fetch daily price history for the active tickers and write to Bronze only
(raw_data + run_log + a JSON snapshot per run).

Loads are incremental per ticker. The watermark is the newest trade date already stored in Bronze;
each run re-fetches the gap since then plus a lookback window (PRICE_LOOKBACK_DAYS), so late
corrections inside that window are still captured. A ticker with nothing stored yet, or a run with
full=True, loads the whole `days` window instead.

If the source revises a price, the new version is appended to raw_data (append only, nothing is
edited) and Silver keeps the latest row per (ticker, date).

Caveat: corporate actions (splits, dividends) restate adj_close for OLD dates, which a short
incremental window cannot see. Run with full=true now and then (for example weekly) to refresh history.
"""
import hashlib
import json
import os
import time
import uuid
from datetime import date, datetime
from typing import Optional

from config import PRICE_LOOKBACK_DAYS, RAW_DIR, VN_TZ
from source import cafef_price
from storage import bronze, watchlist

ADAPTER_VERSION = "cafef-price-v1"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def plan_window(last_date: Optional[date], today: date, bootstrap_days: int,
                lookback_days: int, full: bool = False) -> tuple:
    """Decide how many days back to fetch. Returns (window_days, mode).
    mode is "full" (forced), "bootstrap" (nothing stored yet) or "incremental"."""
    if full:
        return bootstrap_days, "full"
    if last_date is None:
        return bootstrap_days, "bootstrap"
    gap_days = max((today - last_date).days, 0)
    return gap_days + lookback_days, "incremental"


def run_fetch_prices(days: int = 30, delay_s: float = 2.0, provider=None,
                     full: bool = False, today: Optional[date] = None) -> dict:
    """`days` is the window for a first load or a full refresh; later runs fetch only what is new."""
    started = datetime.now(VN_TZ)
    now = started.strftime("%Y-%m-%d %H:%M:%S")
    today = today or started.date()
    run_id = f"fetch-prices-{started:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    os.makedirs(RAW_DIR, exist_ok=True)
    bronze.init_bronze()
    con = bronze.connect()
    summary = {"run_id": run_id, "tickers": {}, "errors": {}}
    try:
        bronze.log_start(con, run_id, "fetch-prices")
        tickers = watchlist.active_tickers(con)
        total_in = total_new = 0
        for i, (ma, san) in enumerate(tickers):
            if i:
                time.sleep(delay_s)
            try:
                last = bronze.latest_price_date(con, ma)
                window, mode = plan_window(last, today, days, PRICE_LOOKBACK_DAYS, full)
                records = cafef_price.fetch_all_records(ma, san, window, provider=provider,
                                                        delay_s=min(delay_s, 1.0))
            except Exception as e:
                summary["errors"][ma] = str(e)
                continue
            rows = cafef_price.parse_prices(records)
            new = 0
            for p in rows:
                payload = json.dumps(p["raw"], ensure_ascii=False, sort_keys=True)
                if bronze.insert_raw(
                        con, run_id, "cafef", "price", ma, payload,
                        _sha(f"{ma}|{p['trade_date']}|{payload}"), now, now[:10],
                        request_params=json.dumps({"days": window, "exchange": san, "mode": mode}),
                        adapter_version=ADAPTER_VERSION):
                    new += 1
            con.commit()
            summary["tickers"][ma] = {"fetched": len(rows), "new": new, "mode": mode,
                                      "window_days": window,
                                      "watermark": last.isoformat() if last else None}
            total_in += len(rows)
            total_new += new
            # One snapshot per run: an incremental run must not overwrite the file of an earlier run today.
            fn = os.path.join(RAW_DIR, f"cafef_price_{ma}_{now[:10]}_{now[11:].replace(':', '')}.json")
            with open(fn, "w", encoding="utf-8") as f:
                json.dump({"ma": ma, "fetched_at": now, "mode": mode, "records": records},
                          f, ensure_ascii=False, indent=2)
            summary["tickers"][ma]["file"] = os.path.basename(fn)
        status = "error" if summary["errors"] and not summary["tickers"] else "ok"
        bronze.log_end(con, run_id, status, total_in, total_new,
                       json.dumps(summary["errors"], ensure_ascii=False) if summary["errors"] else None)
        summary["status"] = status
        return summary
    finally:
        con.close()
