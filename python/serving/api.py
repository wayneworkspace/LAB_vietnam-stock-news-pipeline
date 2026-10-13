"""Small API for n8n to call. Add new endpoints (scraping, prices...) here.
Every endpoint that touches a database runs under one non-blocking lock: an overlapping call gets 409.
The /debug/* endpoints exist only when ENABLE_DEBUG_ENDPOINTS=true."""
import importlib
import logging
import time
import uuid
from typing import Callable, Optional

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import config
from ingestion import fetch_news as ingest_news
from ingestion import fetch_prices as ingest_prices
from source import cafef, cafef_price
from storage import backup, bronze
from storage import watchlist as wl
from serving import digest
from serving.locking import PipelineBusy, pipeline_lock
from transform import enrich, gold, silver

app = FastAPI(title="Stock pyworker")
log = logging.getLogger("pipeline")
debug_router = APIRouter(prefix="/debug")

LIBS = ["duckdb", "pandas", "pyarrow",
        "anthropic", "requests", "dotenv"]


def _locked(fn: Callable, *args, **kwargs):
    """Run fn under the pipeline lock; a second caller gets 409 instead of waiting."""
    try:
        with pipeline_lock():
            return fn(*args, **kwargs)
    except PipelineBusy as e:
        raise HTTPException(409, str(e), headers={"Retry-After": "30"})


def _layer_built(fn: Callable):
    """The previous layer has not been built yet -> 409 (as before)."""
    try:
        return fn()
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/check")
def check():
    result = {}
    for name in LIBS:
        try:
            m = importlib.import_module(name)
            result[name] = getattr(m, "__version__", "ok")
        except Exception as e:
            result[name] = f"ERROR: {e}"
    return result


@app.post("/init-db")
def init_db():
    """Create the Bronze tables if missing. Safe to call repeatedly. Silver and Gold are created by /build-silver, /build-gold."""
    return {"bronze": bronze.init_bronze()}


class Ticker(BaseModel):
    ma: str
    ten: Optional[str] = None
    san: Optional[str] = None
    active: bool = True
    ghi_chu: Optional[str] = None


@app.post("/watchlist")
def watchlist(t: Ticker):
    """Add or update a ticker in the watchlist table (at most 10 active tickers)."""
    try:
        rows = wl.upsert_watchlist(**t.model_dump())
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"watchlist": rows}


@debug_router.post("/cafef-html")
def debug_cafef_html(ma: str = "FPT"):
    """Save the CafeF news page HTML of one ticker to data/debug, to write the news parser."""
    try:
        return cafef.save_debug_html(ma)
    except Exception as e:
        raise HTTPException(502, f"Could not fetch the CafeF page: {e}")


@app.post("/fetch-news")
def fetch_news():
    """Fetch CafeF news for the active watchlist tickers, write raw_data + news + run_log."""
    return _locked(ingest_news.run_fetch_news)


@app.post("/enrich-news")
def enrich_news(limit: int = enrich.MAX_PER_RUN):
    """Read news from Bronze, summarise and score sentiment (-2..+2) with Claude Haiku 4.5, write the results to Bronze."""
    return _locked(enrich.run_enrich, limit=limit)


@debug_router.post("/cafef-price")
def debug_cafef_price(ma: str = "FPT", exchange: str = "HOSE"):
    """Save the raw CafeF price JSON to data/debug, to write the price parser."""
    try:
        return cafef_price.save_debug_json(ma, exchange)
    except Exception as e:
        raise HTTPException(502, f"Could not fetch CafeF prices: {e}")


@app.post("/fetch-prices")
def fetch_prices(days: int = 30, full: bool = False):
    """Fetch price history for the active tickers. Only the days since the last stored price (plus a lookback)
    are fetched; `days` is the window for a first load, and full=true forces a full refresh of that window."""
    return _locked(ingest_prices.run_fetch_prices, days=days, full=full)


@app.post("/build-silver")
def build_silver():
    """Rebuild Silver (prices_daily, news) from Bronze. Safe to re-run."""
    return _locked(silver.build_silver)


@app.post("/build-gold")
def build_gold():
    """Rebuild Gold (dim_ticker, dim_date, fact_price_daily, fact_news) from Silver and export it to Parquet."""
    return _locked(lambda: _layer_built(gold.build_gold))


@app.post("/digest")
def make_digest():
    """Build the morning digest text (under 2000 characters) from Gold for n8n to post to Discord."""
    return _locked(lambda: _layer_built(lambda: {"text": digest.build_digest()}))


@app.post("/backup")
def backup_bronze():
    """Copy the Bronze database to data/backups/ (daily file, newest 7 kept)."""
    return _locked(lambda: _layer_built(backup.backup_bronze))


def _pipeline_steps(days: int, full_prices: bool) -> list:
    return [("fetch-news", ingest_news.run_fetch_news),
            ("fetch-prices", lambda: ingest_prices.run_fetch_prices(days=days, full=full_prices)),
            ("enrich-news", enrich.run_enrich),
            ("build-silver", silver.build_silver),
            ("build-gold", gold.build_gold),
            ("digest", lambda: {"text": digest.build_digest()})]


@app.post("/run-pipeline")
def run_pipeline(days: int = 30, full_prices: bool = False):
    """Run fetch -> enrich -> Silver -> Gold -> digest in order, under one lock.
    Stops at the first step that fails (later steps are "skipped") and answers 500, so the n8n node fails.
    The response lists every step with its status; the digest text is in `text`.
    Every step is logged with the run_id (see `docker compose logs pyworker`)."""
    steps = []
    run_id = uuid.uuid4().hex[:8]
    state = {"run_id": run_id, "status": "ok", "steps": steps}
    failed = False
    try:
        with pipeline_lock():
            for name, fn in _pipeline_steps(days, full_prices):
                if failed:
                    log.warning("run=%s step=%s status=skipped", run_id, name)
                    steps.append({"name": name, "status": "skipped"})
                    continue
                began = time.monotonic()
                entry = {"name": name, "status": "ok"}
                try:
                    result = fn()
                    if isinstance(result, dict) and result.get("status") == "error":
                        raise RuntimeError("the step reported status=error")
                    if isinstance(result, dict) and "text" in result:
                        state["text"] = result["text"]
                except Exception as e:
                    entry.update(status="failed", error=f"{type(e).__name__}: {e}")
                    failed = True
                    log.exception("run=%s step=%s status=failed", run_id, name)
                entry["duration_s"] = round(time.monotonic() - began, 2)
                if entry["status"] == "ok":
                    log.info("run=%s step=%s status=ok duration=%.2fs", run_id, name, entry["duration_s"])
                steps.append(entry)
    except PipelineBusy as e:
        raise HTTPException(409, str(e), headers={"Retry-After": "30"})
    if failed:
        state["status"] = "failed"
    log.info("run=%s finished status=%s", run_id, state["status"])
    return JSONResponse(status_code=500 if failed else 200, content=state)


if config.ENABLE_DEBUG_ENDPOINTS:
    app.include_router(debug_router)
