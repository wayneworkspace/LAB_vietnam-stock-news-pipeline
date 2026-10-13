"""Small HTTP API for n8n. Every pipeline step is an endpoint, and /run-pipeline runs them all in one call.
Add new endpoints (scraping, prices...) here.

Every endpoint that touches a database runs under a single non-blocking lock (serving/locking.py):
an overlapping call gets 409 with Retry-After instead of corrupting or crashing a running step.
The /debug/* endpoints exist only when ENABLE_DEBUG_ENDPOINTS=true."""
import importlib
import threading
from typing import Callable, Optional

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import config
from ingestion import fetch_news as ingest_news
from ingestion import fetch_prices as ingest_prices
from serving import digest, locking, pipeline
from serving.locking import PipelineBusy, PipelineLock
from source import cafef, cafef_price
from storage import bronze
from storage import watchlist as wl
from transform import enrich, gold, silver

router = APIRouter()
debug_router = APIRouter(prefix="/debug", tags=["debug"])

LIBS = ["duckdb", "pandas", "pyarrow",
        "anthropic", "requests", "dotenv"]


def build_steps(**kwargs) -> list:
    """Steps for /run-pipeline. A module-level function so tests can swap in fake steps."""
    return pipeline.default_steps(**kwargs)


def _busy(e: PipelineBusy) -> HTTPException:
    return HTTPException(409, detail={"error": "pipeline busy", "running": e.holder},
                         headers={"Retry-After": "30"})


def _locked(step: str, fn: Callable, *args, **kwargs):
    """Run fn under the pipeline lock; map 'someone else is running' to a 409."""
    try:
        with locking.pipeline_lock(step):
            return fn(*args, **kwargs)
    except PipelineBusy as e:
        raise _busy(e)


def _or_409(fn: Callable):
    """Turn a RuntimeError ('the previous layer has not been built yet') into a 409."""
    try:
        return fn()
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/check")
def check():
    result = {}
    for name in LIBS:
        try:
            m = importlib.import_module(name)
            result[name] = getattr(m, "__version__", "ok")
        except Exception as e:
            result[name] = f"ERROR: {e}"
    return result


@router.post("/init-db")
def init_db():
    """Create the Bronze tables if missing. Safe to call repeatedly. Silver and Gold are created by /build-silver and /build-gold."""
    return {"bronze": bronze.init_bronze()}


class Ticker(BaseModel):
    ma: str
    ten: Optional[str] = None
    san: Optional[str] = None
    active: bool = True
    ghi_chu: Optional[str] = None


@router.post("/watchlist")
def watchlist(t: Ticker):
    """Add or update a ticker in the watchlist table (at most 10 active tickers)."""
    try:
        rows = wl.upsert_watchlist(**t.model_dump())
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"watchlist": rows}


@router.post("/fetch-news")
def fetch_news():
    """Fetch CafeF news for the active watchlist tickers into Bronze (raw_data + run_log)."""
    return _locked("fetch-news", ingest_news.run_fetch_news)


@router.post("/fetch-prices")
def fetch_prices(days: int = 30, full: bool = False):
    """Fetch daily prices into Bronze. Incremental per ticker: only the gap since the last stored trade
    date plus a lookback window. `days` is the window for a first load; full=true forces a full refresh."""
    return _locked("fetch-prices", ingest_prices.run_fetch_prices, days=days, full=full)


@router.post("/enrich-news")
def enrich_news(limit: int = enrich.MAX_PER_RUN):
    """Summarise news and score sentiment (-2..+2) with Claude Haiku 4.5; results are written to Bronze."""
    return _locked("enrich-news", enrich.run_enrich, limit=limit)


@router.post("/build-silver")
def build_silver():
    """Rebuild Silver (prices_daily, news, news_enriched) from Bronze. Safe to re-run."""
    return _locked("build-silver", silver.build_silver)


@router.post("/build-gold")
def build_gold():
    """Rebuild Gold (dim_ticker, dim_date, fact_price_daily, fact_news) from Silver and export Parquet."""
    return _locked("build-gold", lambda: _or_409(gold.build_gold))


@router.post("/digest")
def make_digest():
    """Build the morning digest text (under 2000 characters) from Gold for n8n to post to Discord."""
    return _locked("digest", lambda: _or_409(lambda: {"text": digest.build_digest()}))


def _run_in_background(lock: PipelineLock, steps: list, run_id: str) -> None:
    try:
        pipeline.run_pipeline(steps, run_id)
    finally:
        lock.release()


@router.post("/run-pipeline")
def run_pipeline(days: int = 30, full_prices: bool = False, wait: bool = True):
    """Run every step in order (fetch -> enrich -> Silver -> Gold -> digest) under one lock.

    wait=true (default): blocks until done and returns the final state; the digest is in `text`.
    The HTTP status is 500 when the run failed, so an n8n HTTP node fails and can trigger error handling.
    wait=false: starts in the background and returns 202 at once; poll GET /run-pipeline/status."""
    steps = build_steps(days=days, full_prices=full_prices)
    run_id = pipeline.make_run_id()
    lock = PipelineLock("run-pipeline")
    try:
        lock.acquire()
    except PipelineBusy as e:
        raise _busy(e)
    if not wait:
        try:
            threading.Thread(target=_run_in_background, args=(lock, steps, run_id), daemon=True).start()
        except BaseException:
            lock.release()
            raise
        return JSONResponse(status_code=202, content={"run_id": run_id, "status": "started",
                                                      "status_url": "/run-pipeline/status"})
    try:
        state = pipeline.run_pipeline(steps, run_id)
    finally:
        lock.release()
    return JSONResponse(status_code=500 if state["status"] == "failed" else 200, content=state)


@router.get("/run-pipeline/status")
def run_pipeline_status():
    """State of the latest /run-pipeline run (per-step status and timings), plus whether a step is running now."""
    state = pipeline.read_status()
    state["busy"] = locking.running_step()
    return state


@debug_router.post("/cafef-html")
def debug_cafef_html(ma: str = "FPT"):
    """Save the CafeF news page HTML of one ticker to data/debug, to write or fix the news parser."""
    try:
        return cafef.save_debug_html(ma)
    except Exception as e:
        raise HTTPException(502, f"Could not fetch the CafeF page: {e}")


@debug_router.post("/cafef-price")
def debug_cafef_price(ma: str = "FPT", exchange: str = "HOSE"):
    """Save the raw CafeF price JSON to data/debug, to write or fix the price parser."""
    try:
        return cafef_price.save_debug_json(ma, exchange)
    except Exception as e:
        raise HTTPException(502, f"Could not fetch CafeF prices: {e}")


def create_app(enable_debug: Optional[bool] = None) -> FastAPI:
    """Build the app. enable_debug=None reads ENABLE_DEBUG_ENDPOINTS from the environment."""
    app = FastAPI(title="Stock pyworker")
    app.include_router(router)
    if config.ENABLE_DEBUG_ENDPOINTS if enable_debug is None else enable_debug:
        app.include_router(debug_router)
    return app


app = create_app()
