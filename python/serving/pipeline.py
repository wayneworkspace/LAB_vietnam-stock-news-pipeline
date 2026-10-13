"""One-call pipeline runner: fetch news -> fetch prices -> enrich -> Silver -> Gold -> digest,
with a status record that can be polled while the run is in progress.

Failure rules:
- A required step that fails (raises, or returns status "error") stops the run; later steps are "skipped".
- An optional step (enrich-news) that fails is reported as a "warning" and the run continues, because
  the digest is still useful without fresh sentiment scores.

Status values. Run: running | ok | ok_with_warnings | failed. Step: pending | running | ok | warning | failed | skipped.
The status is also written to PIPELINE_STATUS_FILE after every step, and the run goes into run_log.
"""
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

from config import PIPELINE_STATUS_FILE, VN_TZ
from storage import bronze


@dataclass
class Step:
    name: str
    fn: Callable[[], dict]
    optional: bool = False     # a failure is reported but does not stop the run


def make_run_id() -> str:
    return f"pipeline-{datetime.now(VN_TZ):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"


def default_steps(days: int = 30, full_prices: bool = False) -> list:
    # Imported here so that importing this module does not load the whole stack.
    from ingestion import fetch_news, fetch_prices
    from serving import digest
    from transform import enrich, gold, silver
    return [
        Step("fetch-news", fetch_news.run_fetch_news),
        Step("fetch-prices", lambda: fetch_prices.run_fetch_prices(days=days, full=full_prices)),
        Step("enrich-news", enrich.run_enrich, optional=True),
        Step("build-silver", silver.build_silver),
        Step("build-gold", gold.build_gold),
        Step("digest", lambda: {"text": digest.build_digest()}),
    ]


def _now() -> str:
    return datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _write_status(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def read_status(path: Optional[str] = None) -> dict:
    """Status of the latest run, or {"status": "never_run"}."""
    try:
        with open(path or PIPELINE_STATUS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {"status": "never_run"}


def run_pipeline(steps: list, run_id: Optional[str] = None, status_path: Optional[str] = None) -> dict:
    """Run the steps in order and return the final state. The digest text, when produced, is at state["text"]."""
    run_id = run_id or make_run_id()
    path = status_path or PIPELINE_STATUS_FILE
    state = {"run_id": run_id, "status": "running", "started_at": _now(), "finished_at": None,
             "current_step": None,
             "steps": [{"name": s.name, "optional": s.optional, "status": "pending"} for s in steps]}
    _write_status(path, state)
    bronze.init_bronze()
    log = bronze.connect()
    try:
        bronze.log_start(log, run_id, "run-pipeline")
        stopped = False
        for entry, step in zip(state["steps"], steps):
            if stopped:
                entry["status"] = "skipped"
                continue
            state["current_step"] = step.name
            entry.update(status="running", started_at=_now())
            _write_status(path, state)
            began = time.monotonic()
            failed, result = False, None
            try:
                result = step.fn()
                if isinstance(result, dict) and result.get("status") == "error":
                    failed = True
                    entry["error"] = "the step reported status=error"
            except Exception as e:
                failed = True
                entry["error"] = f"{type(e).__name__}: {e}"
            entry["duration_s"] = round(time.monotonic() - began, 2)
            if isinstance(result, dict):
                if "text" in result:
                    state["text"] = result["text"]
                    entry["summary"] = {"chars": len(result["text"])}
                else:
                    entry["summary"] = result
            entry["status"] = ("warning" if step.optional else "failed") if failed else "ok"
            if failed and not step.optional:
                stopped = True
            _write_status(path, state)
        state["current_step"] = None
        if stopped:
            state["status"] = "failed"
        elif any(e["status"] == "warning" for e in state["steps"]):
            state["status"] = "ok_with_warnings"
        else:
            state["status"] = "ok"
        state["finished_at"] = _now()
        _write_status(path, state)
        problems = [f"{e['name']}: {e.get('error', e['status'])}" for e in state["steps"]
                    if e["status"] in ("failed", "warning")]
        bronze.log_end(log, run_id, "error" if state["status"] == "failed" else "ok",
                       len(steps), sum(e["status"] == "ok" for e in state["steps"]),
                       "; ".join(problems) or None)
        return state
    finally:
        if state["status"] == "running":     # something unexpected escaped; do not leave "running" behind
            state["status"], state["finished_at"] = "failed", _now()
            _write_status(path, state)
        log.close()
