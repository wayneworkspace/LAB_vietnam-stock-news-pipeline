import importlib

from fastapi.testclient import TestClient

import config
from serving import api
from serving.locking import pipeline_lock


def steps(*items):
    return lambda days, full_prices: list(items)


def ok(name, **result):
    return name, lambda: {"status": "ok", **result}


def boom(name):
    def fail():
        raise RuntimeError("boom")
    return name, fail


def test_run_pipeline_reports_every_step_and_returns_the_digest(monkeypatch):
    monkeypatch.setattr(api, "_pipeline_steps", steps(ok("fetch-news"), ("digest", lambda: {"text": "hello"})))
    r = TestClient(api.app).post("/run-pipeline")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["text"] == "hello"          # n8n reads {{ $json.text }}
    assert [s["status"] for s in body["steps"]] == ["ok", "ok"]


def test_a_failing_step_stops_the_run_and_answers_500(monkeypatch):
    monkeypatch.setattr(api, "_pipeline_steps", steps(ok("fetch-news"), boom("build-silver"), ok("build-gold")))
    r = TestClient(api.app).post("/run-pipeline")
    assert r.status_code == 500
    body = r.json()
    assert body["status"] == "failed" and "text" not in body
    assert [s["status"] for s in body["steps"]] == ["ok", "failed", "skipped"]
    assert "boom" in body["steps"][1]["error"]


def test_a_step_returning_status_error_counts_as_failed(monkeypatch):
    monkeypatch.setattr(api, "_pipeline_steps", steps(("fetch-news", lambda: {"status": "error"}), ok("build-gold")))
    r = TestClient(api.app).post("/run-pipeline")
    assert r.status_code == 500 and r.json()["steps"][1]["status"] == "skipped"


def test_overlapping_calls_get_409_and_the_lock_is_freed_afterwards(monkeypatch):
    monkeypatch.setattr(api, "_pipeline_steps", steps(ok("fetch-news")))
    client = TestClient(api.app)
    with pipeline_lock():
        for call in (lambda: client.post("/run-pipeline"), lambda: client.post("/build-silver"),
                     lambda: client.post("/digest")):
            r = call()
            assert r.status_code == 409 and r.headers["retry-after"] == "30"
    assert client.post("/run-pipeline").status_code == 200


def test_the_lock_is_released_when_a_step_fails(monkeypatch):
    monkeypatch.setattr(api, "_pipeline_steps", steps(boom("fetch-news")))
    client = TestClient(api.app)
    assert client.post("/run-pipeline").status_code == 500
    with pipeline_lock():
        pass


def test_debug_endpoints_exist_only_when_enabled(monkeypatch):
    try:
        assert TestClient(api.app).post("/debug/cafef-html").status_code == 404       # default: off
        monkeypatch.setenv("ENABLE_DEBUG_ENDPOINTS", "true")
        importlib.reload(config)
        importlib.reload(api)
        monkeypatch.setattr(api.cafef, "save_debug_html", lambda ma: {"saved": ma})
        assert TestClient(api.app).post("/debug/cafef-html").json() == {"saved": "FPT"}
    finally:
        monkeypatch.undo()
        importlib.reload(config)
        importlib.reload(api)


def test_every_step_is_logged_with_the_run_id_and_failures_keep_their_traceback(monkeypatch, caplog):
    monkeypatch.setattr(api, "_pipeline_steps", steps(ok("fetch-news"), boom("build-silver"), ok("build-gold")))
    with caplog.at_level("INFO", logger="pipeline"):
        body = TestClient(api.app).post("/run-pipeline").json()
    run_id = body["run_id"]
    lines = [r.getMessage() for r in caplog.records if r.name == "pipeline"]
    assert all(run_id in line for line in lines)
    assert any("step=fetch-news status=ok" in line for line in lines)
    assert any("step=build-silver status=failed" in line for line in lines)
    assert any("step=build-gold status=skipped" in line for line in lines)
    assert any(r.exc_info for r in caplog.records)          # the traceback is in the log, not only the error text
    assert any("finished status=failed" in line for line in lines)
