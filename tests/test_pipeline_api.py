import time

import pytest
from fastapi.testclient import TestClient

import config
from serving import api, pipeline
from serving.locking import PipelineLock
from serving.pipeline import Step
from storage import bronze


def ok(name, **extra):
    return Step(name, lambda: {"status": "ok", **extra})


def boom(name, optional=False):
    def fail():
        raise RuntimeError("boom")
    return Step(name, fail, optional=optional)


def good_steps():
    return [ok("fetch-news"), ok("build-gold"), Step("digest", lambda: {"text": "digest text"})]


def run_log(run_id):
    con = bronze.connect()
    try:
        return con.execute("SELECT status, rows_in, rows_out, error FROM run_log WHERE run_id = ?",
                           [run_id]).fetchone()
    finally:
        con.close()


# --- Runner -----------------------------------------------------------------------------------

def test_a_clean_run_reports_every_step_and_hoists_the_digest():
    state = pipeline.run_pipeline(good_steps(), run_id="r1")
    assert state["status"] == "ok"
    assert [s["status"] for s in state["steps"]] == ["ok", "ok", "ok"]
    assert state["text"] == "digest text"
    assert state["steps"][2]["summary"] == {"chars": 11}
    assert state["current_step"] is None and state["finished_at"]
    assert pipeline.read_status()["run_id"] == "r1"
    assert run_log("r1") == ("ok", 3, 3, None)


def test_a_failing_optional_step_is_a_warning_and_the_run_continues():
    steps = [ok("fetch-news"), boom("enrich-news", optional=True), Step("digest", lambda: {"text": "d"})]
    state = pipeline.run_pipeline(steps, run_id="r2")
    assert state["status"] == "ok_with_warnings"
    assert [s["status"] for s in state["steps"]] == ["ok", "warning", "ok"]
    assert "boom" in state["steps"][1]["error"]
    assert state["text"] == "d"
    assert run_log("r2")[0] == "ok"


def test_a_failing_required_step_stops_the_run_and_skips_the_rest():
    state = pipeline.run_pipeline([ok("fetch-news"), boom("build-silver"), ok("build-gold")], run_id="r3")
    assert state["status"] == "failed"
    assert [s["status"] for s in state["steps"]] == ["ok", "failed", "skipped"]
    assert "text" not in state
    status, rows_in, rows_out, error = run_log("r3")
    assert (status, rows_in, rows_out) == ("error", 3, 1)
    assert "build-silver" in error


def test_a_step_that_returns_status_error_counts_as_failed():
    state = pipeline.run_pipeline([Step("fetch-news", lambda: {"status": "error"}), ok("build-gold")], run_id="r4")
    assert state["status"] == "failed"
    assert state["steps"][1]["status"] == "skipped"


def test_status_is_readable_while_the_run_is_in_progress():
    seen = {}

    def peek():
        seen.update(pipeline.read_status())
        return {"status": "ok"}

    pipeline.run_pipeline([ok("first"), Step("second", peek)], run_id="r5")
    assert seen["status"] == "running" and seen["current_step"] == "second"
    assert [s["status"] for s in seen["steps"]] == ["ok", "running"]


def test_status_before_any_run():
    assert pipeline.read_status() == {"status": "never_run"}


def test_default_steps_order_and_which_one_is_optional():
    steps = pipeline.default_steps()
    assert [s.name for s in steps] == ["fetch-news", "fetch-prices", "enrich-news", "build-silver",
                                       "build-gold", "digest"]
    assert [s.name for s in steps if s.optional] == ["enrich-news"]


# --- /run-pipeline ----------------------------------------------------------------------------

@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api, "build_steps", lambda **kw: good_steps())
    return TestClient(api.create_app(enable_debug=False))


def test_run_pipeline_returns_the_digest_as_text_for_the_discord_node(client):
    r = client.post("/run-pipeline")
    assert r.status_code == 200
    assert r.json()["text"] == "digest text" and r.json()["status"] == "ok"


def test_a_failed_run_is_an_http_500_so_that_n8n_can_react(client, monkeypatch):
    monkeypatch.setattr(api, "build_steps", lambda **kw: [boom("fetch-news")])
    r = client.post("/run-pipeline")
    assert r.status_code == 500
    assert r.json()["status"] == "failed"


def test_options_are_passed_through_to_the_step_builder(monkeypatch):
    seen = {}
    monkeypatch.setattr(api, "build_steps", lambda **kw: seen.update(kw) or good_steps())
    TestClient(api.create_app(False)).post("/run-pipeline?days=7&full_prices=true")
    assert seen == {"days": 7, "full_prices": True}


def test_run_pipeline_in_the_background_returns_202_and_can_be_polled(monkeypatch):
    monkeypatch.setattr(api, "build_steps", lambda **kw: [Step("slow", lambda: time.sleep(0.3) or {"status": "ok"}),
                                                          Step("digest", lambda: {"text": "bg"})])
    client = TestClient(api.create_app(False))
    r = client.post("/run-pipeline?wait=false")
    assert r.status_code == 202
    run_id = r.json()["run_id"]
    assert client.get("/run-pipeline/status").json()["busy"]["step"] == "run-pipeline"
    deadline = time.time() + 10
    while time.time() < deadline:
        state = client.get("/run-pipeline/status").json()
        if state.get("status") not in ("running", "never_run"):
            break
        time.sleep(0.05)
    assert state["run_id"] == run_id and state["status"] == "ok" and state["text"] == "bg"
    deadline = time.time() + 5
    while time.time() < deadline and client.get("/run-pipeline/status").json()["busy"]:
        time.sleep(0.05)
    assert client.get("/run-pipeline/status").json()["busy"] is None     # the lock was released by the worker


def test_status_endpoint_before_any_run(client):
    body = client.get("/run-pipeline/status").json()
    assert body["status"] == "never_run" and body["busy"] is None


# --- Overlap protection -----------------------------------------------------------------------

def test_overlapping_calls_get_409_with_the_holder_and_do_not_run(client, monkeypatch):
    monkeypatch.setattr(api.gold, "build_gold", lambda: pytest.fail("must not run while the lock is held"))
    with PipelineLock("manual-run"):
        for path in ["/build-gold", "/build-silver", "/fetch-news", "/digest", "/run-pipeline"]:
            r = client.post(path)
            assert r.status_code == 409, path
            assert r.json()["detail"]["running"]["step"] == "manual-run"
            assert r.headers["retry-after"] == "30"
        assert client.post("/health").status_code == 405      # sanity: unrelated routes are unaffected
        assert client.get("/health").status_code == 200       # health never takes the lock
    assert client.post("/run-pipeline").status_code == 200


def test_step_endpoints_forward_their_options_under_the_lock(client, monkeypatch):
    seen = {}
    monkeypatch.setattr(api.ingest_prices, "run_fetch_prices", lambda **kw: seen.update(kw) or {"status": "ok"})
    assert client.post("/fetch-prices?days=7&full=true").json() == {"status": "ok"}
    assert seen == {"days": 7, "full": True}


def test_missing_previous_layer_is_still_a_409_with_a_hint(client):
    r = client.post("/build-gold")
    assert r.status_code == 409 and "build-silver" in r.json()["detail"]
    r = client.post("/digest")
    assert r.status_code == 409 and "build-gold" in r.json()["detail"]


# --- /debug switch ----------------------------------------------------------------------------

def test_debug_endpoints_are_absent_by_default(monkeypatch):
    assert config.ENABLE_DEBUG_ENDPOINTS is False
    client = TestClient(api.create_app())
    assert client.post("/debug/cafef-html").status_code == 404
    assert client.post("/debug/cafef-price").status_code == 404
    assert "/debug/cafef-html" not in client.get("/openapi.json").json()["paths"]


def test_debug_endpoints_appear_when_enabled(monkeypatch):
    monkeypatch.setattr(api.cafef, "save_debug_html", lambda ma: {"file": f"{ma}.html", "bytes": 1})
    client = TestClient(api.create_app(enable_debug=True))
    assert client.post("/debug/cafef-html?ma=VNM").json() == {"file": "VNM.html", "bytes": 1}


def test_debug_switch_follows_the_environment_flag(monkeypatch):
    monkeypatch.setattr(api.config, "ENABLE_DEBUG_ENDPOINTS", True)
    assert "/debug/cafef-price" in TestClient(api.create_app()).get("/openapi.json").json()["paths"]
    assert TestClient(api.create_app(enable_debug=False)).post("/debug/cafef-price").status_code == 404


def test_env_flag_parsing(monkeypatch):
    for value, expected in [("true", True), ("1", True), ("YES", True), ("on", True),
                            ("false", False), ("0", False), ("", False), ("nonsense", False)]:
        monkeypatch.setenv("SOME_FLAG", value)
        assert config.env_flag("SOME_FLAG") is expected
    monkeypatch.delenv("SOME_FLAG")
    assert config.env_flag("SOME_FLAG", True) is True
