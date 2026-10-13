"""Transform: summarise news and score sentiment from -2 to +2 with Claude Haiku 4.5.
Reads news from Bronze (raw_data) and writes the results to Bronze (news_enriched).
Only the news HEADLINE is used (article bodies are not read). Re-running skips news that already has
a result for the same PROMPT_VERSION."""
import json
import os
import re
import uuid
from datetime import datetime

from config import VN_TZ
from storage import bronze
from transform.prompts import PROMPT_VERSION, SYSTEM

MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
MAX_PER_RUN = int(os.getenv("ENRICH_MAX_PER_RUN", "40"))   # caps the cost of a single run
BATCH = 20
# Estimated USD per 1M tokens. NOT VERIFIED: check against Anthropic's pricing page.
PRICE_IN, PRICE_OUT = 1.0, 5.0


def _parse(text: str) -> list:
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    data = json.loads(text)
    return data if isinstance(data, list) else []


def _title(payload: str) -> str:
    return " ".join(str(json.loads(payload).get("title", "")).split())


def run_enrich(client=None, limit: int = MAX_PER_RUN) -> dict:
    if client is None:
        import anthropic
        client = anthropic.Anthropic()   # reads ANTHROPIC_API_KEY from the environment
    started = datetime.now(VN_TZ)
    now = started.strftime("%Y-%m-%d %H:%M:%S")
    run_id = f"enrich-{started:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    bronze.init_bronze()
    b = bronze.connect()
    out = {"run_id": run_id, "model": MODEL, "prompt_version": PROMPT_VERSION,
           "done": 0, "skipped_invalid": 0, "pending_left": 0, "errors": []}
    try:
        bronze.log_start(b, run_id, "enrich-news")
        raw = b.execute(
            """SELECT substr(r.payload_hash, 1, 16), r.ma, r.payload FROM raw_data r
               WHERE r.source = 'cafef' AND r.data_type = 'news'
                 AND NOT EXISTS (SELECT 1 FROM news_enriched e
                                 WHERE e.news_id = substr(r.payload_hash, 1, 16)
                                   AND e.ma = r.ma AND e.prompt_version = ?)
               ORDER BY r.published_at DESC""", [PROMPT_VERSION]).fetchall()
        rows = [(r[0], r[1], _title(r[2])) for r in raw]
        out["pending_left"] = max(0, len(rows) - limit)
        rows = rows[:limit]
        tin = tout = 0
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            by_id = {f"{r[1]}:{r[0]}": r for r in chunk}
            user = "Các tin cần xử lý (mỗi dòng: id | mã | tiêu đề):\n" + "\n".join(
                f"{k} | {r[1]} | {r[2]}" for k, r in by_id.items())
            try:
                resp = client.messages.create(
                    model=MODEL, max_tokens=2000, system=SYSTEM,
                    messages=[{"role": "user", "content": user}])
                tin += resp.usage.input_tokens
                tout += resp.usage.output_tokens
                results = _parse(resp.content[0].text)
            except Exception as e:
                out["errors"].append(f"batch {i // BATCH + 1}: {e}")
                continue
            for item in results:
                r = by_id.get(str(item.get("id")))
                s = item.get("sentiment")
                if r is None or not isinstance(s, int) or isinstance(s, bool) or not -2 <= s <= 2:
                    out["skipped_invalid"] += 1
                    continue
                b.execute(
                    """INSERT OR IGNORE INTO news_enriched (news_id, ma, summary, sentiment, model,
                           prompt_version, enriched_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [r[0], r[1], str(item.get("summary", ""))[:500], s, MODEL, PROMPT_VERSION, now])
                out["done"] += 1
            b.commit()
        est = (tin * PRICE_IN + tout * PRICE_OUT) / 1_000_000
        b.execute("INSERT INTO cost_log (run_id, model, input_tokens, output_tokens, est_usd, created_at) "
                  "VALUES (?, ?, ?, ?, ?, ?)", [run_id, MODEL, tin, tout, est, now])
        b.commit()
        out.update(input_tokens=tin, output_tokens=tout, est_usd=round(est, 5))
        status = "error" if out["errors"] and out["done"] == 0 else "ok"
        bronze.log_end(b, run_id, status, len(rows), out["done"], "; ".join(out["errors"]) or None)
        out["status"] = status
        return out
    finally:
        b.close()
