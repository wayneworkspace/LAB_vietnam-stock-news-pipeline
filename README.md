# VN Stock Morning Digest Pipeline

A small, idempotent data pipeline that collects news and daily prices for Vietnamese stocks from CafeF, keeps every raw response in a Bronze layer, cleans and models it into Silver and Gold layers, scores news sentiment with Claude, and posts a morning digest to Discord.

## Tech Stack

| Category | Technologies |
|---|---|
| **Programming** | <img src="assets/icon-python.png" alt="Python" height="24" /> Python &nbsp;•&nbsp; <img src="assets/icon-sql.png" alt="SQL" height="24" /> SQL |
| **Data Storage** | <img src="assets/icon-sqlite.png" alt="SQLite" height="24" /> SQLite (Bronze) &nbsp;•&nbsp; <img src="assets/icon-duckdb.png" alt="DuckDB" height="24" /> DuckDB (Silver, Gold) &nbsp;•&nbsp; Parquet (Gold export) |
| **Data Processing** | <img src="assets/icon-pandas.png" alt="pandas" height="24" /> pandas &nbsp;•&nbsp; Requests &nbsp;•&nbsp; BeautifulSoup (lxml) |
| **API** | <img src="assets/icon-fastapi.png" alt="FastAPI" height="24" /> FastAPI (Uvicorn) |
| **ETL & Orchestration** | <img src="assets/icon-n8n.png" alt="n8n" height="24" /> n8n (cron schedule) |
| **Data Modeling** | Medallion (Bronze / Silver / Gold) &nbsp;•&nbsp; Star Schema &nbsp;•&nbsp; OLTP + OLAP &nbsp;•&nbsp; Idempotent rebuilds &nbsp;•&nbsp; Incremental loads |
| **DevOps** | <img src="assets/icon-docker.png" alt="Docker" height="24" /> Docker Compose &nbsp;•&nbsp; pytest |
| **Automation & AI** | <img src="assets/icon-anthropic.png" alt="Anthropic" height="24" /> Anthropic API (Claude Haiku 4.5) &nbsp;•&nbsp; <img src="assets/icon-discord.png" alt="Discord" height="24" /> Discord webhook |

> **Reference only, not investment advice.** Prices come from a single source (CafeF) and are not cross-checked. Sentiment is scored by an AI model from headlines only. This is a learning project.

---

## Overview

**Problem** — A beginner investor wants a short, factual summary of overnight news and the latest prices for a few tickers, and also wants to learn how a real data pipeline works. Doing this by hand every morning is repetitive, and ad-hoc scripts tend to create duplicate rows, lose raw data, and break silently.

**Solution** — A five-layer pipeline with a medallion data model. Raw responses are stored append-only in Bronze. Silver and Gold are rebuilt from scratch on every run, so re-running never creates duplicates. n8n triggers the run on a schedule, and a small FastAPI worker does the actual work. The design philosophy: *"Source knows the website, Storage knows the database, and nothing else knows both."*

**Data Flow** — n8n cron (07:30, Mon–Fri) → `POST /run-pipeline` → Discord webhook. Behind that one call the worker runs, in order: fetch news → fetch prices (incremental) → enrich news → build Silver → build Gold (+ Parquet export) → build the digest. Every step is also available as its own endpoint.

<p align="center">
  <img src="assets/Architecture.png" alt="Architecture: CafeF to Source, Ingestion, Bronze, Silver, Gold, Digest and Discord, orchestrated by n8n" width="900" />
</p>

---

## Architecture

Source → Ingestion → Storage → Transform → Serving

| Layer | Responsibility | Files |
|---|---|---|
| **Source** | Call CafeF and parse HTML/JSON. No database access. | `python/source/cafef.py`, `cafef_price.py` |
| **Ingestion** | Loop over active tickers, call Source, write raw rows to Bronze, log the run. | `python/ingestion/fetch_news.py`, `fetch_prices.py` |
| **Storage** | Bronze schema (SQLite) and watchlist. | `python/storage/bronze.py`, `watchlist.py` |
| **Transform** | Silver cleaning, Gold star schema, trading calendar, AI enrichment, prompts. | `python/transform/silver.py`, `gold.py`, `trading_calendar.py`, `enrich.py`, `prompts.py` |
| **Serving** | HTTP endpoints for n8n, the run-overlap lock, the one-call pipeline runner and the digest text builder. | `python/serving/api.py`, `locking.py`, `pipeline.py`, `digest.py` |

### Data layers

| Layer | Engine | Content |
|---|---|---|
| **Bronze** | SQLite (OLTP) | `raw_data` (append-only, unique per source/type/ticker/payload hash), `news_enriched`, `watchlist`, `run_log`, `cost_log` |
| **Silver** | DuckDB (OLAP) | `prices_daily`, `news`, `news_enriched`: typed, deduplicated, validated |
| **Gold** | DuckDB (OLAP) + Parquet | Star schema: `dim_ticker`, `dim_date`, `fact_price_daily`, `fact_news` |

### Key design decisions

| Decision | How it works |
|---|---|
| **Idempotent by construction** | Bronze uses `INSERT OR IGNORE` with a unique key; Silver and Gold use `CREATE OR REPLACE TABLE`. Running any step twice gives the same result. |
| **Raw data is never edited** | If the source revises a price, the changed payload gets a new hash and a new row. Silver keeps the latest row per (ticker, date). |
| **Incremental price loads** | The watermark is the newest trade date stored in Bronze for each ticker. A run fetches the gap since then plus a lookback (`PRICE_LOOKBACK_DAYS`, default 3), so late corrections are caught while most runs request only a few days. A new ticker, or `full=true`, loads the whole window. |
| **Static trading calendar** | `dim_date` covers every day of the year, not just the days that have data. Weekends and the holidays in `python/reference/vn_market_holidays.csv` are marked closed, and `has_price_data` shows whether a price really exists, so a missing price on a trading day looks like a data gap, not a holiday. |
| **Session close** | News published at or after 14:45 (the HOSE closing auction ends then) counts for the next trading session. The next trading day comes from the calendar, so Friday-evening news maps to Monday even before Monday's price exists. |
| **AI results live in Bronze** | Scored sentiment costs money, so it is stored in the most durable layer and Silver is rebuilt from it. |
| **Versioned prompts, explicit promotion** | `PROMPT_VERSION` is what enrichment writes; `ACTIVE_PROMPT_VERSION` is what Gold reads. A new version never leaks half-finished into the digest (see [Rolling out a new prompt](#rolling-out-a-new-prompt)). |
| **One run at a time** | Every step that touches a database takes one non-blocking lock. An overlapping call gets `409` with the name of the running step instead of crashing it (DuckDB cannot open one file read-write and read-only at once). |
| **Cost control** | Only headlines are sent, in batches of 20, at most 40 items per run. Token usage and estimated cost go to `cost_log` (estimate only, check the official price list). |
| **Polite scraping** | One request per ticker per day, 2-second delay, an honest User-Agent. `robots.txt` was checked (allowed on 2026-10-07). |

---

## Project Structure

```text
.
├── README.md
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── requirements-dev.txt      # + pytest, httpx
├── pytest.ini
├── .env.example
├── .gitignore
├── tests/
└── python/
    ├── config.py
    ├── server.py
    ├── reference/       # vn_market_holidays.csv
    ├── source/          # cafef.py, cafef_price.py
    ├── ingestion/       # fetch_news.py, fetch_prices.py
    ├── storage/         # bronze.py, watchlist.py
    ├── transform/       # silver.py, gold.py, trading_calendar.py, enrich.py, prompts.py
    └── serving/         # api.py, locking.py, pipeline.py, digest.py
```

**Language convention:** code, comments, error messages and docs are in English. Vietnamese is kept only where it is product content or data: the Discord digest text, the LLM prompt, and identifiers such as the `ma` / `san` columns and the `cong_bo` / `bai_bao` news types.

---

## Quick Start

### Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop | Tested on Windows |
| Anthropic API key | Put it in `.env` |
| Discord webhook URL | Create one in your channel settings |

### Setup

```powershell
git clone <repo-url>
cd <repo-folder>
copy .env.example .env      # then fill in the keys listed in the file
docker compose up -d --build
```

| Service | Address |
|---|---|
| n8n | http://localhost:5678 |
| Worker API docs | http://localhost:8000/docs |

### First run

```powershell
curl.exe -X POST http://localhost:8000/init-db
curl.exe -X POST http://localhost:8000/watchlist -H "Content-Type: application/json" -d "{\"ma\":\"FPT\",\"san\":\"HOSE\"}"
curl.exe -X POST http://localhost:8000/run-pipeline
```

`/run-pipeline` returns the state of every step and the digest text in `text`. To run one step at a time (useful while debugging), call the individual endpoints in this order: `/fetch-news`, `/fetch-prices?days=30`, `/enrich-news?limit=5`, `/build-silver`, `/build-gold`, `/digest`.

### Schedule it with n8n

Build one small workflow in n8n (the workflow export is not stored in this repository):

| Order | Node | Settings |
|---|---|---|
| 1 | Schedule Trigger | Cron `30 7 * * 1-5`, timezone `Asia/Bangkok` |
| 2 | HTTP Request | `POST http://pyworker:8000/run-pipeline`, timeout `900000` ms |
| 3 | Discord | Webhook credential, content `{{ $json.text }}` |

The worker answers `500` when a required step fails, so the HTTP node fails and the Discord node does not post a stale digest; attach an *Error Trigger* workflow to be alerted. Store the webhook URL only in an n8n credential, never in code or in a committed file.

Prefer not to hold the HTTP connection open? Call `POST /run-pipeline?wait=false` (returns `202` at once) and poll `GET /run-pipeline/status`.

---

## API Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health`, `/check` | Liveness and installed libraries |
| POST | `/init-db` | Create Bronze tables (safe to repeat) |
| POST | `/watchlist` | Add or update a ticker (max 10 active) |
| POST | `/run-pipeline?days=&full_prices=&wait=` | Run every step below in order under one lock; returns per-step status and the digest in `text` |
| GET | `/run-pipeline/status` | Status and timings of the latest run, and which step is running now |
| POST | `/fetch-news` | Scrape CafeF news into Bronze |
| POST | `/fetch-prices?days=&full=` | Fetch daily prices into Bronze: incremental per ticker; `days` is the first-load window, `full=true` forces a full refresh |
| POST | `/enrich-news?limit=` | Summarize and score sentiment (-2..+2) |
| POST | `/build-silver` | Rebuild Silver from Bronze |
| POST | `/build-gold` | Rebuild Gold from Silver and export Parquet |
| POST | `/digest` | Build the digest text (under 1900 characters) |
| POST | `/debug/cafef-html`, `/debug/cafef-price` | Save sample responses for fixing parsers. **Only present when `ENABLE_DEBUG_ENDPOINTS=true`.** |

A `409` can mean two things, told apart by the body: `{"error": "pipeline busy", "running": {...}}` (another step is running, retry later, see the `Retry-After` header) or a message saying the previous layer has not been built yet.

`/run-pipeline` treats `enrich-news` as optional: if it fails (for example a missing API key) the run continues with a warning, because the digest is still useful without fresh scores. Any other failing step stops the run and the remaining steps are reported as `skipped`.

---

## Configuration

| Item | Where | Notes |
|---|---|---|
| API keys | `.env` | `ANTHROPIC_API_KEY`. Never commit `.env`. |
| Optional settings | `.env` | `ENABLE_DEBUG_ENDPOINTS`, `PRICE_LOOKBACK_DAYS`, `MARKET_CLOSE_TIME`, `ACTIVE_PROMPT_VERSION`, `EXPORT_PARQUET`; defaults are listed in `.env.example`. |
| Data paths | `python/config.py` | `DATA_DIR`, `RAW_DIR`, `GOLD_PARQUET_DIR` and the database files. Overridable by environment variables. |
| Model and limits | `python/transform/enrich.py` | `CLAUDE_MODEL`, `ENRICH_MAX_PER_RUN` (default 40), batch size 20. |
| Prompt | `python/transform/prompts.py` | Bump `PROMPT_VERSION` after any edit; promote with `ACTIVE_PROMPT_VERSION`. |
| Close of session | `MARKET_CLOSE_TIME` (default `14:45`) | HOSE rules: the ATC closing auction ends at 14:45 ([source](https://prime.kisvn.vn/support/trading-regulations/hose-transaction-guidance)). HNX and UPCOM are assumed to match; this has not been verified. |
| Holidays | `python/reference/vn_market_holidays.csv` | See [Yearly maintenance](#yearly-maintenance). |
| Tickers | `watchlist` table | Managed through `POST /watchlist`, no code change needed. |

### Rolling out a new prompt

1. Edit `SYSTEM` in `prompts.py` and bump `PROMPT_VERSION` (for example to `v2`). Gold keeps reading `ACTIVE_PROMPT_VERSION`, so the digest does not change.
2. Run `/enrich-news` until `pending_left` is `0`. Old and new scores sit side by side in Bronze, so they can be compared.
3. Set `ACTIVE_PROMPT_VERSION=v2` in `.env`, recreate the container, run `/build-gold`. Check `fact_news_scored` in the response.

### Yearly maintenance

The holiday file lists the weekdays on which the market is closed. Add next year's rows to `python/reference/vn_market_holidays.csv` once the exchange publishes its schedule. Until then `/build-gold` returns `"holiday_calendar_stale": true` and the calendar falls back to "weekends only" for the missing year. Observed prices always win over the calendar, so a wrong holiday entry cannot hide a day that really traded. The 2026 rows follow the government schedule and have not all been confirmed against HOSE notices (see the comments in the file).

### Reading the Parquet export

After `/build-gold`, `data/gold/parquet/` holds one file per table; the two fact tables are partitioned by `date_key`, one folder per day. Any engine can read it, and a filter on the date reads only that day's file:

```sql
SELECT ma, close
FROM read_parquet('data/gold/parquet/fact_price_daily/*/*.parquet', hive_partitioning = true)
WHERE date_key = DATE '2026-10-14';
```

Daily partitions are deliberately fine-grained to demonstrate partition pruning. For years of history, partition by month to avoid thousands of tiny files.

---

## Testing

```powershell
pip install -r requirements-dev.txt
python -m pytest
```

The tests use throwaway SQLite and DuckDB files and a fake price source, so they need no network and no API key. They cover incremental price loads, the trading calendar, Silver's session-close mapping, Gold's prompt pinning and Parquet export, the lock (including a second process), the pipeline runner and the API switches.

---

## Troubleshooting & Limitations

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Cannot reach `pyworker:8000` from the browser | That hostname only exists inside the Docker network. Use `localhost:8000` from Windows; n8n uses `http://pyworker:8000`. |
| Parser returns nothing | CafeF may have changed its markup. Set `ENABLE_DEBUG_ENDPOINTS=true` in `.env`, recreate the container, run `/debug/cafef-html` or `/debug/cafef-price`, inspect the saved file in `data/debug/`, then fix the parser in `python/source/`. Turn the flag off again afterwards. |
| `/debug/...` returns `404` | The debug endpoints are off by default. See the row above. |
| `/build-gold` or `/digest` returns `409` with a message | The previous layer has not been built yet. Run `/build-silver`, then `/build-gold`. |
| Any endpoint returns `409` with `"pipeline busy"` | Another step or a `/run-pipeline` run is in progress. Check `GET /run-pipeline/status` and retry later. The lock is released automatically if the worker restarts. |
| `/run-pipeline` returns `500` | A required step failed. The body shows which one and why; later steps are `skipped`. |
| `/enrich-news` fails to authenticate | `ANTHROPIC_API_KEY` is missing or wrong in `.env`. Fix it and recreate the container. |
| `holiday_calendar_stale: true` | The holiday file has no rows for the current year. See [Yearly maintenance](#yearly-maintenance). |

### Limitations

| Area | Limitation |
|---|---|
| Data source | Prices come only from CafeF and are not verified against an independent source such as the exchange. |
| Incremental prices | A short window never sees restated `adj_close` values for old dates (splits, dividends). Run `POST /fetch-prices?full=true` now and then, for example weekly. A window that needs more than 20 pages (about 400 rows) fails loudly instead of returning partial data. |
| Trading calendar | The holiday file is maintained by hand once a year. Only HOSE's session times were checked. |
| AI scoring | The model sees headlines, not article bodies. Quality has not been validated. |
| Quality and ops | Parsers have no automated tests yet; there is no alerting or backup. Back up `data/` manually; Silver and Gold can be rebuilt from Bronze, so Bronze matters most. |
| Locking | The lock protects one host (one container). It is not a distributed lock. |
| Scale | Designed for at most 10 tickers; not tuned for large volumes. |
| Dependencies | The n8n image uses `latest`; pin a version before relying on it long-term. `vnstock` is not used because it was quarantined on PyPI at the time of building. |

---

## Roadmap

- [ ] Unit tests for the CafeF parsers (the transforms, calendar, lock and runner are covered)
- [ ] Failure alerts to a Discord channel
- [ ] Cross-check prices against a second source
- [ ] Dashboard on top of Gold (Looker Studio or Metabase)
- [ ] Scheduled backups of `data/bronze/`

---

*Reference only, not investment advice.*
