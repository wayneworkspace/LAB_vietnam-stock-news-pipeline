# VN Stock Morning Digest Pipeline

A small, idempotent data pipeline that collects news and daily prices for Vietnamese stocks from CafeF, keeps every raw response in a Bronze layer, cleans and models it into Silver and Gold layers, scores news sentiment with Claude, and posts a morning digest to Discord.

## Tech Stack

| Category | Technologies |
|---|---|
| **Programming** | <img src="assets/icon-python.png" alt="Python" height="24" /> Python &nbsp;•&nbsp; <img src="assets/icon-sql.png" alt="SQL" height="24" /> SQL |
| **Data Storage** | <img src="assets/icon-sqlite.png" alt="SQLite" height="24" /> SQLite (Bronze) &nbsp;•&nbsp; <img src="assets/icon-duckdb.png" alt="DuckDB" height="24" /> DuckDB (Silver, Gold) |
| **Data Processing** | <img src="assets/icon-pandas.png" alt="pandas" height="24" /> pandas &nbsp;•&nbsp; Requests &nbsp;•&nbsp; BeautifulSoup (lxml) |
| **API** | <img src="assets/icon-fastapi.png" alt="FastAPI" height="24" /> FastAPI (Uvicorn) |
| **ETL & Orchestration** | <img src="assets/icon-n8n.png" alt="n8n" height="24" /> n8n (cron schedule) |
| **Data Modeling** | Medallion (Bronze / Silver / Gold) &nbsp;•&nbsp; Star Schema &nbsp;•&nbsp; OLTP + OLAP &nbsp;•&nbsp; Idempotent rebuilds |
| **DevOps** | <img src="assets/icon-docker.png" alt="Docker" height="24" /> Docker Compose |
| **Automation & AI** | <img src="assets/icon-anthropic.png" alt="Anthropic" height="24" /> Anthropic API (Claude Haiku 4.5) &nbsp;•&nbsp; <img src="assets/icon-discord.png" alt="Discord" height="24" /> Discord webhook |

> **Reference only, not investment advice.** Prices come from a single source (CafeF) and are not cross-checked. Sentiment is scored by an AI model from headlines only. This is a learning project.

---

## Overview

**Problem** — A beginner investor wants a short, factual summary of overnight news and the latest prices for a few tickers, and also wants to learn how a real data pipeline works. Doing this by hand every morning is repetitive, and ad-hoc scripts tend to create duplicate rows, lose raw data, and break silently.

**Solution** — A five-layer pipeline with a medallion data model. Raw responses are stored append-only in Bronze. Silver and Gold are rebuilt from scratch on every run, so re-running never creates duplicates. n8n triggers the steps on a schedule, and a small FastAPI worker does the actual work. The design philosophy: *"Source knows the website, Storage knows the database, and nothing else knows both."*

**Data Flow** — n8n cron (07:30, Mon–Fri) → `POST /run-pipeline` → Discord webhook. Behind that one call the worker runs, in order: fetch news → fetch prices (incremental) → enrich news → build Silver → build Gold (+ Parquet export) → check that prices are fresh → build the digest. Every step is also available as its own endpoint.

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
| **Serving** | HTTP endpoints for n8n, the run-overlap lock and the digest text builder. | `python/serving/api.py`, `locking.py`, `digest.py` |

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
| **Incremental price loads** | The watermark is the newest trade date stored in Bronze per ticker. A run fetches the gap since then plus 3 days of lookback (`PRICE_LOOKBACK_DAYS` in `config.py`). A new ticker, or `full=true`, loads the whole window. |
| **Static `dim_date`** | Built from a calendar (weekends + `python/reference/vn_market_holidays.csv`), not from the days that have data, so a day with missing data is not mistaken for a holiday. |
| **Session close** | News published at or after `MARKET_CLOSE_TIME` (14:45, when the HOSE closing auction ends) counts for the next trading session. |
| **One run at a time** | Every step that touches a database takes one non-blocking file lock. An overlapping call gets `409` (DuckDB cannot open one file read-write and read-only at once). |
| **AI results live in Bronze** | Scored sentiment costs money, so it is stored in the most durable layer and Silver is rebuilt from it. |
| **Versioned prompts** | `PROMPT_VERSION` is part of the primary key, so v1 and v2 results can be compared side by side. Gold reads only `ACTIVE_PROMPT_VERSION`, so a half-scored new version never reaches the digest. |
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
    └── serving/         # api.py, locking.py, digest.py
```

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

The container runs as a normal user (uid 1000), not root. On Linux, create the data folder yourself first (`mkdir data`) so that user can write to it; Docker Desktop on Windows needs nothing.

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

`/run-pipeline` returns the status of every step and the digest text in `text`. To run one step at a time (useful while debugging), call the individual endpoints in this order: `/fetch-news`, `/fetch-prices?days=30`, `/enrich-news?limit=5`, `/build-silver`, `/build-gold`, `/digest`.

### Schedule it with n8n

Build one small workflow in n8n (the workflow export is not stored in this repository):

| Order | Node | Settings |
|---|---|---|
| 1 | Schedule Trigger | Cron `30 7 * * 1-5`, timezone `Asia/Bangkok` |
| 2 | HTTP Request | `POST http://pyworker:8000/run-pipeline`, timeout `900000` ms |
| 3 | Discord | Webhook credential, content `{{ $json.text }}` |

`/run-pipeline` answers `500` when a step fails (the remaining steps are reported as `skipped`), so the HTTP node fails and no stale digest is posted. Store the webhook URL only in an n8n credential, never in code or in a committed file.

### Failure alerts (Discord)

A failed run answers `500`, but nobody sees that unless n8n tells them. `n8n/error-alert.workflow.json` is a two-node workflow (Error Trigger → Discord) that posts the workflow name, the error and a link to the failed execution:

1. n8n → *Import from file* → `n8n/error-alert.workflow.json`. Open the Discord node and select your webhook credential (create it with the Discord webhook URL; the URL is never stored in this repository). Activate the workflow.
2. Open the scheduled pipeline workflow → *Settings* → *Error workflow* → choose *Stock pipeline - failure alert*.

The import has not been tested against a live n8n here; if a node shows a parameter warning, re-select the authentication ("Webhook") and credential.

### Freshness check

Before the digest, `/run-pipeline` runs `check-freshness`: for every active ticker, the newest stored price must be at least the last trading day before today (weekends and the holiday file decide what a trading day is; today's price is never required). A stale or missing ticker fails the run with a message such as `Stale prices (expected at least 2026-10-13): FPT: newest price 2026-10-09`, so no stale digest is posted and the alert above fires. A holiday missing from `python/reference/vn_market_holidays.csv` also shows up here as stale until it is added.

---

## API Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health`, `/check` | Liveness and installed libraries |
| POST | `/init-db` | Create Bronze tables (safe to repeat) |
| POST | `/watchlist` | Add or update a ticker (max 10 active) |
| POST | `/run-pipeline?days=&full_prices=` | Run every step below in order under one lock; returns per-step status and the digest in `text` |
| POST | `/backup` | Copy the Bronze database to `data/backups/` (one file per day, newest 7 kept) |
| POST | `/fetch-news` | Scrape CafeF news into Bronze |
| POST | `/fetch-prices?days=&full=` | Fetch daily prices into Bronze, incrementally per ticker; `days` is the first-load window, `full=true` refreshes the whole window |
| POST | `/enrich-news?limit=` | Summarize and score sentiment (-2..+2) |
| POST | `/build-silver` | Rebuild Silver from Bronze |
| POST | `/build-gold` | Rebuild Gold from Silver and export Parquet |
| POST | `/digest` | Build the digest text (under 1900 characters) |
| POST | `/debug/cafef-html`, `/debug/cafef-price` | Save sample responses for fixing parsers. **Only present when `ENABLE_DEBUG_ENDPOINTS=true`.** |

---

## Configuration

| Item | Where | Notes |
|---|---|---|
| API keys | `.env` | `ANTHROPIC_API_KEY` and others listed in `.env.example`. Never commit `.env`. |
| Debug endpoints | `.env` | `ENABLE_DEBUG_ENDPOINTS` (default `false`). |
| Data paths | `python/config.py` | `DATA_DIR`, `RAW_DIR`, and the three database files. Overridable by environment variables. |
| Model and limits | `python/transform/enrich.py` | `CLAUDE_MODEL`, `ENRICH_MAX_PER_RUN` (default 40), batch size 20. |
| Prompt | `python/transform/prompts.py` | Bump `PROMPT_VERSION` after any edit; promote it with `ACTIVE_PROMPT_VERSION` in `.env`. |
| Close of session | `python/config.py` | `MARKET_CLOSE_TIME = "14:45"`: the HOSE closing auction ends then ([source](https://prime.kisvn.vn/support/trading-regulations/hose-transaction-guidance)). HNX and UPCOM are assumed to match; not verified. |
| Holidays | `python/reference/vn_market_holidays.csv` | Weekdays on which the market is closed. Add next year's rows once the exchange publishes its schedule; a year with no rows is treated as weekends only. The 2026 rows follow the government schedule and are not all confirmed against HOSE notices (see the file). |
| Tickers | `watchlist` table | Managed through `POST /watchlist`, no code change needed. |

**Backups:** Bronze is the only layer that cannot be rebuilt. Add a second small n8n workflow: Schedule Trigger (daily, for example 02:00) → HTTP Request `POST http://pyworker:8000/backup`. Each run writes `data/backups/bronze-YYYY-MM-DD.sqlite` (SQLite online backup) and keeps the newest 7. These files sit next to the data, so also copy `data/backups/` to another drive or machine. To restore: stop the containers, copy a backup over `data/bronze/bronze.sqlite`, start again, then run `/build-silver` and `/build-gold`.

**Rolling out a new prompt:** edit `SYSTEM` and bump `PROMPT_VERSION` (Gold keeps reading the old `ACTIVE_PROMPT_VERSION`); run `/enrich-news` until `pending_left` is `0`; then set `ACTIVE_PROMPT_VERSION` in `.env`, recreate the container and run `/build-gold`.

**Parquet export:** `/build-gold` also writes `data/gold/parquet/`. The two fact tables are partitioned by `date_key` (one folder per day, to demonstrate partition pruning; for years of history use coarser partitions):

```sql
SELECT ma, close
FROM read_parquet('data/gold/parquet/fact_price_daily/*/*.parquet', hive_partitioning = true)
WHERE date_key = DATE '2026-10-14';
```

---


## Troubleshooting & Limitations

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Cannot reach `pyworker:8000` from the browser | That hostname only exists inside the Docker network. Use `localhost:8000` from Windows; n8n uses `http://pyworker:8000`. |
| Parser returns nothing | CafeF may have changed its markup. Run `/debug/cafef-html` or `/debug/cafef-price`, inspect the saved file in `data/debug/`, then fix the parser in `python/source/`. |
| Need to see why a run failed | `docker compose logs -f pyworker`. Every `/run-pipeline` line carries the `run_id` that is also in the response, e.g. `run=1a2b3c4d step=build-silver status=failed`, followed by the traceback. `docker compose ps` shows the `healthy` state of `pyworker`. |
| Any endpoint returns `409` "Another pipeline step is already running" | A step or `/run-pipeline` is in progress. Retry later (see `Retry-After`). The lock is released automatically if the worker restarts. |
| `/debug/...` returns `404` | The debug endpoints are off by default. Set `ENABLE_DEBUG_ENDPOINTS=true` in `.env` and recreate the container. |
| `/build-gold` or `/digest` returns 409 | The previous layer has not been built yet. Run `/build-silver`, then `/build-gold`. |
| `/run-pipeline` returns `500` with `Stale prices` | The newest price is older than the last trading day. Check `/fetch-prices` (CafeF down or markup changed) and whether a holiday is missing from the holiday file. |
| `/enrich-news` fails to authenticate | `ANTHROPIC_API_KEY` is missing or wrong in `.env`. Fix it and recreate the container. |

### Limitations

| Area | Limitation |
|---|---|
| Incremental prices | A short window never sees restated `adj_close` values for old dates (splits, dividends). Run `POST /fetch-prices?full=true` now and then, for example weekly. |
| Data source | Prices come only from CafeF and are not verified against an independent source such as the exchange. |
| AI scoring | The model sees headlines, not article bodies. Quality has not been validated. |
| Quality and ops | Tests cover the transforms, calendar, lock and API switches (`pip install -r requirements-dev.txt`, then `python -m pytest`); parsers have none. GitHub Actions (`.github/workflows/ci.yml`) runs the tests and builds the Docker image on every pull request. Failure alerts need the n8n error workflow (see above). `POST /backup` copies Bronze daily (schedule it in n8n); Silver and Gold can be rebuilt from Bronze, so Bronze matters most. |
| Scale | Designed for at most 10 tickers; not tuned for large volumes. |
| Dependencies | Python packages and the n8n image are pinned (`requirements.txt`, `docker-compose.yaml`); upgrade them on purpose and let CI confirm. `vnstock` is not used because it was quarantined on PyPI at the time of building. |

---

## Roadmap

- [ ] Unit tests for parsers and SQL transforms
- [x] Failure alerts to a Discord channel (n8n error workflow)
- [ ] Cross-check prices against a second source
- [ ] Dashboard on top of Gold (Looker Studio or Metabase)
- [x] Scheduled backups of `data/bronze/` (`POST /backup` + an n8n schedule)

---

*Reference only, not investment advice.*
