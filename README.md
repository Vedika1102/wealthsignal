# WealthSignal

WealthSignal is an applied AI/ML project that ingests real SEC `13F-HR` filings, computes institutional portfolio changes, scores advisor-worthy materiality, and ranks downstream client portfolio impact.

This repository starts with the `13F ingestion foundation`, because a credible platform depends on:

1. reliable parsing of real filings,
2. clean normalized holdings data,
3. quarter-over-quarter change computation,
4. explainable downstream scoring.

## V1 Focus

The initial build targets:

- SEC filing ingestion
- XML holdings parsing
- normalized holdings models
- delta computation
- materiality feature generation
- client impact scoring
- alert APIs

## Repository Layout

```text
wealthsignal/
├── .env.example
├── docker-compose.yml
├── README.md
├── pyproject.toml
└── services/
    ├── decision_api/
    │   ├── Dockerfile
    │   └── app/
    │       └── main.py
    └── pipeline_worker/
        ├── Dockerfile
        ├── src/
        │   └── wealthsignal_pipeline/
        │       ├── __init__.py
        │       ├── delta_engine.py
        │       ├── edgar_client.py
        │       ├── feature_engineering.py
        │       ├── alerting.py
        │       ├── ingest.py
        │       ├── materiality.py
        │       ├── models.py
        │       ├── portfolios.py
        │       ├── persistence.py
        │       ├── parser.py
        │       ├── recommendation.py
        │       ├── reference_data.py
        │       ├── storage.py
        │       └── worker_health.py
        └── tests/
            └── test_parser.py
```

## Current State

Implemented:

- base project scaffold
- holdings data models
- first-pass 13F information table parser
- SEC submissions and filing artifact discovery utilities
- quarter-over-quarter position delta engine
- primary document metadata parsing
- SQLite and PostgreSQL persistence support via `DATABASE_URL`
- ingest CLI for pulling recent 13F filings end to end
- first-pass materiality feature generation
- explainable rule-based alert scoring
- coarse sector enrichment for holdings and alerts
- optional official SEC 13F securities-list enrichment and local cache support
- MinIO-compatible raw filing artifact storage
- synthetic client portfolio impact scoring
- persisted alert and client impact records
- persisted feature rows with weak labels
- numpy logistic-regression baseline with stored probabilities and metrics
- optional scikit-learn/XGBoost model comparison with calibration data, best-model artifact persistence, and MLflow-ready tracking
- ranked client recommendations with content-similarity scoring and historical precedent retrieval
- decision API endpoints for filings, alerts, and governance
- Dockerfiles for `pipeline-worker` and `decision-api`
- `docker-compose.yml` with PostgreSQL, MinIO, Redis, MLflow, pipeline-worker, and decision-api
- live SEC artifact resolution verified against a real 13F filing
- unit tests for parser, SEC utilities, persistence, and decisioning

Next:

- add richer sector and reference-data enrichment
- persist client portfolios as first-class entities
- add recommendation ranking and precedent retrieval
- expose feature rows and richer model explanations through the API

## Local Ingest Example

```bash
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.cli `
  --cik 1067983 `
  --user-agent "Vedika Shinde Research wealthsignal@example.com" `
  --db-path data/wealthsignal.db `
  --reference-data-path data/reference/sec_official_13f_list.json
```

To refresh the official SEC 13F securities list before ingest:

```bash
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.cli `
  --cik 1067983 `
  --user-agent "Vedika Shinde Research wealthsignal@example.com" `
  --db-path data/wealthsignal.db `
  --reference-data-path data/reference/sec_official_13f_list.json `
  --refresh-official-13f-list
```

To train and persist the advanced Phase 2 comparison set as part of the same run:

```bash
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.cli `
  --cik 1067983 `
  --user-agent "Vedika Shinde Research wealthsignal@example.com" `
  --db-path data/wealthsignal.db `
  --reference-data-path data/reference/sec_official_13f_list.json `
  --train-advanced-models `
  --model-output-path data/models/materiality-best.joblib `
  --mlflow-experiment wealthsignal-materiality
```

This prints:

- whether the official 13F securities list was loaded or refreshed,
- official-list match coverage across ingested holdings,
- latest ingested 13F filings,
- stored quarter-over-quarter delta size,
- top explainable alert candidates,
- top impacted synthetic client for each alert,
- baseline model metrics,
- model probabilities next to rule-engine alerts.

## API Example

```bash
$env:PYTHONPATH="services/pipeline_worker/src"
uvicorn services.decision_api.app.main:app --reload
```

Current endpoints:

- `GET /health`
- `GET /filings`
- `GET /filings/{accession_number}/changes`
- `GET /alerts`
- `GET /alerts/{alert_id}`
- `GET /recommendations/{client_id}`
- `GET /api/v1/recommendations/{client_id}`
- `GET /models/latest`
- `GET /governance/materiality-policy`

`GET /models/latest` returns the legacy single-run fields for backward compatibility and, when advanced comparison training has been run, also includes the latest `models` comparison array plus best-model artifact metadata.

## Docker Compose

Copy `.env.example` to `.env` if you want to override defaults, then start the stack:

```bash
docker compose up --build
```

This brings up:

- `postgres` on `localhost:5432`
- `minio` on `localhost:9000`
- `minio console` on `localhost:9001`
- `redis` on `localhost:6379`
- `mlflow` on `localhost:5000`
- `pipeline-worker health` on `localhost:8090/health`
- `decision-api` on `localhost:8000`

To run a real ingest inside the compose environment:

```bash
docker compose run --rm pipeline-worker python -m wealthsignal_pipeline.cli `
  --cik 1067983 `
  --user-agent "Vedika Shinde Research wealthsignal@example.com" `
  --db-path data/wealthsignal.db `
  --reference-data-path data/reference/sec_official_13f_list.json
```
