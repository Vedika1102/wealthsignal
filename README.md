# WealthSignal

WealthSignal is evolving into an auditable temporal ML platform that ingests public SEC `13F-HR` filings, forecasts next-quarter institutional holdings, detects unusual observed changes, and explains their relevance to synthetic client portfolios.

The authoritative product and ML definition is [docs/WealthSignal_Forecasting_Spec.md](docs/WealthSignal_Forecasting_Spec.md). The repository implements the historical bulk-data, objective temporal-target, leakage-audit, Protocol V1 evaluation, forecast persistence, and forecast API paths. Protocol V2 Cloud 1 and the exact 10-, 25-, and official 50-manager Cloud 2 Bronze-to-Silver checkpoints are complete; Cloud 3 Gold data and the attributed NAVIS research lane remain in progress. The legacy observed-change and weak-label workflow remains separate.

The historical bulk-data workflow and its amendment, checksum, and output contracts are documented in [docs/SEC_13F_Bulk_Dataset.md](docs/SEC_13F_Bulk_Dataset.md).

The objective manager-security-quarter table, expanding-window splits, and leakage audit are documented in [docs/WealthSignal_Temporal_Dataset.md](docs/WealthSignal_Temporal_Dataset.md).

Leakage-safe persistence, EMA, popularity, ridge, and gradient-boosted forecasting baselines are documented in [docs/WealthSignal_Forecasting_Baselines.md](docs/WealthSignal_Forecasting_Baselines.md).

The measured ten-manager, six-target-quarter validation expansion is recorded in [docs/WealthSignal_Expanded_Validation.md](docs/WealthSignal_Expanded_Validation.md). Its Protocol V1 final-test fold has been consumed and must not be reused for tuning.

The [candidate-universe sensitivity study](docs/WealthSignal_Candidate_Universe_Sensitivity.md) selects the 100-negative cap. [Forecast Comparison Protocol V1](docs/ai-governance/forecast-comparison-protocol-v1.md) freezes the model set, metrics, promotion gate, and one-time final-test procedure.

[Final-Test Report V1](docs/WealthSignal_Final_Test_Report_V1.md) preserves the one-time holdout result. Persistence wins the primary final-test ranking and MAE metrics; no learned model is promoted, and the Protocol V1 holdout is now consumed.

[Forecast Persistence and Lineage](docs/WealthSignal_Forecast_Persistence.md) documents the separate forecast-run and prediction schemas, checksum-traced materialization CLI, idempotency contract, and measured V1 reconciliation.

[Forecast API and Honest Product Surface](docs/WealthSignal_Forecast_API.md) documents typed run-provenance and paginated manager-forecast endpoints, concept separation, errors, traceability, and local latency measurements.

[Protocol V2 Design](docs/WealthSignal_Protocol_V2_Design.md) declares a 50-manager main current-data study, nested 10/25-manager engineering subsets, an optional 99-manager scale study, point-in-time identity and missingness rules, 2024 Q1–2026 Q1 validation, and an unavailable-at-freeze 2026 Q2 prospective holdout before any new download or evaluation.

[Cloud Execution Plan](docs/WealthSignal_Cloud_Execution_Plan.md) moves Protocol V2 PySpark/Delta construction to Databricks and reserves RunPod for checksum-frozen NAVIS GPU experiments after graph reconciliation.

This repository starts with the `13F ingestion foundation`, because a credible platform depends on:

1. reliable parsing of real filings,
2. clean normalized holdings data,
3. quarter-over-quarter change computation,
4. explainable downstream scoring.

## V1 Scope

The V1 build covers or targets:

- SEC filing ingestion
- XML holdings parsing
- normalized holdings models
- delta computation
- manager-security-quarter feature generation
- objective next-quarter weight, rank, and holding-action targets
- leakage-safe expanding-window evaluation
- persistence and EMA forecasting baselines
- client impact scoring
- observed-change and forecast APIs

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

The abbreviated tree above shows the original runtime foundation. The forecasting path additionally includes `bulk_dataset.py`, `temporal_dataset.py`, and `forecasting_baselines.py`, with dedicated bulk, temporal, and baseline tests under `services/pipeline_worker/tests/`.

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
- persisted client portfolios and normalized client holdings
- persisted alert and client impact records
- persisted feature rows with weak labels
- numpy logistic-regression baseline with stored probabilities and metrics
- optional scikit-learn/XGBoost model comparison with calibration data, best-model artifact persistence, and MLflow-ready tracking
- ranked client recommendations with content-similarity scoring and historical precedent retrieval
- decision API endpoints for filings, alerts, and governance
- Dockerfiles for `pipeline-worker` and `decision-api`
- `docker-compose.yml` with PostgreSQL, MinIO, Redis, MLflow, pipeline-worker, and decision-api
- live SEC artifact resolution verified against a real 13F filing
- immutable official SEC bulk-package ingestion and normalized dataset manifests
- manager-security-quarter features and objective next-quarter targets
- expanding-window split manifests and automated temporal-leakage audits
- persistence, EMA, popularity, ridge, gradient-boosting, and logistic action baselines
- checksum-gated Protocol V1 validation and one-time final-test artifacts
- lineage-aware SQLite/PostgreSQL forecast persistence and idempotent persistence-reference materialization
- typed, paginated forecast-run and manager-forecast API endpoints with provenance and limitations
- checksum-frozen Protocol V2 design with a new prospective evaluation window
- unit tests for parser, SEC utilities, historical/temporal datasets, baselines, persistence, and decisioning

Next:

- use persistence as the V1 reference forecast without presenting it as a learned-model breakthrough
- construct leakage-audited Cloud 3 Gold partitions from the accepted official 50-manager Cloud 2 tables, preserving the prospective Q2 2026 guard

### Legacy materiality path

The existing materiality classifier, weak labels, manual gold-set workflow, and recommendation precedents are retained for reproducibility while the forecasting path is built. They are not the primary ML objective, and their metrics must not be presented as evidence of next-quarter forecasting performance. The deterministic materiality score remains useful as an observed-change severity policy.

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
- `GET /api/v1/forecast-runs`
- `GET /api/v1/forecast-runs/{run_id}`
- `GET /api/v1/forecast-runs/{run_id}/managers/{manager_cik}`
- `GET /filings`
- `GET /filings/{accession_number}/changes`
- `GET /alerts`
- `GET /alerts/{alert_id}`
- `GET /recommendations/{client_id}`
- `GET /api/v1/recommendations/{client_id}`
- `GET /clients`
- `GET /clients/{client_id}`
- `POST /clients/{client_id}/portfolio`
- `GET /models/latest`
- `GET /governance/materiality-policy`

`GET /models/latest` returns the legacy single-run fields for backward compatibility and, when advanced comparison training has been run, also includes the latest `models` comparison array plus best-model artifact metadata.

## Manual Gold-Set Workflow

Export a deterministic review queue from persisted feature rows:

```bash
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.gold_dataset export `
  --db-path data/wealthsignal.db `
  --output data/evaluation/materiality_gold.csv `
  --limit 300
```

Reviewers complete `manual_label`, `review_reason`, `reviewer_id`, and `reviewed_at` using the rubric in `docs/ai-governance/materiality-labeling-rubric.md`. Validate the completed dataset before evaluation:

```bash
python -m wealthsignal_pipeline.gold_dataset validate `
  --input data/evaluation/materiality_gold.csv
```

Generate a versioned JSON evaluation report after validation:

```bash
python -m wealthsignal_pipeline.gold_dataset evaluate `
  --input data/evaluation/materiality_gold.csv `
  --output data/evaluation/materiality_evaluation.json `
  --db-path data/wealthsignal.db
```

The report includes confusion-matrix counts, precision, recall, F1, PR-AUC, Brier score, top-decile precision, and rule performance slices by sector and event type. Stored-model results are explicitly marked `diagnostic_in_sample` until gold-set events are excluded from training.

Gold-set files under `data/` remain local by default. Publish only an explicitly approved, versioned dataset with appropriate provenance.

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
