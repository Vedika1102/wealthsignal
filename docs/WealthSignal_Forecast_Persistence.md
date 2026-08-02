# Forecast Persistence and Lineage

## Purpose

`wealthsignal_pipeline.forecast_materialization` materializes the honest persistence reference forecast into forecast-specific SQLite or PostgreSQL tables. These tables are intentionally separate from legacy `model_runs` and `model_predictions`, which represent the weak-label materiality classifier.

The materializer reads only identifiers, feature-quarter availability, and current observed weights from the checksum-verified temporal table. It does not read `target_weight`, target rank, or future action truth when creating a served forecast.

## Stored lineage

Each `forecast_runs` row records:

- deterministic run ID and completion status;
- model name and version;
- temporal dataset ID and manifest checksum;
- protocol version and checksum;
- Git revision and materializer implementation checksum;
- latest source cutoff and forecast target quarter;
- generation timestamp, limitations, SEC package lineage, and prediction count.

Each `forecast_predictions` row records:

- example ID and manager CIK;
- stable `security_key`, CUSIP, and issuer name;
- feature report period and filing-availability cutoff;
- target report quarter;
- predicted weight and deterministic manager-level rank;
- contributing source filing accession numbers.

The primary key is `(run_id, example_id)`. The serving index is `(cik, target_report_period, predicted_rank)`. Inserts are transactional. Repeating the same materialization returns the existing run and writes no duplicates; an identity collision or count mismatch fails visibly.

## CLI

```powershell
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.forecast_materialization `
  --temporal-dataset data/temporal-expanded/temporal-d7c615d8d674f581 `
  --target-quarter 2014-12-31 `
  --protocol docs/ai-governance/forecast-comparison-protocol-v1.md `
  --protocol-version v1 `
  --db-path data/forecast-v1.db
```

## Measured V1 reconciliation

The local V1 materialization created run `forecast-9e7d6f342c1eddd15ac1` with 100,822 unique examples for nine eligible managers and source cutoff 2014-12-16. Prediction ranks are contiguous from one within every manager. The first execution took 7.87 seconds; an idempotent second execution took 5.96 seconds and inserted no additional rows. A 20-row manager/quarter lookup used `idx_forecast_predictions_manager_target` and averaged approximately 0.035 ms across 200 local SQLite executions.

These timings describe one local machine and ignored database artifact. They are not production latency claims. Protocol V1 remains a historical evaluation; its consumed truth is not persisted in the forecast tables.
