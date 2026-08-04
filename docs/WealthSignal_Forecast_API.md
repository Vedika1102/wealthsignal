# Forecast API and Honest Product Surface

## Boundary

The forecast API serves persisted next-quarter holdings predictions from `forecast_runs` and `forecast_predictions`. It does not serve target truth, observed holdings, observed position changes, weak labels, or the failed new-position/exit classifier outputs as forecasts.

Every manager response declares:

- `concept: predicted_future_holdings`;
- `observed_holdings_included: false`;
- `investment_advice: false`;
- model, dataset, protocol, cutoff, target-quarter, generation, and limitation metadata.

The legacy `/alerts`, `/recommendations`, and `/models/latest` routes remain separate observed-change and weak-label surfaces.

## Endpoints

### `GET /api/v1/forecast-runs`

Lists forecast runs with `limit` and `offset`. The response includes total count and complete run provenance.

### `GET /api/v1/forecast-runs/{run_id}`

Returns one run with model/version, dataset and manifest checksum, protocol and checksum, code and implementation identities, source cutoff, target quarter, package lineage, prediction count, and limitations. Unknown runs return `404`.

### `GET /api/v1/forecast-runs/{run_id}/managers/{manager_cik}`

Requires `target_quarter` and accepts `limit` from 1 through 500 plus a non-negative `offset`. Manager CIKs are normalized to ten digits. The response is ordered by deterministic predicted rank and includes security identity, feature availability, source filing accessions, predicted weight, and predicted rank. Unknown runs, target quarters, or manager forecasts return `404`; malformed pagination and CIKs return `422`.

Example:

```text
GET /api/v1/forecast-runs/forecast-9e7d6f342c1eddd15ac1/managers/19617
    ?target_quarter=2014-12-31&limit=20&offset=0
```

The first saved response row traces:

- manager CIK `0000019617`;
- source accession `0000019617-14-000501`;
- feature report period `2014-09-30`, available `2014-12-08`;
- security key `78462F103|UNIT||SH|DFND|5`, CUSIP `78462F103`;
- predicted target quarter `2014-12-31`;
- persistence weight `0.03526813781428676`, rank `1`;
- dataset `d7c615d8d674f581` and Protocol V1 through the enclosing run.

This is a historical reproducibility example, not a current recommendation.

## Verification and local benchmark

Contract tests cover typed OpenAPI responses, pagination, CIK normalization, missing resources, invalid parameters, and exclusion of target/action truth. The database test verifies that SQLite uses `idx_forecast_predictions_manager_target` for the manager-quarter ranking query.

Against the local 100,822-row V1 database, 200 in-process TestClient requests for a 20-row manager forecast measured approximately 26.76 ms p50, 36.08 ms p95, and 54.52 ms maximum. These are local development measurements including connection/schema initialization per request, not production service-level claims.
