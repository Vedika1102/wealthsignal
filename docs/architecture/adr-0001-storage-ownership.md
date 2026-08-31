# ADR 0001: Durable storage ownership

- Status: accepted for design; deployment not authorized
- Date: 2026-08-17

## Decision

S3 owns durable cross-platform raw, curated, graph, model, report, and manifest objects. Databricks owns distributed transformation and governed Delta access through Unity Catalog. RDS stores only compact operational and approved serving records. Git stores code, schemas, configuration, and small verified reports.

## Consequences

Large datasets and model binaries remain outside Git and RDS. Databricks must use an IAM role and Unity Catalog external locations rather than static AWS keys. Migration cannot delete accepted managed tables until checksum and reload gates pass.
