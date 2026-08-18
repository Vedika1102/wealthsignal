# ADR 0003: Compute escalation

- Status: accepted
- Date: 2026-08-17

## Decision

Databricks Free Edition remains the default for the Cloud 4 portfolio demonstration. The next run uses the deterministic `portfolio-demo` sample across all nine frozen folds; the retained full-volume failures are sufficient capacity evidence and do not need to be reproduced. If the sample cannot complete on serverless compute, compare a contract-preserving rewrite with one bounded CPU-only job-compute run before requesting paid-resource approval. The complete portfolio exercise has a USD 75 stop limit.

## Consequences

No quota or compatibility failure may be bypassed by weakening the frozen protocol. Paid Databricks, AWS deployment, and RunPod remain separately approval-gated with a cost ceiling, shutdown behavior, and cleanup plan.
