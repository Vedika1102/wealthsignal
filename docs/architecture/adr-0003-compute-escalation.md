# ADR 0003: Compute path after the Cloud 4 gate

- Status: accepted
- Decision date: 2026-08-31
- Supersedes: the interim 2026-08-17 Free Edition-first decision

## Context

The sample-scale Cloud 4 portfolio gate passed on Databricks Free Edition. Run `614617193746493` completed in `101.561` parent-run seconds (`65.141` pipeline seconds), reused all nine fold checkpoints, and reconciled persistence plus EMA `0.4`, `0.6`, and `0.8` over 412 manager-quarter groups per model. The largest absolute graph/tabular delta was `2.220446049250313e-16`, below the frozen `1e-12` tolerance.

The retained full-volume attempts failed before a first fold checkpoint. One exposed a graph-table schema conflict; the other three exceeded Spark Connect's fixed `268,435,456`-byte model-response limit during preprocessing. They are capacity and compatibility evidence for the unchanged implementation, not evidence that the frozen protocol is invalid.

Databricks documents Free Edition as serverless-only, quota-limited, without custom compute or custom workspace storage locations. AWS S3 access in a full Databricks account is governed through a Unity Catalog storage credential and external location. These platform facts make the portfolio demonstration and the later AWS/S3 integration separate decisions:

- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Connect Unity Catalog to an AWS S3 external location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/)

## Decision

No paid-compute escalation is justified by Cloud 4. Free Edition is sufficient for the production-shaped, sample-scale portfolio demonstration, and the successful run is the accepted A1 evidence. Do not rerun either the unchanged full-volume job or the already-passed sample job. A full-volume Cloud 4 run is optional research work outside the portfolio scope.

Keep the current Free Edition workspace as an isolated, no-cost engineering environment. Do not present its provider-managed storage as the target durable AWS data plane. If architecture milestone A3 is separately approved, the target integration is an AWS-owned S3 bucket exposed to an AWS-linked Databricks workspace through an IAM-role-backed Unity Catalog storage credential and external location. Start with serverless CPU compute. Classic jobs compute is a fallback only after a new bounded gate demonstrates a serverless incompatibility that cannot be removed without changing the frozen contract.

RunPod remains the separately approval-gated GPU lane for NAVIS. This ADR does not authorize a RunPod account, volume, pod, data transfer, or spend.

## Alternatives and escalation boundary

| Path | Decision now | Runtime boundary | Compatibility and migration work | Cost and cleanup boundary |
|---|---|---:|---|---|
| Existing Free Edition sample path | Selected for the portfolio demonstration; complete | Measured `101.561` seconds end to end | None; preserve the accepted artifacts | USD `0`; no new run |
| Serverless-compatible full-volume rewrite | Not required; consider only for a new full-volume research requirement | First gate: one fold, hard stop at 2 hours | Replace oversized Spark ML model responses while preserving folds, features, and metrics | Prefer no-cost quota; no run is currently authorized |
| AWS-linked Databricks serverless trial | Deferred to an approved S3 integration requirement | One bounded job, hard stop at 2 hours | Terraform/IAM/Unity Catalog external-location setup and checksum reconciliation | Maximum request USD `25` for the bounded job; remove temporary jobs and credentials after evidence capture |
| CPU-only classic jobs compute | Rejected unless the approved serverless gate exposes a blocking limitation | Auto-terminate after 15 idle minutes; hard stop at 2 hours | New account/workspace and cluster policy; highest migration burden | Maximum request USD `25`; terminate cluster and delete job immediately after capture |

The complete portfolio exercise retains a USD `75` stop limit across AWS, Databricks, and RunPod. A future proposal must provide a current price quote, estimated runtime, maximum authorized spend, named shutdown owner, and exact cleanup commands before any resource is created. A trial credit is not permission to create a resource.

## Consequences

- The honest Cloud 4 result is a passed sample-scale engineering demonstration alongside retained full-volume capacity failures.
- No model-performance, full-volume, production-readiness, or promotion claim follows from A1.
- No quota or compatibility failure may be bypassed by weakening the candidate, fold, feature, metric, or prospective-access rules.
- AWS account use, region, budget recipient, Terraform apply, Unity Catalog credential creation, and any paid compute still require explicit owner approval.
- Prospective 2026 Q2 truth remains closed.
