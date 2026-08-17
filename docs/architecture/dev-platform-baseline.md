# Development platform baseline

Measured read-only on 2026-08-17. No resource was created or modified.

| Item | Evidence | Decision or blocker |
|---|---|---|
| AWS CLI | v2.27.1; authenticated IAM user in an existing account | Identity exists; account ownership and use authorization still require owner confirmation. |
| AWS configured region | `us-east-1` | Treat as the proposed dev region, not an approved deployment region. |
| AWS budget | No project ceiling declared in repository evidence | A3 planning/apply is blocked until the owner declares a maximum spend and alert thresholds. |
| Databricks CLI | v1.10.0; OAuth identity active | Read-only identity succeeded. |
| Databricks compute | No active job runs at inspection time | Does not imply future Free Edition capacity. |
| Data classification | Public SEC source data; internally derived research artifacts and lineage; synthetic client portfolios only; credentials confidential | Do not introduce real client data. Secrets belong in platform secret stores and never in Git or logs. |
| Environments | `local` and proposed `dev`; no `prod` deployment | Add production only after Cloud 4 acceptance and a real serving requirement. |

Before any AWS resource creation, the owner must confirm account use, region, intended budget, cost-alert recipients, and maximum approved spend.
