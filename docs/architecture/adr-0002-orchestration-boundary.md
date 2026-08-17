# ADR 0002: Orchestration boundary

- Status: accepted for design; deployment not authorized
- Date: 2026-08-17

## Decision

Step Functions owns cross-service workflow state, retries, and audit transitions. Databricks Workflows owns Bronze-to-Silver-to-Gold and ML task dependencies. EventBridge may start the cross-service workflow only after scheduling approval.

## Consequences

The same production workflow must not be scheduled independently in both systems. Every transition records stable run, revision, input/output checksum, timing, status, and failure fields. Retries preserve accepted artifacts.
