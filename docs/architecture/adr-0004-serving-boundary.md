# ADR 0004: Serving boundary

- Status: accepted for design; deployment not authorized
- Date: 2026-08-17

## Decision

ECS Fargate runs the existing FastAPI service behind an ALB when deployment is approved. RDS PostgreSQL stores only promoted forecasts and operational records. Experimental Gold tables and unpromoted MLflow runs never become API outputs.

## Consequences

EKS, API Gateway, Cognito, and ElastiCache require measured requirements rather than portfolio breadth. Promotion is artifact-driven and reviewer-approved. The API must retain lineage and limitations for every served forecast.
