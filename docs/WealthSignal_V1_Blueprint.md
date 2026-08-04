# WealthSignal V1 Blueprint

> **Legacy architecture document.** This blueprint describes the original materiality-classification prototype. [WealthSignal Product and ML Specification](WealthSignal_Forecasting_Spec.md) is authoritative for current product direction, objective next-quarter targets, temporal evaluation, and legacy-component disposition. Materiality is now a secondary observed-change severity policy, not the primary ML target.

## Goal

Build a credible applied AI/ML project for a role like JPMC Applied AI/ML by focusing on a real financial workflow:

1. ingest real SEC `13F-HR` filings,
2. detect meaningful institutional portfolio changes,
3. score how relevant those changes are to advisor-managed client portfolios,
4. deliver explainable alerts through a production-style backend.

This should look like an `AI/ML software system`, not a notebook demo and not a generic dashboard.

---

## Why This Project Is Strong

This project demonstrates:

- financial data ingestion from a real regulatory source,
- event-driven backend processing,
- feature engineering on temporal portfolio data,
- a defensible ML classification problem,
- explainability and governance,
- portfolio analytics,
- deployable APIs and monitoring.

That is much closer to `Applied AI/ML` than a single notebook, a Kaggle competition clone, or a thin Streamlit wrapper.

---

## Original Core Product Thesis

WealthSignal is an `institutional holdings intelligence system` for wealth managers.

It answers:

- Which major funds made important changes in their reported holdings?
- Are those changes likely to be advisor-worthy, or just routine rebalancing?
- Which client portfolios are most exposed to those changes?
- What should an advisor review first?

Important framing:

The following framing documents the original prototype. The current primary ML target is objective next-quarter holdings forecasting as defined in `docs/WealthSignal_Forecasting_Spec.md`.

- `13F data is delayed`, not real-time trading data.
- The platform should position itself as `post-filing decision support`, not instant alpha generation.
- The ML target should be `advisor-worthy materiality`, not “predict the market.”

---

## What To Keep From The Original Prompt

Keep:

- real SEC EDGAR ingestion,
- quarter-over-quarter holdings delta computation,
- materiality scoring,
- portfolio overlap and client impact scoring,
- explainability,
- governance,
- production-style APIs,
- monitoring and auditability.

Cut or defer from V1:

- Spring Boot gateway,
- collaborative filtering based on advisor behavior,
- fine-tuned summarization and QA models,
- full EKS + Helm + Terraform + multi-environment production rollout,
- seven separate microservices.

Those items add scope faster than they add credibility.

---

## Credible V1 Scope

### In Scope

1. `13F ingestion pipeline`
2. `holdings normalization and delta engine`
3. `materiality classifier`
4. `client portfolio impact engine`
5. `alert API`
6. `basic search/filter APIs`
7. `model explainability and governance artifacts`
8. `containerized local deployment`
9. `monitoring and audit logs`

### Explicitly Out of Scope for V1

1. custom fine-tuned LLMs,
2. collaborative filtering using advisor action history,
3. Java gateway,
4. Kubernetes,
5. full AWS Terraform estate,
6. polished frontend beyond a minimal operator UI or API docs.

---

## Recommended V1 Architecture

Use a modular monorepo with `3 deployable services`, not `7 microservices`.

### Service A: `pipeline-worker`

Responsibilities:

- poll EDGAR for new `13F-HR` and `13F-HR/A` filings,
- download raw filing artifacts,
- parse holdings,
- normalize identifiers,
- compute quarter-over-quarter deltas,
- build model features,
- persist features and alert candidates.

Suggested stack:

- Python 3.11
- `requests`, `lxml`, `pandas`, `pydantic`
- PostgreSQL
- MinIO or S3-compatible object storage
- Celery or Dramatiq with Redis, or a lightweight job scheduler

### Service B: `decision-api`

Responsibilities:

- serve materiality predictions,
- calculate client exposure overlap,
- store and retrieve alerts,
- expose explainability results,
- serve governance metadata.

Suggested stack:

- FastAPI
- scikit-learn / XGBoost
- SHAP
- SQLAlchemy

### Service C: `ops-ui` or minimal frontend

Responsibilities:

- show recent filings,
- show materiality explanations,
- show impacted client portfolios,
- show alert status and audit history.

Suggested stack:

- React + TypeScript
- or keep this minimal and rely on Swagger plus a thin admin UI

---

## Data Sources

### Required

1. `SEC EDGAR 13F filings`
2. `Official List of Section 13(f) Securities`
3. `market data` for prices and sector metadata

Practical sources:

- SEC EDGAR filing archives
- SEC filing search / submission archives
- `yfinance` or another free market data source for price history
- a static mapping file for ticker/sector enrichment where needed

### Synthetic But Realistic

1. client portfolios
2. advisor users
3. alert preferences

Use realistic synthetic portfolios with:

- 20 to 60 holdings,
- sector concentration,
- growth / income / balanced styles,
- large-cap bias for wealth clients.

---

## Legacy Financial Problem Definition

The platform is not trying to predict stock returns directly.

It is solving:

`Given a newly published 13F filing, determine whether the reported changes are material enough to warrant advisor review, and rank the relevance to client portfolios.`

This remains a useful observed-change triage problem, but it is no longer WealthSignal's primary predictive task.

---

## Legacy ML Problem Formulation

### Legacy Task: Materiality Classification

Binary classification:

- `1 = advisor-worthy material shift`
- `0 = routine rebalance / low-action update`

### Better Target Name

Do not call it “alpha prediction.”

Call it:

- `materiality classification`
- `advisor action prioritization`
- `institutional shift significance scoring`

### Input Unit

Use `fund-quarter-position-change events` as the atomic unit.

One event could represent:

- a new position,
- a full exit,
- a major increase,
- a major decrease,
- a sector rotation pattern.

### Output

For each event:

- materiality probability,
- calibrated alert tier,
- top contributing features,
- linked impacted portfolios,
- recommended review priority.

---

## Label Strategy

This is the hardest part. A weak label strategy makes the project look fake.

### Bad Version

“Material if price moved a lot later.”

That is not the same as advisor relevance.

### Better V1 Strategy

Use a `three-layer label design`.

#### Layer 1: Policy Rules

Create deterministic candidate labels using a written rubric:

- new position enters top 10 by market value,
- full exit of prior top 20 holding,
- absolute position value change above threshold,
- position weight change above threshold,
- sector allocation change above threshold,
- portfolio turnover above threshold.

This gives you candidate positive events.

#### Layer 2: Manual Gold Set

Hand-label a smaller evaluation set, for example `150 to 300 events`.

Rubric:

- Would a wealth advisor reasonably review this?
- Is this change likely strategic rather than mechanical?
- Does it affect concentrated or thematic exposure?

This can evaluate the observed-change severity policy independently, but it is not the ground-truth dataset for next-quarter holdings forecasting.

#### Layer 3: Secondary Outcome Signals

Use market or portfolio follow-on behavior only as secondary context:

- post-filing abnormal return,
- volatility change,
- sector dispersion,
- repeat behavior by the same fund.

These are useful features or validation signals, not the primary target definition.

---

## Feature Design

### Filing / Position Features

- filing date
- report quarter
- filer CIK
- issuer
- CUSIP
- ticker
- shares
- market value
- position weight in fund
- prior position weight

### Delta Features

- `pct_shares_change`
- `pct_value_change`
- `weight_delta`
- `rank_delta`
- `is_new_position`
- `is_exited_position`
- `is_top10_entry`
- `is_top10_exit`

### Fund-Level Context

- portfolio turnover rate
- concentration change
- sector rotation score
- historical aggressiveness of filer
- average holding count
- average churn in prior quarters

### Market Context

- sector return trailing 1m / 3m
- realized volatility
- benchmark drawdown
- market regime proxy

### Client Relevance Features

- overlap with client holdings
- sector overlap
- concentration-adjusted exposure
- look-through exposure to impacted names

---

## Portfolio Impact Engine

This is one of the strongest parts of the project if done well.

For each alert candidate:

1. map changed holdings to client portfolio holdings,
2. compute direct overlap,
3. compute sector overlap,
4. score exposure severity,
5. assign review priority.

### Suggested Impact Score

Example formula:

`impact_score = 0.5 * direct_overlap + 0.3 * sector_overlap + 0.2 * client_concentration_multiplier`

Keep the first version interpretable.

This is better than jumping straight to recommendation models with weak data.

---

## Search Scope for V1

Do not start with semantic search or RAG.

V1 search should be:

- structured filters,
- keyword search,
- saved views.

Examples:

- “show all top-10 new positions by Tiger Global in the last 4 quarters”
- “show filings with large tech allocation decreases”
- “show alerts impacting income portfolios”

Semantic search can be a `phase 2` enhancement.

---

## Governance Scope for V1

This is worth keeping because it helps with JPMC credibility.

Required artifacts:

1. model card
2. label rubric
3. feature dictionary
4. audit log of predictions
5. explainability output
6. data lineage note
7. limitations section

Important limitations to document:

- 13F is delayed by up to 45 days after quarter-end.
- 13F does not show the full economic exposure of a fund.
- 13F does not fully represent short positions or all derivatives.
- ticker mapping and amended filings can introduce noise.

---

## Storage and Data Model

### Core Tables

- `filers`
- `filings`
- `holding_snapshots`
- `holding_deltas`
- `materiality_features`
- `model_predictions`
- `client_portfolios`
- `client_holdings`
- `alerts`
- `alert_impacts`
- `prediction_audit_log`

### Object Storage

Store raw filing payloads:

- original filing text,
- XML information tables,
- parsed normalized artifact snapshots.

---

## API Scope

### Pipeline / Internal

- `POST /internal/ingest/run`
- `POST /internal/filings/{id}/process`
- `POST /internal/alerts/recompute`

### Product / External

- `GET /filings`
- `GET /filings/{id}`
- `GET /filings/{id}/changes`
- `GET /alerts`
- `GET /alerts/{id}`
- `GET /clients/{id}/impact`
- `POST /clients/{id}/portfolio`
- `GET /governance/model-card`
- `GET /governance/feature-dictionary`

---

## Evaluation Plan

### Classification

- precision
- recall
- F1
- PR-AUC
- calibration curve
- confusion matrix on manual gold set

Because positives are rare, optimize for:

- high precision at top alert tiers,
- stable ranking quality,
- interpretable false positives.

### Portfolio Relevance

Evaluate whether high-ranked alerts actually correspond to:

- larger direct overlap,
- higher client concentration,
- more meaningful advisor review cases.

Even if this is heuristic, document it explicitly.

---

## Implementation Phases

### Phase 0: Project Setup

- monorepo scaffold
- local Docker Compose
- PostgreSQL
- MinIO
- Redis
- FastAPI skeleton

### Phase 1: 13F Ingestion

- top 20 to 50 filers
- filing parser
- raw artifact persistence
- normalized holdings table

### Phase 2: Delta Engine

- prior-quarter lookup
- position change computation
- fund-level aggregates
- feature store tables

### Phase 3: Materiality Model

- baseline rule engine
- manual label set
- train logistic regression and XGBoost
- SHAP explanations
- calibration

### Phase 4: Portfolio Impact + Alerts

- synthetic client portfolio generator
- overlap scoring
- alert creation
- alert API

### Phase 5: Governance + Monitoring

- model card
- feature dictionary
- audit logging
- Prometheus metrics
- simple dashboard or API explorer

### Phase 6: Optional Stretch

- hybrid search
- summarization
- AWS deployment story

---

## What A Strong Demo Looks Like

A reviewer should be able to:

1. run the stack locally,
2. ingest real 13F data,
3. inspect normalized holdings and computed deltas,
4. see a model classify an event as material,
5. view a client portfolio impact score,
6. inspect the explanation for why the alert fired,
7. review governance docs and model limitations.

That is enough to be credible.

---

## What Will Make This Project Weak

- pretending 13F is real-time,
- using only synthetic filing data,
- skipping label design,
- adding LLM features before core analytics work,
- building many services with little depth,
- optimizing the UI while the ML and data model are still shallow.

---

## Recommended Final Positioning

Describe the project as:

`An applied AI platform that ingests real SEC 13F disclosures, detects advisor-worthy institutional portfolio shifts, scores downstream client portfolio impact, and delivers explainable alerts with governance artifacts.`

That is a strong story for an applied AI/ML role.
