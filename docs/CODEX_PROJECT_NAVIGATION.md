# WealthSignal Codex Project Navigation Guide

## Purpose

This document is a reusable set of prompts and checkpoints for completing WealthSignal with Codex. It is designed for a project owner who is learning the financial domain while building an advanced, end-to-end AI/ML portfolio project.

WealthSignal should become an auditable institutional-holdings forecasting and portfolio-intelligence platform. It should ingest public SEC Form 13F data, construct quarterly manager portfolios, predict next-quarter holdings, detect unusual observed changes, and explain portfolio relevance. The primary ML target must come from future filings rather than manually guessed labels.

Do not ask Codex to execute this entire document in one turn. Use one milestone prompt at a time, review the output, and commit verified work before continuing.

---

## Project North Star

### Working title

**WealthSignal: Institutional Holdings Forecasting and Portfolio Intelligence**

### One-sentence description

WealthSignal is an end-to-end temporal ML platform that ingests public SEC 13F filings, forecasts institutional portfolio holdings, detects unusual allocation changes, and explains their relevance to client portfolios.

### Primary ML question

> Given all information available through quarter `t`, which securities will have the highest portfolio weights for an institutional manager in quarter `t+1`?

### Objective targets

The next observed filing supplies the ground truth:

- next-quarter portfolio weight;
- next-quarter holding rank;
- new position;
- full exit;
- increased position;
- decreased position.

### Explicit non-goals

WealthSignal does not:

- predict stock returns;
- provide investment advice;
- claim that 13F data is real-time;
- infer a manager's complete economic exposure;
- use rule-generated weak labels as independent model truth;
- present copied open-source work as original work.

---

## Target Role Skills Coverage

WealthSignal must demonstrate the requested job skills through coherent project components. A tool counts as demonstrated only when its implementation is tested, documented, and supported by a reproducible artifact. Do not add a library merely to list it on a resume.

| Required skill | WealthSignal use | Evidence required |
|---|---|---|
| Logistic regression | Binary new-position and exit baselines | Temporal evaluation report and saved coefficients |
| Random forest | Nonlinear action-classification baseline | Per-fold metrics and feature importance |
| Gradient boosting machine | XGBoost ranking, classification, and weight-regression models | Baseline comparison, SHAP report, saved artifact |
| Classification | Predict new, exit, increase, decrease, or unchanged actions | Precision, recall, F1, confusion matrix, calibration |
| Regression | Predict next-quarter normalized holding weight | MAE, RMSE, rank correlation, residual analysis |
| Multinomial regression | Predict multi-class holding action | Per-class metrics and coefficient interpretation |
| Multivariate analysis | Analyze correlated holding, manager, sector, and market features | Documented analysis notebook/report |
| Discriminant analysis | LDA/QDA action-classification baseline | Leakage-safe temporal comparison |
| Principal component analysis | Reduce correlated manager-style and sector-exposure features | Explained-variance and stability plots |
| Factor analysis | Derive interpretable latent manager-style factors | Factor loadings, interpretation, robustness checks |
| Time-series analysis | Persistence, EMA, lagged features, rolling validation, and sequence forecasting | Walk-forward evaluation and forecast diagnostics |
| Python | Primary ingestion, feature engineering, modeling, API, tests, and monitoring | Packaged code, tests, CLI, API |
| SAS | Independent statistical validation and benchmark programs on exported analytical tables | Versioned `.sas` programs and captured outputs when SAS is available |
| SQL | PostgreSQL analytical schema, transformations, window queries, indexes, and query optimization | Versioned SQL, `EXPLAIN ANALYZE`, correctness tests |
| Pandas | Local analytical-table processing and model diagnostics | Tested transformations and notebooks/reports |
| NumPy | Numerical features, metrics, baseline calculations | Unit-tested numerical functions |
| Scikit-learn | Classical models, preprocessing, PCA, LDA/QDA, calibration, and metrics | Reproducible pipelines and evaluation artifacts |
| TensorFlow | Sequence-model experiment for quarterly holding histories | Saved model and comparison with simpler baselines |
| PyTorch | Temporal graph holdings model | Reproducible graph experiment and ablation |
| Matplotlib and Seaborn | Data quality, EDA, residuals, calibration, drift, and model-comparison plots | Generated versioned report figures |
| PySpark | Distributed SEC bulk-data ETL and large-scale feature aggregation | Partitioned pipeline, local/cluster mode, scale benchmark |
| Data containers | Parquet partitions, Spark DataFrames, database tables, and typed API models | Schemas and data contracts |
| Multithreading | Concurrent bounded network/file retrieval where safe | Benchmark and rate-limit tests |
| Multiprocessing | CPU-bound parsing or feature jobs | Determinism and speedup benchmark |
| Excel | Interactive manager/model report using pivots, XLOOKUP or INDEX-MATCH, formulas, and charts | Verified workbook populated from exported data |
| VBA | Optional refresh/navigation macros stored as reviewed source | `.bas` source, security note, and manual verification |
| PowerPoint | Executive and technical project presentation | Verified deck with measured results and architecture |
| GitHub | Version control, issues, pull requests, CI, releases, and documentation | Commit history and passing GitHub Actions |
| Bitbucket | Not required when GitHub supplies the same version-control evidence | Do not duplicate hosting solely for a keyword |

### Tooling principles

- Use both TensorFlow and PyTorch only because they answer different questions: TensorFlow for a sequence benchmark and PyTorch for the temporal graph model.
- Keep scikit-learn models as strong, interpretable baselines. Advanced neural models must beat or meaningfully complement them.
- Use PySpark for the historical bulk build and large-scale aggregations; retain Pandas for compact diagnostics and local model analysis.
- Use SAS as an independent validation lane, not as a duplicate production pipeline. If SAS access is unavailable, commit runnable programs but do not claim executed SAS results.
- Use Excel and PowerPoint as communication products generated from verified artifacts, not as sources of model truth.
- Demonstrate SQL optimization using measured query plans and timings, not merely by storing data in PostgreSQL.
- Do not claim three years of experience from one project. Describe the project scope and results accurately while representing experience duration separately.

---

## Verified Current-State Baseline

This snapshot was re-established from the repository after Protocol V1 was frozen and evaluated. Treat it as orientation evidence, not a permanent project metric. Future Codex sessions must remeasure it before relying on it.

### Implemented foundation

- SEC submissions discovery and filing-artifact download;
- official SEC bulk-dataset package ingestion with manifests and checksums;
- Form 13F XML parsing;
- normalized holding and filing models;
- amendment, duplicate, and effective-filing resolution;
- quarter-over-quarter position deltas;
- SQLite and PostgreSQL persistence;
- MinIO/S3-compatible raw-artifact storage;
- official 13F securities-list enrichment;
- rule-based materiality scoring;
- immutable manager-security-quarter temporal datasets;
- objective next-quarter weight, new-position, and exit targets;
- expanding-window validation/test manifests and automated leakage audits;
- persistence, EMA, institutional-popularity, Ridge, histogram gradient-boosting, and logistic action baselines;
- frozen comparison protocols with checksum-gated final-test access;
- lineage-aware forecast-run and forecast-prediction persistence for SQLite and PostgreSQL;
- idempotent persistence-reference forecast materialization with security, filing, dataset, protocol, model, and code lineage;
- typed, paginated forecast-run and manager-forecast API endpoints with provenance, limitations, and concept separation;
- pre-download Protocol V2 design with deterministic cohort policy and a prospective 2026 Q2 holdout;
- synthetic client portfolios and portfolio-impact scoring;
- FastAPI endpoints and Docker Compose services;
- 60 passing tests after forecast persistence and API implementation.

### Measured local data snapshot

The declared expanded forecasting cohort and frozen Protocol V1 produced:

| Artifact or measure | Count/value |
|---|---:|
| Institutional managers | 10 |
| SEC bulk packages | 7 |
| Normalized holdings | 696,577 |
| Eligible manager-quarters | 57 |
| Temporal examples | 585,858 |
| Target quarters | 6 |
| Validation folds | 3 |
| Final-test folds | 1, now consumed |
| Target-candidate coverage | 79.3149% |
| Leakage issues | 0 |

The selected negative-candidate cap is 100. Raising it to 500 added 22,800 examples but improved target coverage by only 0.0729 percentage points, so the larger cap was rejected.

Protocol V1's one-time final test evaluated quarter 2014-12-31 on 100,822 examples. Persistence was the honest winner: NDCG@10 0.837113, NDCG@20 0.821875, Recall@10 0.755556, and weight MAE 0.00003548. Ridge did not pass the frozen promotion gate. The new-position and exit classifiers are not suitable for serving. These values must be traced to `docs/WealthSignal_Final_Test_Report_V1.md`; do not re-run or tune against this test.

### Reuse decisions

Keep and harden:

- SEC client, parser, reference-data, and storage components;
- core filing and holding models;
- persistence abstraction;
- position-delta calculations;
- FastAPI and container foundation;
- existing fixtures and relevant tests.

Implement next:

- Protocol V2 development-data acquisition and deterministic cohort materialization under the frozen design;
- a research-reproduction lane for NAVIS after graph reconciliation;
- downstream portfolio-impact workflow using only a model that passes its promotion policy;
- analytical SQL and indexes;
- sector and identifier enrichment.

Deprecate from the primary ML path:

- `weak_label` as predictive ground truth;
- manual materiality labeling as the principal evaluation strategy;
- materiality-classifier model claims;
- recommendation precedents trained on weak-label predictions.

The rule-based materiality score may remain as a clearly named observed-change severity policy. It must be separate from objective forecasting targets.

### Confirmed risks and blockers

1. Protocol V1, forecast persistence, and forecast APIs are checkpointed locally; Protocol V2 design remains uncommitted until reviewed.
2. Protocol V2 development packages have not been downloaded and the deterministic 25-manager CIK list has not yet been materialized.
3. The local V1 forecast database is ignored under `data/`; a clean checkout must rebuild it from checksum-verified source artifacts with the documented CLI.
4. Protocol V1's final test is consumed. It cannot be reused for NAVIS development, feature selection, candidate changes, or threshold tuning.
5. The failed action classifiers remain diagnostic and must not be introduced into alerts, portfolio impact, or the forecast API.
6. Ten managers and six target quarters demonstrate the method but do not reproduce the research paper's scale or establish broad external validity.
7. Long-term security identity, corporate actions, and historical index-membership policies still need explicit treatment.
8. Current Python dependencies do not yet declare a graph-learning framework or PySpark. Add them only inside the milestone that uses them and with reproducible lock/config updates.

### Required implementation order

Follow this dependency order from the actual stopping point:

1. Review and checkpoint the Protocol V2 design without downloading data or reopening the consumed V1 test.
2. Acquire the declared development packages, materialize and checksum the training-only manager cohort, and build leakage-audited validation data.
3. Freeze the selected V2 candidate/model configuration before the prospective 2026 Q2 source becomes available.
4. Create a framework-independent temporal bipartite graph adapter and prove that graph snapshots reconcile with the tabular portfolios and baseline metrics.
5. Reproduce NAVIS against a frozen upstream revision, record every deviation, and compare it fairly with the same persistence and EMA baselines.
6. Implement one clearly original WealthSignal extension and ablation after the reproduction is credible.
7. Add PySpark/SQL scaling, monitoring, Excel, PowerPoint, dashboard, and verified resume metrics as measured engineering and communication layers.

At the start of every milestone, Codex must verify that its prerequisite milestone is actually complete rather than relying on this document or the README.

---

## How to Work With Codex

### Recommended session pattern

For every milestone:

1. Start with the relevant prompt in this document.
2. Ask Codex to inspect the repository and report its findings before changing files.
3. Approve or adjust the proposed scope.
4. Ask Codex to implement only that milestone.
5. Require tests, data checks, and documentation.
6. Review the resulting diff and measured outcomes.
7. Commit the milestone before moving to the next one.

### Universal guardrails

Include these instructions when a Codex session starts losing context:

```text
Work only inside the WealthSignal repository. Preserve unrelated user changes.
Inspect existing code, tests, documentation, and git status before editing.
Do not invent model results, dataset sizes, performance improvements, or resume metrics.
Use only information available at or before prediction time; prevent temporal leakage.
Prefer objective labels derived from the next SEC filing.
Treat open-source repositories as attributed references, not code to copy blindly.
Keep financial claims conservative and document the limitations of Form 13F.
Implement the smallest complete milestone, test it, and report remaining work.
Do not push, publish, deploy, or incur cloud cost unless explicitly authorized.
```

### Standard completion request

Append this to implementation prompts:

```text
Before finishing:
1. run the relevant tests and static checks;
2. inspect key generated data or API responses;
3. verify there is no temporal leakage;
4. update documentation for behavior that changed;
5. report files changed, commands run, test results, measured outcomes, limitations, and the next recommended milestone;
6. do not claim completion if any acceptance criterion remains unmet.
```

---

## Prompt 0 — Full Repository Orientation

Use this at the beginning of a new Codex task or after a long break.

```text
Act as the senior ML engineer orienting yourself in the WealthSignal repository.
This is a diagnostic task only; do not edit files.

Inspect:
- git status and recent commits;
- README and all architecture/governance documentation;
- package and service layout;
- database models and migrations or schema creation;
- SEC ingestion and parsing flow;
- feature and target generation;
- training, evaluation, and model persistence;
- APIs and Docker services;
- tests and test configuration;
- local data artifacts without dumping sensitive or excessively large content.

Explain in plain language:
1. what the system currently does end to end;
2. which components are complete, partial, obsolete, or unverified;
3. where subjective weak-label materiality logic is still coupled to ML training;
4. what can be reused for next-quarter holdings forecasting;
5. the top five technical risks;
6. the first milestone that should be implemented next.

Support every important conclusion with a file path, symbol, test result, or inspected data fact. Do not infer completion from documentation alone.
```

Expected output:

- current-state architecture;
- evidence-backed gap analysis;
- recommended next milestone;
- no file changes.

---

## Prompt 1 — Rewrite the Product and ML Specification

```text
Revise WealthSignal's product and ML specification around objective next-quarter institutional-holdings forecasting.

First inspect the existing README, blueprint, finance glossary, model card, labeling rubric, and current implementations. Then propose a scoped documentation change before editing.

The revised specification must define:
- users and business workflow;
- primary prediction unit;
- observation cutoff and prediction horizon;
- regression and ranking targets derived from quarter t+1;
- new-position and exit definitions;
- inclusion universe for managers and securities;
- amendment-handling policy;
- baseline models;
- temporal train/validation/test design;
- primary and secondary metrics;
- model-promotion rules;
- explainability and audit requirements;
- known 13F limitations;
- explicit non-advice language;
- which legacy materiality components are retained, renamed, deprecated, or removed.

Keep the scope implementable by one person. Clearly distinguish current behavior from planned behavior. Do not claim unimplemented features.
```

Acceptance criteria:

- one authoritative project specification;
- consistent terminology across documentation;
- objective targets defined mathematically or procedurally;
- no remaining claim that weak labels are independent ground truth;
- open-source attribution policy documented.

---

## Prompt 2 — Open-Source Benchmark Audit

Reference repository:

- `https://github.com/e-izdfr/portfolio-holdings-prediction`

Associated research:

- `https://arxiv.org/abs/2607.12067`

```text
Audit the public portfolio-holdings-prediction repository and associated paper as a potential research reference for WealthSignal.

Do not copy code yet. Inspect its license, data preparation, dataset schema, temporal splits, baselines, models, losses, metrics, configuration, reproducibility, and reported limitations.

Compare it with WealthSignal component by component. Produce a reuse decision table containing:
- component;
- upstream implementation;
- current WealthSignal implementation;
- reuse, adapt, reproduce, or replace decision;
- reason;
- attribution requirement;
- integration risk;
- verification method.

Identify the smallest benchmark that WealthSignal should reproduce first. Separate research reproduction from original extensions and production engineering. Flag anything that is undocumented, non-reproducible, incompatible, or vulnerable to leakage.
```

Acceptance criteria:

- license verified;
- exact reusable concepts identified;
- no blind code import;
- attribution plan written;
- minimal reproduction experiment defined.

---

## Prompt 3 — Dataset Contract and SEC Bulk Ingestion

Official source:

- `https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets`

```text
Design and implement a reproducible historical dataset pipeline using official SEC quarterly Form 13F bulk datasets.

Before editing, inspect the current EDGAR client, parser, models, persistence layer, CLI, local data layout, and tests. Reuse sound components where practical.

Requirements:
- configurable start and end quarters;
- configurable manager and security universe;
- immutable raw-download manifest with source URL, retrieval time, size, and checksum;
- idempotent downloads and processing;
- parsed normalized holdings;
- explicit 13F-HR and 13F-HR/A amendment handling;
- deterministic deduplication;
- typed values and stable identifiers;
- data-quality report per quarter;
- resumable execution;
- respectful SEC access behavior;
- unit tests using small local fixtures;
- no large raw datasets committed to git.

Create a compact sample mode that can run in CI without network access. Document how to run a larger local build.
```

Required measured outputs:

- quarters requested and processed;
- filings processed;
- holdings processed;
- unique managers and securities;
- duplicates resolved;
- amendments resolved;
- invalid rows rejected;
- identifier coverage;
- runtime and output size.

Acceptance criteria:

- rerunning produces the same normalized result;
- interrupted runs can resume;
- malformed fixtures fail clearly;
- every normalized record traces to a source artifact;
- dataset scale is reported from execution, not estimated.

---

## Prompt 4 — Temporal Dataset and Leakage Audit

```text
Build the supervised temporal dataset for next-quarter holdings prediction.

For each manager-security-quarter example, ensure features use only information available through quarter t and targets use quarter t+1. Do not use filing information, aggregate statistics, universe membership, normalization values, or peer features computed from the future.

Implement:
- manager-quarter portfolio reconstruction;
- normalized portfolio weights;
- current and lagged holding ranks;
- holding duration and recency;
- weight and rank momentum;
- manager turnover and concentration features;
- aggregate security ownership features calculated as of t;
- sector exposure where reliable;
- next-quarter weight and rank targets;
- new, exit, increase, and decrease targets;
- explicit missing-quarter behavior;
- expanding-window split manifests;
- negative-sampling policy if required.

Create an automated leakage audit that checks feature timestamps, target timestamps, split boundaries, duplicated events, and manager-quarter overlap. Add tests demonstrating that deliberately leaked fixtures are rejected.
```

Acceptance criteria:

- deterministic dataset build;
- feature dictionary with availability time;
- explicit split files or manifests;
- leakage tests fail when future data is introduced;
- class balance and graph sparsity reported.

---

## Prompt 5 — Baseline Models

```text
Implement strong, reproducible baselines for WealthSignal before adding advanced models.

Required baselines:
1. current-quarter persistence;
2. exponential moving average of historical weights;
3. aggregate institutional-popularity ranking;
4. regularized linear or logistic model;
5. gradient-boosted tree ranking or regression model if dependencies permit.

Use identical temporal folds and candidate universes for every model. Tune only using training and validation periods. Keep the final test period untouched until the comparison protocol is fixed.

Calculate:
- NDCG@10 and NDCG@20;
- Recall@10;
- rank correlation;
- portfolio-weight MAE;
- new-position and exit precision/recall where applicable;
- metrics by quarter and manager cohort;
- training and inference runtime.

Persist configurations, metrics, artifacts, dataset version, and git revision. Add tests for metric correctness and deterministic inference.
```

Acceptance criteria:

- all models evaluated on the same folds;
- persistence is treated as a serious baseline;
- final-test access is controlled and documented;
- no model is called successful solely because its raw metric looks high;
- comparison report includes variability across time.

---

## Prompt 6 — Temporal Graph Representation

```text
Implement a temporal bipartite graph representation for institutional holdings.

Model managers and securities as nodes and reported portfolio weights as time-indexed edges. First create a framework-independent graph dataset with documented schemas. Then add the minimum adapter required by the selected graph-learning implementation.

Requirements:
- stable node mappings;
- quarter-indexed edge snapshots;
- edge weights based on normalized holdings;
- train-only computation for learned encodings or normalization;
- documented treatment of new managers and securities;
- graph statistics per quarter;
- compatibility with baseline evaluation;
- small deterministic graph fixtures;
- memory and runtime measurements.

Do not introduce a graph neural network until the graph dataset and persistence/EMA baselines pass all checks.
```

Acceptance criteria:

- graph snapshots reconcile with normalized holdings;
- no future nodes or edges leak into training features unless explicitly allowed and justified;
- graph construction is reproducible;
- scale and sparsity are measured.

---

## Prompt 7 — Advanced Model Reproduction

```text
Reproduce one advanced temporal graph benchmark for next-quarter portfolio-weight ranking, with explicit attribution to its source.

Before implementation, document:
- source repository and license;
- algorithm and loss function;
- expected inputs and outputs;
- differences between its published dataset and WealthSignal's dataset;
- compute requirements;
- reproduction success criteria.

Integrate it through a clean model interface rather than coupling upstream research code to the entire application. Compare it with persistence, EMA, and the best tabular model on identical temporal folds.

Record configuration, random seeds, environment, dataset hash, git revision, runtime, memory use, and per-fold metrics. If published results cannot be reproduced, report the discrepancy instead of tuning against the final test set.
```

Acceptance criteria:

- attribution retained in code and documentation;
- reproducible command or pipeline stage;
- identical evaluation protocol across models;
- measured rather than asserted improvement;
- resource cost compared with simpler baselines.

---

## Prompt 8 — Original ML Extension

Choose one extension only after the benchmark works.

Recommended options:

- sector-aware graph features;
- manager-style embeddings;
- graph and tabular ensemble;
- calibrated uncertainty estimates;
- cold-start manager evaluation;
- explanation method for predicted holdings;
- robustness under missing or amended filings.

```text
Design one original extension to the reproduced holdings-prediction benchmark.

State a falsifiable hypothesis, such as: adding manager-style and sector-exposure features improves NDCG@10 for high-turnover managers without materially increasing inference latency.

Define:
- treatment and baseline;
- eligible data;
- evaluation folds;
- primary metric;
- secondary and guardrail metrics;
- ablation study;
- resource budget;
- success and failure criteria.

Implement the smallest experiment that tests the hypothesis. Do not combine several unrelated enhancements. Report negative results honestly.
```

Acceptance criteria:

- hypothesis written before final-test evaluation;
- ablation isolates the extension's effect;
- temporal folds are unchanged;
- statistical and operational significance are both discussed.

---

## Prompt 9 — Experiment Tracking and Model Registry

```text
Productionize experiment tracking and model registration for WealthSignal.

Inspect the existing MLflow service and model persistence code before changing it. Ensure every run records:
- run identifier;
- dataset and split-manifest hashes;
- source data cutoff;
- feature schema version;
- code revision;
- model configuration and seed;
- per-fold and aggregate metrics;
- training duration and resource use;
- model artifact checksum;
- evaluation and lineage artifacts.

Implement a model-promotion gate. A model may be marked as a candidate only if data checks, leakage checks, tests, required metrics, and artifact completeness pass. Promotion must not occur merely because a run completed.
```

Acceptance criteria:

- every served model traces to data, code, configuration, and evaluation;
- incomplete runs cannot be promoted;
- registry behavior has tests;
- a clean environment can reproduce a selected run.

---

## Prompt 10 — Forecast and Explanation API

```text
Extend the FastAPI decision service to serve versioned holdings forecasts and explanations.

Required behavior:
- retrieve manager history;
- return next-quarter ranked holdings forecasts;
- expose observed quarter-over-quarter changes separately from forecasts;
- return model version and data cutoff;
- include baseline comparison;
- include feature attribution or explanation where supported;
- return uncertainty or model confidence without overstating probability meaning;
- expose model metrics and governance metadata;
- write a prediction audit record;
- validate inputs and handle unknown managers cleanly.

Add typed request/response models, unit tests, integration tests, and example requests. Measure response latency using a repeatable local benchmark.
```

Suggested endpoints:

- `GET /managers`
- `GET /managers/{cik}/holdings`
- `GET /managers/{cik}/forecast`
- `GET /managers/{cik}/changes`
- `GET /models/latest`
- `GET /models/{version}/metrics`
- `GET /governance/model-card`
- `POST /portfolios/impact`

Acceptance criteria:

- stored forecasts return without retraining;
- every response includes provenance;
- p50 and p95 latency are measured;
- APIs do not describe outputs as trading advice.

---

## Prompt 11 — Portfolio-Impact Layer

```text
Implement a transparent portfolio-impact layer for observed and forecast institutional changes.

Use synthetic client portfolios only, and label them clearly as synthetic. Keep the impact calculation deterministic and auditable. Separate direct security overlap, sector overlap, and client concentration. Do not use the forecast model to produce autonomous investment recommendations.

Return:
- impacted holdings;
- direct overlap;
- sector overlap;
- concentration contribution;
- total impact score;
- reason codes;
- data and model versions;
- limitations.

Put configurable weights and thresholds in visible policy configuration rather than hardcoding unexplained constants. Add edge-case tests for empty portfolios, unknown identifiers, zero weights, and concentrated portfolios.
```

Acceptance criteria:

- calculations reconcile with inputs;
- policy configuration is versioned;
- synthetic data is never presented as real client data;
- explanations identify the components of every score.

---

## Prompt 12 — Monitoring, Drift, and Reliability

```text
Add production-style monitoring for data, model, and service behavior.

Measure:
- ingestion successes, failures, retries, and duration;
- records accepted and rejected;
- missing identifier and sector rates;
- manager and security coverage;
- feature distribution changes;
- graph density and turnover changes;
- ranking performance when next-quarter truth arrives;
- forecast distribution changes;
- API request count, failures, and latency;
- model version currently served.

Create deterministic drift fixtures and tests. Define warning and blocking thresholds with reasons. Add health and readiness checks. Scheduled ingestion must be idempotent and safe to retry.
```

Acceptance criteria:

- monitoring distinguishes data drift from service failure;
- alerts have documented thresholds;
- retry behavior is tested;
- monitoring output can be demonstrated locally.

---

## Prompt 13 — Testing and Reproducibility Hardening

```text
Audit WealthSignal for production-readiness gaps and implement a focused testing and reproducibility milestone.

Cover:
- parser and amendment fixtures;
- dataset idempotency;
- feature availability timing;
- temporal split boundaries;
- deliberate leakage rejection;
- metric correctness;
- training/inference feature parity;
- model serialization consistency;
- API schemas and error responses;
- database persistence;
- Docker health checks;
- clean-environment setup.

Measure test count, pass rate, core-module coverage, runtime, and any skipped or flaky tests. Do not chase a coverage number by testing trivial lines; prioritize financial and temporal correctness.
```

Acceptance criteria:

- one documented command runs the core checks;
- no flaky tests in repeated local runs;
- critical pipeline paths have meaningful coverage;
- a clean checkout can run the sample pipeline.

---

## Prompt 13A — Large-Scale PySpark and SQL Pipeline

```text
Implement and benchmark the large-scale analytical-data path for WealthSignal using PySpark and PostgreSQL.

PySpark responsibilities:
- read partitioned SEC quarterly data;
- apply explicit schemas;
- normalize and deduplicate holdings;
- resolve amendments using deterministic window logic;
- construct manager-quarter totals and normalized weights;
- generate lagged features and next-quarter targets;
- write partitioned Parquet analytical tables;
- run in a small local mode and a configurable larger mode.

SQL responsibilities:
- create normalized analytical tables and indexes;
- implement representative manager-history, security-ownership, and forecast queries;
- use window functions and appropriate materialized summaries;
- capture `EXPLAIN ANALYZE` before and after optimization;
- test that optimized queries return identical results.

For CPU-heavy local parsing, compare sequential and multiprocessing execution. For independent bounded I/O, test controlled multithreading while respecting SEC access rules. Report hardware, input scale, runtime, throughput, memory, and speedup. Do not claim distributed scale from a tiny sample.
```

Acceptance criteria:

- local sample pipeline is reproducible;
- Spark and non-Spark sample outputs reconcile;
- partitions and schemas are documented;
- parallel processing is deterministic;
- query optimization has measured evidence;
- no unbounded concurrency against SEC systems.

---

## Prompt 13B — Statistical Modeling Breadth and SAS Validation

```text
Create a leakage-safe classical-statistics benchmark suite for WealthSignal.

Using the same temporal folds and analytical table, implement:
- binary logistic regression for new-position and exit targets;
- multinomial logistic regression for holding actions;
- linear or regularized regression for next-quarter portfolio weight;
- linear and quadratic discriminant analysis as classification baselines;
- PCA for correlated manager and exposure features;
- exploratory factor analysis for interpretable manager-style factors;
- multivariate diagnostics for correlation, covariance, collinearity, and residual behavior;
- persistence, exponential smoothing, and at least one statistically appropriate time-series benchmark.

Use scikit-learn and appropriate Python statistical libraries for the primary reproducible implementation. Export a stable analytical table and create versioned SAS programs that reproduce selected descriptive statistics, logistic/multinomial models, PCA or factor analysis, and scoring validation.

If no SAS runtime is available, validate the SAS source structurally, document the required SAS product/procedures, and clearly mark execution as pending. Never report SAS output that was not actually produced.

Generate Matplotlib and Seaborn figures for class balance, correlations, PCA variance, factor loadings, calibration, residuals, temporal performance, and baseline comparisons.
```

Acceptance criteria:

- every method answers a documented analytical question;
- all predictive comparisons use identical temporal boundaries;
- assumptions and limitations are explained;
- redundant or invalid methods are not presented as competitive merely to satisfy a checklist;
- Python and executed SAS results reconcile within documented tolerance;
- figures are generated from saved evaluation artifacts.

---

## Prompt 14 — Dashboard and Demo

```text
Design and implement a minimal portfolio-quality WealthSignal demo.

The demo should help a reviewer answer four questions:
1. What data was ingested?
2. What did the manager hold historically?
3. What does the model forecast for the next quarter, and why?
4. How well did it perform against simple baselines?

Include:
- dataset scale and cutoff;
- manager history;
- predicted versus actual top holdings for completed test quarters;
- baseline comparison;
- per-quarter evaluation;
- explanation view;
- synthetic portfolio-impact example;
- model version and limitations.

Do not display invented live predictions, fake client data, or annualized investment returns. Optimize for a five-minute interview demonstration and clear failure handling.
```

Acceptance criteria:

- a reviewer can run the demo from documented steps;
- all displayed metrics trace to saved evaluation artifacts;
- the demo makes baseline comparison visible;
- limitations are visible rather than hidden.

---

## Prompt 14A — Interactive Excel Report

```text
Create a verified interactive Excel report from committed or reproducibly generated WealthSignal result extracts.

The workbook should include:
- an instructions and data-cutoff sheet;
- manager and quarter selectors;
- actual versus predicted top holdings;
- model and baseline comparison;
- data-quality summary;
- portfolio-impact example using explicitly synthetic holdings;
- pivot tables or pivot-ready structured tables;
- XLOOKUP where supported and an INDEX/MATCH example for compatibility;
- conditional formatting, charts, filters, and frozen headers;
- formulas that remain auditable rather than pasted results;
- source and model-version fields.

If VBA adds genuine value, create a small reviewed macro for refresh, reset filters, or navigation. Store the VBA source in the repository, document how to import it, and include a macro-security note. Do not require macros for core workbook correctness.

Use the spreadsheet artifact workflow to render and visually verify every sheet. Check formulas, totals, clipping, filters, and chart readability before delivery.
```

Acceptance criteria:

- displayed numbers reconcile with source evaluation artifacts;
- no formula errors;
- interactive features have instructions;
- workbook works without VBA for core analysis;
- any macro source is transparent and optional.

---

## Prompt 14B — Portfolio PowerPoint

```text
Create a concise interview-ready PowerPoint presentation for WealthSignal using verified repository artifacts only.

Include approximately 8–10 slides:
1. problem and user;
2. Form 13F data and limitations;
3. end-to-end architecture;
4. data scale and quality;
5. objective targets and leakage-safe temporal design;
6. models from statistical baselines through graph ML;
7. measured comparison with persistence;
8. production engineering, monitoring, and lineage;
9. demo workflow;
10. results, limitations, and future work.

Use actual generated figures and metrics. Include attribution for open-source research. Add speaker notes that help the project owner explain every slide without overstating finance expertise or model performance. Render and inspect every slide before finalizing.
```

Acceptance criteria:

- every quantitative claim traces to an artifact;
- visuals are readable and consistent;
- open-source contributions are attributed;
- presentation fits a five-to-seven-minute walkthrough;
- speaker notes explain likely interview questions.

---

## Prompt 15 — Final Governance and Portfolio Review

```text
Perform a final evidence-based review of WealthSignal as an AI/ML portfolio project. Do not edit initially.

Evaluate:
- problem clarity;
- objective label integrity;
- temporal leakage prevention;
- baseline strength;
- model comparison quality;
- reproducibility;
- explainability;
- auditability and lineage;
- data and model monitoring;
- API and deployment quality;
- financial limitations;
- open-source attribution;
- demonstration readiness;
- whether every README claim is supported by code or an artifact.

Produce a severity-ranked gap list. Then, only after approval, fix the highest-value gaps. Remove or rewrite claims that cannot be verified.
```

Acceptance criteria:

- no unsupported performance claims;
- no ambiguous ownership of open-source work;
- no use of subjective labels as objective truth;
- reproducible demo and evaluation;
- complete limitations and model card.

---

## Prompt 16 — Generate Verified Resume Metrics

Use this only after the implementation and final evaluation are complete.

```text
Generate resume-ready outcomes for WealthSignal using only repository evidence and saved execution artifacts.

Inspect dataset manifests, evaluation reports, MLflow records, test and coverage output, API benchmark results, monitoring artifacts, and git history. Do not estimate or round upward. If a metric is unavailable, say so and provide the exact command or implementation needed to measure it.

Create:
1. a verified metrics table containing value, unit, source artifact, and reproduction command;
2. three concise resume bullets for an AI/ML engineering role;
3. one project description for a portfolio page;
4. a 30-second interview explanation;
5. a two-minute technical explanation;
6. a list of claims that must not be made.

Every number in the resume bullets must trace to a committed artifact or reproducible command.
```

---

## Metrics That Must Be Captured Automatically

### Dataset metrics

- number of source quarterly packages;
- date range and cutoff;
- filings processed;
- unique managers;
- unique securities;
- normalized holdings;
- temporal graph edges;
- rejected records;
- duplicates and amendments resolved;
- CUSIP or reference-data coverage;
- data-quality pass rate;
- ingestion runtime and output size.

### Model metrics

- models compared;
- temporal folds;
- NDCG@10 and NDCG@20;
- Recall@10;
- rank correlation;
- portfolio-weight MAE;
- new-position precision and recall;
- exit precision and recall;
- improvement over persistence and EMA;
- per-quarter variability;
- training time and peak memory;
- inference latency.

### Engineering metrics

- automated test count and pass rate;
- core-module coverage;
- pipeline runtime;
- API p50 and p95 latency;
- data-quality rule count;
- retry/recovery test results;
- reproducibility status from a clean checkout;
- model-lineage completeness;
- drift scenarios detected.
- PySpark records processed per second and partition count;
- sequential versus multithreaded or multiprocessing runtime;
- SQL query latency before and after indexing or query optimization;
- SAS validation status and reconciliation tolerance;
- Excel formula, pivot, and visual verification status;
- PowerPoint slide-render verification status.

### Recommended metrics artifact

Ask Codex to maintain a machine-readable artifact such as:

```text
artifacts/project_metrics.json
```

It should be generated from source artifacts, never manually populated with aspirational values.

---

## Resume Bullet Templates

Use only after replacing placeholders with verified results:

```text
- Built an end-to-end temporal ML platform processing [X] SEC Form 13F filings across [Y] institutional managers and [Z] quarters, producing a versioned manager-security graph with [N] ownership edges.

- Developed and evaluated [M] holdings-forecasting models using expanding-window validation, achieving [NDCG] NDCG@10 and [DELTA]% improvement over the persistence baseline on the untouched holdout period.

- Productionized model training and inference with MLflow, FastAPI, PostgreSQL, Docker, and CI, achieving [LATENCY] ms p95 API latency, [COVERAGE]% core-module test coverage, and complete source-to-prediction lineage.
```

If the advanced model does not beat persistence, use an honest alternative:

```text
- Benchmarked [M] temporal, graph, and tabular forecasting approaches across [F] walk-forward folds, demonstrating that a persistence baseline remained strongest and documenting model accuracy, stability, latency, and failure modes through reproducible evaluation artifacts.
```

A rigorous negative result is better than a fabricated improvement.

---

## Interview Understanding Checklist

Before listing the project on a resume, be able to explain:

### Finance

- what a Form 13F reports;
- why it is delayed;
- what it omits;
- CIK, CUSIP, holding, portfolio weight, new position, and exit;
- why a forecast is not investment advice.

### Machine learning

- why the target is objective;
- why random splitting would leak time information;
- why persistence is a strong baseline;
- what NDCG@10 measures;
- how negative candidates are selected;
- why graph structure may help;
- how uncertainty and drift are handled.

### Engineering

- how data moves from the SEC to the API;
- how amendments and duplicate filings are handled;
- how a prediction traces back to its source data;
- how models are compared and promoted;
- how the pipeline retries safely;
- how the system is tested and monitored.

### Open-source integrity

- which repository and paper were referenced;
- what their licenses permit;
- which components were reproduced or adapted;
- which components and extensions are original;
- where attribution is recorded.

---

## Questions to Ask Codex When Something Is Confusing

### Plain-language explanation

```text
Explain this component as if I know Python and basic ML but have no finance background. Cover what goes in, what comes out, why it exists, one concrete example, its main failure mode, and the file where it is implemented.
```

### Trace one record

```text
Trace one representative manager-security event from the raw SEC record through normalization, feature generation, model input, prediction, evaluation, database persistence, and API response. Show the actual identifiers and values from a local fixture or small sample. Do not modify files.
```

### Explain a metric

```text
Explain [METRIC] using one real WealthSignal evaluation example. Show how it is calculated, why it is appropriate, what a naive baseline achieves, and what the metric does not prove.
```

### Find leakage

```text
Try to falsify the claim that WealthSignal's evaluation is leakage-free. Trace every feature's availability time, universe construction, normalization, target creation, model selection, and final-test access. Report evidence and counterexamples before proposing fixes.
```

### Review a pull request or milestone

```text
Review the current branch against milestone [NAME]. Focus on correctness, temporal leakage, data integrity, reproducibility, attribution, test quality, and unsupported documentation claims. Do not edit. Rank findings by severity and cite exact files and lines.
```

### Prepare for an interview

```text
Act as an interviewer for an applied AI/ML role. Ask me one WealthSignal question at a time, starting with the business problem and progressing through data engineering, temporal validation, models, explainability, deployment, monitoring, and limitations. After each answer, identify inaccuracies and help me improve it without giving me a script I cannot defend.
```

---

## Research Paper Relationship and Attribution

The research reference is **Institutional Equity Holdings Prediction Using Node Affinities of Dynamic Graphs** by Emad Izadifar and Zahed Rahmati (arXiv:2607.12067, 2026), with the authors' MIT-licensed implementation at `https://github.com/e-izdfr/portfolio-holdings-prediction`.

The paper represents quarterly 13F portfolios as a temporal bipartite graph:

- manager nodes on one side;
- security nodes on the other;
- a weighted manager-security edge representing portfolio weight in a quarter;
- next-quarter edge affinity/weight and ranking as the prediction problem.

NAVIS, **Node Affinity prediction using Virtual State**, is the paper's model; it is not the name of the graph representation. The paper reports experiments over 99 managers, 503 S&P 500 securities, 209,351 temporal edges, and 48 quarters from 2013 through 2025. Its reported NDCG is 0.9127 for NAVIS with features, 0.9121 without features, 0.8891 for persistence, and 0.8882 for EMA. These numbers are research-reference values, not WealthSignal results.

Keep two lanes visibly separate:

1. **Reproduction lane:** reproduce the published task, model, data policy, split policy, and metrics as closely as access allows. Pin the upstream commit, preserve the MIT license and notices, and document every deviation.
2. **WealthSignal extension lane:** retain the project's own official-SEC ingestion, immutable manifests, amendment handling, leakage controls, governance, serving, monitoring, portfolio impact, current-data evaluation, and one original modeled improvement.

Portfolio-safe wording after the work is complete: “Reproduced and extended the NAVIS institutional-holdings forecasting approach in an independently engineered SEC 13F pipeline.” Do not claim to have invented NAVIS, and do not claim reproduction until the upstream experiment has actually been run and reconciled.

---

## Revised Executable Milestones From the Current Stop Point

Run these prompts in order. Each prompt is intentionally bounded so Codex can implement, test, and leave a reviewable checkpoint.

### Milestone 0 — Preserve and Reconcile Protocol V1

Status: completed and checkpointed locally in commit `14c5d01`.

```text
Read docs/CODEX_PROJECT_NAVIGATION.md, docs/WealthSignal_Final_Test_Report_V1.md, docs/WealthSignal_Candidate_Universe_Sensitivity.md, and docs/ai-governance/forecast-comparison-protocol-v1.md completely. Inspect the full uncommitted diff and current Git status.

Do not rerun the consumed final test and do not change model choices, features, candidate policy, thresholds, or reported results. Reconcile README and project documentation with the saved V1 artifacts, especially stale statements that forecasting is unimplemented or the final test is still locked. Run the existing test and compilation checks in the declared environment. Verify all generated metrics against saved artifacts. Report unrelated user changes separately and do not overwrite them.

Deliver a clean, reviewable V1 checkpoint proposal, including the exact files to commit, evidence that the 56 tests pass, and any unresolved reproducibility risk. Do not push or open a pull request unless explicitly asked.
```

### Milestone 1 — Forecast Persistence and End-to-End Lineage

Status: implemented locally; review and checkpoint pending.

```text
Inspect the temporal dataset, forecasting baseline, persistence schemas, CLI, and API. Implement forecast-specific persistence for the honest V1 persistence model without reusing the consumed test for tuning.

First fix forecast prediction records so every row retains example_id, manager CIK, security_key, CUSIP, feature/source cutoff quarter, target quarter, predicted weight, and predicted rank. Add idempotent forecast-run and forecast-prediction storage for SQLite and PostgreSQL. Persist the dataset ID and manifest checksum, source package/filing lineage, protocol version and checksum, model contract/version, code revision when available, generation timestamp, run status, and limitations. Do not treat target_weight or future truth as an input to a served forecast.

Add migrations or compatibility logic, transaction safety, indexes justified by serving queries, unit/integration tests, and a compact CLI path that materializes one forecast run twice and proves idempotency. Keep legacy materiality model tables separate. Update documentation and report row counts, reconciliation checks, query timing, and test results.
```

### Milestone 2 — Forecast API and Honest Product Surface

Status: implemented locally; review and checkpoint pending.

```text
Using the lineage-aware forecast tables, implement typed API endpoints to list forecast runs, retrieve one run with provenance, and retrieve a manager's ranked forecast for a target quarter with pagination. Clearly label observed holdings, observed changes, and predicted future weights as different concepts.

Return manager and security identifiers, predicted weight/rank, source cutoff, target quarter, model/dataset/protocol identities, generated_at, and limitations. Exclude the failed action classifiers from alerts and client decisions. Add deterministic contract tests, error and pagination tests, a query-plan check, and a measured local latency benchmark. Update the API documentation with one traceable example from source filing to returned forecast.
```

### Milestone 3 — Current-Data Protocol V2

Status: design frozen locally before download; review and checkpoint pending.

```text
Design Protocol V2 before downloading or evaluating new data. Inventory the latest complete official SEC 13F bulk packages available at execution time, then declare a larger manager cohort, historical coverage, security-universe policy, source cutoff, validation windows, and a brand-new untouched final or prospective window.

Create new immutable dataset and split IDs; never mutate V1 artifacts or reuse V1's consumed test. Address amendments, missing manager-quarters, corporate actions, security identity, and survivorship bias from using today's S&P 500 membership. Run sensitivity studies only on validation windows. Freeze and checksum Protocol V2 before its one-time final evaluation. Report coverage, scale, leakage, missingness, compute cost, and comparability with both V1 and the paper.
```

### Milestone 4 — Graph Adapter and Baseline Reconciliation

```text
Implement a framework-independent temporal bipartite graph adapter from the immutable manager-security-quarter data. Managers and securities are nodes; a dated weighted edge represents an observed portfolio weight. Preserve manager CIK, security_key/CUSIP, report quarter, availability cutoff, and dataset lineage.

Add tests proving each graph snapshot reconciles to the corresponding normalized portfolios: node/edge counts, per-manager weight sums, identities, time ordering, and no future edges before cutoff. Recompute persistence and EMA through the graph adapter and reconcile their metrics with the tabular implementation within documented tolerances. Do not add NAVIS or a graph library until this contract passes.
```

### Milestone 5 — NAVIS Reproduction

```text
Audit the NAVIS paper and official MIT-licensed repository. Pin the paper version and upstream commit hash, preserve attribution/license notices, and write a reproduction matrix covering data universe, quarters, features, candidate construction, splits, loss, hyperparameters, metrics, seeds, and hardware. Mark exact matches, approximations, and unavailable inputs.

Implement NAVIS behind the graph-adapter contract, preferably as an isolated research module rather than coupling upstream code directly to the API. Compare NAVIS, persistence, and EMA under one frozen Protocol V2. Include multiple seeds, runtime/memory, confidence intervals or bootstrap uncertainty where appropriate, and ablations with/without node features. Do not promote NAVIS unless it passes a predeclared gate on untouched data.
```

### Milestone 6 — One Original WealthSignal Extension

```text
After the reproduction is credible, propose three bounded original extensions and select one using validation-only evidence, implementation cost, and portfolio relevance. Good candidates include candidate retrieval for unseen positions, graph-plus-tabular residual modeling, uncertainty-calibrated rankings, temporal manager embeddings, historically correct index membership, or cold-start handling.

Write a hypothesis and frozen ablation plan before implementation. Compare the extension with NAVIS, persistence, and EMA on identical examples and splits. Report effect size, uncertainty, compute cost, failure cases, and whether the added complexity earned promotion. Keep failed experiments as documented evidence rather than rewriting the story.
```

### Milestone 7 — Scale, Product Evidence, and Portfolio Packaging

```text
Scale the immutable transformations with PySpark and optimized SQL only after the single-node contract is stable. Benchmark results against the Python reference for exact reconciliation, wall-clock time, memory, partitions, and query plans. Add monitoring for data freshness, missing quarters, drift, forecast reconciliation, and lineage completeness.

Generate the dashboard, advanced Excel report, PowerPoint case study, architecture diagram, model card, and resume metrics only from saved verified artifacts. Separate paper-reported metrics from WealthSignal-measured metrics. Produce a portfolio narrative covering the problem, data, leakage controls, baselines, NAVIS reproduction, original extension, deployment, measurable outcomes, limitations, and next experiment.
```

### Extension Brainstorm Backlog

Do not implement all of these. After Milestone 5, choose one primary research contribution and keep the rest as future work:

- learned retrieval of plausible new-position candidates;
- graph-plus-gradient-boosting residual ensemble;
- uncertainty-aware top-k forecasts and abstention;
- manager-style embeddings and peer-cluster transfer;
- point-in-time S&P 500 membership to remove survivorship bias;
- cold-start evaluation for unseen managers or securities;
- drift detection across filing regimes and market conditions;
- counterfactual client-portfolio exposure and risk impact;
- prospective evaluation when the next SEC quarter becomes available.

---

## Definition of Done

WealthSignal is portfolio-ready only when:

- the primary target is derived objectively from future filings;
- historical data construction is reproducible and traceable;
- amendments, duplicates, and missing quarters have explicit policies;
- temporal leakage tests pass;
- strong baselines are implemented;
- advanced models use the same evaluation protocol;
- final-test results are untouched by tuning;
- all reported metrics come from saved artifacts;
- model versions trace to data, code, and configuration;
- APIs return provenance and limitations;
- tests cover critical temporal and financial logic;
- a clean environment can run a compact demonstration;
- open-source code and ideas are attributed;
- the project owner can explain the system and its limitations;
- resume claims are generated only from verified measurements.
- classical statistical models and advanced models share a fair evaluation protocol;
- PySpark scale and SQL optimization claims have benchmark evidence;
- SAS work is labeled executed or unexecuted accurately;
- Excel and PowerPoint outputs reconcile with saved project metrics.

---

## Recommended Immediate Starting Prompt

Copy this into a new Codex task opened in the WealthSignal repository:

```text
Read docs/CODEX_PROJECT_NAVIGATION.md and the three Protocol V2 governance files completely. Review the frozen Protocol V2 design and execute only its development-data acquisition and training-period cohort-selection stage.

Treat Protocol V1's final test as consumed and Protocol V2's 2026 Q2 target as unavailable and untouched. Verify the protocol/config checksums before downloading. Acquire only the declared development packages through May 2026, derive the 25-manager cohort using only 2019 Q1–2023 Q4 report quarters, and persist its ordered CIK list and checksum. Do not acquire or inspect the future 2026 Q2 truth source.
```
