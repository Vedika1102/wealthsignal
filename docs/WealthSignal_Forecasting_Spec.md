# WealthSignal Product and ML Specification

## Status and authority

This document is the authoritative product and machine-learning specification for WealthSignal. It supersedes the original materiality-classification framing in `README.md`, `docs/WealthSignal_V1_Blueprint.md`, and the materiality classifier model card wherever those documents conflict with this specification.

The repository implements the historical bulk-data, manager-security-quarter target, leakage-audit, and Protocol V1 baseline-evaluation portions of this specification. Protocol V1's final test is consumed, persistence is the reference winner, and no learned model was promoted. Forecast-specific database persistence, APIs, monitoring, current-data Protocol V2, and advanced research models remain planned. The legacy observed-change, weak-label classification, and synthetic portfolio-impact runtime remains available but separate from objective forecasting.

## Product objective

WealthSignal is an auditable institutional-holdings forecasting and portfolio-intelligence platform built from public SEC Form 13F filings.

Its primary question is:

> Given all information available through reporting quarter `t`, which securities will have the highest reported portfolio weights for an institutional manager in quarter `t+1`?

The system is intended for analysts, model reviewers, and wealth-management technology teams who need to inspect institutional holding histories, compare forecasts with simple baselines, detect unusual observed changes, and understand possible relevance to explicitly synthetic client portfolios.

WealthSignal does not predict security returns, provide investment advice, claim real-time knowledge, or infer a manager's complete economic exposure.

## User workflow

1. An operator builds a versioned historical 13F dataset through a declared cutoff.
2. The pipeline reconstructs each manager's reported quarterly portfolio and creates leakage-safe forecasting examples.
3. Baselines and candidate models train only on eligible historical quarters.
4. A reviewer compares ranking, weight, and action metrics on fixed temporal folds.
5. A promoted model generates stored next-quarter holdings forecasts with data and model provenance.
6. An analyst reviews forecasts separately from observed quarter-over-quarter changes.
7. Optional portfolio-impact analysis uses clearly labeled synthetic portfolios and deterministic policy components.

## Prediction unit and timing

The atomic supervised example is a `manager-security-quarter` tuple `(m, s, t)`.

- `m` is a manager identified by normalized CIK.
- `s` is a security represented by the project's versioned security-identity policy.
- `t` is the report-calendar quarter, not the filing accession order.
- Features may use only information whose availability timestamp is on or before the observation cutoff for `t`.
- Targets come only from the resolved filing for `t+1`.

The default observation cutoff is the timestamp at which the selected filing for quarter `t` became available from the SEC. A model run must record its source-data cutoff. Peer aggregates, normalization statistics, candidate universes, and encodings must be constructed without using information first observed after that cutoff.

The prediction horizon is one reported quarter. Missing intervening quarters must not be silently treated as exits; they require an explicit eligibility or missing-quarter status.

## Portfolio reconstruction

For manager `m` and quarter `t`, define reported portfolio value as:

`V(m,t) = sum_s value(m,s,t)`

and normalized holding weight as:

`w(m,s,t) = value(m,s,t) / V(m,t)`

when `V(m,t) > 0`.

Rank securities within each manager-quarter by normalized weight, using a documented deterministic tie-breaker. The analytical dataset must retain the selected filing accession, report period, filing date, source-artifact reference, and dataset version.

## Objective targets

All primary targets are derived procedurally from the resolved filing for `t+1`.

### Weight regression

`target_weight(m,s,t) = w(m,s,t+1)`

Eligible candidate securities absent from the resolved `t+1` portfolio receive weight zero. Rows whose next quarter is unavailable or ineligible receive no target and must not enter supervised evaluation.

### Holdings ranking

The ranking target is the ordering induced by `target_weight`. Evaluation compares predicted and observed top holdings within the same declared candidate universe.

### Holding rank

`target_rank(m,s,t)` is the deterministic rank in the resolved `t+1` portfolio. Securities absent at `t+1` receive a documented out-of-portfolio rank or missing indicator rather than an ambiguous null.

### Holding actions

- `new`: weight is zero at `t` and positive at `t+1`.
- `exit`: weight is positive at `t` and zero at `t+1`.
- `increase`: weight is positive in both quarters and increases beyond a documented numerical tolerance.
- `decrease`: weight is positive in both quarters and decreases beyond that tolerance.
- `unchanged`: weight is positive in both quarters and the absolute change is within tolerance.

The tolerance belongs in versioned configuration and must not be embedded as an unexplained formula constant.

## Manager and security universe

Initial implementation should remain feasible for one developer:

- Use managers with a configurable minimum number of consecutive, resolved quarterly filings.
- Record all included and excluded managers with reason codes.
- Build the security universe from securities observable through `t`; never use future ownership to decide eligibility at `t`.
- Include current holdings plus a documented negative-candidate strategy for possible new positions.
- Version CUSIP normalization, issuer mapping, and any ticker or sector enrichment.
- Treat unresolved identifiers as explicit quality states rather than silently merging them.

The first reproducible sample may use a small manager set, but reported results must state the actual scale and must not imply broad market coverage.

## Filing and amendment policy

The historical dataset must define one effective filing per manager and report quarter.

- Preserve every original `13F-HR` and `13F-HR/A` artifact and its metadata.
- Resolve amendments deterministically using filing type, filing date, amendment type when available, and accession number.
- Do not assume every amendment is a complete replacement; distinguish restatements from additions when SEC metadata permits.
- Record the selected accession and all superseded accessions in the dataset manifest.
- Make resolution idempotent and test ordinary filings, restatements, additions, duplicates, and malformed records.

The current repository's filing-selection behavior is not yet the authoritative amendment policy and must be reviewed during the historical dataset milestone.

## Baseline models

Every candidate model must be compared on identical examples, candidate universes, and temporal folds against:

1. current-quarter persistence;
2. exponential moving average of historical weights;
3. aggregate institutional-popularity ranking computed as of `t`;
4. regularized linear regression for next-quarter weight;
5. regularized logistic models for new-position and exit targets.

Tree-based, sequence, or graph models are additions to these baselines, not replacements for them.

## Temporal evaluation design

- Split by explicit report quarter, never by random row assignment or lexicographic accession order.
- Use expanding-window folds: train on earlier quarters, validate on a later quarter or block, and move forward in time.
- Managers may appear across folds, but no manager-quarter may cross split boundaries.
- Fit preprocessing, encodings, candidate construction, and normalization on eligible training information only.
- Fix the final test period and keep it untouched by feature selection, hyperparameter tuning, model selection, and threshold selection.
- Persist fold manifests containing report periods, row identifiers, manager counts, candidate counts, dataset hash, and creation configuration.
- Run an automated leakage audit before training or evaluation.

## Metrics

Primary ranking metrics:

- NDCG@10;
- NDCG@20;
- Recall@10.

Primary weight metric:

- mean absolute error for next-quarter normalized weight.

Secondary metrics:

- Spearman rank correlation;
- RMSE for normalized weight;
- new-position precision and recall;
- exit precision and recall;
- per-quarter and manager-cohort results;
- training time, inference time, and peak memory.

Every comparison must include variability across temporal folds and the corresponding persistence and EMA results. A model is not successful merely because one aggregate metric is high.

## Model promotion rules

A run may become a candidate only when:

- dataset and split manifests are complete and hashed;
- data-quality and leakage checks pass;
- required baselines were evaluated on identical folds;
- required metrics and per-fold results are present;
- training and inference feature schemas match;
- the artifact, configuration, code revision, and source cutoff are recorded;
- deterministic reload and inference tests pass;
- limitations and failed checks are visible.

Promotion to a served model requires reviewer approval. Completion of training or superiority on one metric is insufficient.

## Explainability and audit requirements

Every stored forecast must trace to:

- manager, security, and target quarter;
- source-data cutoff and selected filing accessions;
- dataset and split-manifest versions;
- feature-schema version;
- model name, version, configuration, and artifact checksum;
- code revision;
- predicted weight or rank and applicable confidence information;
- baseline prediction;
- explanation or feature attribution when supported;
- creation timestamp and prediction audit identifier.

Explanations must describe model inputs and comparative signals without implying knowledge of manager intent.

## Observed changes and portfolio impact

Observed quarter-over-quarter changes remain useful but are separate from forecasting:

- The deterministic materiality score is retained and renamed conceptually as an `observed-change severity policy`.
- Alerts may summarize large observed changes after a filing is available.
- Portfolio impact must separate direct security overlap, sector overlap, and concentration contribution.
- Client examples remain explicitly synthetic unless a future authorized data source changes that scope.
- Outputs must not be framed as autonomous recommendations or investment advice.

## Legacy component disposition

| Existing component | Disposition | Required treatment |
|---|---|---|
| SEC client, parser, storage, normalized holdings | Retain and harden | Reuse in the historical dataset pipeline |
| Position delta engine | Retain | Use for observed-change analysis and historical features where leakage-safe |
| Rule materiality score | Retain and rename | Observed-change severity policy, not predictive truth |
| `weak_label` | Deprecate as an ML target | Preserve only for legacy reproducibility and migration tests |
| Materiality classifiers | Deprecate from primary path | Do not use their metrics as forecasting evidence |
| Manual materiality gold set | Retain as optional policy evaluation | Not the principal forecasting evaluation dataset |
| Weak-label precedent recommendations | Deprecate from primary path | Do not present as forecast explanations |
| Synthetic portfolio impact | Adapt | Keep deterministic, auditable, and separate from advice |
| Existing model-run schema and API | Adapt substantially | Add forecasting targets, folds, lineage, provenance, and audit records |

Legacy endpoints and commands may remain temporarily for backward compatibility, but documentation and responses must identify them as legacy until they are removed or replaced.

## Known Form 13F limitations

- Filings are delayed and represent a past quarter-end snapshot.
- Only reportable long positions are included; shorts, many derivatives, cash, and non-reportable assets are omitted.
- Reported value is not a complete measure of economic exposure or investment intent.
- Amendments, identifier changes, corporate actions, and manager structure can disrupt longitudinal identity.
- Apparent changes may reflect reporting mechanics rather than active decisions.
- Forecast accuracy does not establish investment value or expected return.

## Open-source attribution policy

- Record every external repository, paper, dataset, and material implementation idea before adaptation.
- Verify and retain the applicable license, copyright notice, and citation requirements.
- Distinguish direct reuse, adaptation, independent reproduction, and original WealthSignal extensions.
- Do not copy code until compatibility, attribution, data assumptions, and leakage risks have been reviewed.
- Preserve attribution in source files and documentation wherever an upstream implementation materially influenced the result.
- Report reproduction failures or deviations instead of implying that published results were matched.

## Implementation sequence

1. Approve this specification and align legacy documentation.
2. Build reproducible official SEC historical bulk ingestion with manifests and quality reports.
3. Build the manager-security-quarter dataset, objective `t+1` targets, split manifests, and leakage audit.
4. Implement persistence, EMA, popularity, and regularized statistical baselines.
5. Add advanced models only after fair baseline evaluation works.
6. Add lineage-aware forecast persistence and APIs.
7. Produce Excel, presentation, dashboard, and resume artifacts only from verified result extracts.

## Acceptance conditions for the specification milestone

- This file is the sole authoritative ML specification.
- README and legacy governance documents distinguish current behavior from the target system.
- Objective targets and temporal boundaries are defined procedurally.
- No documentation presents weak labels as independent predictive truth.
- Materiality remains only a secondary observed-change policy and optional policy-evaluation problem.
- Planned functionality is not described as implemented.
