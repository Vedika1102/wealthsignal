# WealthSignal Materiality Classifier Model Card

> **Legacy model card.** This classifier reproduces a weak-label materiality policy and is not the primary WealthSignal ML system. Its outputs and metrics are not evidence of next-quarter holdings forecasting performance. See `docs/WealthSignal_Forecasting_Spec.md` for the authoritative product and ML specification.

## Purpose

The WealthSignal materiality classifier ranks quarter-over-quarter 13F position changes by the likelihood that an advisor should review them. It is intended to support analyst triage, not to automate trading, portfolio construction, or client communication without human review.

## Training Data

- Source: persisted `position_features` rows derived from real SEC 13F filings already ingested by WealthSignal
- Label: `weak_label`, generated from deterministic rules that emphasize large weight shifts, top-rank entries/exits, and high rule-based materiality scores
- Features: the existing 20 engineered position-change features, including weight deltas, value deltas, rank transitions, turnover ratio, and turnover share
- Legacy validation split: 3 folds constructed from lexicographically sorted filing accessions. This is not an acceptable report-quarter forecasting split and must not be reused for the primary forecasting path.

## Candidate Models

- `numpy-logistic-baseline`
- `logistic_regression`
- `decision_tree`
- `random_forest`
- `xgboost` when the optional dependency is available

The legacy advanced model is selected by PR-AUC and persisted as a joblib artifact for backward-compatible API loading. It must not be promoted as a holdings-forecasting model.

## Metrics

Each advanced candidate records:

- accuracy
- precision
- recall
- F1
- PR-AUC
- calibration curve points

Metric values are stored in the `model_runs` table per training run and exposed through `GET /models/latest`.

## Explainability

- Rule-based materiality scoring remains available and should be reviewed alongside ML output.
- Logistic models retain coefficient-level interpretability.
- XGBoost runs store top SHAP feature-importance summaries when SHAP is installed.

## Limitations

- Labels are weakly supervised and inherit the bias of the heuristic rules used to create them.
- The model learns a target generated partly from the same engineered change signals it receives as features; its results measure policy imitation rather than objective future-outcome prediction.
- Reported metrics are calculated for the legacy materiality task and cannot be compared with forecasting metrics such as NDCG, Recall@K, rank correlation, or next-quarter weight error.
- Training data is limited to already ingested 13F filings and may underrepresent less common sectors, filing styles, or market regimes.
- 13F data is delayed and omits positions that are not reportable under the filing regime.
- Model probabilities are prioritization signals, not calibrated estimates of economic impact.

## Fairness Considerations

- Sector imbalance can cause the model to over-alert on heavily represented sectors such as Technology or Financials.
- Large-cap issuers may dominate historical examples, reducing sensitivity to smaller but advisor-relevant changes elsewhere.
- Weak labels may encode a preference for size and rank movement over contextual portfolio intent.

Fairness monitoring should compare alert rates, precision, and false-positive patterns by sector and filer cohort before promoting a new model to production use.

## Governance Notes

- The model is advisory-support tooling and requires human review before any downstream action.
- Do not promote new versions of this classifier into the primary forecasting path. Preserve it only for legacy reproducibility or explicitly scoped observed-change policy experiments.
- Persisted model artifacts and run metadata should remain traceable to the filing dataset snapshot used for training.
