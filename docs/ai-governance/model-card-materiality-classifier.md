# WealthSignal Materiality Classifier Model Card

## Purpose

The WealthSignal materiality classifier ranks quarter-over-quarter 13F position changes by the likelihood that an advisor should review them. It is intended to support analyst triage, not to automate trading, portfolio construction, or client communication without human review.

## Training Data

- Source: persisted `position_features` rows derived from real SEC 13F filings already ingested by WealthSignal
- Label: `weak_label`, generated from deterministic rules that emphasize large weight shifts, top-rank entries/exits, and high rule-based materiality scores
- Features: the existing 20 engineered position-change features, including weight deltas, value deltas, rank transitions, turnover ratio, and turnover share
- Temporal split: 3-fold time-based validation using filing accession chronology to avoid random leakage across quarters

## Candidate Models

- `numpy-logistic-baseline`
- `logistic_regression`
- `decision_tree`
- `random_forest`
- `xgboost` when the optional dependency is available

The best advanced model is selected by PR-AUC and persisted as a joblib artifact for downstream API loading.

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
- New model versions should be treated as staging candidates until metrics, drift, and fairness checks are reviewed.
- Persisted model artifacts and run metadata should remain traceable to the filing dataset snapshot used for training.
