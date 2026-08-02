# WealthSignal Final-Test Report V1

## Execution record

- Run: `baselines-d3a74d251b1226d3`
- Evaluation quarter: 2014-12-31
- Dataset: `temporal-d7c615d8d674f581`
- Baseline contract: version 4
- Recorded Git revision: `25bdec0c1d85aa5fcc49b393d2706a473471cc49`
- Forecasting implementation SHA-256: `fd10c7a0fa52b83fcf57a80ac119c10cfab4ec9a878161c39b45e2593b15ab95`
- Protocol SHA-256: `c8af1ee45852bede4ca7df90dafe596ad1babb0da99c09bb2a0be7a968482d52`
- Status: `evaluated_with_fixed_protocol`
- Test examples: 100,822 across the ten-manager cohort

The test fold was opened once after the candidate policy, models, hyperparameters, metrics, and promotion gate were frozen. Results were not used for retuning.

The recorded Git revision is the repository base commit because the forecasting milestone was still uncommitted when the run executed. The manifest separately hashes the exact forecasting implementation, temporal manifest, protocol, report, and predictions. The future checkpoint commit must preserve those files but cannot retroactively change the consumed run's Git revision; this is a visible V1 lineage limitation rather than a reason to rewrite the artifact.

## Final-test results

| Model | NDCG@10 | NDCG@20 | Recall@10 | Rank correlation | Weight MAE | Weight RMSE |
|---|---:|---:|---:|---:|---:|---:|
| Persistence | **0.837113** | **0.821875** | **0.755556** | 0.705444 | **0.00003548** | 0.00043980 |
| EMA | 0.816844 | 0.811923 | 0.677778 | **0.706519** | 0.00003697 | 0.00044185 |
| Ridge | 0.832220 | 0.818155 | 0.722222 | 0.519023 | 0.00003829 | **0.00043647** |
| Institutional popularity | 0.615312 | 0.606470 | 0.466667 | 0.636108 | 0.00010099 | 0.00082515 |
| Gradient boosting | 0.695636 | 0.722935 | 0.533333 | 0.562997 | 0.00009197 | 0.00064880 |

Persistence is the final-test reference winner on NDCG@10, NDCG@20, Recall@10, and MAE. Ridge has slightly lower RMSE but does not satisfy the frozen promotion gate because it is worse than persistence on both primary ranking metrics and MAE.

The shared logistic action baselines are not viable. New-position precision and recall are both zero on the final test. Exit precision is 0.0664 and exit recall is 0.00107. They remain diagnostic and must not drive alerts, client-impact outputs, or served decisions.

## Decision

No learned model is promoted. Persistence remains the V1 benchmark forecast. This result supports the project principle that complex models must earn their operational cost against a strong temporal baseline.

The final test is now consumed for Protocol V1. It must not be used for further feature selection, hyperparameter tuning, candidate-policy changes, or threshold tuning. Future model development requires later untouched quarters or a newly declared prospective evaluation window.
