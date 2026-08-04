# WealthSignal Forecast Comparison Protocol V1

Status: **frozen for one final-test evaluation**  
Frozen dataset: `temporal-d7c615d8d674f581`  
Temporal manifest SHA-256: `114dafa3de03ebb4b175b24ac9a1e154272cb8a3eb7eaaac8dfb37dc7831139a`  
Candidate policy: current holdings plus at most 100 peer-observed negatives  
Baseline contract: version 4  
Validation evidence run: `baselines-79fa077390d8fd78`

## Fixed comparison set

Weight/ranking models:

1. current-quarter persistence;
2. EMA with alpha 0.6 over current, previous, and second-lag weights;
3. as-of-cutoff institutional popularity;
4. standardized ridge regression with alpha 1.0;
5. histogram gradient boosting with 100 iterations, depth 4, learning rate 0.05, L2 regularization 1.0, and seed 42.

Action models:

- standardized L2 logistic regression for new positions;
- standardized L2 logistic regression for exits;
- probability threshold 0.5;
- maximum 1,000 iterations and seed 42.

No model, feature, candidate rule, threshold, or hyperparameter may change after final-test access under this protocol.

## Fixed metrics

Ranking: NDCG@10, NDCG@20, and top-ten Recall@10.  
Weight: MAE and RMSE.  
Secondary: rank correlation, new-position precision/recall, exit precision/recall, per-quarter results, manager-size cohorts, and training/inference runtime.

All models use identical candidate rows and expanding-window folds. Results must include persistence and EMA and must preserve fold-level variability.

## Candidate gate

A learned weight model may be called a candidate only if, on validation:

- its mean NDCG@10 and Recall@10 are both at least as high as persistence;
- it improves NDCG@10 in at least two of three validation folds;
- its mean weight MAE is no more than 5% worse than persistence;
- data checks, leakage checks, deterministic tests, schemas, configurations, lineage, and artifacts are complete.

Under current validation evidence, no learned model passes this gate. Ridge is marginally higher on aggregate NDCG@10 and Recall@10 but has approximately 47.6% worse MAE than persistence. Persistence remains the V1 reference forecast.

The action models are diagnostic only: their validation precision and recall are too weak for candidate status. They must not drive alerts or client-impact outputs.

## One-time final-test procedure

1. Confirm the repository tests and leakage audit pass.
2. Record the checksum of this protocol in the run configuration.
3. Execute the baseline CLI once with `--include-final-test` and this file as `--comparison-protocol`.
4. Preserve every result, including unfavorable or null results.
5. Do not tune, rerun with altered choices, or replace the test quarter after inspection.
6. Report validation and final-test results separately.

Final-test execution measures generalization; it does not automatically promote a model. Served-model promotion still requires explicit reviewer approval and complete forecast provenance.
