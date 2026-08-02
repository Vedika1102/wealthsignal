# Forecasting Baselines

`wealthsignal_pipeline.forecasting_baselines` evaluates all baseline models on the immutable candidate universe and expanding-window folds saved by the temporal dataset builder.

Implemented baselines:

- current-quarter weight persistence;
- three-quarter exponential moving average;
- as-of-cutoff aggregate institutional popularity;
- standardized ridge weight regression;
- deterministic histogram gradient-boosted weight regression.
- standardized L2-regularized logistic models for new-position and exit targets.

The evaluation reports NDCG@10, NDCG@20, Recall@10, Spearman-style rank correlation, portfolio-weight MAE and RMSE, new-position precision/recall, exit precision/recall, and training/inference runtime. Action metrics use the shared regularized logistic baselines rather than inferring actions from each weight score. Recall@10 is the overlap between the predicted and observed top-ten holdings divided by the number of observable relevant positions up to ten. Metrics are calculated per manager-target-quarter, target quarter, and manager-size cohort, then aggregated across validation folds with temporal variability retained. Cohorts use the number of currently held candidates: small below 50, medium from 50 through 199, and large at 200 or more.

## Final-test control

The command evaluates validation folds only by default. A final test fold can be opened only with both `--include-final-test` and a non-empty, versioned `--comparison-protocol` file. The protocol checksum is recorded in the immutable run identity. Do not create that protocol merely to inspect a test result; first fix model choices, features, hyperparameters, metrics, and promotion rules using training and validation evidence.

## Run

```powershell
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.forecasting_baselines `
  --temporal-dataset data/temporal-real/temporal-2b2a0d0f6e571579 `
  --output-root data/baseline-runs
```

Outputs include `config.json`, `predictions.csv`, per-fold model metadata, `comparison_report.json`, and a checksum-traced `manifest.json` containing the temporal dataset version and Git revision.

The narrow Berkshire validation dataset verifies execution and audit behavior only. Its single validation quarter and single manager cannot establish temporal variability, cross-manager generalization, or model success. A broader declared manager universe with more eligible quarters is required before unlocking the final test fold or promoting a model.
