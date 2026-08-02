# Manager-Security-Quarter Temporal Dataset

## Purpose

`wealthsignal_pipeline.temporal_dataset` converts one or more immutable SEC bulk datasets into the objective supervised table defined by `docs/WealthSignal_Forecasting_Spec.md`.

Each example represents one manager, one eligible security candidate, and one feature quarter `t`. Features use only filings available by the manager's selected filing date for `t`; targets come from the exact next report-calendar quarter `t+1`.

## Inputs

Every input directory must be produced by `wealthsignal_pipeline.bulk_dataset` and contain:

- `manifest.json`;
- `normalized_holdings.csv`;
- a matching SHA-256 checksum for that CSV in the manifest.

The temporal builder rejects a modified or incomplete source dataset. Multiple non-overlapping normalized datasets may be combined; identical holding IDs are deduplicated only when their full typed records agree.

## Candidate universe

For each manager-quarter cutoff, candidates consist of:

1. every security held by that manager at `t`;
2. up to `negative_candidate_limit` other securities ranked by peer-owner count and aggregate peer weight.

Peer statistics use each peer manager's latest report whose report period is no later than `t` and whose filing date is no later than the feature cutoff. A security is never eligible merely because it appears in `t+1`.

The quality report measures target-holding candidate coverage. Imperfect coverage is expected because genuinely new securities that were not observable before the cutoff cannot be added without leaking future information.

## Eligibility and missing quarters

- A manager must have an exact next report-calendar quarter for a row to receive a target.
- Missing intervening quarters are not treated as exits.
- If the selected `t+1` filing was already available on or before the selected `t` filing, the pair is rejected as temporally ambiguous. This occurs in real data when late amendments are selected for old reporting periods.
- Filing-date equality is also rejected because the current source provides dates, not reliable intraday ordering.

These exclusions and their counts are recorded in `quality_report.json`.

## Features

The generated feature dictionary documents availability for:

- current, previous, and second-lag portfolio weights;
- weight and rank momentum;
- holding history and recency;
- manager turnover, concentration HHI, and top-10 share;
- peer owner count and aggregate peer weight.

All cross-manager features are reconstructed as of the row-specific cutoff. No target-quarter aggregate is used.

## Objective targets

For the exact next quarter, the table records:

- normalized target weight;
- target rank;
- new-position indicator;
- exit indicator;
- increase indicator;
- decrease indicator;
- unchanged indicator;
- one categorical action label.

The action tolerance is versioned in the dataset identity. Candidate securities absent from `t+1` receive target weight zero and an out-of-portfolio rank of target portfolio size plus one.

## Expanding-window splits

The split manifest groups examples by target report quarter.

- Training target quarters must be strictly earlier than the evaluation target quarter.
- Early folds are validation folds.
- The configured final target quarter or quarters are marked `test`.
- Example IDs and complete manager feature-quarters may not overlap between train and evaluation.
- If too few target quarters exist, the manifest reports `insufficient_target_quarters` instead of fabricating a fold.

## Leakage audit

The build fails before publication if it detects:

- duplicate example IDs;
- a target other than exact `t+1`;
- target information available on or before the feature cutoff;
- a candidate first observed after the feature cutoff;
- unknown split references;
- train/evaluation example overlap;
- train/evaluation manager-quarter overlap;
- training targets that are not earlier than evaluation;
- evaluation examples assigned to the wrong quarter.

Tests include deliberately leaked fixtures for future candidates, invalid horizons, and split overlap.

## Build command

```powershell
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.temporal_dataset `
  --input-dataset data/historical/sec-13f-<first-id> `
  --input-dataset data/historical/sec-13f-<second-id> `
  --output-root data/temporal `
  --negative-candidate-limit 100 `
  --minimum-train-target-quarters 2 `
  --final-test-quarters 1
```

## Outputs

The immutable `temporal-<dataset-id>` directory contains:

- `manager_security_quarter.csv`;
- `feature_dictionary.json`;
- `split_manifest.json`;
- `leakage_report.json`;
- `quality_report.json`;
- `manifest.json` with input and output checksums.

Interrupted staging directories are recognized by dataset identity and rebuilt safely. Completed datasets are never overwritten.

## Validation boundary

The first real-data check used a deliberately narrow manager universe. The subsequent Protocol V1 cohort expanded to ten managers, 57 eligible manager-quarters, six target quarters, and 585,858 examples. This establishes a reproducible local evaluation contract but not broad external validity.

Persistence, EMA, institutional-popularity, ridge, gradient-boosted, and regularized logistic action baselines have now been evaluated on the saved folds. Protocol V1 was frozen before one-time final-test access; that holdout is now consumed. The next implementation boundary is lineage-aware forecast persistence, followed by forecast APIs. Later model development requires a newly declared untouched Protocol V2 window.
