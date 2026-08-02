# Candidate-Universe Sensitivity Study

## Study design

The study varies only `negative_candidate_limit` while keeping the normalized source dataset, ten-manager cohort, target construction, six target quarters, temporal folds, action tolerance, and baseline configurations fixed. The evaluated caps are 0, 100, and 500 peer-observed candidates per manager-quarter. Every temporal dataset passed the leakage audit with zero issues. Only the three validation folds were evaluated; the final test fold was not accessed.

## Results

| Negative cap | Examples | Target coverage | Persistence NDCG@10 | Persistence Recall@10 | Persistence MAE |
|---:|---:|---:|---:|---:|---:|
| 0 | 580,158 | 79.2852% | 0.826377 | 0.730000 | 0.00008108 |
| 100 | 585,858 | 79.3149% | 0.815209 | 0.720000 | 0.00006070 |
| 500 | 608,658 | 79.3878% | 0.815175 | 0.720000 | 0.00004243 |

The lower MAE at larger caps is not interpreted as better forecasting: adding many zero-target candidates changes the evaluation denominator and mechanically makes all-zero-like predictions look stronger. Ranking metrics and candidate coverage are more informative for this sensitivity decision.

Moving from 100 to 500 adds 22,800 examples while improving target coverage by only 0.0729 percentage points. A zero cap cannot represent peer-observed new-position candidates at all. The selected cap is therefore **100**: it preserves an explicit new-position candidate lane without accepting the processing and class-imbalance cost of 500 for negligible measured coverage gain.

## Selected immutable dataset

- Temporal dataset: `temporal-d7c615d8d674f581`
- Negative-candidate cap: 100
- Examples: 585,858
- Leakage issues: 0
- Validation folds: 3
- Locked test folds: 1

This is a pragmatic local-study choice, not a universal optimum. A future data-contract version may replace the fixed cap with sector-stratified, liquidity-aware, or learned retrieval, but it must be evaluated without changing the frozen V1 final-test protocol.
