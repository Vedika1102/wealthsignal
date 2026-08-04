# Expanded Historical Validation

## Declared cohort

The expanded local validation cohort uses ten managers that appear in every selected SEC package from 2013 Q3 through 2015 Q1:

| CIK | Manager reference |
|---|---|
| 0001067983 | Berkshire Hathaway |
| 0001364742 | BlackRock |
| 0000102909 | Vanguard |
| 0000093751 | State Street |
| 0000019617 | JPMorgan Chase |
| 0000895421 | Morgan Stanley |
| 0000886982 | Goldman Sachs |
| 0000070858 | Bank of America |
| 0001214717 | Geode Capital Management |
| 0000073124 | Northern Trust |

The sparse 2013 Q2 source package is excluded because only one cohort manager appears in that local package. Manager names are descriptive references; CIKs are the actual reproducible filters.

## Measured datasets

The immutable normalized dataset is `sec-13f-5f4bbf00dbcb19b6` under the ignored `data/historical-expanded` directory. It processed seven packages, 84 submissions, 70 effective filings, and 696,577 normalized holdings across 10 managers. It resolved 14 amendments and 149,490 duplicates, rejected 206 invalid rows, and measured 100% identifier coverage under the current CUSIP validation contract.

The immutable temporal dataset is `temporal-d7c615d8d674f581` under `data/temporal-expanded`. It contains 585,858 examples, 57 eligible manager-quarters, six target quarters, three validation folds, and one locked test fold. The leakage audit passed with zero issues. Target-holding candidate coverage is 79.31% with the 100-negative-candidate cap.

## Validation-only baseline result

Run `baselines-79fa077390d8fd78` evaluates the three validation folds under baseline contract version 4 and does not access the final test fold. This version adds RMSE and regularized logistic new-position and exit baselines.

| Model | Mean NDCG@10 | Fold SD | Mean Recall@10 | Mean rank correlation | Mean weight MAE |
|---|---:|---:|---:|---:|---:|
| Persistence | 0.815209 | 0.059789 | 0.720000 | 0.767223 | 0.0000607 |
| EMA | 0.810592 | 0.054445 | 0.706667 | 0.774409 | 0.0000651 |
| Ridge | 0.815737 | 0.055672 | 0.723333 | 0.553353 | 0.0000896 |
| Institutional popularity | 0.586111 | 0.053090 | 0.443333 | 0.600993 | 0.0002840 |
| Gradient boosting | 0.646023 | 0.042002 | 0.510000 | 0.652000 | 0.0003851 |

These figures are validation evidence, not final-test or model-promotion results. Persistence remains the strongest weight-MAE baseline, while ridge has a marginally higher mean NDCG@10. The difference is not treated as success without uncertainty analysis, a fixed comparison protocol, and an untouched final-test result.

## Known limitations

- Ten managers do not represent the full 13F population.
- Six target quarters remain a short historical window.
- Candidate coverage of 79.31% requires sensitivity analysis before final evaluation.
- Corporate actions and long-horizon security identity remain governed by the current conservative CUSIP-based contract.
- Protocol V1's final test fold was intentionally locked during validation and is now consumed under the frozen comparison protocol.

The candidate-universe sensitivity study selects a cap of 100, and `docs/ai-governance/forecast-comparison-protocol-v1.md` froze the one-time final-test procedure. That procedure has now been executed and is reported separately in `docs/WealthSignal_Final_Test_Report_V1.md`; the Protocol V1 test fold is consumed.
