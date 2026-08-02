# WealthSignal Forecast Comparison Protocol V2

Status: **design frozen before data download or evaluation**  
Freeze date: 2026-08-02  
Primary purpose: current-data, leakage-safe evaluation and later NAVIS reproduction  
Protocol V1 relationship: independent dataset, splits, candidates, and untouched window; V1's consumed holdout is never reused

## Official-source inventory at freeze time

The authoritative source is the SEC Form 13F Data Sets page. On the freeze date, it covered July 2013 through May 2026 and identified `2026 March April May 13F` as the newest published package, sized 94.81 MB. The SEC states that these are flattened, as-filed structured submissions, are updated quarterly, and may contain filer or extraction errors.

Protocol V2 declares the following source range before download:

- development history: every SEC quarterly package from `2019 Q1 13F` through `2023 Q4 13F`;
- modern filing windows: `2024 January February`, `2024 March April May`, `2024 June July August`, `2024 September October November`, `2024 December 2025 January February`, `2025 March April May`, `2025 June July August`, `2025 September October November`, `2025 December 2026 January February`, and `2026 March April May`;
- prospective test source: the first official SEC quarterly package published after the 2026 Q2 filing deadline that covers June through August 2026 filings.

Package labels are filing windows, not target quarters. Every example is assigned by the filing's `report_period`, never by package name. Every acquired ZIP must retain the SEC URL, retrieval time, byte size, and SHA-256 checksum. Package substitutions or SEC refreshes create a new dataset identity.

The SEC's published deadline for report quarter 2026 Q2 is August 14, 2026. That target is unavailable at the freeze date and is therefore a genuinely prospective untouched test.

## Time boundaries

| Purpose | Target report quarters | Permitted use |
|---|---|---|
| Cohort selection and initial training | 2019 Q1–2023 Q4 | Manager selection, feature work, fitting |
| Walk-forward validation | 2024 Q1–2026 Q1 | Candidate-cap selection, model choice, thresholds, promotion-gate evidence |
| Prospective test | 2026 Q2 | One evaluation after the complete official source is published and checksummed |

Training expands through time. For each validation target quarter, training targets must be strictly earlier. Protocol V2 test truth must not be downloaded, opened, counted, profiled, or used until all V2 model and promotion choices are frozen in a signed evaluation manifest.

## Manager cohort

The final manager list is derived only from 2019 Q1–2023 Q4 data by this deterministic rule:

1. Normalize CIKs to ten digits.
2. Resolve effective filings and exact report quarters under the amendment policy below.
3. Require at least 16 of the 20 selection-period report quarters, including 2023 Q4.
4. Compute each eligible manager's median reported long-market value over its eligible selection-period quarters.
5. Sort descending by median value, then ascending by CIK.
6. Select the first 25 managers and freeze the ordered CIK list with a checksum.

Future continuity, validation performance, and test availability may not affect cohort selection. A selected manager missing a later quarter receives an explicit missing status; it is not silently replaced. This prevents future-survivor selection.

Twenty-five managers are the declared local target. If fewer than 25 satisfy the predeclared eligibility rule, use all eligible managers and record the shortfall without weakening the rule.

## Effective filing and missingness policy

- Resolve `13F-HR` and `13F-HR/A` by normalized CIK and report period.
- Start with the latest ordinary filing available under the row cutoff.
- Apply amendments explicitly identified as additions as additive filings.
- Treat identified restatements as replacements.
- Quarantine ambiguous amendment chains rather than guessing their semantics.
- Record contributing, superseded, and quarantined accessions.
- Require an exact next calendar report quarter for supervised targets.
- A missing next quarter is missing, not an exit.
- Reject a pair if selected target information was available on or before the feature cutoff.
- Do not backfill confidential-treatment releases into an earlier information set.

## Security identity policy

The V2 prediction identity is `normalized CUSIP + instrument side`, where instrument side is `LONG`, `PUT`, or `CALL`.

- Aggregate duplicate information-table rows with the same V2 identity within an effective manager filing, including rows split by discretion or other-manager fields.
- Preserve the original information-table keys and accessions as lineage.
- Do not merge PUT, CALL, and long positions.
- Do not link CUSIP changes with ticker or issuer-name fuzzy matching.
- A CUSIP change is an identity break unless an independently sourced, point-in-time crosswalk supplies old identity, new identity, effective date, source, and checksum.
- Corporate-action crosswalks available after a feature cutoff cannot rewrite that cutoff's features.
- Report unresolved identity breaks and their affected target mass.

No present-day S&P 500 or other current index membership filter is permitted. The universe is derived from point-in-time reported holdings, avoiding today's-membership survivorship bias.

## Candidate policy

For each manager feature quarter, candidates contain:

1. every V2 security identity held by that manager at the feature cutoff;
2. peer-observed identities available by the same cutoff, ranked by owner count, aggregate peer weight, then security identity.

Candidate caps `100`, `250`, and `500` may be compared on validation only. Select the smallest cap whose mean target-mass coverage is within 0.25 percentage points of the best validation cap, with deterministic ties favoring the smaller cap. After selection, freeze one cap for the prospective test.

Report candidate count, target-position coverage, target-weight-mass coverage, new-position coverage, and zero-target share per quarter and manager. Do not interpret all-candidate MAE improvements without these coverage and class-balance measures.

## Features and availability

All features must be available by the selected feature filing's SEC availability timestamp. Permitted initial features are current/lagged weights and ranks, history and recency, weight/rank momentum, manager turnover and concentration, and peer ownership aggregates reconstructed as of the row cutoff.

Preprocessing, cohort-derived statistics, encodings, and normalization must be fit inside each expanding training window. No validation or test aggregate may influence a feature. Sector or crosswalk data is permitted only with a point-in-time availability record.

## Fixed baselines and research lane

Required on identical candidate rows and folds:

- current-quarter persistence;
- EMA, with alpha selected on validation from `{0.4, 0.6, 0.8}`;
- as-of-cutoff institutional popularity;
- standardized Ridge, with alpha selected on validation from `{0.1, 1.0, 10.0}`;
- regularized new-position and exit logistic diagnostics with validation-selected class weighting and thresholds;
- the graph-adapter versions of persistence and EMA, which must reconcile with tabular metrics before NAVIS.

NAVIS remains a separate attributed reproduction lane. Its upstream revision, data deviations, features, loss, hyperparameters, and seeds must be frozen before prospective-test access. The failed Protocol V1 action models remain excluded from alerts and client decisions.

## Metrics

Primary ranking metrics: manager-macro NDCG@10, NDCG@20, and top-ten Recall@10.  
Primary weight metrics: manager-macro MAE and RMSE on the fixed candidate set.  
Required context: nonzero-target MAE/RMSE, target-weight-mass coverage, rank correlation, new/exit precision and recall, per-quarter and manager-cohort results, fold variability, runtime, peak memory, and candidate counts.

Every learned model is compared with persistence and EMA on identical rows. Confidence intervals use a manager-block bootstrap with a frozen seed and resample count declared in the later evaluation manifest.

## Promotion gate

A learned weight/ranking model becomes a candidate only if it:

- passes dataset, checksum, quality, leakage, schema, reload, and deterministic-inference checks;
- improves mean validation NDCG@10 over persistence and improves it in at least six of the nine validation quarters;
- does not reduce mean validation Recall@10;
- has all-candidate MAE no more than 5% worse than persistence;
- records complete data, protocol, code, model, seed, runtime, and artifact lineage.

The prospective test is evaluated once. A favorable test does not automatically authorize serving; reviewer approval is still required. An unfavorable result is preserved and reported without retuning against 2026 Q2.

## Prospective-test release conditions

Before acquiring or opening 2026 Q2 truth:

1. Persist the ordered manager CIK list and checksum.
2. Persist all development package checksums and the final temporal dataset/split IDs.
3. Freeze the candidate cap, features, preprocessing, models, hyperparameters, seeds, bootstrap configuration, metrics, and promotion decision rule.
4. Pass tabular/graph reconciliation, temporal leakage, artifact reload, and clean-checkout tests.
5. Create a release manifest that hashes this protocol and every selected configuration.
6. Obtain explicit reviewer authorization for the one-time prospective evaluation.

## Known limitations

- Form 13F is delayed, long-biased, and incomplete with respect to a manager's economic exposure.
- SEC bulk data is as filed and may contain filer or extraction errors.
- Confidential treatment and later amendments can make observed history incomplete.
- CUSIP identity breaks remain unless supported by point-in-time crosswalks.
- Twenty-five large, persistent managers do not represent the full filer population.
- The prospective window is one quarter; it tests temporal generalization but not every market regime.
- Forecasts are research and decision-support outputs, not investment advice.
