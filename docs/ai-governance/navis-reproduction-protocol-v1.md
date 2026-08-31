# WealthSignal NAVIS Reproduction Protocol V1

Status: **execution configuration frozen; validation and promotion pending**

Freeze date: 2026-08-31
Prospective target: **2026 Q2 remains unopened**

This document freezes how WealthSignal will reproduce and adapt NAVIS before any RunPod work or prospective evaluation. It is not a reproduction result, a model-promotion record, or authorization to rent compute. The machine-readable authority is `forecast-protocol-v2-model-freeze.json`.

## Attribution and immutable upstream pin

The research reference is Izadifar and Rahmati, [*Institutional Equity Holdings Prediction Using Node Affinities of Dynamic Graphs*](https://arxiv.org/abs/2607.12067), arXiv `2607.12067v1`, submitted July 13, 2026. The paper links the [official implementation repository](https://github.com/e-izdfr/portfolio-holdings-prediction), which is frozen for this work at commit `4bf8ad0b6c2f1bed27b8c469d8f8a5d527c7e9f8`, Git tree `b0456fbcb878f920d846c8deba5bbab5ae105e35`.

The repository is MIT-licensed, copyright 2026 Emad Izadifar. Every derived source bundle or container must carry its `LICENSE` file and identify local changes. WealthSignal may say that it reproduces or adapts the attributed approach only after a measured run reconciles; it may not claim to have invented NAVIS.

Paper-reported values such as NAVIS NDCG@10 `0.9127` with features, `0.9121` without features, persistence `0.8891`, and EMA `0.8882` are reference values on the paper's dataset. They are never WealthSignal results.

## Reproduction matrix

| Dimension | Paper or pinned code | WealthSignal frozen choice | Classification |
|---|---|---|---|
| Task | Predict next-quarter manager-to-security affinity/portfolio weights | Same task on Protocol V2 candidate rows | Conceptual match |
| Graph | Directed, weighted, quarterly manager-security bipartite graph | Same, with point-in-time availability and explicit identity lineage | Match with stronger availability controls |
| Managers | Random sample of 99 | Deterministic main cohort of 50; nested 10/25 subsets | Intentional deviation |
| Securities | 503 fuzzy-matched S&P 500 CUSIPs, match threshold 80 | Point-in-time reported `CUSIP + LONG/PUT/CALL` identities; no current-index filter; candidate cap 500 | Intentional deviation to avoid survivorship and identity leakage |
| History | Q2 2013-Q1 2025, 48 quarters | Selection/training from 2019 Q1; nine expanding validation targets from 2024 Q1-Q1 2026 | Intentional deviation |
| Split | One chronological 70/15/15 split: 40/4/4 quarters | Nine expanding folds; every training target precedes its evaluation target | Protocol V2 requirement |
| Candidate set | Fixed 503-class universe | Per-manager cutoff candidates: all current holdings plus up to 500 peer-observed negatives | Intentional deviation; identical rows for all compared models |
| Features | 23 time-varying node features, including sector and AUM inputs | Featureless primary reproduction plus a 23-slot availability-safe ablation with unavailable fields fixed to zero | Partial approximation |
| Loss | Pinned code uses `LambdaNDCGLoss2` plus `HindgeRegularization` | Same objective, restricted to the frozen candidate mask | Match with candidate-mask adaptation |
| Metric | NDCG@10; paper also records loss | Manager-macro NDCG@10 primary, plus the complete Protocol V2 metric set | Match plus extensions |
| Epochs | Paper: 50 without features, 500 with; pinned scripts set 500 for both | Paper values: 50 featureless, 500 feature-enabled | Paper-preferred resolution of source conflict |
| Seed | Script parses `--seed`, then overwrites it with `2`; comment lists 1-5 | Smoke seed `2`; official seeds `1, 2, 3` | Explicit reproducibility repair |
| Hardware | Not declared by the paper/repository | Smoke target 16-24 GB GPU; initial validation target RTX 4090 24 GB, 48-64 GB RAM, 8-16 vCPU | WealthSignal declaration, not an upstream match |

Because the data universe, history, identity rules, candidates, and folds differ, Cloud 6 will be an independently engineered reproduction and extension of the method, not a numeric replication of the paper's table.

## Upstream audit findings

The pin is intentionally more specific than `main`: there are no releases or tags, and the public README contains only the project title and one-sentence description. The NAVIS instructions name TGB and `pytorchltr` but provide no Python, CUDA, PyTorch, TGB, PyG, or transitive dependency lock.

The following differences are frozen as auditable adaptations:

1. Both upstream `navis.py` variants define `batch_size=200`, learning rate `0.0001`, `max_classes=20`, `IS_MULTI=False`, and regularization delta `0.001`.
2. Both scripts set `epochs=500`, whereas the paper's result table states 50 epochs for featureless NAVIS and 500 with features.
3. Both scripts parse a seed argument but immediately replace it with seed `2`.
4. The scripts evaluate and log the test loader after every epoch. WealthSignal will never place the prospective loader in that loop; epoch selection uses validation only.
5. The instructions require an unversioned edit to `pytorchltr`. WealthSignal pins `pytorchltr==0.2.1`, verifies its source archive SHA-256 `58d60f1da45ab73d29f9594922b9ee601a7f65b42759585baba799f83b0fc945`, and applies `third_party/patches/pytorchltr-0.2.1-lambda-ndcg-exponent-clamp.patch`.
6. The upstream feature script consumes a 23-dimensional array but its positional normalization constants do not document a complete mapping to the paper's listed features. WealthSignal therefore declares its mapping below instead of guessing.

## Frozen Protocol V2 inputs

- Ordered main cohort: 50 CIKs, checksum `23617b83308e9b073212f9eb493e57921877eacc887f2fcdd923cf3b9ebfc3ff`.
- Engineering subsets: first 10 and first 25 CIKs in that ordering.
- Selected candidate cap: 500, chosen on full-volume Cloud 3 validation coverage by the frozen smallest-within-0.25-percentage-points rule.
- Official source table: `workspace.gold.temporal_examples_50_cap_500`, 5,154,259 rows and 28 target quarters.
- Validation targets: 2024 Q1 through 2026 Q1, nine expanding folds.
- Prospective target: 2026 Q2; no acquisition, opening, counting, or profiling is allowed.

The Cloud 4 `portfolio-demo` graph is engineering evidence, not the official full-volume NAVIS validation bundle. It may support the approved 10-manager smoke only after export checksums pass. It may not support a full-volume performance claim.

## Candidates and preprocessing

Every model sees the same manager-quarter candidate rows. A class outside a row's frozen candidate mask is excluded from loss and evaluation rather than treated as an additional zero target. Security ties are broken by the frozen `security_key` ascending. Events are ordered by report period, availability timestamp, CIK, and security key.

Tabular inputs retain the 13 frozen Cloud 4 columns: current, previous, and lag-two weights; current and previous ranks; weight and rank momentum; manager turnover and HHI; peer owner count and aggregate peer weight; holding-history quarters; and quarters since last held. Missing finite inputs are replaced with the training-fold approximate median (`percentile_approx`, accuracy `10000`). Learned tabular inputs are standardized with the training-fold sample mean and sample standard deviation; a constant column maps to zero. Ridge and logistic predictions use the frozen Spark settings in the machine-readable manifest.

NAVIS trains in `float32`. Non-finite inputs or scores are a hard failure. Raw scores drive ranking metrics. For weight metrics only, scores are clipped at zero and L1-normalized within the frozen candidate set; an all-zero row deterministically becomes uniform over its candidates. No target value participates in this postprocessing.

## Feature ablation layout

Featureless NAVIS is the primary reproduction lane. The optional feature-enabled ablation uses a shared 23-slot node vector and structured zero padding:

| Slots | Node type | Value |
|---|---|---|
| 0 | manager | `log1p` of current holding count |
| 1 | manager | AUM unavailable from the frozen weight-only graph, fixed zero |
| 2 | manager | current portfolio HHI |
| 3-13 | manager | point-in-time sector exposures unavailable, fixed zero |
| 14 | manager | half-L1 turnover from the exact previous quarter |
| 15 | manager | cosine similarity between current and exact-previous-quarter weight vectors; zero if unavailable |
| 16 | manager | `-sum(w * log(w))` over current top-ten holdings, without renormalizing the top ten |
| 17 | security | current owner count within the frozen cohort |
| 18 | security | current aggregate manager weight |
| 19 | security | total shares unavailable from the graph bundle, fixed zero |
| 20 | security | mean current weight across owning managers |
| 21 | security | population standard deviation of current weights across owning managers |
| 22 | security | point-in-time sector unavailable, fixed zero |

All nonconstant numeric features are standardized from training snapshots only. Constant and unavailable slots remain zero. No present-day sector classification, AUM backfill, shares backfill, or later crosswalk may enter this ablation. The feature projection is a learned linear map from 23 inputs to the global security-class dimension and is added to the NAVIS memory score. Sector embedding is disabled because the required point-in-time input is unavailable.

## NAVIS optimization and selection

- Architecture: pinned per-node memory and virtual-state logic, isolated behind the WealthSignal graph adapter.
- Batch size: 200 chronological events.
- Optimizer: Adam, learning rate `1e-4`, betas `(0.9, 0.999)`, epsilon `1e-8`, weight decay `0`, AMSGrad disabled.
- Objective: `LambdaNDCGLoss2 + 1.0 * HindgeRegularization`.
- Ranking truncation and regularization `top_k`: 20.
- Hinge delta: `0.001`.
- Epoch ceilings: 50 featureless; 500 feature-enabled.
- No dropout, gradient clipping, mixed precision, scheduler, or early stopping.
- Determinism: seeded Python, NumPy, CPU, and CUDA generators; deterministic PyTorch algorithms; cuDNN benchmark disabled; stable data ordering.
- Smoke: first 10 managers, first validation fold, featureless, seed 2, two epochs maximum. Metrics are engineering-only.
- Scaling diagnostic: first 25 managers, first validation fold, featureless, seed 2, one epoch. It can size memory but cannot select a model.
- Official validation: 50 managers, all nine folds, both variants, seeds 1/2/3, fresh state per fold and seed.

For each variant, one epoch is selected by highest seed-averaged, manager-macro NDCG@10 across all nine validation folds; ties choose the earlier epoch. If a single NAVIS variant must be carried forward, choose the higher validation NDCG@10 and break a tie in favor of featureless NAVIS. This choice and the promotion result must be written into a later signed release manifest before prospective access.

The prospective 2026 Q2 loader must be absent from training, checkpoint selection, debugging, and validation code. It is evaluated once only after every release condition passes and a reviewer explicitly authorizes access.

## Metrics, uncertainty, and promotion

Primary metrics are manager-macro NDCG@10, NDCG@20, Recall@10, candidate-set weight MAE, and RMSE. Required context includes nonzero-target errors, target-mass coverage, rank correlation, per-quarter and per-manager results, seed variability, candidate counts, runtime, parameters, peak VRAM/RAM, and measured cost.

Confidence intervals use a manager-block percentile bootstrap with 2,000 resamples, seed `20260831`, and a two-sided 95% interval. Each sampled manager retains all of its quarters. NAVIS scores are averaged across the three seeds before manager blocks are resampled. Report paired NAVIS-minus-baseline effects on identical rows.

NAVIS can become a candidate only if the complete Protocol V2 gate passes: it improves mean validation NDCG@10 over persistence, wins in at least six of nine quarters, does not reduce mean Recall@10, has all-candidate MAE no more than 5% worse than persistence, and has complete checksum, quality, leakage, reload, deterministic-inference, runtime, seed, and artifact lineage. Training completion is never promotion. Serving additionally requires reviewer approval.

## Graph bundle contract

The transfer unit is `wealthsignal-graph-bundle-v2`, never a live Unity Catalog table reference. It contains:

- `manager_nodes.parquet`: `manager_node_id`, `cik`;
- `security_nodes.parquet`: `security_node_id`, `security_key`, `cusip`;
- `chronological_edges/`: `manager_node_id`, `security_node_id`, `report_period`, `feature_available_at`, `weight`, partitioned by report period;
- `forecast_examples/`: the Cloud 4 example schema and frozen candidate mask, partitioned by target report period;
- optional `node_features/`: the declared 23-slot vectors with `report_period` and availability timestamp;
- `split-manifest.json`, `bundle-manifest.json`, the Protocol V2 files, and the upstream MIT `LICENSE`.

The bundle manifest records schema version, source table and partition identities, row counts, column schemas, per-file SHA-256 values, canonical manifest SHA-256, Git revision, protocol/config/cohort/split checksums, and the Cloud 4 graph fingerprints. Export is rejected if it contains target quarter `2026-06-30`, an availability timestamp after a row cutoff, a node-ID collision, a missing checksum, an unexpected column, or a persistence/EMA reconciliation delta above `1e-12`.

The existing sample evidence has 50 manager nodes, 24,630 security nodes, 185,589 chronological edges, and 279,423 forecast examples. Its distributed `xxhash64` sums are frozen in the machine-readable manifest. These distributed fingerprints are integrity evidence, not substitutes for the per-file SHA-256 values required at export.

## Environment and execution gates

Direct dependency pins are Python `3.11`, PyTorch `2.12.1`, PyG `2.8.0`, `py-tgb` `2.2.0`, `pytorchltr` `0.2.1` plus the frozen patch, NumPy `2.5.0`, matplotlib `3.11.0`, tqdm `4.68.3`, and Weights & Biases `0.28.0` in offline mode. They were available by the pinned repository date; upstream did not declare its actual versions. Cloud 5 must resolve them into a hash-locked Linux environment and record the container base digest before execution. A dependency incompatibility is a measured smoke failure and requires a reviewed freeze amendment; it is not permission to float to latest versions.

Cloud 5 remains blocked on separate owner approval for any RunPod resource or cost. Once approved, it must create only the bounded storage/compute described in the cloud plan, verify the bundle after transfer, record hardware and environment facts, reload a checkpoint, save evidence, and terminate the pod immediately. Cloud 6 requires a new approval after the smoke report is accepted.
