# WealthSignal Cloud Execution Plan

## August 2026 execution checkpoint and next Codex steps

Cloud 2 exact reconciliation and the Cloud 3 Gold gate are complete. The accepted 50-manager cap-500 table contains 5,154,259 examples across 28 target quarters. The nine-quarter validation window selected cap 500, and the saved gate reports zero leakage, duplicate, null-key, prospective, and nested-cap reconciliation violations. The next milestone is Cloud 4. Prospective Q2 2026 truth remains unopened, and RunPod is not authorized until graph/tabular reconciliation passes.

Two Cloud 4 Free Edition submissions are retained as measured failures. Run `743583852242260` at commit `c2b6ac7` failed because MLflow implicitly read the serverless-restricted `spark.mlflow.modelRegistryUri`; commit `ce4c14e` now sets both Databricks MLflow URIs explicitly. Run `19355950313133` at commit `ce4c14e` then passed that point and failed when serverless environment client `2` rejected the `pyspark.ml` `VectorAssembler` constructor. This is a serverless-environment compatibility failure, not evidence of quota exhaustion, timeout, or insufficient memory.

The approved compute decision is **Free Edition first**. Serverless environment version 4 adds Spark ML support, so do not buy compute merely to retry the unchanged client-2 submission. First update and test the job on environment client `4`, add a bounded compatibility smoke gate, and remove avoidable repeated Spark actions. Paid classic job compute becomes an option only after the optimized environment-4 path demonstrates quota exhaustion, inadequate capacity, or another required operation that serverless does not support. Creating a paid account, trial, classic cluster, or other billable resource still requires explicit owner approval.

The bounded environment-client-4 gate passed in Free Edition run `818005580828490` at commit `c73b98b`. It processed exactly 20,000 rows ending at 2023 Q4, trained on 16,037 rows, evaluated and reloaded 3,963 rows, observed both logistic classes, and completed MLflow, preprocessing, Ridge, logistic, and Delta write/reload checks in 53.307 pipeline seconds. Four later full-volume attempts failed before the first fold checkpoint: one graph schema conflict and three Spark Connect model-response overflows in combined preprocessing, the imputer, and the scaler. Evidence is preserved in `docs/ai-governance/cloud4-free-edition-nine-fold-gate.json`. Do not rerun the unchanged full-volume job for the portfolio demonstration.

The portfolio decision is now **production-shaped, sample-scale**. Preserve the full-volume failures as honest capacity evidence; do not spend money proving that Free Edition cannot run the unchanged workload again. The next executable Cloud 4 target is the deterministic `portfolio-demo` profile across all nine frozen folds. It exercises the same Spark/Delta/MLflow/checkpoint paths in isolated tables but does not authorize model promotion or full-volume performance claims.

Execute the following milestones in order:

1. **Prepare the Free Edition compatibility gate. Completed.** Environment client `4` passed the bounded MLflow, Spark ML, and write/reload smoke without model selection or prospective access.
2. **Make Cloud 4 restartable and quota-aware. Completed for the official runner.** Logistic thresholds share one aggregation, fold evidence is checkpointed and reusable after late failures, and deterministic hash node IDs with collision gates replace global single-partition numbering.
3. **Run the nine-fold portfolio demonstration.** Use `databricks/cloud4-portfolio-demo-submit.json`. Require all frozen folds, graph preparation, preprocessing, required models, diagnostics, MLflow lineage, isolated Delta tables, and reloadable checkpoints. Label all resulting metrics as engineering-demo evidence, not model-performance evidence.
4. **Build and gate the graph contract.** Persist deterministic manager/security node maps, chronological availability-timestamped weighted edges, forecast examples, checksums, and distributed fingerprints. Require persistence and EMA to reconcile exactly with tabular metrics.
5. **Escalate compute only if the sample fails.** Prefer the existing Free Edition serverless environment. If the sample cannot complete, propose one bounded CPU-only job-compute run with a USD 75 total project ceiling and immediate cleanup; wait for explicit approval.
6. **Freeze the V2 configuration.** Save the selected candidates, preprocessing, hyperparameters, action thresholds, seeds, metrics, bootstrap settings, environment, and promotion decision before prospective access.
7. **Begin Cloud 5 only after acceptance.** Pin and attribute NAVIS, transfer only the checksum-frozen graph bundle, run a bounded 10-manager smoke test, preserve measured failures and costs, and terminate rented compute after artifact capture.
8. **Build AWS only through the architecture gates.** Follow `docs/WealthSignal_AWS_Databricks_Architecture_Plan.md`. S3 is the durable cross-platform storage layer under Databricks processing; AWS creation, deployment, schedules, and spending remain separately approved actions.

Cloud allocation for the remaining work:

| Workload | Default platform | Reason |
|---|---|---|
| SEC packages, PySpark, Delta tables, reconciliation, SQL, lineage | Databricks | Already connected and proven on the ten-manager run |
| Graph extraction and baseline reconciliation | Databricks CPU | Data-local work; no GPU required |
| NAVIS smoke tests, multi-seed training, ablations | RunPod GPU | Short-lived GPU rental with explicit manual lifecycle |
| Local laptop | Git, small unit tests, documentation, API development | Avoid large-data materialization and model training |
| AWS | Optional S3/container/API/MLOps demonstration | Useful portfolio signal only when bounded by budgets and cleanup controls |

### Exact prompt for the next Codex task

```text
Read docs/WealthSignal_AWS_Databricks_Architecture_Plan.md, docs/WealthSignal_Cloud_Execution_Plan.md, docs/CODEX_PROJECT_NAVIGATION.md, the Protocol V2 governance files, cloud4_contract.py, its tests, and both retained Cloud 4 gate reports completely. Continue only the portfolio-scale A1 milestone.

Verify the deterministic `portfolio-demo` profile, all-nine-fold selection, isolated Delta tables, MLflow evidence, graph/tabular reconciliation, and restart checkpoints. Validate the submission JSON and full local test suite. Do not submit jobs, create AWS resources, access prospective truth, or incur cost without explicit approval. Escalate compute only if the sample-scale run fails, and keep the complete portfolio exercise below the USD 75 stop limit.
```

## Decision

Use Databricks for Protocol V2 data engineering, SQL, PySpark, Delta/Parquet artifacts, leakage audits, tabular baselines, and MLflow lineage. Use RunPod only after graph reconciliation for PyTorch/NAVIS GPU training. GitHub remains the source of truth for code, tests, protocols, checksums, and verified reports.

This replaces the failed full in-memory local build. The local machine has 15.85 GB RAM and a 4-core/8-thread Intel i5; the attempted builder exceeded 28 GB private memory without finalizing. No future 50- or 99-manager production build may use that unbounded Python-object path.

## Last verified connection state

- Databricks CLI v1.10.0 and the `wealthsignal` browser/OAuth profile were used for the accepted Cloud 1–3 jobs in workspace `dbc-df5ef74b-9c89.cloud.databricks.com`; authentication and workspace identity must be reverified immediately before Cloud 4 submission.
- Cloud 1–3 job history exists in the workspace. Do not infer current compute state or remaining Free Edition quota from the earlier checkpoint; inspect both before submitting Cloud 4.
- RunPod CLI and account verification are deferred by the project owner until the graph/NAVIS stages; no RunPod resource is authorized before that verification is completed.
- `DATABRICKS_TOKEN` and `RUNPOD_API_KEY` are not configured in the project environment; the Databricks connection uses OAuth.
- Credentials must never be committed, printed, stored in notebooks, or placed in repository files.
- Cloud resource creation requires explicit approval because it may create charges.

Prefer browser/OAuth authentication for Databricks. Use environment variables or platform secret stores for API keys. Commit only placeholder names in `.env.example`.

## Responsibility boundary

| System | Responsibilities | Prohibited use |
|---|---|---|
| GitHub | Code, PRs, tests, frozen protocols, checksums, schemas, small reports | Raw SEC ZIPs, secrets, large data/model artifacts |
| Databricks | Bronze/Silver/Gold pipeline, PySpark, Delta/Parquet, SQL, leakage audits, baselines, MLflow | Opening prospective Q2 2026 truth before authorization |
| RunPod | Graph/NAVIS smoke tests, training, seeds, ablations, checkpoints | SEC ETL, long-term storage, unattended idle GPU time |
| Local machine | Development, fixtures, API, reports, verification | Full 50/99-manager in-memory builds |

## Storage modes

### Low-cost mode

Use Databricks Free Edition default storage for Bronze/Silver/Gold tables. Export only the frozen graph bundle and checksum manifest for RunPod. Use a small RunPod network volume while training, then back up final artifacts and delete the volume when no longer needed.

Free Edition is quota-limited and has no SLA. If quotas or restricted outbound access block the 50-manager build, stop and use the paid mode rather than changing the protocol.

### Production-style mode

Use a dedicated S3-compatible object store as the shared artifact layer:

```text
wealthsignal/
  bronze/sec-13f/package=YYYYqQ/
  silver/effective-filings/
  silver/normalized-holdings/report_year=YYYY/
  gold/manager-security-quarter/target_year=YYYY/
  gold/graph-bundles/protocol-v2-design-2/
  models/navis/<run_id>/
  reports/<run_id>/
```

Grant Databricks write access to Bronze/Silver/Gold and RunPod read access to the frozen graph bundle. Give RunPod write access only to its model-artifact prefix. Never use administrator credentials.

## Cloud milestone sequence

### Cloud 0 — Safety and account connection

1. Stop the unbounded local Python build after explicit authorization and record its peak memory and staging identity.
2. Preserve the 30-package source manifest and frozen 50-manager cohort manifest.
3. Create or identify the Databricks workspace and RunPod account.
4. Install CLIs only from official sources in an isolated environment.
5. Authenticate without printing credentials.
6. Verify read-only identity/account commands.
7. Set a RunPod spending limit and require manual GPU creation.

Acceptance: both connections are verified, no paid compute or GPU exists, Git status is clean, and secret scanning finds no credentials.

Current status: the Databricks portion is verified without active compute. RunPod verification remains intentionally deferred and must be completed before Cloud 5; this deferral does not authorize a Pod, GPU, volume, or API key. Cloud 1 may proceed on Databricks only after its resource-creation actions receive explicit approval.

### Cloud 1 — Databricks repository and contract bootstrap

1. Import the approved GitHub branch into a Databricks Git folder.
2. Create `bronze`, `silver`, `gold`, and `mlflow` namespaces using supported workspace capabilities.
3. Upload or reacquire only the 30 declared development packages.
4. Recompute all hashes in Databricks and match the committed source manifest.
5. Verify cohort checksum `23617b83308e9b073212f9eb493e57921877eacc887f2fcdd923cf3b9ebfc3ff`.
6. Assert that no package containing prospective Q2 2026 truth is present.

Acceptance: 30/30 hashes match, the cohort hash matches, prospective-source count is zero, and a bootstrap report is saved.

### Cloud 2 — PySpark Bronze-to-Silver pipeline

Implement an out-of-core PySpark pipeline with explicit SEC TSV schemas, source lineage, early manager filtering, V2 security identity, deterministic duplicate aggregation, explicit amendment handling, bad-record quarantine, idempotent Delta/Parquet writes, and measured row-quality/runtime/shuffle/output metrics.

Partition holdings by report year/quarter rather than manager to avoid small-file explosion. Never collect complete holdings to the Spark driver.

Run 10 managers first and reconcile with the Python fixture. Run 25 for scale verification, then all 50 for the official dataset. The 10/25 results are engineering evidence, not main V2 claims.

The checked-in 10-manager Databricks handoff payload is `databricks/cloud2-10-handoff-submit.json`. It runs three tasks in one immutable Git-pinned job:

1. `cloud2_spark.py` for the distributed Bronze/Silver build;
2. `v2_reference.py` for the pure-Python Protocol V2 reference CSV on the same frozen sources;
3. `cloud2_reconcile.py` for exact Spark-versus-reference reconciliation.

After both report artifacts exist, validate the scale-up gate with `python -m wealthsignal_pipeline.cloud2_handoff gate-report`.

### Cloud 3 — Gold temporal validation dataset

Construct each target quarter independently using only required historical windows and as-of availability data. Use Spark windows/joins rather than Python row objects and write partitioned manager-security-quarter examples without driver collection.

- compare candidate caps 100/250/500 on validation only;
- store fold definitions as quarter predicates, not repeated example-ID arrays;
- run leakage checks in distributed SQL;
- checksum every partition and final manifest;
- record executor/driver usage and retried tasks.

Acceptance: immutable 50-manager Gold dataset, zero leakage violations, reload/reconciliation tests, and no prospective Q2 2026 truth access.

### Cloud 4 — Baselines and graph contract

Run persistence, EMA, popularity, Ridge, histogram gradient boosting, and logistic action diagnostics on the frozen validation partitions, with every run tracked in MLflow.

Build a framework-independent graph bundle containing manager/security node maps, chronological weighted edges, optional availability-timestamped node features, graph statistics, and dataset/split/protocol checksums. Graph persistence and EMA must reconcile with tabular metrics before RunPod is authorized.

### Cloud 5 — RunPod NAVIS smoke test

1. Pin the upstream NAVIS commit and preserve MIT attribution.
2. Build a reproducible environment lock/container.
3. Create a 50–100 GB RunPod network volume only after graph reconciliation and approval.
4. Transfer and verify only the frozen graph bundle.
5. Run the 10-manager smoke test on a 16–24 GB GPU.
6. Record CUDA/PyTorch versions, GPU, host RAM, batch size, parameters, epoch time, peak VRAM/RAM, checkpoint checksum, metrics, and cost.
7. Terminate compute immediately after saving artifacts.

Acceptance: deterministic forward pass, checkpoint reload, metric calculation, bounded memory, and no holdout access.

### Cloud 6 — NAVIS 25/50-manager validation

Use 25 managers for debugging only. Run the official 50-manager comparison with validation-selected hyperparameters and at least three seeds for featureless and feature-enabled NAVIS.

Initial target: RTX 4090, 24 GB VRAM, 48–64 GB host RAM, 8–16 vCPUs, and 50–100 GB storage. Escalate to A40/A6000 48 GB only if measured 25-manager VRAM projects beyond the safe 24 GB limit.

Compare NAVIS, persistence, and EMA on identical candidates/folds. Report effect size, manager-block uncertainty, runtime, cost, peak memory, and failures. Training completion alone never authorizes promotion.

### Cloud 7 — Optional 99-manager scale and original extension

After the 50-manager reproduction is stable, use the first 99 managers from the frozen ordering and report 50/99 results separately. Never use prospective Q2 2026 performance to select cohort size. Then predeclare and test one original extension such as learned candidate retrieval, uncertainty-aware ranking, or a graph-plus-tabular residual model.

### Cloud 8 — Freeze and prospective evaluation

Freeze dataset, graph, features, candidates, preprocessing, models, hyperparameters, seeds, metrics, environment, and promotion gate in a signed release manifest. Only after reviewer authorization and complete SEC publication may prospective Q2 2026 truth be acquired and evaluated once.

## Exact Codex prompts

### Connection prompt

```text
Read docs/WealthSignal_Cloud_Execution_Plan.md and all Protocol V2 governance files completely. Execute only Cloud 0 — Safety and account connection.

Do not create paid compute, a GPU Pod, a network volume, or cloud storage without explicit approval. Do not print, persist, or commit credentials. Detect the active unbounded local build and report its process ID, memory, and incomplete staging identity; request authorization before terminating it. Preserve the 30-package source manifest and frozen 50-manager cohort manifest.

Inspect whether official Databricks and RunPod CLIs are installed. If installation is needed, propose exact official commands and wait for approval. Guide me through browser/OAuth or environment-based authentication, then run only read-only account/workspace identity checks. Run secret scanning and git status before completion. Return connection status, profile/workspace identifiers without secrets, cost controls, and the exact Cloud 1 prompt.
```

### Databricks bootstrap prompt

```text
Read docs/WealthSignal_Cloud_Execution_Plan.md and execute only Cloud 1. Verify authentication without exposing credentials. Do not create RunPod resources.

Import the approved GitHub branch, establish Bronze/Silver/Gold namespaces, and transfer or reacquire only the 30 declared development packages. Recompute every checksum and the frozen 50-manager cohort hash. Add an automated guard proving no package containing prospective Q2 2026 truth is present. Save a bootstrap report and update documentation with measured evidence. Stop if Free Edition limitations require a protocol or storage change.
```

### PySpark prompt

```text
Read docs/WealthSignal_Cloud_Execution_Plan.md and execute Cloud 2, then Cloud 3 only after Cloud 2 acceptance passes. Implement the out-of-core PySpark Bronze/Silver/Gold pipelines without collecting full holdings or examples to the driver.

Reconcile the 10-manager subset with the Python reference, measure 25-manager scaling, and build the official 50-manager immutable dataset. Apply all frozen V2 identity, amendment, availability, missing-quarter, candidate, and leakage policies. Persist partition checksums, Spark/runtime metrics, quality metrics, and a reloadable manifest. Do not access prospective Q2 2026 truth or begin graph/NAVIS work.
```

### RunPod NAVIS prompt

```text
Read docs/WealthSignal_Cloud_Execution_Plan.md and execute Cloud 5 only after Cloud 4 graph reconciliation passes. Pin and attribute the official NAVIS implementation, reproduce its environment, transfer only the checksum-frozen graph bundle, and verify it on RunPod.

Request approval before creating any billable Pod or volume. Begin with the 10-manager smoke test, record hardware/software lineage, peak VRAM/RAM, epoch time, checkpoint reload, metrics, and cost, then terminate compute immediately. Do not run 25/50 managers, tune against a holdout, or access prospective Q2 2026 truth in this task. Return the measured capacity recommendation and exact Cloud 6 prompt.
```

## Cost and security rules

- No secrets in Git, notebooks, MLflow parameters, stdout, screenshots, or reports.
- No GPU without an estimated maximum cost and explicit approval.
- Every Pod has a termination checklist.
- RunPod volumes are temporary; important artifacts are backed up elsewhere.
- Artifacts are immutable by dataset/run ID and verified after transfer.
- Failed and interrupted runs remain visible in experiment metadata.
- Free-tier quota exhaustion is reported rather than bypassed by weakening evaluation rules.
