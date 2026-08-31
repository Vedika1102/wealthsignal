from pathlib import Path

import pytest

from wealthsignal_pipeline.cloud4_contract import (
    binary_metrics,
    canonical_sha256,
    select_smallest_best,
    validate_upstream_report,
    checkpoint_tables_ready,
    execution_checkpoint_tables,
    execution_suffix,
    select_execution_folds,
    FOLD_ACTION_TABLE,
    FOLD_CHECKPOINT_TABLE,
    FOLD_METRICS_TABLE,
    FOLD_TRIAL_TABLE,
    PORTFOLIO_DEMO_PROFILE,
    PORTFOLIO_DEMO_SUFFIX,
    PROTOCOL_SHA256,
)


def _upstream() -> dict[str, object]:
    return {
        "status": "passed",
        "selected_candidate_cap": 500,
        "prospective_q2_2026_truth_accessed": False,
        "cap_reports": {"500": {"leakage": {"future_candidates": 0}, "partition_manifest_sha256": "abc"}},
        "split_manifest": {"folds": [{} for _ in range(9)]},
    }


def test_upstream_gate_accepts_cloud3_contract() -> None:
    assert validate_upstream_report(_upstream()) == []


def test_upstream_gate_rejects_prospective_access_and_leakage() -> None:
    report = _upstream()
    report["prospective_q2_2026_truth_accessed"] = True
    report["cap_reports"]["500"]["leakage"]["future_candidates"] = 1
    assert validate_upstream_report(report) == [
        "upstream_prospective_guard_failed", "upstream_leakage_nonzero"
    ]


def test_grid_selection_is_deterministic_and_prefers_smaller_ties() -> None:
    assert select_smallest_best({0.4: 0.2, 0.6: 0.1, 0.8: 0.1}) == 0.6
    assert select_smallest_best({0.1: 0.7, 1.0: 0.8}, maximize=True) == 1.0


def test_manifest_hash_is_order_independent() -> None:
    assert canonical_sha256({"a": 1, "b": 2}) == canonical_sha256({"b": 2, "a": 1})


def test_frozen_protocol_hash_matches_committed_protocol() -> None:
    repository_root = Path(__file__).resolve().parents[3]
    protocol_path = repository_root / "docs" / "ai-governance" / "forecast-comparison-protocol-v2.md"
    assert PROTOCOL_SHA256 == canonical_sha256(protocol_path.read_text(encoding="utf-8"))


def test_binary_metrics_handles_normal_and_empty_predictions() -> None:
    metrics = binary_metrics(3, 1, 2)
    assert metrics["precision"] == 0.75
    assert metrics["recall"] == 0.6
    assert metrics["f1"] == pytest.approx(2 / 3)
    assert binary_metrics(0, 0, 4) == {"precision": 0.0, "recall": 0.0, "f1": 0.0}


def test_cloud4_source_has_no_graph_framework_or_prospective_read() -> None:
    from wealthsignal_pipeline import cloud4_contract

    source = open(cloud4_contract.__file__, encoding="utf-8").read()
    assert "__file__" not in source
    assert "torch" not in source.lower()
    assert "tensorflow" not in source.lower()
    assert "2026-06-30" in source
    assert "collect()" not in source
    assert ".toPandas(" not in source
    assert "LogisticRegression" in source
    assert "mlflow.start_run" in source
    assert 'mlflow.set_tracking_uri("databricks")' in source
    assert 'mlflow.set_registry_uri("databricks")' in source
    assert '"split_manifest_sha256"' in source
    assert '"protocol_sha256": PROTOCOL_SHA256' in source
    assert '"graph_statistics"' in source
    assert '"manager_concentration_hhi"' in source
    assert '"nonzero_target_mae"' in source
    assert '"nonzero_target_rmse"' in source
    assert '"rank_correlation"' in source
    assert '"full_outer"' in source
    assert '"missing_graph_groups"' in source
    assert 'F.xxhash64("cik")' in source
    assert 'F.xxhash64("security_key")' in source
    assert "threshold_expressions" in source
    assert "probabilities.agg(*threshold_expressions)" in source
    assert '.option("overwriteSchema", "true")' in source
    assert "F.percentile_approx" in source
    assert "def impute_frame" in source
    assert "Imputer(" not in source
    assert "F.stddev_samp" in source
    assert "def scale_frame" in source
    assert "StandardScaler(" not in source
    assert "Pipeline(stages=preprocessing)" not in source
    assert '"executed_fold_count": 1' in source
    assert '"model_selection_performed": False' in source
    assert '"prospective_q2_2026_truth_accessed": False' in source
    assert "cloud4-model-fit-failure.json" in source
    assert "checkpoint_reload_counts" in source
    assert '"engineering_demonstration_only": bool(sample_profile)' in source
    assert '"model_performance_claim_authorized": not bool(sample_profile)' in source
    assert '"sample_statistics": sample_statistics' in source


def test_cloud4_source_executes_without_file_global_like_databricks() -> None:
    from wealthsignal_pipeline import cloud4_contract

    source_path = Path(cloud4_contract.__file__)
    namespace = {"__name__": "databricks_task"}
    exec(compile(source_path.read_bytes(), str(source_path), "exec"), namespace)
    assert namespace["PROTOCOL_SHA256"] == PROTOCOL_SHA256


def test_cloud4_submissions_use_environment_client_four() -> None:
    import json
    from pathlib import Path

    for name in ("cloud4-submit.json", "cloud4-smoke-submit.json", "cloud4-first-fold-submit.json"):
        payload = json.loads(Path("databricks", name).read_text(encoding="utf-8"))
        assert payload["environments"][0]["spec"]["client"] == "4"


def test_first_fold_submission_is_bounded_and_uses_the_full_volume_runner() -> None:
    import json
    from pathlib import Path

    payload = json.loads(Path("databricks/cloud4-first-fold-submit.json").read_text(encoding="utf-8"))
    task = payload["tasks"][0]["spark_python_task"]
    assert task["python_file"].endswith("cloud4_contract.py")
    assert task["parameters"] == ["--first-fold-gate"]
    assert payload["timeout_seconds"] == 7200


def test_first_fold_gate_selects_only_the_first_frozen_fold() -> None:
    folds = [{"fold_id": "fold-1"}, {"fold_id": "fold-2"}]
    assert select_execution_folds(folds, first_fold_gate=True) == [folds[0]]
    assert select_execution_folds(folds, first_fold_gate=False) == folds
    with pytest.raises(ValueError, match="at least one validation fold"):
        select_execution_folds([], first_fold_gate=True)


def test_first_fold_gate_uses_isolated_restart_tables() -> None:
    official = execution_checkpoint_tables()
    gate = execution_checkpoint_tables(first_fold_gate=True)
    assert set(official.values()).isdisjoint(set(gate.values()))
    assert all(name.endswith("_first_fold_gate") for name in gate.values())
    assert checkpoint_tables_ready(set(gate.values()), first_fold_gate=True)
    assert not checkpoint_tables_ready(set(official.values()), first_fold_gate=True)


def test_restart_requires_the_complete_checkpoint_set() -> None:
    tables = {FOLD_METRICS_TABLE, FOLD_ACTION_TABLE, FOLD_TRIAL_TABLE, FOLD_CHECKPOINT_TABLE}
    assert checkpoint_tables_ready(tables)
    assert not checkpoint_tables_ready(tables - {FOLD_ACTION_TABLE})


def test_portfolio_demo_uses_isolated_tables_and_all_folds() -> None:
    official = execution_checkpoint_tables()
    demo = execution_checkpoint_tables(sample_profile=PORTFOLIO_DEMO_PROFILE)
    assert set(official.values()).isdisjoint(set(demo.values()))
    assert all(name.endswith(PORTFOLIO_DEMO_SUFFIX) for name in demo.values())
    assert checkpoint_tables_ready(set(demo.values()), sample_profile=PORTFOLIO_DEMO_PROFILE)
    folds = [{"fold_id": f"fold-{index}"} for index in range(1, 10)]
    assert select_execution_folds(folds, first_fold_gate=False) == folds


def test_cloud4_execution_profiles_are_validated_and_mutually_exclusive() -> None:
    assert execution_suffix(sample_profile=PORTFOLIO_DEMO_PROFILE) == PORTFOLIO_DEMO_SUFFIX
    with pytest.raises(ValueError, match="cannot be combined"):
        execution_suffix(first_fold_gate=True, sample_profile=PORTFOLIO_DEMO_PROFILE)
    with pytest.raises(ValueError, match="Unknown Cloud 4 sample profile"):
        execution_suffix(sample_profile="random-sample")


def test_portfolio_demo_submission_runs_all_folds_on_sample_profile() -> None:
    import json
    from pathlib import Path

    payload = json.loads(Path("databricks/cloud4-portfolio-demo-submit.json").read_text(encoding="utf-8"))
    task = payload["tasks"][0]["spark_python_task"]
    assert task["python_file"].endswith("cloud4_contract.py")
    assert task["parameters"] == ["--sample-profile", "portfolio-demo"]
    assert payload["timeout_seconds"] == 7200
    assert payload["environments"][0]["spec"]["client"] == "4"
