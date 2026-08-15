import pytest

from wealthsignal_pipeline.cloud4_contract import (
    binary_metrics,
    canonical_sha256,
    select_smallest_best,
    validate_upstream_report,
    checkpoint_tables_ready,
    FOLD_ACTION_TABLE,
    FOLD_CHECKPOINT_TABLE,
    FOLD_METRICS_TABLE,
    FOLD_TRIAL_TABLE,
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


def test_binary_metrics_handles_normal_and_empty_predictions() -> None:
    metrics = binary_metrics(3, 1, 2)
    assert metrics["precision"] == 0.75
    assert metrics["recall"] == 0.6
    assert metrics["f1"] == pytest.approx(2 / 3)
    assert binary_metrics(0, 0, 4) == {"precision": 0.0, "recall": 0.0, "f1": 0.0}


def test_cloud4_source_has_no_graph_framework_or_prospective_read() -> None:
    from wealthsignal_pipeline import cloud4_contract

    source = open(cloud4_contract.__file__, encoding="utf-8").read()
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


def test_cloud4_submissions_use_environment_client_four() -> None:
    import json
    from pathlib import Path

    for name in ("cloud4-submit.json", "cloud4-smoke-submit.json"):
        payload = json.loads(Path("databricks", name).read_text(encoding="utf-8"))
        assert payload["environments"][0]["spec"]["client"] == "4"


def test_restart_requires_the_complete_checkpoint_set() -> None:
    tables = {FOLD_METRICS_TABLE, FOLD_ACTION_TABLE, FOLD_TRIAL_TABLE, FOLD_CHECKPOINT_TABLE}
    assert checkpoint_tables_ready(tables)
    assert not checkpoint_tables_ready(tables - {FOLD_ACTION_TABLE})
