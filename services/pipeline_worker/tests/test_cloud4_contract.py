import pytest

from wealthsignal_pipeline.cloud4_contract import (
    binary_metrics,
    canonical_sha256,
    select_smallest_best,
    validate_upstream_report,
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
    assert '"split_manifest_sha256"' in source
    assert '"graph_statistics"' in source
