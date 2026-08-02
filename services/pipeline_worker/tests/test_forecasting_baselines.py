from __future__ import annotations

import json
from pathlib import Path

import pytest

from wealthsignal_pipeline.forecasting_baselines import ndcg_at_k, rank_correlation, recall_at_k, run_baselines
from wealthsignal_pipeline.temporal_dataset import build_temporal_dataset
import test_temporal_dataset as temporal_fixture


def _build_input_dataset(root: Path) -> Path:
    return temporal_fixture.TemporalDatasetTests()._write_normalized_dataset(root / "source")


def test_metric_correctness() -> None:
    truth = [3.0, 2.0, 0.0]
    assert ndcg_at_k(truth, truth, 2) == pytest.approx(1.0)
    assert recall_at_k(truth, truth, 1) == pytest.approx(1.0)
    assert recall_at_k(truth, [0.0, 3.0, 2.0], 1) == pytest.approx(0.0)
    assert rank_correlation(truth, truth) == pytest.approx(1.0)
    assert rank_correlation(truth, list(reversed(truth))) == pytest.approx(-1.0)


def test_baseline_run_is_deterministic_and_test_is_locked(tmp_path: Path) -> None:
    source = _build_input_dataset(tmp_path)
    temporal = build_temporal_dataset([source], output_root=tmp_path / "temporal", negative_candidate_limit=2, minimum_train_target_quarters=2, final_test_quarters=1)
    first = run_baselines(temporal, output_root=tmp_path / "models")
    second = run_baselines(temporal, output_root=tmp_path / "models")
    assert first == second
    manifest = json.loads((first / "manifest.json").read_text())
    report = json.loads((first / "comparison_report.json").read_text())
    assert manifest["final_test_status"] == "locked_not_evaluated"
    assert report["final_test_evaluated"] is False
    assert all(fold["role"] == "validation" for fold in report["evaluated_folds"])
    assert {fold["model"] for fold in report["evaluated_folds"]} == {"persistence", "ema", "institutional_popularity", "ridge", "gradient_boosting"}
    assert all("weight_rmse" in fold["metrics"] for fold in report["evaluated_folds"])
    assert (first / "fold-1-logistic_new.json").exists()
    assert (first / "fold-1-logistic_exit.json").exists()


def test_final_test_requires_fixed_protocol(tmp_path: Path) -> None:
    source = _build_input_dataset(tmp_path)
    temporal = build_temporal_dataset([source], output_root=tmp_path / "temporal", negative_candidate_limit=2, minimum_train_target_quarters=2, final_test_quarters=1)
    with pytest.raises(ValueError, match="comparison_protocol"):
        run_baselines(temporal, output_root=tmp_path / "models", include_final_test=True)
