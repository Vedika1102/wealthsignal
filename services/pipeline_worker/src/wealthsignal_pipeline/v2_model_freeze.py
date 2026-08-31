"""Offline integrity checks for the Protocol V2/NAVIS execution freeze."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


DEFAULT_MANIFEST = Path("docs/ai-governance/forecast-protocol-v2-model-freeze.json")
EXPECTED_STATUS = "execution_configuration_frozen_validation_and_promotion_pending"
PROSPECTIVE_QUARTER = "2026-06-30"


def _git_normalized_bytes(path: Path) -> bytes:
    """Return text bytes as Git stores them when core.autocrlf normalizes to LF."""

    payload = path.read_bytes()
    return payload.replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def normalized_file_sha256(path: Path) -> str:
    return hashlib.sha256(_git_normalized_bytes(path)).hexdigest()


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return value


def validate_model_freeze(
    repository_root: Path, manifest_path: Path = DEFAULT_MANIFEST,
) -> list[str]:
    """Return every freeze-integrity failure without touching cloud state."""

    root = repository_root.resolve()
    manifest_file = manifest_path if manifest_path.is_absolute() else root / manifest_path
    manifest = _load_json(manifest_file)
    failures: list[str] = []

    def require(condition: bool, reason: str) -> None:
        if not condition:
            failures.append(reason)

    require(manifest.get("status") == EXPECTED_STATUS, "unexpected_freeze_status")
    require(manifest.get("protocol_version") == "v2-design-2", "unexpected_protocol_version")

    artifacts = manifest.get("local_artifacts", [])
    require(isinstance(artifacts, list) and bool(artifacts), "local_artifacts_missing")
    seen_paths: set[str] = set()
    for artifact in artifacts if isinstance(artifacts, list) else []:
        relative = str(artifact.get("path", ""))
        expected = str(artifact.get("sha256", ""))
        require(relative not in seen_paths, f"duplicate_local_artifact:{relative}")
        seen_paths.add(relative)
        require(len(expected) == 64 and expected != "0" * 64, f"invalid_artifact_hash:{relative}")
        path = root / relative
        if not path.is_file():
            failures.append(f"missing_local_artifact:{relative}")
            continue
        actual = normalized_file_sha256(path)
        require(actual == expected, f"local_artifact_hash_mismatch:{relative}:{actual}")

    protocol_hashes = manifest.get("protocol_hashes", {})
    protocol_relative = str(protocol_hashes.get("path", ""))
    protocol_path = root / protocol_relative
    if protocol_path.is_file():
        normalized = _git_normalized_bytes(protocol_path)
        normalized_hash = hashlib.sha256(normalized).hexdigest()
        require(
            normalized_hash == protocol_hashes.get("git_normalized_file_sha256"),
            "protocol_file_hash_mismatch",
        )
        protocol_text = normalized.decode("utf-8")
        require(
            canonical_sha256(protocol_text)
            == protocol_hashes.get("cloud4_canonical_json_string_sha256"),
            "cloud4_protocol_hash_interpretation_mismatch",
        )
    else:
        failures.append("protocol_file_missing")

    cohort = _load_json(root / "docs/ai-governance/forecast-protocol-v2-manager-cohort.json")
    ordered_ciks = cohort.get("main_ordered_ciks", [])
    require(len(ordered_ciks) == 50, "main_cohort_count_mismatch")
    ordered_hash = canonical_sha256(ordered_ciks)
    require(ordered_hash == cohort.get("main_ordered_ciks_sha256"), "cohort_internal_hash_mismatch")

    data_freeze = manifest.get("data_freeze", {})
    require(data_freeze.get("main_manager_count") == 50, "freeze_manager_count_mismatch")
    require(data_freeze.get("selected_candidate_cap") == 500, "freeze_candidate_cap_mismatch")
    require(
        data_freeze.get("main_ordered_ciks_sha256") == ordered_hash,
        "freeze_cohort_hash_mismatch",
    )
    target_windows = data_freeze.get("target_windows", {})
    require(target_windows.get("validation_fold_count") == 9, "validation_fold_count_mismatch")
    require(target_windows.get("prospective_test") == PROSPECTIVE_QUARTER, "prospective_quarter_mismatch")

    development_sources = _load_json(
        root / "docs/ai-governance/forecast-protocol-v2-development-sources.json"
    )
    require(
        development_sources.get("prospective_2026q3_included") is False,
        "development_sources_include_prospective_package",
    )

    cloud3 = _load_json(root / "docs/ai-governance/cloud3-gold-report.json")
    require(cloud3.get("status") == "passed", "cloud3_not_passed")
    require(
        cloud3.get("decision") == "go_for_cloud4_baselines_and_graph_contract",
        "cloud3_decision_mismatch",
    )
    require(
        cloud3.get("candidate_cap_study", {}).get("selected_candidate_cap") == 500,
        "cloud3_cap_decision_mismatch",
    )
    require(
        cloud3.get("guards", {}).get("prospective_q2_2026_truth_accessed") is False,
        "cloud3_prospective_guard_failed",
    )

    cloud4 = _load_json(root / "docs/ai-governance/cloud4-portfolio-demo.json")
    require(cloud4.get("status") == "passed", "cloud4_sample_not_passed")
    require(
        cloud4.get("evidence_classification") == "sample-scale engineering demonstration only",
        "cloud4_evidence_classification_mismatch",
    )
    require(
        cloud4.get("guards", {}).get("prospective_q2_2026_truth_accessed") is False,
        "cloud4_prospective_guard_failed",
    )
    interpretation = cloud4.get("artifacts", {}).get("protocol_hash_interpretation", {})
    require(interpretation.get("same_protocol_text") is True, "cloud4_protocol_hash_not_reconciled")
    require(
        interpretation.get("git_normalized_file_value")
        == protocol_hashes.get("git_normalized_file_sha256"),
        "cloud4_raw_protocol_hash_mismatch",
    )
    require(
        interpretation.get("cloud4_value")
        == protocol_hashes.get("cloud4_canonical_json_string_sha256"),
        "cloud4_canonical_protocol_hash_mismatch",
    )

    tabular = manifest.get("frozen_tabular_models", {})
    require(tabular.get("ema", {}).get("selected_alpha") == 0.8, "ema_selection_mismatch")
    require(tabular.get("ridge", {}).get("selected_alpha") == 0.1, "ridge_selection_mismatch")
    for action in ("new_position", "exit"):
        selected = tabular.get("action_diagnostics", {}).get(action, {})
        require(selected.get("class_weight_mode") == "balanced", f"{action}_weight_mode_mismatch")
        require(selected.get("threshold") == 0.5, f"{action}_threshold_mismatch")
    require(
        tabular.get("action_diagnostics", {}).get("serving_or_alert_authorized") is False,
        "action_diagnostic_serving_guard_failed",
    )

    upstream = manifest.get("navis_upstream", {})
    repository = upstream.get("repository", {})
    require(upstream.get("paper", {}).get("version") == "v1", "navis_paper_version_mismatch")
    require(
        repository.get("commit") == "4bf8ad0b6c2f1bed27b8c469d8f8a5d527c7e9f8",
        "navis_upstream_commit_mismatch",
    )
    require(repository.get("license") == "MIT", "navis_license_mismatch")
    require(len(upstream.get("known_reproducibility_gaps", [])) >= 6, "navis_gaps_not_preserved")

    execution = manifest.get("navis_execution", {})
    variants = execution.get("variants", {})
    require(variants.get("featureless", {}).get("maximum_epochs") == 50, "featureless_epoch_mismatch")
    require(
        variants.get("availability_safe_features", {}).get("maximum_epochs") == 500,
        "feature_epoch_mismatch",
    )
    require(execution.get("seeds", {}).get("official_validation") == [1, 2, 3], "navis_seed_mismatch")
    require(execution.get("loss", {}).get("top_k") == 20, "navis_top_k_mismatch")
    require(execution.get("loss", {}).get("delta") == 0.001, "navis_delta_mismatch")
    require(
        execution.get("prospective_loader_permitted_during_training_or_selection") is False,
        "prospective_loader_guard_failed",
    )

    feature_indices = sorted(
        index
        for item in manifest.get("feature_ablation_layout", [])
        for index in item.get("indices", [])
    )
    require(feature_indices == list(range(23)), "feature_layout_not_exactly_23_slots")

    bootstrap = manifest.get("metrics_and_uncertainty", {}).get("bootstrap", {})
    require(bootstrap.get("resamples") == 2000, "bootstrap_resample_count_mismatch")
    require(bootstrap.get("seed") == 20260831, "bootstrap_seed_mismatch")

    graph = manifest.get("graph_bundle_contract", {})
    require(PROSPECTIVE_QUARTER in graph.get("forbidden_target_report_periods", []), "graph_guard_missing")
    require(graph.get("reconciliation_tolerance") == 1e-12, "graph_tolerance_mismatch")
    require(
        graph.get("current_materialization_status")
        == "contract frozen; immutable file export not yet created",
        "graph_export_status_mismatch",
    )

    guards = manifest.get("guards", {})
    false_guards = (
        "prospective_q2_2026_truth_accessed",
        "prospective_source_downloaded",
        "navis_run_completed",
        "model_promoted",
        "model_serving_authorized",
        "cloud4_rerun_started",
        "aws_resources_created",
        "runpod_resources_created",
        "paid_databricks_resources_created",
    )
    for key in false_guards:
        require(guards.get(key) is False, f"guard_not_false:{key}")
    require(guards.get("cost_incurred_by_this_milestone_usd") == 0, "unexpected_milestone_cost")

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    failures = validate_model_freeze(args.repository_root, args.manifest)
    print(json.dumps({"status": "passed" if not failures else "failed", "failures": failures}, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
