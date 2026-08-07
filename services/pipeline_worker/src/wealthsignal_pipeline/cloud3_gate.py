"""Reconcile and gate the persisted Protocol V2 Cloud 3 Gold tables."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .cloud3_gold import CAPS, REPORT_ROOT, select_candidate_cap


REQUIRED_COLUMNS = {
    "example_id", "cik", "security_key", "report_period", "feature_available_at",
    "target_report_period", "target_available_at", "current_weight", "previous_weight",
    "lag2_weight", "peer_owner_count", "peer_aggregate_weight", "target_weight",
    "target_rank", "target_action", "negative_rank", "target_year", "target_quarter",
}


def validate_report_contract(report: dict[str, object]) -> list[str]:
    reasons: list[str] = []
    coverage = {int(key): float(value) for key, value in report["validation_coverage_by_cap"].items()}
    if int(report["selected_candidate_cap"]) != select_candidate_cap(coverage):
        reasons.append("selected_candidate_cap_mismatch")
    folds = report["split_manifest"]["folds"]
    if len(folds) != 9:
        reasons.append("validation_fold_count_mismatch")
    for fold in folds:
        quarter = fold["evaluation_target_quarter"]
        if fold["train_predicate"] != f"target_report_period < DATE '{quarter}'":
            reasons.append(f"invalid_train_predicate:{fold['fold_id']}")
        if fold["evaluation_predicate"] != f"target_report_period = DATE '{quarter}'":
            reasons.append(f"invalid_evaluation_predicate:{fold['fold_id']}")
    if report.get("prospective_q2_2026_truth_accessed") is not False:
        reasons.append("prospective_guard_failed")
    return reasons


def run_gate() -> dict[str, object]:
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.appName("wealthsignal-cloud3-gate").getOrCreate()
    report_path = Path(REPORT_ROOT) / "cloud3-gold-50-manager.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    blocking_reasons = validate_report_contract(report)
    tables = {cap: spark.table(f"workspace.gold.temporal_examples_50_cap_{cap}") for cap in CAPS}
    selected_cap = int(report["selected_candidate_cap"])
    selected = tables[selected_cap]

    missing_columns = sorted(REQUIRED_COLUMNS - set(selected.columns))
    if missing_columns:
        blocking_reasons.append("missing_required_columns:" + ",".join(missing_columns))
    null_required_rows = selected.filter(
        F.col("example_id").isNull() | F.col("cik").isNull() | F.col("security_key").isNull()
        | F.col("report_period").isNull() | F.col("target_report_period").isNull()
    ).count()
    duplicate_rows = selected.groupBy("example_id").count().filter("count > 1").count()
    prospective_rows = selected.filter(F.col("target_report_period") >= F.lit("2026-06-30").cast("date")).count()
    selected_row_count = selected.count()
    expected_selected_row_count = int(report["cap_reports"][str(selected_cap)]["reload_row_count"])
    if null_required_rows:
        blocking_reasons.append("null_required_rows")
    if duplicate_rows:
        blocking_reasons.append("duplicate_examples")
    if prospective_rows:
        blocking_reasons.append("prospective_rows")
    if selected_row_count != expected_selected_row_count:
        blocking_reasons.append("selected_reload_row_count_mismatch")

    nested_reconciliation: dict[str, object] = {}
    superset = tables[max(CAPS)]
    for cap in CAPS[:-1]:
        expected = superset.filter((F.col("negative_rank") == 0) | (F.col("negative_rank") <= cap))
        actual = tables[cap]
        missing_in_cap = expected.exceptAll(actual).count()
        unexpected_in_cap = actual.exceptAll(expected).count()
        nested_reconciliation[str(cap)] = {
            "expected_rows": expected.count(),
            "actual_rows": actual.count(),
            "missing_in_cap_table": missing_in_cap,
            "unexpected_in_cap_table": unexpected_in_cap,
            "passed": missing_in_cap == 0 and unexpected_in_cap == 0,
        }
        if missing_in_cap or unexpected_in_cap:
            blocking_reasons.append(f"cap_{cap}_nested_reconciliation_failed")

    output = {
        "cloud_milestone": "Cloud 3 Gold reconciliation",
        "passed": not blocking_reasons,
        "selected_candidate_cap": selected_cap,
        "selected_table": f"workspace.gold.temporal_examples_50_cap_{selected_cap}",
        "selected_row_count": selected_row_count,
        "selected_reload_row_count": expected_selected_row_count,
        "null_required_rows": null_required_rows,
        "duplicate_examples": duplicate_rows,
        "prospective_rows": prospective_rows,
        "missing_required_columns": missing_columns,
        "nested_reconciliation": nested_reconciliation,
        "blocking_reasons": blocking_reasons,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    output_path = Path(REPORT_ROOT) / "cloud3-gold-50-manager-reconciliation.json"
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(output, sort_keys=True))
    if blocking_reasons:
        raise ValueError(f"Cloud 3 reconciliation failed: {blocking_reasons}")
    return output


if __name__ == "__main__":
    run_gate()
