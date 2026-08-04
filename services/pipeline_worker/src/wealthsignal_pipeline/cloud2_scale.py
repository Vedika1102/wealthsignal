"""Validate Cloud 2 scale output and nested-cohort prefix stability."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


REPORT_ROOT = "/Volumes/workspace/silver/cloud2_reports"


PREFIX_MISMATCH_FIELDS = (
    "missing_in_baseline", "missing_in_scaled", "value_mismatches", "shares_mismatches",
    "weight_mismatches", "rank_mismatches", "availability_mismatches", "accession_mismatches",
)


def scale_go_no_go(metrics: dict[str, object]) -> tuple[bool, list[str]]:
    """Return the frozen 25-to-50 acceptance decision and blocking reasons."""
    reasons = [field for field in PREFIX_MISMATCH_FIELDS if int(metrics.get(field, 0) or 0) != 0]
    if int(metrics.get("prospective_rows", 0) or 0) != 0:
        reasons.append("prospective_rows")
    if float(metrics.get("portfolio_weight_max_abs_error", 0.0) or 0.0) > 1e-12:
        reasons.append("portfolio_weight_max_abs_error")
    return not reasons, reasons


def _detail(spark, table: str) -> dict[str, object]:
    row = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()
    return {
        "table": table,
        "format": row.get("format"),
        "num_files": row.get("numFiles"),
        "size_in_bytes": row.get("sizeInBytes"),
        "partition_columns": row.get("partitionColumns"),
    }


def run_scale_validation(manager_count: int, baseline_manager_count: int = 10) -> dict[str, object]:
    from pyspark.sql import SparkSession, functions as F

    if manager_count != 25 or baseline_manager_count != 10:
        raise ValueError("the current scale gate is frozen to 10 -> 25 managers")
    spark = SparkSession.builder.getOrCreate()
    baseline_table = f"workspace.silver.normalized_holdings_{baseline_manager_count}"
    scaled_table = f"workspace.silver.normalized_holdings_{manager_count}"
    baseline = spark.table(baseline_table)
    baseline_ciks = baseline.select("cik").distinct()
    scaled_prefix = spark.table(scaled_table).join(F.broadcast(baseline_ciks), "cik")
    keys = ["cik", "report_period", "security_key"]
    left = baseline.select(
        *keys, F.col("value_usd").alias("b_value"), F.col("shares_or_principal").alias("b_shares"),
        F.col("weight").alias("b_weight"), F.col("rank").alias("b_rank"),
        F.col("available_at").alias("b_available"), F.sort_array("source_accession_numbers").alias("b_accessions"),
    )
    right = scaled_prefix.select(
        *keys, F.col("value_usd").alias("s_value"), F.col("shares_or_principal").alias("s_shares"),
        F.col("weight").alias("s_weight"), F.col("rank").alias("s_rank"),
        F.col("available_at").alias("s_available"), F.sort_array("source_accession_numbers").alias("s_accessions"),
    )
    joined = left.join(right, keys, "full_outer")
    present = F.col("b_value").isNotNull() & F.col("s_value").isNotNull()
    prefix = joined.agg(
        F.sum(F.col("b_value").isNull().cast("long")).alias("missing_in_baseline"),
        F.sum(F.col("s_value").isNull().cast("long")).alias("missing_in_scaled"),
        F.sum((present & (F.col("b_value") != F.col("s_value"))).cast("long")).alias("value_mismatches"),
        F.sum((present & (F.abs(F.col("b_shares") - F.col("s_shares")) > 1e-6)).cast("long")).alias("shares_mismatches"),
        F.sum((present & (F.abs(F.col("b_weight") - F.col("s_weight")) > 1e-12)).cast("long")).alias("weight_mismatches"),
        F.sum((present & (F.col("b_rank") != F.col("s_rank"))).cast("long")).alias("rank_mismatches"),
        F.sum((present & (F.col("b_available") != F.col("s_available"))).cast("long")).alias("availability_mismatches"),
        F.sum((present & ~F.col("b_accessions").eqNullSafe(F.col("s_accessions"))).cast("long")).alias("accession_mismatches"),
    ).first().asDict()
    scaled = spark.table(scaled_table)
    weight_error = scaled.groupBy("cik", "report_period").agg(
        F.abs(F.sum("weight") - F.lit(1.0)).alias("error")
    ).agg(F.max("error")).first()[0]
    quality = {
        "manager_count": scaled.select("cik").distinct().count(),
        "holding_rows": scaled.count(),
        "report_period_count": scaled.select("report_period").distinct().count(),
        "min_report_period": str(scaled.agg(F.min("report_period")).first()[0]),
        "max_report_period": str(scaled.agg(F.max("report_period")).first()[0]),
        "prospective_rows": scaled.filter(F.col("report_period") > F.lit("2026-03-31").cast("date")).count(),
        "portfolio_weight_max_abs_error": weight_error,
        "scan_partitions": scaled.select(F.spark_partition_id().alias("partition_id")).distinct().count(),
    }
    decision, reasons = scale_go_no_go({**prefix, **quality})
    metrics = {
        "cloud_milestone": "Cloud 2 25-manager scaling",
        "baseline_manager_count": baseline_manager_count,
        **prefix, **quality,
        "holdings_detail": _detail(spark, scaled_table),
        "filings_detail": _detail(spark, f"workspace.silver.effective_filings_{manager_count}"),
        "observed_peak_memory_bytes": None,
        "memory_measurement_note": "Databricks serverless Jobs API does not expose peak driver/executor memory for this run.",
        "cost_projection": {
            "projected_incremental_usd_if_free_edition_quota_suffices": 0.0,
            "paid_mode_projection_usd": None,
            "note": "No DBU usage or paid rate is exposed for this Free Edition serverless run; do not infer paid cost.",
        },
        "go_for_50_manager_build": decision,
        "blocking_reasons": reasons,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    Path(REPORT_ROOT).mkdir(parents=True, exist_ok=True)
    report = Path(REPORT_ROOT) / "cloud2-25-manager-scale.json"
    report.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(metrics, sort_keys=True))
    if not decision:
        raise RuntimeError("25-manager scale gate failed; 50-manager build remains unauthorized")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manager-count", type=int, default=25)
    parser.add_argument("--baseline-manager-count", type=int, default=10)
    args = parser.parse_args()
    run_scale_validation(args.manager_count, args.baseline_manager_count)


if __name__ == "__main__":
    main()
