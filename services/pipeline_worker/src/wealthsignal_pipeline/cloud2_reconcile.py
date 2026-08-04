"""Reconcile the pure-Python Protocol V2 reference with Cloud 2 Silver output."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


REFERENCE_ROOT = "/Volumes/workspace/silver/cloud2_reports"
REPORT_ROOT = "/Volumes/workspace/silver/cloud2_reports"


def run_reconciliation(manager_count: int, reference_path: str) -> dict[str, object]:
    from pyspark.sql import SparkSession, functions as F, types as T

    spark = SparkSession.builder.getOrCreate()
    schema = T.StructType([
        T.StructField("cik", T.StringType(), False),
        T.StructField("report_period", T.DateType(), False),
        T.StructField("security_key", T.StringType(), False),
        T.StructField("value_usd", T.LongType(), False),
        T.StructField("shares_or_principal", T.DoubleType(), False),
        T.StructField("portfolio_weight", T.DoubleType(), False),
        T.StructField("holding_rank", T.IntegerType(), False),
        T.StructField("available_at", T.DateType(), False),
        T.StructField("source_accession_numbers", T.StringType(), False),
    ])
    python = (
        spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(reference_path)
        .withColumn("python_accessions", F.sort_array(F.from_json("source_accession_numbers", "array<string>")))
        .drop("source_accession_numbers")
    )
    spark_rows = spark.table(f"workspace.silver.normalized_holdings_{manager_count}").select(
        "cik", "report_period", "security_key",
        F.col("value_usd").alias("spark_value_usd"),
        F.col("shares_or_principal").alias("spark_shares"),
        F.col("weight").alias("spark_weight"),
        F.col("rank").alias("spark_rank"),
        F.col("available_at").alias("spark_available_at"),
        F.sort_array("source_accession_numbers").alias("spark_accessions"),
    )
    joined = python.alias("p").join(
        spark_rows.alias("s"), ["cik", "report_period", "security_key"], "full_outer"
    )
    missing_in_python = F.col("portfolio_weight").isNull()
    missing_in_spark = F.col("spark_weight").isNull()
    present = ~missing_in_python & ~missing_in_spark
    weight_delta = F.abs(F.col("portfolio_weight") - F.col("spark_weight"))
    shares_delta = F.abs(F.col("shares_or_principal") - F.col("spark_shares"))
    row = joined.agg(
        F.count("*").alias("joined_rows"),
        F.sum(missing_in_python.cast("long")).alias("missing_in_python"),
        F.sum(missing_in_spark.cast("long")).alias("missing_in_spark"),
        F.sum((present & (F.col("value_usd") != F.col("spark_value_usd"))).cast("long")).alias("value_mismatches"),
        F.sum((present & (F.col("holding_rank") != F.col("spark_rank"))).cast("long")).alias("rank_mismatches"),
        F.sum((present & (shares_delta > F.lit(1e-6))).cast("long")).alias("shares_mismatches"),
        F.sum((present & (weight_delta > F.lit(1e-12))).cast("long")).alias("weight_mismatches"),
        F.sum((present & (F.col("available_at") != F.col("spark_available_at"))).cast("long")).alias("availability_mismatches"),
        F.sum((present & ~F.col("python_accessions").eqNullSafe(F.col("spark_accessions"))).cast("long")).alias("accession_mismatches"),
        F.max(F.when(present, weight_delta)).alias("max_weight_delta"),
        F.max(F.when(present, shares_delta)).alias("max_shares_delta"),
    ).first().asDict()
    mismatch_fields = [
        "missing_in_python", "missing_in_spark", "value_mismatches", "rank_mismatches",
        "shares_mismatches", "weight_mismatches", "availability_mismatches", "accession_mismatches",
    ]
    metrics = {
        "cloud_milestone": "Cloud 2 reconciliation",
        "manager_count": manager_count,
        "reference_path": reference_path,
        **row,
        "passed": all((row.get(field) or 0) == 0 for field in mismatch_fields),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    Path(REPORT_ROOT).mkdir(parents=True, exist_ok=True)
    report_path = Path(REPORT_ROOT) / f"cloud2-{manager_count}-manager-reconciliation.json"
    report_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(metrics, sort_keys=True))
    if not metrics["passed"]:
        raise RuntimeError("Cloud 2 reconciliation failed; scaling remains paused")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manager-count", type=int, default=10)
    parser.add_argument("--reference-path")
    args = parser.parse_args()
    reference_path = args.reference_path or f"{REFERENCE_ROOT}/protocol-v2-python-reference-{args.manager_count}.csv"
    run_reconciliation(args.manager_count, reference_path)


if __name__ == "__main__":
    main()
