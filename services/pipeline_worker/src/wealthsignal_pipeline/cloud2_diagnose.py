"""Produce a bounded, machine-readable Cloud 2 reconciliation mismatch inventory."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


EXTRACTED_ROOT = "/Volumes/workspace/bronze/sec_13f_extracted"
REFERENCE_ROOT = "/Volumes/workspace/silver/cloud2_reports"
REPORT_ROOT = "/Volumes/workspace/silver/cloud2_reports"


def run_diagnostics(manager_count: int, reference_path: str) -> dict[str, object]:
    from pyspark.sql import SparkSession, functions as F, types as T

    spark = SparkSession.builder.getOrCreate()
    schema = T.StructType([
        T.StructField("cik", T.StringType()), T.StructField("report_period", T.DateType()),
        T.StructField("security_key", T.StringType()), T.StructField("value_usd", T.LongType()),
        T.StructField("shares_or_principal", T.DoubleType()), T.StructField("portfolio_weight", T.DoubleType()),
        T.StructField("holding_rank", T.IntegerType()), T.StructField("available_at", T.DateType()),
        T.StructField("source_accession_numbers", T.StringType()),
    ])
    python = (
        spark.read.option("header", True).option("mode", "FAILFAST").option("escape", '"')
        .schema(schema).csv(reference_path)
        .withColumn("python_accessions", F.sort_array(F.from_json("source_accession_numbers", "array<string>")))
        .drop("source_accession_numbers")
    )
    silver = spark.table(f"workspace.silver.normalized_holdings_{manager_count}").select(
        "cik", "report_period", "security_key",
        F.col("value_usd").alias("spark_value_usd"), F.col("shares_or_principal").alias("spark_shares"),
        F.col("weight").alias("spark_weight"), F.col("rank").alias("spark_rank"),
        F.col("available_at").alias("spark_available_at"),
        F.sort_array("source_accession_numbers").alias("spark_accessions"),
    )
    mismatch = (
        python.join(silver, ["cik", "report_period", "security_key"], "full_outer")
        .filter(F.col("portfolio_weight").isNull() | F.col("spark_weight").isNull())
        .withColumn("mismatch_side", F.when(F.col("spark_weight").isNull(), "python_only").otherwise("spark_only"))
        .withColumn("trace_accessions", F.coalesce("python_accessions", "spark_accessions"))
    )
    raw = spark.read.options(header=True, sep="\t", quote="\u0000", mode="PERMISSIVE").csv(
        f"{EXTRACTED_ROOT}/package=*/INFOTABLE.tsv"
    )
    raw_cusip = F.trim(F.col("CUSIP"))
    normalized = F.upper(F.regexp_replace(raw_cusip, r"[^A-Z0-9*@#]", ""))
    raw = raw.select(
        F.trim("ACCESSION_NUMBER").alias("trace_accession"), raw_cusip.alias("raw_cusip"),
        normalized.alias("normalized_cusip"), F.upper(F.trim(F.coalesce("PUTCALL", F.lit("")))).alias("raw_put_call"),
    ).withColumn(
        "raw_security_key",
        F.concat("normalized_cusip", F.lit("|"), F.when(F.col("raw_put_call").isin("PUT", "CALL"), F.col("raw_put_call")).otherwise(F.lit("LONG"))),
    )
    selected = spark.table(f"workspace.silver.effective_filings_{manager_count}").select(
        F.col("accession_number").alias("trace_accession")
    ).distinct().withColumn("selected_by_spark", F.lit(True))
    traced = (
        mismatch.withColumn("trace_accession", F.explode_outer("trace_accessions"))
        .join(raw, "trace_accession", "left").join(selected, "trace_accession", "left")
        .groupBy(*mismatch.columns)
        .agg(
            F.sort_array(F.collect_set("trace_accession")).alias("traced_accessions"),
            F.sort_array(F.collect_set("raw_cusip")).alias("raw_cusips"),
            F.sort_array(F.collect_set("normalized_cusip")).alias("normalized_cusips"),
            F.sort_array(F.collect_set("raw_security_key")).alias("raw_security_keys"),
            F.min(F.coalesce("selected_by_spark", F.lit(False)).cast("int")).cast("boolean").alias("all_accessions_selected_by_spark"),
        )
        .withColumn(
            "exclusion_reason",
            F.when(F.col("mismatch_side") == "spark_only", "absent_from_python_reference")
            .when(~F.col("all_accessions_selected_by_spark"), "source_accession_not_selected_by_spark")
            .when(F.array_contains("raw_security_keys", F.col("security_key")), "missing_after_selected_raw_holding")
            .otherwise("raw_security_key_differs_after_spark_normalization"),
        )
    )
    table = f"workspace.silver.cloud2_reconciliation_mismatches_{manager_count}"
    traced.write.format("delta").mode("overwrite").saveAsTable(table)
    output_path = f"{REPORT_ROOT}/cloud2-{manager_count}-manager-mismatch-inventory"
    traced.orderBy("mismatch_side", "cik", "report_period", "security_key").coalesce(1).write.mode("overwrite").json(output_path)
    counts = {row["exclusion_reason"]: row["count"] for row in traced.groupBy("exclusion_reason").count().collect()}
    metrics = {
        "manager_count": manager_count, "mismatch_rows": traced.count(), "reason_counts": counts,
        "inventory_table": table, "inventory_path": output_path,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    Path(REPORT_ROOT).mkdir(parents=True, exist_ok=True)
    (Path(REPORT_ROOT) / f"cloud2-{manager_count}-manager-diagnostic.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(json.dumps(metrics, sort_keys=True))
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manager-count", type=int, default=10)
    parser.add_argument("--reference-path")
    args = parser.parse_args()
    run_diagnostics(args.manager_count, args.reference_path or f"{REFERENCE_ROOT}/protocol-v2-python-reference-{args.manager_count}.csv")


if __name__ == "__main__":
    main()
