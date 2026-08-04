"""Databricks Cloud 2 Bronze-to-Silver PySpark pipeline for Protocol V2."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile


SOURCE_VOLUME = "/Volumes/workspace/bronze/sec_13f_development"
EXTRACTED_ROOT = "/Volumes/workspace/bronze/sec_13f_extracted"
REPORT_ROOT = "/Volumes/workspace/silver/cloud2_reports"
COHORT_PATH = "docs/ai-governance/forecast-protocol-v2-manager-cohort.json"
MANIFEST_PATH = "docs/ai-governance/forecast-protocol-v2-development-sources.json"
TABLES = ("SUBMISSION", "COVERPAGE", "INFOTABLE")


def security_key_expression(cusip_column: str = "cusip", put_call_column: str = "put_call") -> str:
    """Return the frozen V2 CUSIP-plus-side Spark SQL expression."""
    return (
        f"concat({cusip_column}, '|', "
        f"case when upper(trim(coalesce({put_call_column}, ''))) in ('PUT','CALL') "
        f"then upper(trim({put_call_column})) else 'LONG' end)"
    )


def repository_root(script_path: str | Path | None = None) -> Path:
    """Resolve the Git checkout under local Python and Databricks' exec wrapper."""
    path = Path(script_path or sys.argv[0]).resolve()
    for parent in (path, *path.parents):
        if (parent / COHORT_PATH).is_file() and (parent / MANIFEST_PATH).is_file():
            return parent
    raise RuntimeError(f"cannot locate WealthSignal repository from {path}")


def sec_date_expression(column: str) -> str:
    """Parse the two date encodings present in SEC bulk packages and fixtures."""
    return (
        f"coalesce(to_date(try_to_timestamp({column}, 'dd-MMM-yyyy')), "
        f"to_date(try_to_timestamp({column}, 'yyyy-MM-dd')))"
    )


def _extract_packages(source_root: Path, extracted_root: Path, packages: list[str]) -> None:
    """Stream TSV members to managed storage without retaining row objects."""
    for package in packages:
        destination = extracted_root / f"package={package}"
        destination.mkdir(parents=True, exist_ok=True)
        archive_path = source_root / f"{package}_form13f.zip"
        with ZipFile(archive_path) as archive:
            members = {Path(name).name.upper(): name for name in archive.namelist()}
            for table in TABLES:
                output = destination / f"{table}.tsv"
                if output.exists():
                    continue
                member = members.get(f"{table}.TSV")
                if member is None:
                    raise ValueError(f"{archive_path} lacks {table}.tsv")
                with archive.open(member) as source, output.open("wb") as target:
                    while block := source.read(1024 * 1024):
                        target.write(block)


def run_pipeline(manager_count: int) -> dict[str, object]:
    from pyspark.sql import SparkSession, Window, functions as F, types as T

    if manager_count not in {10, 25, 50}:
        raise ValueError("manager_count must be one of 10, 25, or 50")
    started = time.monotonic()
    spark = SparkSession.builder.getOrCreate()
    repo_root = repository_root()
    source_root = repo_root / "services" / "pipeline_worker" / "src"
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    from wealthsignal_pipeline.identifiers import cusip_normalization_sql
    cohort = json.loads((repo_root / COHORT_PATH).read_text(encoding="utf-8"))
    manifest = json.loads((repo_root / MANIFEST_PATH).read_text(encoding="utf-8"))
    managers = cohort["main_ordered_ciks"][:manager_count]
    packages = [row["package"] for row in manifest["packages"]]
    _extract_packages(Path(SOURCE_VOLUME), Path(EXTRACTED_ROOT), packages)

    read_options = {"header": "true", "sep": "\t", "quote": "\u0000", "mode": "PERMISSIVE"}
    submissions = spark.read.options(**read_options).csv(f"{EXTRACTED_ROOT}/package=*/SUBMISSION.tsv")
    covers = spark.read.options(**read_options).csv(f"{EXTRACTED_ROOT}/package=*/COVERPAGE.tsv")
    info = spark.read.options(**read_options).csv(f"{EXTRACTED_ROOT}/package=*/INFOTABLE.tsv")

    manager_df = spark.createDataFrame([(value,) for value in managers], "cik string")
    submissions = (
        submissions
        .select(
            F.trim(F.col("ACCESSION_NUMBER")).alias("accession_number"),
            F.lpad(F.trim(F.col("CIK")), 10, "0").alias("cik"),
            F.expr(sec_date_expression("FILING_DATE")).alias("filing_date"),
            F.expr(sec_date_expression("PERIODOFREPORT")).alias("report_period"),
            F.upper(F.trim(F.col("SUBMISSIONTYPE"))).alias("submission_type"),
            F.col("_metadata.file_path").alias("source_file"),
        )
        .join(F.broadcast(manager_df), "cik")
        .filter(F.col("submission_type").isin("13F-HR", "13F-HR/A"))
        .filter(F.col("report_period").between("2019-03-31", "2026-03-31"))
        .withColumn("package", F.regexp_extract("source_file", r"package=([^/]+)", 1))
    )
    covers = covers.select(
        F.trim(F.col("ACCESSION_NUMBER")).alias("accession_number"),
        F.trim(F.col("FILINGMANAGER_NAME")).alias("filer_name"),
        F.upper(F.trim(F.coalesce(F.col("AMENDMENTTYPE"), F.lit("")))).alias("amendment_type"),
        F.coalesce(F.col("AMENDMENTNO").cast("int"), F.lit(0)).alias("amendment_number"),
    )
    filings = submissions.join(covers, "accession_number", "left")
    valid_accessions = filings.select("accession_number").distinct()

    cleaned_cusip = F.expr(cusip_normalization_sql("CUSIP"))
    holdings = (
        info.join(F.broadcast(valid_accessions), F.trim(info["ACCESSION_NUMBER"]) == valid_accessions["accession_number"])
        .drop(valid_accessions["accession_number"])
        .select(
            F.trim(F.col("ACCESSION_NUMBER")).alias("accession_number"),
            F.trim(F.col("INFOTABLE_SK")).alias("info_table_key"),
            F.trim(F.col("NAMEOFISSUER")).alias("issuer_name"),
            F.trim(F.col("TITLEOFCLASS")).alias("title_of_class"),
            cleaned_cusip.alias("cusip"),
            F.upper(F.trim(F.coalesce(F.col("PUTCALL"), F.lit("")))).alias("put_call"),
            F.regexp_replace(F.col("VALUE"), ",", "").cast("long").alias("reported_value"),
            F.regexp_replace(F.col("SSHPRNAMT"), ",", "").cast("double").alias("shares_or_principal"),
        )
        .filter(F.length("cusip") == 9)
        .withColumn("security_key", F.expr(security_key_expression()))
        .join(filings.select("accession_number", "filing_date"), "accession_number")
        .withColumn(
            "value_usd",
            F.when(F.col("filing_date") < F.lit("2023-01-03").cast("date"), F.col("reported_value") * 1000)
            .otherwise(F.col("reported_value")),
        )
    )
    per_accession = holdings.groupBy("accession_number", "security_key", "cusip", "put_call").agg(
        F.sum("value_usd").alias("value_usd"),
        F.sum("shares_or_principal").alias("shares_or_principal"),
        F.first("issuer_name", ignorenulls=True).alias("issuer_name"),
        F.first("title_of_class", ignorenulls=True).alias("title_of_class"),
    )

    order_window = Window.partitionBy("cik", "report_period").orderBy(
        "filing_date", "amendment_number", "accession_number"
    )
    filing_order = filings.withColumn("filing_order", F.row_number().over(order_window))
    base_order = filing_order.groupBy("cik", "report_period").agg(
        F.max(F.when(F.col("submission_type") == "13F-HR", F.col("filing_order"))).alias("base_order")
    )
    candidates = (
        filing_order.join(base_order, ["cik", "report_period"])
        .filter(F.col("filing_order") >= F.coalesce("base_order", F.lit(1)))
        .withColumn("is_additive", F.col("amendment_type").rlike("NEW HOLDING|ADD"))
    )
    replacement_order = candidates.groupBy("cik", "report_period").agg(
        F.max(F.when(~F.col("is_additive"), F.col("filing_order"))).alias("replacement_order")
    )
    selected = (
        candidates.join(replacement_order, ["cik", "report_period"])
        .filter(
            (F.col("filing_order") == F.col("replacement_order"))
            | (F.col("is_additive") & (F.col("filing_order") > F.col("replacement_order")))
            | (F.col("replacement_order").isNull() & F.col("is_additive"))
        )
    )
    effective = selected.join(per_accession, "accession_number").groupBy(
        "cik", "report_period", "security_key", "cusip", "put_call"
    ).agg(
        F.sum("value_usd").alias("value_usd"),
        F.sum("shares_or_principal").alias("shares_or_principal"),
        F.first("issuer_name", ignorenulls=True).alias("issuer_name"),
        F.first("title_of_class", ignorenulls=True).alias("title_of_class"),
        F.max("filing_date").alias("available_at"),
        F.sort_array(F.collect_set("accession_number")).alias("source_accession_numbers"),
    )
    portfolio_window = Window.partitionBy("cik", "report_period")
    rank_window = Window.partitionBy("cik", "report_period").orderBy(F.desc("value_usd"), "security_key")
    normalized = (
        effective.withColumn("portfolio_value_usd", F.sum("value_usd").over(portfolio_window))
        .withColumn("weight", F.col("value_usd") / F.col("portfolio_value_usd"))
        .withColumn("rank", F.row_number().over(rank_window))
        .withColumn("report_year", F.year("report_period"))
        .withColumn("report_quarter", F.quarter("report_period"))
    )

    table_suffix = str(manager_count)
    holdings_table = f"workspace.silver.normalized_holdings_{table_suffix}"
    filings_table = f"workspace.silver.effective_filings_{table_suffix}"
    (normalized.write.format("delta").mode("overwrite").partitionBy("report_year", "report_quarter").saveAsTable(holdings_table))
    (selected.write.format("delta").mode("overwrite").saveAsTable(filings_table))

    metrics = {
        "cloud_milestone": "Cloud 2",
        "manager_count": manager_count,
        "package_count": len(packages),
        "submission_rows": submissions.count(),
        "selected_filing_rows": selected.count(),
        "normalized_holding_rows": normalized.count(),
        "distinct_managers": normalized.select("cik").distinct().count(),
        "distinct_report_periods": normalized.select("report_period").distinct().count(),
        "min_report_period": str(normalized.agg(F.min("report_period")).first()[0]),
        "max_report_period": str(normalized.agg(F.max("report_period")).first()[0]),
        "prospective_rows": normalized.filter(F.col("report_period") > F.lit("2026-03-31").cast("date")).count(),
        "portfolio_weight_max_abs_error": normalized.groupBy("cik", "report_period").agg(
            F.abs(F.sum("weight") - F.lit(1.0)).alias("error")
        ).agg(F.max("error")).first()[0],
        "invalid_cusip_rows": info.join(F.broadcast(valid_accessions), F.trim(info["ACCESSION_NUMBER"]) == valid_accessions["accession_number"]).filter(F.length(cleaned_cusip) != 9).count(),
        "duplicate_rows_resolved": holdings.count() - per_accession.count(),
        "holdings_table": holdings_table,
        "filings_table": filings_table,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    Path(REPORT_ROOT).mkdir(parents=True, exist_ok=True)
    report_path = Path(REPORT_ROOT) / f"cloud2-{manager_count}-manager.json"
    report_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(metrics, sort_keys=True))
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manager-count", type=int, required=True, choices=(10, 25, 50))
    args = parser.parse_args()
    run_pipeline(args.manager_count)


if __name__ == "__main__":
    main()
