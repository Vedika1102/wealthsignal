"""Build the Protocol V2 Cloud 3 temporal Gold datasets on Databricks."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path


REPORT_ROOT = "/Volumes/workspace/gold/cloud3_reports"
CAPS = (100, 250, 500)
VALIDATION_START = "2024-03-31"
VALIDATION_END = "2026-03-31"
PROSPECTIVE_QUARTER = "2026-06-30"


def select_candidate_cap(coverage_by_cap: dict[int, float], tolerance: float = 0.0025) -> int:
    """Apply the frozen smallest-cap-within-tolerance rule."""

    if tuple(sorted(coverage_by_cap)) != CAPS:
        raise ValueError(f"Candidate-cap study must contain exactly {CAPS}")
    best = max(coverage_by_cap.values())
    return min(cap for cap in CAPS if best - coverage_by_cap[cap] <= tolerance)


def predicate_split_manifest(target_quarters: list[str]) -> dict[str, object]:
    """Describe expanding folds as predicates rather than repeated example IDs."""

    validation = [quarter for quarter in sorted(target_quarters) if VALIDATION_START <= quarter <= VALIDATION_END]
    folds = [
        {
            "fold_id": f"validation-{index}",
            "role": "validation",
            "train_predicate": f"target_report_period < DATE '{quarter}'",
            "evaluation_predicate": f"target_report_period = DATE '{quarter}'",
            "evaluation_target_quarter": quarter,
        }
        for index, quarter in enumerate(validation, start=1)
    ]
    return {
        "strategy": "protocol_v2_expanding_window_by_target_report_quarter",
        "protocol_version": "v2-design-2",
        "validation_window": [VALIDATION_START, VALIDATION_END],
        "all_target_quarters": sorted(target_quarters),
        "status": "ready" if folds else "insufficient_validation_quarters",
        "folds": folds,
    }


def _canonical_sha256(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _next_quarter(column, F):
    return F.last_day(F.add_months(column, 3))


def _build_base(spark):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    holdings = spark.table("workspace.silver.normalized_holdings_50").select(
        "cik", "report_period", "security_key", "cusip", "issuer_name", "available_at", "weight", "rank"
    )
    manager_quarters = holdings.groupBy("cik", "report_period").agg(
        F.max("available_at").alias("feature_available_at"),
        F.count("*").alias("portfolio_size"),
        F.sum(F.col("weight") * F.col("weight")).alias("manager_concentration_hhi"),
    )
    top10 = (
        holdings.withColumn(
            "top10_weight", F.when(F.col("rank") <= 10, F.col("weight")).otherwise(F.lit(0.0))
        )
        .groupBy("cik", "report_period")
        .agg(F.sum("top10_weight").alias("manager_top10_share"))
    )
    manager_quarters = manager_quarters.join(top10, ["cik", "report_period"])
    target_quarters = manager_quarters.select(
        F.col("cik"),
        F.col("report_period").alias("target_report_period"),
        F.col("feature_available_at").alias("target_available_at"),
        F.col("portfolio_size").alias("target_portfolio_size"),
    )
    eligible = (
        manager_quarters.withColumn("target_report_period", _next_quarter(F.col("report_period"), F))
        .join(target_quarters, ["cik", "target_report_period"])
        .filter(F.col("target_available_at") > F.col("feature_available_at"))
        .filter(F.col("target_report_period") <= F.lit(VALIDATION_END).cast("date"))
    )

    # For every manager cutoff, select each peer manager's latest filing that was
    # actually available by that cutoff, then aggregate peer ownership.
    peer_filings = manager_quarters.select(
        F.col("cik").alias("peer_cik"),
        F.col("report_period").alias("peer_report_period"),
        F.col("feature_available_at").alias("peer_available_at"),
    )
    cutoffs = eligible.select("cik", "report_period", "feature_available_at").distinct()
    peer_candidates = cutoffs.join(
        peer_filings,
        (F.col("peer_report_period") <= F.col("report_period"))
        & (F.col("peer_available_at") <= F.col("feature_available_at")),
    )
    peer_window = Window.partitionBy("cik", "report_period", "peer_cik").orderBy(
        F.desc("peer_report_period"), F.desc("peer_available_at")
    )
    latest_peer = peer_candidates.withColumn("peer_order", F.row_number().over(peer_window)).filter(
        F.col("peer_order") == 1
    )
    peer_holdings = holdings.select(
        F.col("cik").alias("peer_cik"),
        F.col("report_period").alias("peer_report_period"),
        "security_key",
        F.col("weight").alias("peer_weight"),
    )
    peer_stats = (
        latest_peer.join(peer_holdings, ["peer_cik", "peer_report_period"])
        .groupBy("cik", "report_period", "security_key")
        .agg(F.count("*").alias("peer_owner_count"), F.sum("peer_weight").alias("peer_aggregate_weight"))
    )

    current = holdings.select(
        "cik", "report_period", "security_key", "cusip", "issuer_name",
        F.col("weight").alias("current_weight"), F.col("rank").alias("current_rank"),
    )
    negative_rank_window = Window.partitionBy("cik", "report_period").orderBy(
        F.desc("peer_owner_count"), F.desc("peer_aggregate_weight"), "security_key"
    )
    negatives = (
        peer_stats.join(current.select("cik", "report_period", "security_key").withColumn("is_current", F.lit(1)),
                        ["cik", "report_period", "security_key"], "left")
        .filter(F.col("is_current").isNull())
        .withColumn("negative_rank", F.row_number().over(negative_rank_window))
        .filter(F.col("negative_rank") <= max(CAPS))
    )
    candidate_keys = (
        current.select("cik", "report_period", "security_key").withColumn("negative_rank", F.lit(0))
        .unionByName(negatives.select("cik", "report_period", "security_key", "negative_rank"))
    )

    reference_window = Window.partitionBy("security_key").orderBy("available_at", "report_period", "cik")
    security_reference = (
        holdings.select("security_key", "cusip", "issuer_name", "report_period", "available_at", "cik")
        .withColumn("reference_order", F.row_number().over(reference_window))
        .filter(F.col("reference_order") == 1)
        .select(
            "security_key", F.col("cusip").alias("reference_cusip"),
            F.col("issuer_name").alias("reference_issuer_name"),
            F.col("report_period").alias("security_first_seen_period"),
            F.col("available_at").alias("security_first_seen_available_at"),
        )
    )
    previous = current.select(
        "cik", F.col("report_period").alias("previous_report_period"), "security_key",
        F.col("current_weight").alias("previous_weight"), F.col("current_rank").alias("previous_rank"),
    )
    previous_sizes = manager_quarters.select(
        "cik", F.col("report_period").alias("previous_report_period"),
        F.col("portfolio_size").alias("previous_portfolio_size"),
    )
    lag2 = current.select(
        "cik", F.col("report_period").alias("lag2_report_period"), "security_key",
        F.col("current_weight").alias("lag2_weight"),
    )
    target = holdings.select(
        "cik", F.col("report_period").alias("target_report_period"), "security_key",
        F.col("weight").alias("target_weight"), F.col("rank").alias("target_rank"),
    )
    current_for_target = current.select(
        "cik", "security_key", _next_quarter(F.col("report_period"), F).alias("target_report_period")
    ).withColumn("held_in_feature_quarter", F.lit(1))
    target_new_counts = (
        target.join(current_for_target, ["cik", "target_report_period", "security_key"], "left")
        .groupBy("cik", "target_report_period")
        .agg(
            F.sum(F.col("held_in_feature_quarter").isNull().cast("long")).alias("total_new_positions")
        )
    )

    # Manager turnover is defined on the union of exact t and t-1 positions.
    turnover_keys = current.select("cik", "report_period", "security_key", "current_weight").join(
        previous.withColumn("report_period", _next_quarter(F.col("previous_report_period"), F)),
        ["cik", "report_period", "security_key"], "full"
    )
    turnover = turnover_keys.groupBy("cik", "report_period").agg(
        (F.sum(F.abs(F.coalesce("current_weight", F.lit(0.0)) - F.coalesce("previous_weight", F.lit(0.0)))) / 2)
        .alias("manager_turnover")
    )

    history = holdings.alias("history").join(
        cutoffs.alias("cutoff"),
        (F.col("history.cik") == F.col("cutoff.cik"))
        & (F.col("history.report_period") <= F.col("cutoff.report_period")),
    ).groupBy(
        F.col("cutoff.cik").alias("cik"), F.col("cutoff.report_period").alias("report_period"),
        F.col("history.security_key").alias("security_key"),
    ).agg(
        F.count("*").alias("holding_history_quarters"),
        F.max("history.report_period").alias("last_held_period"),
    )

    rows = (
        eligible.join(candidate_keys, ["cik", "report_period"])
        .join(current, ["cik", "report_period", "security_key"], "left")
        .withColumn("previous_report_period", F.last_day(F.add_months("report_period", -3)))
        .join(previous_sizes, ["cik", "previous_report_period"], "left")
        .join(previous, ["cik", "previous_report_period", "security_key"], "left")
        .withColumn("lag2_report_period", F.last_day(F.add_months("report_period", -6)))
        .join(lag2, ["cik", "lag2_report_period", "security_key"], "left")
        .join(target, ["cik", "target_report_period", "security_key"], "left")
        .join(target_new_counts, ["cik", "target_report_period"], "left")
        .join(peer_stats, ["cik", "report_period", "security_key"], "left")
        .join(security_reference, "security_key")
        .join(turnover, ["cik", "report_period"], "left")
        .join(history, ["cik", "report_period", "security_key"], "left")
        .withColumn("current_weight", F.coalesce("current_weight", F.lit(0.0)))
        .withColumn("previous_weight", F.coalesce("previous_weight", F.lit(0.0)))
        .withColumn("lag2_weight", F.coalesce("lag2_weight", F.lit(0.0)))
        .withColumn("target_weight", F.coalesce("target_weight", F.lit(0.0)))
        .withColumn("current_rank", F.coalesce("current_rank", F.col("portfolio_size") + 1))
        .withColumn("previous_rank", F.coalesce("previous_rank", F.coalesce("previous_portfolio_size", F.lit(0)) + 1))
        .withColumn("target_rank", F.coalesce("target_rank", F.col("target_portfolio_size") + 1))
        .withColumn("weight_momentum", F.col("current_weight") - F.col("previous_weight"))
        .withColumn("rank_momentum", F.col("previous_rank") - F.col("current_rank"))
        .withColumn(
            "quarters_since_last_held",
            F.when(F.col("last_held_period").isNull(), F.lit(-1)).otherwise(
                F.round(F.months_between("report_period", "last_held_period") / 3).cast("int")
            ),
        )
        .withColumn(
            "target_action",
            F.when((F.col("current_weight") == 0) & (F.col("target_weight") > 0), "new")
            .when((F.col("current_weight") > 0) & (F.col("target_weight") == 0), "exit")
            .when(F.col("target_weight") > F.col("current_weight") + F.lit(1e-8), "increase")
            .when(F.col("target_weight") < F.col("current_weight") - F.lit(1e-8), "decrease")
            .otherwise("unchanged"),
        )
        .withColumn("target_is_new", (F.col("target_action") == "new").cast("int"))
        .withColumn("target_is_exit", (F.col("target_action") == "exit").cast("int"))
        .withColumn("target_is_increase", (F.col("target_action") == "increase").cast("int"))
        .withColumn("target_is_decrease", (F.col("target_action") == "decrease").cast("int"))
        .withColumn("target_is_unchanged", (F.col("target_action") == "unchanged").cast("int"))
        .withColumn("example_id", F.substring(F.sha2(F.concat_ws("|", "cik", "report_period", "security_key"), 256), 1, 24))
        .withColumn("cusip", F.coalesce("cusip", "reference_cusip"))
        .withColumn("issuer_name", F.coalesce("issuer_name", "reference_issuer_name"))
        .withColumn("target_year", F.year("target_report_period"))
        .withColumn("target_quarter", F.quarter("target_report_period"))
    )
    return rows


def _leakage_counts(rows, F) -> dict[str, int]:
    checks = {
        "duplicate_examples": rows.groupBy("example_id").count().filter("count > 1").count(),
        "invalid_target_horizon": rows.filter(_next_quarter(F.col("report_period"), F) != F.col("target_report_period")).count(),
        "target_available_too_early": rows.filter(F.col("target_available_at") <= F.col("feature_available_at")).count(),
        "future_candidates": rows.filter(
            (F.col("security_first_seen_period") > F.col("report_period"))
            | (F.col("security_first_seen_available_at") > F.col("feature_available_at"))
        ).count(),
        "prospective_rows": rows.filter(F.col("target_report_period") >= F.lit(PROSPECTIVE_QUARTER).cast("date")).count(),
    }
    return checks


def run_pipeline() -> dict[str, object]:
    from pyspark.sql import SparkSession, functions as F

    started = time.monotonic()
    spark = SparkSession.builder.appName("wealthsignal-cloud3-gold").getOrCreate()
    base = _build_base(spark)
    cap_reports: dict[str, object] = {}
    coverage_by_cap: dict[int, float] = {}
    all_target_quarters: list[str] = []

    # Materialize the cap-500 superset first. Smaller frozen caps are filtered
    # from that accepted Delta table because serverless compute does not support
    # DataFrame.persist/cache operations.
    for cap in reversed(CAPS):
        rows = base.filter((F.col("negative_rank") == 0) | (F.col("negative_rank") <= cap))
        leakage = _leakage_counts(rows, F)
        if any(leakage.values()):
            raise ValueError(f"Cloud 3 cap {cap} leakage audit failed: {leakage}")
        table = f"workspace.gold.temporal_examples_50_cap_{cap}"
        rows.write.format("delta").mode("overwrite").partitionBy("target_year", "target_quarter").saveAsTable(table)
        reloaded = spark.table(table)
        row_count = reloaded.count()
        target_quarters = [str(row[0]) for row in reloaded.select("target_report_period").distinct().orderBy("target_report_period").collect()]
        all_target_quarters = target_quarters

        manager_quarter = reloaded.groupBy("cik", "target_report_period").agg(
            F.sum("target_weight").alias("covered_weight"),
            F.sum((F.col("target_weight") > 0).cast("long")).alias("covered_target_positions"),
            F.max("target_portfolio_size").alias("target_positions"),
            F.max("total_new_positions").alias("total_new_positions"),
            F.sum("target_is_new").alias("covered_new_positions"),
            F.sum((F.col("target_is_new") + F.col("target_is_increase") > 0).cast("long")).alias("positive_target_rows"),
            F.count("*").alias("candidate_rows"),
        )
        mean_coverage = float(manager_quarter.agg(F.avg("covered_weight")).first()[0] or 0.0)
        coverage_by_cap[cap] = mean_coverage
        aggregate_coverage = manager_quarter.agg(
            (F.sum("covered_target_positions") / F.sum("target_positions")).alias("target_candidate_coverage"),
            F.avg("covered_weight").alias("mean_target_weight_mass_coverage"),
            (F.sum("covered_new_positions") / F.sum("total_new_positions")).alias("new_position_coverage"),
            (1 - F.sum("positive_target_rows") / F.sum("candidate_rows")).alias("zero_target_share"),
        ).first().asDict()
        coverage_by_target_quarter = [row.asDict() for row in (
            manager_quarter.groupBy("target_report_period").agg(
                (F.sum("covered_target_positions") / F.sum("target_positions")).alias("target_candidate_coverage"),
                F.avg("covered_weight").alias("mean_target_weight_mass_coverage"),
                (1 - F.sum("positive_target_rows") / F.sum("candidate_rows")).alias("zero_target_share"),
            ).orderBy("target_report_period").collect()
        )]
        partition_rows = (
            reloaded.withColumn("row_hash", F.xxhash64(*[F.col(name) for name in reloaded.columns]))
            .groupBy("target_report_period")
            .agg(F.count("*").alias("row_count"), F.sum(F.col("row_hash").cast("decimal(38,0)")).alias("hash_sum"))
            .orderBy("target_report_period").collect()
        )
        partition_checksums = [
            {
                "target_report_period": str(item["target_report_period"]),
                "row_count": item["row_count"],
                "distributed_xxhash64_sum": str(item["hash_sum"]),
            }
            for item in partition_rows
        ]
        detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()
        cap_reports[str(cap)] = {
            "table": table,
            "row_count": row_count,
            "manager_quarter_count": manager_quarter.count(),
            "mean_target_weight_mass_coverage": mean_coverage,
            "target_candidate_coverage": float(aggregate_coverage["target_candidate_coverage"] or 0.0),
            "new_position_coverage": float(aggregate_coverage["new_position_coverage"] or 0.0),
            "zero_target_share": float(aggregate_coverage["zero_target_share"] or 0.0),
            "coverage_by_target_quarter": coverage_by_target_quarter,
            "target_quarters": target_quarters,
            "leakage": leakage,
            "partition_columns": detail.get("partitionColumns", []),
            "num_files": detail.get("numFiles"),
            "size_in_bytes": detail.get("sizeInBytes"),
            "partition_checksums": partition_checksums,
            "partition_manifest_sha256": _canonical_sha256(partition_checksums),
            "reload_row_count": row_count,
        }
        if cap == max(CAPS):
            base = reloaded

    selected_cap = select_candidate_cap(coverage_by_cap)
    split_manifest = predicate_split_manifest(all_target_quarters)
    report = {
        "cloud_milestone": "Cloud 3 Gold temporal validation dataset",
        "status": "passed",
        "manager_count": 50,
        "candidate_caps_validation_only": list(CAPS),
        "candidate_cap_selection_tolerance": 0.0025,
        "selected_candidate_cap": selected_cap,
        "coverage_by_cap": {str(key): value for key, value in coverage_by_cap.items()},
        "split_manifest": split_manifest,
        "cap_reports": cap_reports,
        "prospective_q2_2026_truth_accessed": False,
        "observed_peak_memory_bytes": None,
        "memory_note": "Databricks serverless Jobs API does not expose peak driver or executor memory.",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    report["manifest_sha256"] = _canonical_sha256(report)
    Path(REPORT_ROOT).mkdir(parents=True, exist_ok=True)
    output = Path(REPORT_ROOT) / "cloud3-gold-50-manager.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return report


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    run_pipeline()


if __name__ == "__main__":
    main()
