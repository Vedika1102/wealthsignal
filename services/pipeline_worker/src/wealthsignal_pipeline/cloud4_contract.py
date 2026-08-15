"""Protocol V2 Cloud 4 tabular baselines and framework-neutral graph contract.

The job is intentionally Spark-native: the accepted cap-500 Gold table remains
in Unity Catalog and no full dataset is collected on the driver.  It writes
portable Delta tables plus a checksum-bearing JSON manifest.  Graph-learning
framework dependencies are deliberately outside this module.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GOLD_TABLE = "workspace.gold.temporal_examples_50_cap_500"
OUTPUT_SCHEMA = "workspace.gold"
REPORT_ROOT = "/Volumes/workspace/gold/cloud4_reports"
PROSPECTIVE_QUARTER = "2026-06-30"
VALIDATION_START = "2024-03-31"
VALIDATION_END = "2026-03-31"
EMA_ALPHAS = (0.4, 0.6, 0.8)
RIDGE_ALPHAS = (0.1, 1.0, 10.0)
ACTION_THRESHOLDS = (0.25, 0.5, 0.75)
ACTION_CLASS_WEIGHT_MODES = ("none", "balanced")
FEATURE_COLUMNS = (
    "current_weight", "previous_weight", "lag2_weight", "current_rank",
    "previous_rank", "weight_momentum", "rank_momentum", "manager_turnover",
    "manager_concentration_hhi", "peer_owner_count", "peer_aggregate_weight",
    "holding_history_quarters", "quarters_since_last_held",
)


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_upstream_report(report: dict[str, Any]) -> list[str]:
    """Return blocking reasons instead of trusting documentation alone."""
    reasons: list[str] = []
    if report.get("status") != "passed":
        reasons.append("cloud3_not_passed")
    if int(report.get("selected_candidate_cap", -1)) != 500:
        reasons.append("selected_cap_is_not_500")
    if report.get("prospective_q2_2026_truth_accessed") is not False:
        reasons.append("upstream_prospective_guard_failed")
    cap = report.get("cap_reports", {}).get("500", {})
    if any(int(value) for value in cap.get("leakage", {}).values()):
        reasons.append("upstream_leakage_nonzero")
    if not cap.get("partition_manifest_sha256"):
        reasons.append("missing_upstream_partition_checksum")
    folds = report.get("split_manifest", {}).get("folds", [])
    if len(folds) != 9:
        reasons.append("validation_fold_count_mismatch")
    return reasons


def select_smallest_best(scores: dict[float, float], *, maximize: bool = False) -> float:
    """Deterministically select a frozen grid value, preferring smaller ties."""
    if not scores:
        raise ValueError("At least one validation score is required")
    ordered = sorted((float(key), float(value)) for key, value in scores.items())
    best_value = (max if maximize else min)(value for _, value in ordered)
    return min(key for key, value in ordered if abs(value - best_value) <= 1e-12)


def binary_metrics(true_positive: int, false_positive: int, false_negative: int) -> dict[str, float]:
    """Return stable diagnostic metrics for an action-classifier threshold."""
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def _metric_rows(frame, score_column: str, model_name: str, fold_id: str, F, Window):
    predicted = Window.partitionBy("cik", "target_report_period").orderBy(
        F.desc(score_column), F.asc("security_key")
    )
    ideal = Window.partitionBy("cik", "target_report_period").orderBy(
        F.desc("target_weight"), F.asc("security_key")
    )
    ranked = (
        frame.withColumn("predicted_rank", F.row_number().over(predicted))
        .withColumn("ideal_rank", F.row_number().over(ideal))
        .withColumn("relevant", (F.col("target_weight") > 0).cast("double"))
        .withColumn("discount", F.log2(F.col("predicted_rank") + 1.0))
        .withColumn("ideal_discount", F.log2(F.col("ideal_rank") + 1.0))
    )
    return ranked.groupBy("cik", "target_report_period").agg(
        F.sum(F.when(F.col("predicted_rank") <= 10, F.col("target_weight") / F.col("discount")).otherwise(0.0)).alias("dcg10"),
        F.sum(F.when(F.col("ideal_rank") <= 10, F.col("target_weight") / F.col("ideal_discount")).otherwise(0.0)).alias("idcg10"),
        F.sum(F.when(F.col("predicted_rank") <= 20, F.col("target_weight") / F.col("discount")).otherwise(0.0)).alias("dcg20"),
        F.sum(F.when(F.col("ideal_rank") <= 20, F.col("target_weight") / F.col("ideal_discount")).otherwise(0.0)).alias("idcg20"),
        F.sum(F.when(
            (F.col("predicted_rank") <= 10) & (F.col("ideal_rank") <= 10) & (F.col("relevant") > 0), 1
        ).otherwise(0)).alias("top10_hits"),
        F.least(F.lit(10), F.sum("relevant")).alias("top10_denominator"),
        F.avg(F.abs(F.col(score_column) - F.col("target_weight"))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col(score_column) - F.col("target_weight"), 2))).alias("rmse"),
        F.avg(F.when(
            F.col("target_weight") > 0,
            F.abs(F.col(score_column) - F.col("target_weight")),
        )).alias("nonzero_target_mae"),
        F.sqrt(F.avg(F.when(
            F.col("target_weight") > 0,
            F.pow(F.col(score_column) - F.col("target_weight"), 2),
        ))).alias("nonzero_target_rmse"),
        F.corr("predicted_rank", "ideal_rank").alias("rank_correlation"),
        F.count("*").alias("candidate_count"),
    ).select(
        F.lit(fold_id).alias("fold_id"), F.lit(model_name).alias("model_name"),
        "cik", "target_report_period",
        F.when(F.col("idcg10") > 0, F.col("dcg10") / F.col("idcg10")).otherwise(0.0).alias("ndcg_at_10"),
        F.when(F.col("idcg20") > 0, F.col("dcg20") / F.col("idcg20")).otherwise(0.0).alias("ndcg_at_20"),
        F.when(F.col("top10_denominator") > 0, F.col("top10_hits") / F.col("top10_denominator")).otherwise(0.0).alias("recall_at_10"),
        "mae", "rmse", "nonzero_target_mae", "nonzero_target_rmse",
        F.coalesce("rank_correlation", F.lit(0.0)).alias("rank_correlation"),
        "candidate_count",
    )


def _table_fingerprint(frame, columns: list[str], F) -> dict[str, Any]:
    row = frame.select(
        F.xxhash64(*[F.col(name) for name in columns]).cast("decimal(38,0)").alias("row_hash")
    ).agg(F.count("*").alias("row_count"), F.sum("row_hash").alias("hash_sum")).first()
    return {"row_count": int(row["row_count"]), "distributed_xxhash64_sum": str(row["hash_sum"])}


def run_cloud4() -> dict[str, Any]:
    import mlflow
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
    from pyspark.ml.functions import vector_to_array
    from pyspark.ml.regression import GBTRegressor, LinearRegression
    from pyspark.sql import SparkSession, functions as F, Window

    started = time.monotonic()
    spark = SparkSession.builder.appName("wealthsignal-cloud4-contract").getOrCreate()
    upstream_path = Path("/Volumes/workspace/gold/cloud3_reports/cloud3-gold-50-manager.json")
    upstream = json.loads(upstream_path.read_text(encoding="utf-8"))
    blockers = validate_upstream_report(upstream)
    gold = spark.table(GOLD_TABLE)
    prospective_rows = gold.filter(F.col("target_report_period") >= F.lit(PROSPECTIVE_QUARTER).cast("date")).count()
    if prospective_rows:
        blockers.append("prospective_rows_present")
    if blockers:
        raise ValueError(f"Cloud 4 prerequisite gate failed: {blockers}")

    mlflow.set_experiment("/Shared/wealthsignal-protocol-v2-cloud4")
    mlflow_run = mlflow.start_run(run_name="cloud4-50-manager-cap-500")
    mlflow.log_params({
        "candidate_cap": 500,
        "validation_start": VALIDATION_START,
        "validation_end": VALIDATION_END,
        "prospective_quarter": PROSPECTIVE_QUARTER,
        "ema_grid": json.dumps(EMA_ALPHAS),
        "ridge_grid": json.dumps(RIDGE_ALPHAS),
        "action_threshold_grid": json.dumps(ACTION_THRESHOLDS),
        "action_class_weight_modes": json.dumps(ACTION_CLASS_WEIGHT_MODES),
    })

    validation = gold.filter(F.col("target_report_period").between(
        F.lit(VALIDATION_START).cast("date"), F.lit(VALIDATION_END).cast("date")
    ))
    manager_nodes = gold.select("cik").distinct().withColumn(
        "manager_node_id", F.row_number().over(Window.orderBy("cik")) - 1
    ).select("manager_node_id", "cik")
    security_nodes = gold.select("security_key", "cusip").distinct().withColumn(
        "security_node_id", F.row_number().over(Window.orderBy("security_key")) - 1
    ).select("security_node_id", "security_key", "cusip")
    edges = (
        gold.filter(F.col("current_weight") > 0)
        .select("cik", "security_key", "cusip", "report_period", "feature_available_at", F.col("current_weight").alias("weight"))
        .dropDuplicates(["cik", "security_key", "report_period"])
        .join(manager_nodes, "cik").join(security_nodes, ["security_key", "cusip"])
        .select("manager_node_id", "security_node_id", "report_period", "feature_available_at", "weight")
    )
    examples = gold.join(manager_nodes, "cik").join(security_nodes, ["security_key", "cusip"]).select(
        "example_id", "cik", "security_key", "manager_node_id", "security_node_id", "report_period", "target_report_period",
        "feature_available_at", "target_available_at", "current_weight", "previous_weight", "lag2_weight",
        "target_weight", "target_rank", "target_action", "negative_rank", *FEATURE_COLUMNS[3:],
    )

    tables = {
        "manager_nodes": manager_nodes,
        "security_nodes": security_nodes,
        "chronological_edges": edges,
        "forecast_examples": examples,
    }
    table_names: dict[str, str] = {}
    fingerprints: dict[str, Any] = {}
    for name, frame in tables.items():
        table_name = f"{OUTPUT_SCHEMA}.graph_50_cap_500_{name}"
        writer = frame.write.format("delta").mode("overwrite")
        if name in {"chronological_edges", "forecast_examples"}:
            partition_column = "report_period" if name == "chronological_edges" else "target_report_period"
            writer = writer.partitionBy(partition_column)
        writer.saveAsTable(table_name)
        reloaded = spark.table(table_name)
        table_names[name] = table_name
        fingerprints[name] = _table_fingerprint(reloaded, sorted(reloaded.columns), F)

    edge_bounds = edges.agg(
        F.min("report_period").alias("first_report_period"),
        F.max("report_period").alias("last_report_period"),
        F.min("feature_available_at").alias("first_available_at"),
        F.max("feature_available_at").alias("last_available_at"),
    ).first().asDict()
    graph_statistics = {
        "manager_nodes": fingerprints["manager_nodes"]["row_count"],
        "security_nodes": fingerprints["security_nodes"]["row_count"],
        "chronological_edges": fingerprints["chronological_edges"]["row_count"],
        "forecast_examples": fingerprints["forecast_examples"]["row_count"],
        **{name: str(value) for name, value in edge_bounds.items()},
    }

    # Score-only baselines are evaluated through the same metric path used by
    # graph reconciliation. Learned models are fitted within each expanding fold.
    scored_metrics = None
    hyperparameter_trials: list[dict[str, Any]] = []
    action_trials: list[dict[str, Any]] = []
    folds = upstream["split_manifest"]["folds"]
    for fold in folds:
        fold_id = fold["fold_id"]
        quarter = fold["evaluation_target_quarter"]
        train = gold.filter(F.col("target_report_period") < F.lit(quarter).cast("date"))
        evaluate = validation.filter(F.col("target_report_period") == F.lit(quarter).cast("date"))
        score_frames = [
            ("persistence", evaluate.withColumn("score", F.col("current_weight"))),
            ("popularity", evaluate.withColumn("score", F.coalesce("peer_aggregate_weight", F.lit(0.0)))),
        ]
        for alpha in EMA_ALPHAS:
            score_frames.append((f"ema_{alpha}", evaluate.withColumn(
                "score", F.lit(alpha) * F.col("current_weight")
                + F.lit((1-alpha)*alpha) * F.col("previous_weight")
                + F.lit((1-alpha)**2) * F.col("lag2_weight")
            )))

        imputed = [f"{name}_imputed" for name in FEATURE_COLUMNS]
        assembler = VectorAssembler(inputCols=imputed, outputCol="raw_features")
        preprocessing = [Imputer(inputCols=list(FEATURE_COLUMNS), outputCols=imputed), assembler,
                         StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)]
        preprocessing_model = Pipeline(stages=preprocessing).fit(train)
        prepared_train = preprocessing_model.transform(train)
        prepared_evaluate = preprocessing_model.transform(evaluate)
        for alpha in RIDGE_ALPHAS:
            estimator = LinearRegression(
                featuresCol="features", labelCol="target_weight", predictionCol="score",
                regParam=alpha, elasticNetParam=0.0, maxIter=50,
            )
            prediction = estimator.fit(prepared_train).transform(prepared_evaluate).withColumn(
                "score", F.greatest(F.col("score"), F.lit(0.0))
            )
            score_frames.append((f"ridge_{alpha}", prediction))
        gbt = GBTRegressor(
            featuresCol="features", labelCol="target_weight", predictionCol="score",
            maxDepth=5, maxIter=40, seed=20260802,
        )
        score_frames.append(("histogram_gradient_boosting", gbt.fit(prepared_train).transform(prepared_evaluate)))

        for action_name, label_column in (("new_position", "target_is_new"), ("exit", "target_is_exit")):
            counts = prepared_train.agg(
                F.count("*").alias("total"), F.sum(label_column).alias("positive")
            ).first().asDict()
            total = int(counts["total"])
            positive = int(counts["positive"] or 0)
            if positive == 0 or positive == total:
                raise ValueError(f"Fold {fold_id} has only one class for {action_name}")
            balanced_positive_weight = (total - positive) / positive
            for weight_mode in ACTION_CLASS_WEIGHT_MODES:
                positive_weight = balanced_positive_weight if weight_mode == "balanced" else 1.0
                weighted_train = prepared_train.withColumn(
                    "action_weight", F.when(F.col(label_column) == 1, F.lit(positive_weight)).otherwise(F.lit(1.0))
                )
                classifier = LogisticRegression(
                    featuresCol="features", labelCol=label_column, weightCol="action_weight",
                    probabilityCol="action_probability", predictionCol="action_prediction",
                    rawPredictionCol="action_raw_prediction", regParam=1.0, elasticNetParam=0.0,
                    maxIter=50, standardization=False,
                )
                probabilities = classifier.fit(weighted_train).transform(prepared_evaluate).withColumn(
                    "positive_probability", vector_to_array("action_probability")[1]
                )
                for threshold in ACTION_THRESHOLDS:
                    summary = probabilities.agg(
                        F.sum(F.when(
                            (F.col("positive_probability") >= threshold) & (F.col(label_column) == 1), 1
                        ).otherwise(0)).alias("true_positive"),
                        F.sum(F.when(
                            (F.col("positive_probability") >= threshold) & (F.col(label_column) == 0), 1
                        ).otherwise(0)).alias("false_positive"),
                        F.sum(F.when(
                            (F.col("positive_probability") < threshold) & (F.col(label_column) == 1), 1
                        ).otherwise(0)).alias("false_negative"),
                    ).first().asDict()
                    counts_for_metric = {name: int(summary[name] or 0) for name in summary}
                    action_trials.append({
                        "fold_id": fold_id, "target_report_period": quarter, "action": action_name,
                        "class_weight_mode": weight_mode, "positive_class_weight": positive_weight,
                        "threshold": threshold, **counts_for_metric, **binary_metrics(**counts_for_metric),
                    })

        for model_name, scored in score_frames:
            metrics = _metric_rows(scored, "score", model_name, fold_id, F, Window)
            summary = metrics.agg(F.avg("ndcg_at_10").alias("ndcg_at_10"), F.avg("mae").alias("mae")).first()
            hyperparameter_trials.append({"fold_id": fold_id, "model_name": model_name,
                                          "ndcg_at_10": float(summary["ndcg_at_10"]), "mae": float(summary["mae"])})
            scored_metrics = metrics if scored_metrics is None else scored_metrics.unionByName(metrics)

    metrics_table = f"{OUTPUT_SCHEMA}.cloud4_baseline_metrics_50_cap_500"
    scored_metrics.write.format("delta").mode("overwrite").saveAsTable(metrics_table)
    action_metrics_table = f"{OUTPUT_SCHEMA}.cloud4_action_diagnostics_50_cap_500"
    spark.createDataFrame(action_trials).write.format("delta").mode("overwrite").saveAsTable(action_metrics_table)

    # Recompute persistence and every EMA from the portable examples table and
    # require exact aggregate agreement with their tabular counterparts.
    graph_examples = spark.table(table_names["forecast_examples"])
    reconciliation: dict[str, Any] = {}
    for model_name in ["persistence", *[f"ema_{alpha}" for alpha in EMA_ALPHAS]]:
        if model_name == "persistence":
            graph_scored = graph_examples.withColumn("score", F.col("current_weight"))
        else:
            alpha = float(model_name.split("_")[1])
            graph_scored = graph_examples.withColumn(
                "score", F.lit(alpha) * F.col("current_weight")
                + F.lit((1-alpha)*alpha) * F.col("previous_weight")
                + F.lit((1-alpha)**2) * F.col("lag2_weight")
            )
        graph_metrics = None
        for fold in folds:
            quarter = fold["evaluation_target_quarter"]
            part = graph_scored.filter(F.col("target_report_period") == F.lit(quarter).cast("date"))
            item = _metric_rows(part, "score", model_name, fold["fold_id"], F, Window)
            graph_metrics = item if graph_metrics is None else graph_metrics.unionByName(item)
        tabular = scored_metrics.filter(F.col("model_name") == model_name)
        reconciliation_keys = ["fold_id", "model_name", "cik", "target_report_period"]
        compared = tabular.withColumn("_tabular_present", F.lit(1)).alias("t").join(
            graph_metrics.withColumn("_graph_present", F.lit(1)).alias("g"),
            reconciliation_keys,
            "full_outer",
        )
        delta = compared.agg(
            F.sum(F.when(F.col("_tabular_present").isNull(), 1).otherwise(0)).alias("missing_tabular_groups"),
            F.sum(F.when(F.col("_graph_present").isNull(), 1).otherwise(0)).alias("missing_graph_groups"),
            F.max(F.abs(F.col("t.ndcg_at_10") - F.col("g.ndcg_at_10"))).alias("ndcg10_max_abs_delta"),
            F.max(F.abs(F.col("t.ndcg_at_20") - F.col("g.ndcg_at_20"))).alias("ndcg20_max_abs_delta"),
            F.max(F.abs(F.col("t.recall_at_10") - F.col("g.recall_at_10"))).alias("recall10_max_abs_delta"),
            F.max(F.abs(F.col("t.mae") - F.col("g.mae"))).alias("mae_max_abs_delta"),
            F.max(F.abs(F.col("t.rmse") - F.col("g.rmse"))).alias("rmse_max_abs_delta"),
            F.max(F.abs(F.col("t.nonzero_target_mae") - F.col("g.nonzero_target_mae"))).alias("nonzero_mae_max_abs_delta"),
            F.max(F.abs(F.col("t.nonzero_target_rmse") - F.col("g.nonzero_target_rmse"))).alias("nonzero_rmse_max_abs_delta"),
            F.max(F.abs(F.col("t.rank_correlation") - F.col("g.rank_correlation"))).alias("rank_correlation_max_abs_delta"),
            F.count("*").alias("groups"),
        ).first().asDict()
        mismatch_counts = ("missing_tabular_groups", "missing_graph_groups")
        metric_deltas = (
            "ndcg10_max_abs_delta", "ndcg20_max_abs_delta", "recall10_max_abs_delta",
            "mae_max_abs_delta", "rmse_max_abs_delta", "nonzero_mae_max_abs_delta",
            "nonzero_rmse_max_abs_delta", "rank_correlation_max_abs_delta",
        )
        passed = (
            all(int(delta[name] or 0) == 0 for name in mismatch_counts)
            and all(float(delta[name] or 0.0) <= 1e-12 for name in metric_deltas)
        )
        reconciliation[model_name] = {**delta, "passed": passed}
        if not passed:
            raise ValueError(f"Graph/tabular reconciliation failed for {model_name}: {delta}")

    mean_mae_by_model: dict[str, float] = {}
    for model_name in [*[f"ema_{alpha}" for alpha in EMA_ALPHAS], *[f"ridge_{alpha}" for alpha in RIDGE_ALPHAS]]:
        values = [item["mae"] for item in hyperparameter_trials if item["model_name"] == model_name]
        mean_mae_by_model[model_name] = sum(values) / len(values)
    selected_hyperparameters = {
        "ema_alpha": select_smallest_best({alpha: mean_mae_by_model[f"ema_{alpha}"] for alpha in EMA_ALPHAS}),
        "ridge_alpha": select_smallest_best({alpha: mean_mae_by_model[f"ridge_{alpha}"] for alpha in RIDGE_ALPHAS}),
        "selection_metric": "mean_validation_manager_macro_mae",
        "tie_policy": "smallest_value",
    }
    selected_action_diagnostics: dict[str, Any] = {}
    for action_name in ("new_position", "exit"):
        candidates: dict[tuple[str, float], list[float]] = {}
        for item in action_trials:
            if item["action"] == action_name:
                key = (str(item["class_weight_mode"]), float(item["threshold"]))
                candidates.setdefault(key, []).append(float(item["f1"]))
        mean_f1 = {key: sum(values) / len(values) for key, values in candidates.items()}
        best_f1 = max(mean_f1.values())
        selected_key = min(key for key, value in mean_f1.items() if abs(value - best_f1) <= 1e-12)
        selected_action_diagnostics[action_name] = {
            "class_weight_mode": selected_key[0], "threshold": selected_key[1],
            "mean_validation_f1": best_f1, "selection_metric": "mean_validation_f1",
        }
    report = {
        "cloud_milestone": "Cloud 4 baselines and graph contract",
        "status": "passed",
        "source_table": GOLD_TABLE,
        "selected_candidate_cap": 500,
        "upstream_manifest_sha256": upstream["manifest_sha256"],
        "upstream_partition_manifest_sha256": upstream["cap_reports"]["500"]["partition_manifest_sha256"],
        "split_manifest_sha256": canonical_sha256(upstream["split_manifest"]),
        "protocol_sha256": canonical_sha256(Path("docs/ai-governance/forecast-comparison-protocol-v2.md").read_text(encoding="utf-8")),
        "graph_schema_version": 1,
        "graph_tables": table_names,
        "graph_fingerprints": fingerprints,
        "graph_statistics": graph_statistics,
        "baseline_metrics_table": metrics_table,
        "action_diagnostics_table": action_metrics_table,
        "hyperparameter_trials": hyperparameter_trials,
        "action_diagnostic_trials": action_trials,
        "graph_tabular_reconciliation": reconciliation,
        "selected_hyperparameters": selected_hyperparameters,
        "selected_action_diagnostics": selected_action_diagnostics,
        "mlflow_run_id": mlflow_run.info.run_id,
        "prospective_q2_2026_truth_accessed": False,
        "framework_dependencies": [],
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    report["manifest_sha256"] = canonical_sha256(report)
    mlflow.log_dict(report, "cloud4-50-manager-report.json")
    mlflow.log_metrics({
        "graph_reconciliation_models": float(len(reconciliation)),
        "graph_reconciliation_failures": float(sum(not value["passed"] for value in reconciliation.values())),
    })
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gold.cloud4_reports")
    Path(REPORT_ROOT, "cloud4-50-manager.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True, default=str))
    mlflow.end_run(status="FINISHED")
    return report


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    run_cloud4()


if __name__ == "__main__":
    main()
