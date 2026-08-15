"""Bounded Cloud 4 Free Edition compatibility smoke gate.

This job is engineering evidence only.  It reads a deterministic pre-validation
slice, exercises MLflow and Spark ML, and writes only a disposable smoke table.
It never selects model configuration or reads the prospective quarter.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from .cloud4_contract import FEATURE_COLUMNS, GOLD_TABLE, PROSPECTIVE_QUARTER, REPORT_ROOT
except ImportError:  # Databricks Git-sourced tasks execute this file directly.
    from cloud4_contract import FEATURE_COLUMNS, GOLD_TABLE, PROSPECTIVE_QUARTER, REPORT_ROOT


SMOKE_MAX_ROWS = 20_000
SMOKE_HISTORY_END = "2023-12-31"
SMOKE_TABLE = "workspace.gold.cloud4_environment4_smoke_predictions"


def validate_smoke_contract(*, source_rows: int, train_rows: int, evaluation_rows: int,
                            reload_rows: int, prospective_rows: int,
                            label_classes: int) -> list[str]:
    reasons: list[str] = []
    if source_rows <= 0 or source_rows > SMOKE_MAX_ROWS:
        reasons.append("source_row_bound_failed")
    if train_rows <= 0 or evaluation_rows <= 0:
        reasons.append("empty_smoke_split")
    if reload_rows != evaluation_rows:
        reasons.append("write_reload_count_mismatch")
    if prospective_rows:
        reasons.append("prospective_guard_failed")
    if label_classes != 2:
        reasons.append("logistic_label_class_count_mismatch")
    return reasons


def run_smoke() -> dict[str, object]:
    import mlflow
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from pyspark.sql import SparkSession, functions as F

    started = time.monotonic()
    spark = SparkSession.builder.appName("wealthsignal-cloud4-environment4-smoke").getOrCreate()
    gold = spark.table(GOLD_TABLE)
    prospective_rows = gold.filter(
        F.col("target_report_period") >= F.lit(PROSPECTIVE_QUARTER).cast("date")
    ).limit(1).count()
    if prospective_rows:
        raise ValueError("Prospective rows are present in the frozen Gold source")

    # Deterministic and bounded before any ML action.  This period predates the
    # Protocol V2 validation window, so the smoke cannot select a configuration.
    source = (
        gold.filter(F.col("target_report_period") <= F.lit(SMOKE_HISTORY_END).cast("date"))
        .orderBy("example_id").limit(SMOKE_MAX_ROWS)
    )
    source_rows = source.count()
    train = source.filter(F.pmod(F.xxhash64("example_id"), F.lit(5)) != 0)
    evaluate = source.filter(F.pmod(F.xxhash64("example_id"), F.lit(5)) == 0)
    train_rows = train.count()
    evaluation_rows = evaluate.count()
    label_classes = train.select("target_is_new").distinct().count()
    initial_reasons = validate_smoke_contract(
        source_rows=source_rows, train_rows=train_rows, evaluation_rows=evaluation_rows,
        reload_rows=evaluation_rows, prospective_rows=prospective_rows, label_classes=label_classes,
    )
    if initial_reasons:
        raise ValueError(f"Cloud 4 smoke input gate failed: {initial_reasons}")

    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks")
    mlflow.set_experiment("/Shared/wealthsignal-protocol-v2-cloud4")
    run = mlflow.start_run(run_name="cloud4-environment4-compatibility-smoke")
    mlflow.log_params({
        "engineering_smoke_only": True, "environment_client": 4,
        "history_end": SMOKE_HISTORY_END, "maximum_rows": SMOKE_MAX_ROWS,
    })

    imputed = [f"{name}_smoke_imputed" for name in FEATURE_COLUMNS]
    preprocessing = Pipeline(stages=[
        Imputer(inputCols=list(FEATURE_COLUMNS), outputCols=imputed),
        VectorAssembler(inputCols=imputed, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
    ]).fit(train)
    prepared_train = preprocessing.transform(train)
    prepared_evaluate = preprocessing.transform(evaluate)
    ridge = LinearRegression(
        featuresCol="features", labelCol="target_weight", predictionCol="ridge_prediction",
        regParam=1.0, elasticNetParam=0.0, maxIter=3,
    ).fit(prepared_train)
    logistic = LogisticRegression(
        featuresCol="features", labelCol="target_is_new", predictionCol="new_position_prediction",
        probabilityCol="new_position_probability", rawPredictionCol="new_position_raw_prediction",
        regParam=1.0, elasticNetParam=0.0, maxIter=3,
    ).fit(prepared_train)
    predictions = logistic.transform(ridge.transform(prepared_evaluate)).select(
        "example_id", "target_report_period", "ridge_prediction", "new_position_prediction"
    )
    predictions.write.format("delta").mode("overwrite").saveAsTable(SMOKE_TABLE)
    reload_rows = spark.table(SMOKE_TABLE).count()
    reasons = validate_smoke_contract(
        source_rows=source_rows, train_rows=train_rows, evaluation_rows=evaluation_rows,
        reload_rows=reload_rows, prospective_rows=prospective_rows, label_classes=label_classes,
    )
    report = {
        "cloud_milestone": "Cloud 4 Free Edition compatibility smoke",
        "status": "passed" if not reasons else "failed",
        "engineering_smoke_only": True,
        "environment_client": 4,
        "source_table": GOLD_TABLE,
        "history_end": SMOKE_HISTORY_END,
        "maximum_rows": SMOKE_MAX_ROWS,
        "source_rows": source_rows,
        "train_rows": train_rows,
        "evaluation_rows": evaluation_rows,
        "logistic_label_classes": label_classes,
        "smoke_table": SMOKE_TABLE,
        "reload_rows": reload_rows,
        "mlflow_run_id": run.info.run_id,
        "prospective_q2_2026_truth_accessed": False,
        "blocking_reasons": reasons,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    mlflow.log_dict(report, "cloud4-environment4-smoke.json")
    mlflow.log_metrics({"source_rows": source_rows, "reload_rows": reload_rows})
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gold.cloud4_reports")
    Path(REPORT_ROOT, "cloud4-environment4-smoke.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))
    mlflow.end_run(status="FINISHED" if not reasons else "FAILED")
    if reasons:
        raise ValueError(f"Cloud 4 smoke failed: {reasons}")
    return report


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    run_smoke()


if __name__ == "__main__":
    main()
