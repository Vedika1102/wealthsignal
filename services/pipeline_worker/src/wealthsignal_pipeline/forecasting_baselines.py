from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import subprocess
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


BASELINE_CONTRACT_VERSION = 4
FEATURE_COLUMNS = (
    "current_weight", "previous_weight", "lag2_weight", "weight_momentum",
    "current_rank", "previous_rank", "rank_momentum", "holding_history_quarters",
    "quarters_since_last_held", "manager_turnover", "manager_concentration_hhi",
    "manager_top10_share", "peer_owner_count", "peer_aggregate_weight",
)


def ndcg_at_k(y_true: Sequence[float], y_score: Sequence[float], k: int) -> float:
    if k < 1:
        raise ValueError("k must be positive")
    truth = np.asarray(y_true, dtype=float)
    score = np.asarray(y_score, dtype=float)
    if truth.shape != score.shape:
        raise ValueError("y_true and y_score must have equal shape")
    if not truth.size:
        return 0.0
    order = np.lexsort((np.arange(score.size), -score))[:k]
    ideal = np.lexsort((np.arange(truth.size), -truth))[:k]
    discounts = 1.0 / np.log2(np.arange(2, len(order) + 2))
    ideal_discounts = 1.0 / np.log2(np.arange(2, len(ideal) + 2))
    dcg = float(np.sum(truth[order] * discounts))
    idcg = float(np.sum(truth[ideal] * ideal_discounts))
    return dcg / idcg if idcg else 0.0


def recall_at_k(y_true: Sequence[float], y_score: Sequence[float], k: int) -> float:
    truth = np.asarray(y_true, dtype=float)
    score = np.asarray(y_score, dtype=float)
    positive = np.flatnonzero(truth > 0)
    if not positive.size:
        return 0.0
    relevant_count = min(k, positive.size)
    relevant_order = positive[np.lexsort((positive, -truth[positive]))[:relevant_count]]
    predicted_order = np.lexsort((np.arange(score.size), -score))[:k]
    return len(set(relevant_order.tolist()) & set(predicted_order.tolist())) / relevant_count


def rank_correlation(y_true: Sequence[float], y_score: Sequence[float]) -> float:
    truth = np.asarray(y_true, dtype=float)
    score = np.asarray(y_score, dtype=float)
    if truth.size < 2 or np.all(truth == truth[0]) or np.all(score == score[0]):
        return 0.0
    return float(np.corrcoef(_average_ranks(truth), _average_ranks(score))[0, 1])


def _average_ranks(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(len(values), dtype=float)
    start = 0
    while start < len(values):
        end = start + 1
        while end < len(values) and values[order[end]] == values[order[start]]:
            end += 1
        ranks[order[start:end]] = (start + end - 1) / 2.0 + 1.0
        start = end
    return ranks


def run_baselines(
    temporal_dataset: str | Path,
    *,
    output_root: str | Path,
    ema_alpha: float = 0.6,
    ridge_alpha: float = 1.0,
    include_gradient_boosting: bool = True,
    include_final_test: bool = False,
    comparison_protocol: str | Path | None = None,
) -> Path:
    if not 0 < ema_alpha <= 1:
        raise ValueError("ema_alpha must be in (0, 1]")
    dataset = Path(temporal_dataset)
    manifest = _load_verified_dataset(dataset)
    split_manifest = json.loads((dataset / "split_manifest.json").read_text(encoding="utf-8"))
    rows = _read_rows(dataset / "manager_security_quarter.csv")
    protocol_sha = None
    if include_final_test:
        if comparison_protocol is None:
            raise ValueError("Final-test evaluation requires a fixed comparison_protocol file")
        protocol = Path(comparison_protocol)
        if not protocol.is_file() or not protocol.read_text(encoding="utf-8").strip():
            raise ValueError("comparison_protocol must be a non-empty file")
        protocol_sha = _sha256_file(protocol)

    config = {
        "contract_version": BASELINE_CONTRACT_VERSION,
        "implementation_sha256": _sha256_file(Path(__file__)),
        "temporal_dataset_id": manifest["dataset_id"],
        "temporal_manifest_sha256": _sha256_file(dataset / "manifest.json"),
        "ema_alpha": ema_alpha,
        "ridge_alpha": ridge_alpha,
        "include_gradient_boosting": include_gradient_boosting,
        "include_final_test": include_final_test,
        "comparison_protocol_sha256": protocol_sha,
        "random_seed": 42,
    }
    run_id = hashlib.sha256(_canonical_json(config).encode()).hexdigest()[:16]
    output = Path(output_root) / f"baselines-{run_id}"
    if (output / "manifest.json").exists():
        return output
    if output.exists():
        raise ValueError(f"Refusing to overwrite incomplete output: {output}")
    staging = Path(output_root) / f".baselines-{run_id}.building"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    by_id = {row["example_id"]: row for row in rows}
    all_predictions: list[dict[str, object]] = []
    fold_reports: list[dict[str, object]] = []
    for fold in split_manifest.get("folds", []):
        if fold["role"] == "test" and not include_final_test:
            continue
        train = [by_id[value] for value in fold["train_example_ids"]]
        evaluate = [by_id[value] for value in fold["evaluation_example_ids"]]
        model_predictions, action_predictions, timings, artifacts = _fit_predict_models(
            train, evaluate, ema_alpha, ridge_alpha, include_gradient_boosting
        )
        for model_name, predicted in model_predictions.items():
            prediction_rows = _prediction_rows(fold, evaluate, model_name, predicted, action_predictions)
            all_predictions.extend(prediction_rows)
            fold_reports.append({
                "fold_id": fold["fold_id"], "role": fold["role"],
                "evaluation_target_quarter": fold["evaluation_target_quarter"],
                "model": model_name, "train_examples": len(train),
                "evaluation_examples": len(evaluate),
                "metrics": evaluate_predictions(prediction_rows),
                **timings[model_name],
            })
        for name, payload in artifacts.items():
            (staging / f"{fold['fold_id']}-{name}.json").write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )

    _write_csv(staging / "predictions.csv", all_predictions)
    comparison = _comparison_report(fold_reports, split_manifest, include_final_test)
    (staging / "comparison_report.json").write_text(json.dumps(comparison, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (staging / "config.json").write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    outputs = {p.name: {"sha256": _sha256_file(p), "size_bytes": p.stat().st_size} for p in staging.iterdir() if p.is_file()}
    result_manifest = {
        "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat(),
        "config": config, "git_revision": _git_revision(), "outputs": outputs,
        "final_test_status": "evaluated_with_fixed_protocol" if include_final_test else "locked_not_evaluated",
    }
    (staging / "manifest.json").write_text(json.dumps(result_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staging, output)
    return output


def _fit_predict_models(train, evaluate, ema_alpha, ridge_alpha, include_gradient_boosting):
    predictions = {
        "persistence": np.asarray([float(r["current_weight"]) for r in evaluate]),
        "ema": np.asarray([_ema(r, ema_alpha) for r in evaluate]),
        "institutional_popularity": np.asarray([float(r["peer_aggregate_weight"]) for r in evaluate]),
    }
    timings = {name: {"training_runtime_seconds": 0.0, "inference_runtime_seconds": 0.0} for name in predictions}
    artifacts: dict[str, object] = {}
    x_train = np.asarray([[_float(r[c]) for c in FEATURE_COLUMNS] for r in train])
    y_train = np.asarray([float(r["target_weight"]) for r in train])
    x_eval = np.asarray([[_float(r[c]) for c in FEATURE_COLUMNS] for r in evaluate])
    action_predictions = {}
    for target, label in (("target_is_new", "new"), ("target_is_exit", "exit")):
        classifier = Pipeline([("scale", StandardScaler()), ("model", LogisticRegression(C=1.0, max_iter=1000, random_state=42))])
        y_action = np.asarray([int(r[target]) for r in train])
        if len(np.unique(y_action)) < 2:
            action_predictions[label] = np.full(len(evaluate), bool(y_action[0]) if len(y_action) else False)
            artifacts[f"logistic_{label}"] = {"features": list(FEATURE_COLUMNS), "constant_class": int(y_action[0]) if len(y_action) else 0}
        else:
            start = time.perf_counter(); classifier.fit(x_train, y_action); train_time = time.perf_counter() - start
            start = time.perf_counter(); probabilities = classifier.predict_proba(x_eval)[:, 1]; infer_time = time.perf_counter() - start
            action_predictions[label] = probabilities >= 0.5
            artifacts[f"logistic_{label}"] = {"features": list(FEATURE_COLUMNS), "coefficients": classifier.named_steps["model"].coef_[0].tolist(), "intercept": float(classifier.named_steps["model"].intercept_[0]), "threshold": 0.5, "training_runtime_seconds": train_time, "inference_runtime_seconds": infer_time}
    ridge = Pipeline([("scale", StandardScaler()), ("model", Ridge(alpha=ridge_alpha))])
    start = time.perf_counter(); ridge.fit(x_train, y_train); train_time = time.perf_counter() - start
    start = time.perf_counter(); predictions["ridge"] = np.maximum(0.0, ridge.predict(x_eval)); infer_time = time.perf_counter() - start
    timings["ridge"] = {"training_runtime_seconds": train_time, "inference_runtime_seconds": infer_time}
    artifacts["ridge"] = {"features": list(FEATURE_COLUMNS), "coefficients": ridge.named_steps["model"].coef_.tolist(), "intercept": float(ridge.named_steps["model"].intercept_)}
    if include_gradient_boosting:
        tree = HistGradientBoostingRegressor(max_iter=100, max_depth=4, learning_rate=0.05, l2_regularization=1.0, random_state=42)
        start = time.perf_counter(); tree.fit(x_train, y_train); train_time = time.perf_counter() - start
        start = time.perf_counter(); predictions["gradient_boosting"] = np.maximum(0.0, tree.predict(x_eval)); infer_time = time.perf_counter() - start
        timings["gradient_boosting"] = {"training_runtime_seconds": train_time, "inference_runtime_seconds": infer_time}
        artifacts["gradient_boosting"] = {"features": list(FEATURE_COLUMNS), "parameters": tree.get_params()}
    return predictions, action_predictions, timings, artifacts


def _prediction_rows(fold, rows, model_name, predicted, action_predictions):
    result = []
    for index, (row, score) in enumerate(zip(rows, predicted)):
        result.append({
            "fold_id": fold["fold_id"], "role": fold["role"], "model": model_name,
            "example_id": row["example_id"], "cik": row["cik"],
            "feature_report_period": row["feature_report_period"], "target_report_period": row["target_report_period"],
            "current_weight": float(row["current_weight"]), "target_weight": float(row["target_weight"]),
            "target_is_new": int(row["target_is_new"]), "target_is_exit": int(row["target_is_exit"]),
            "predicted_weight": float(score), "predicted_is_new": int(action_predictions["new"][index]),
            "predicted_is_exit": int(action_predictions["exit"][index]),
        })
    return result


def evaluate_predictions(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    groups: dict[tuple[str, str], list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        groups[(str(row["target_report_period"]), str(row["cik"]))].append(row)
    group_metrics = []
    for (quarter, cik), values in sorted(groups.items()):
        truth = [float(v["target_weight"]) for v in values]
        score = [float(v["predicted_weight"]) for v in values]
        current = [float(v["current_weight"]) for v in values]
        predicted_new = [bool(int(v["predicted_is_new"])) if "predicted_is_new" in v else s > 1e-8 and c <= 1e-8 for v, s, c in zip(values, score, current)]
        predicted_exit = [bool(int(v["predicted_is_exit"])) if "predicted_is_exit" in v else s <= 1e-8 and c > 1e-8 for v, s, c in zip(values, score, current)]
        held_count = sum(float(v["current_weight"]) > 0 for v in values)
        cohort = "small" if held_count < 50 else "medium" if held_count < 200 else "large"
        group_metrics.append({
            "target_quarter": quarter, "cik": cik, "examples": len(values),
            "manager_cohort": cohort, "current_portfolio_candidates_held": held_count,
            "ndcg_at_10": ndcg_at_k(truth, score, 10), "ndcg_at_20": ndcg_at_k(truth, score, 20),
            "recall_at_10": recall_at_k(truth, score, 10), "rank_correlation": rank_correlation(truth, score),
            "weight_mae": float(np.mean(np.abs(np.asarray(truth) - np.asarray(score)))),
            "weight_rmse": float(np.sqrt(np.mean((np.asarray(truth) - np.asarray(score)) ** 2))),
            **_binary_metrics([int(v["target_is_new"]) for v in values], predicted_new, "new"),
            **_binary_metrics([int(v["target_is_exit"]) for v in values], predicted_exit, "exit"),
        })
    metric_names = ["ndcg_at_10", "ndcg_at_20", "recall_at_10", "rank_correlation", "weight_mae", "weight_rmse", "new_precision", "new_recall", "exit_precision", "exit_recall"]
    aggregate = {name: float(np.mean([g[name] for g in group_metrics])) for name in metric_names} if group_metrics else {name: 0.0 for name in metric_names}
    aggregate["manager_quarter_groups"] = len(group_metrics)
    aggregate["by_manager_quarter"] = group_metrics
    aggregate["by_target_quarter"] = _slice_metrics(group_metrics, "target_quarter", metric_names)
    aggregate["by_manager_cohort"] = _slice_metrics(group_metrics, "manager_cohort", metric_names)
    return aggregate


def _slice_metrics(groups, dimension, metric_names):
    buckets = defaultdict(list)
    for group in groups:
        buckets[group[dimension]].append(group)
    return {
        label: {name: float(np.mean([row[name] for row in values])) for name in metric_names}
        | {"manager_quarter_groups": len(values)}
        for label, values in sorted(buckets.items())
    }


def _binary_metrics(truth, predicted, prefix):
    tp = sum(bool(t) and bool(p) for t, p in zip(truth, predicted)); fp = sum(not bool(t) and bool(p) for t, p in zip(truth, predicted)); fn = sum(bool(t) and not bool(p) for t, p in zip(truth, predicted))
    return {f"{prefix}_precision": tp / (tp + fp) if tp + fp else 0.0, f"{prefix}_recall": tp / (tp + fn) if tp + fn else 0.0}


def _comparison_report(folds, split_manifest, include_final_test):
    validation = [f for f in folds if f["role"] == "validation"]
    by_model = defaultdict(list)
    for fold in validation:
        by_model[fold["model"]].append(fold)
    summary = {}
    for model, values in sorted(by_model.items()):
        summary[model] = {}
        for metric in ("ndcg_at_10", "ndcg_at_20", "recall_at_10", "rank_correlation", "weight_mae", "weight_rmse", "new_precision", "new_recall", "exit_precision", "exit_recall"):
            scores = [v["metrics"][metric] for v in values]
            summary[model][metric] = {"mean": float(np.mean(scores)), "std_across_folds": float(np.std(scores)), "folds": len(scores)}
    return {
        "evaluation_policy": "validation folds only unless a fixed comparison protocol explicitly unlocks final test",
        "final_test_evaluated": include_final_test,
        "available_test_folds": sum(f["role"] == "test" for f in split_manifest.get("folds", [])),
        "evaluated_folds": folds, "validation_summary": summary,
        "success_claim": "No model is promoted from this report; promotion requires a predeclared protocol and broader temporal/manager evidence.",
    }


def _ema(row, alpha):
    weights = [float(row["current_weight"]), float(row["previous_weight"]), float(row["lag2_weight"])]
    numerator = sum(alpha * ((1 - alpha) ** i) * value for i, value in enumerate(weights))
    denominator = sum(alpha * ((1 - alpha) ** i) for i in range(len(weights)))
    return numerator / denominator


def _float(value):
    value = float(value)
    return value if math.isfinite(value) else 0.0


def _read_rows(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path, rows):
    fields = list(rows[0]) if rows else ["fold_id", "role", "model", "example_id"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(rows)


def _load_verified_dataset(dataset):
    manifest = json.loads((dataset / "manifest.json").read_text(encoding="utf-8"))
    for name in ("manager_security_quarter.csv", "split_manifest.json"):
        expected = manifest.get("outputs", {}).get(name, {}).get("sha256")
        if not expected or _sha256_file(dataset / name) != expected:
            raise ValueError(f"Temporal dataset checksum mismatch: {name}")
    return manifest


def _sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _git_revision():
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate leakage-safe WealthSignal forecasting baselines")
    parser.add_argument("--temporal-dataset", required=True); parser.add_argument("--output-root", required=True)
    parser.add_argument("--ema-alpha", type=float, default=0.6); parser.add_argument("--ridge-alpha", type=float, default=1.0)
    parser.add_argument("--without-gradient-boosting", action="store_true"); parser.add_argument("--include-final-test", action="store_true")
    parser.add_argument("--comparison-protocol")
    args = parser.parse_args(list(argv) if argv is not None else None)
    output = run_baselines(args.temporal_dataset, output_root=args.output_root, ema_alpha=args.ema_alpha, ridge_alpha=args.ridge_alpha, include_gradient_boosting=not args.without_gradient_boosting, include_final_test=args.include_final_test, comparison_protocol=args.comparison_protocol)
    print(output); return 0


if __name__ == "__main__":
    raise SystemExit(main())
