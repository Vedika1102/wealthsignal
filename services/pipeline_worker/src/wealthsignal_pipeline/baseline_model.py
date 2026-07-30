from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .models import ModelPrediction, PersistedFeatureRow, PositionFeatures


FEATURE_COLUMNS = [
    "current_weight",
    "previous_weight",
    "weight_delta",
    "abs_weight_delta",
    "current_value_thousands",
    "previous_value_thousands",
    "value_delta_thousands",
    "abs_value_delta_thousands",
    "value_pct_change",
    "shares_pct_change",
    "is_new_position",
    "is_exited_position",
    "current_rank",
    "previous_rank",
    "entered_top10",
    "exited_top10",
    "entered_top20",
    "exited_top20",
    "turnover_ratio",
    "change_share_of_turnover",
]


@dataclass(slots=True)
class BaselineModelFit:
    """Result of fitting the numpy logistic baseline."""

    feature_names: list[str]
    coefficients: list[float]
    intercept: float
    means: list[float]
    scales: list[float]
    metrics: dict[str, float]
    predictions: list[ModelPrediction]


def assign_weak_label(feature: PositionFeatures, *, rule_score: int) -> int:
    """Generate a weak supervised target from deterministic heuristics.

    This target is intentionally stricter than `should_alert`. It identifies
    changes that look strongly strategic, so the baseline model learns to
    separate high-conviction events from lower-signal alerts.
    """

    if feature.entered_top10 or feature.exited_top10:
        return 1
    if feature.abs_weight_delta >= 0.015 and feature.abs_value_delta_thousands >= 100_000:
        return 1
    if (feature.is_new_position or feature.is_exited_position) and feature.abs_weight_delta >= 0.0075:
        return 1
    if rule_score >= 70:
        return 1
    return 0


def train_logistic_baseline(
    feature_rows: list[PersistedFeatureRow],
    *,
    epochs: int = 600,
    learning_rate: float = 0.15,
    regularization: float = 0.01,
) -> BaselineModelFit:
    """Train a simple logistic regression baseline using numpy only."""

    if not feature_rows:
        raise ValueError("No feature rows available for training")

    labels = np.array([row.weak_label for row in feature_rows], dtype=float)
    unique_labels = set(int(value) for value in labels.tolist())
    if len(unique_labels) < 2:
        raise ValueError("Need both positive and negative weak labels to train the baseline model")

    matrix = np.array([_feature_vector(row) for row in feature_rows], dtype=float)
    means = matrix.mean(axis=0)
    scales = matrix.std(axis=0)
    scales[scales == 0] = 1.0
    standardized = (matrix - means) / scales

    sample_count, feature_count = standardized.shape
    weights = np.zeros(feature_count, dtype=float)
    intercept = 0.0

    for _ in range(epochs):
        logits = standardized @ weights + intercept
        probabilities = _sigmoid(logits)
        errors = probabilities - labels
        grad_w = (standardized.T @ errors) / sample_count + (regularization * weights)
        grad_b = float(errors.mean())
        weights -= learning_rate * grad_w
        intercept -= learning_rate * grad_b

    probabilities = _sigmoid(standardized @ weights + intercept)
    predictions = (probabilities >= 0.5).astype(int)

    metrics = _classification_metrics(labels, predictions, probabilities)
    output_predictions = [
        ModelPrediction(
            run_id=0,
            current_accession_number=row.current_accession_number,
            holding_key=row.holding_key,
            issuer_name=row.issuer_name,
            cusip=row.cusip,
            probability=float(probability),
            predicted_label=int(prediction),
            weak_label=row.weak_label,
            rule_score=row.rule_score,
        )
        for row, probability, prediction in zip(feature_rows, probabilities.tolist(), predictions.tolist())
    ]

    return BaselineModelFit(
        feature_names=FEATURE_COLUMNS.copy(),
        coefficients=weights.tolist(),
        intercept=float(intercept),
        means=means.tolist(),
        scales=scales.tolist(),
        metrics=metrics,
        predictions=output_predictions,
    )


def _feature_vector(row: PersistedFeatureRow) -> list[float]:
    return [
        row.current_weight,
        row.previous_weight,
        row.weight_delta,
        row.abs_weight_delta,
        float(row.current_value_thousands),
        float(row.previous_value_thousands),
        float(row.value_delta_thousands),
        float(row.abs_value_delta_thousands),
        row.value_pct_change if row.value_pct_change is not None else 0.0,
        row.shares_pct_change if row.shares_pct_change is not None else 0.0,
        float(row.is_new_position),
        float(row.is_exited_position),
        float(row.current_rank if row.current_rank is not None else 999.0),
        float(row.previous_rank if row.previous_rank is not None else 999.0),
        float(row.entered_top10),
        float(row.exited_top10),
        float(row.entered_top20),
        float(row.exited_top20),
        row.turnover_ratio,
        row.change_share_of_turnover,
    ]


def _sigmoid(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -30, 30)
    return 1.0 / (1.0 + np.exp(-clipped))


def _classification_metrics(
    labels: np.ndarray,
    predictions: np.ndarray,
    probabilities: np.ndarray,
) -> dict[str, float]:
    tp = float(((predictions == 1) & (labels == 1)).sum())
    tn = float(((predictions == 0) & (labels == 0)).sum())
    fp = float(((predictions == 1) & (labels == 0)).sum())
    fn = float(((predictions == 0) & (labels == 1)).sum())
    total = max(float(labels.shape[0]), 1.0)

    precision = tp / max(tp + fp, 1.0)
    recall = tp / max(tp + fn, 1.0)
    accuracy = (tp + tn) / total
    if precision + recall == 0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)

    eps = 1e-9
    log_loss = float(
        -np.mean((labels * np.log(probabilities + eps)) + ((1 - labels) * np.log((1 - probabilities) + eps)))
    )

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "log_loss": log_loss,
    }
