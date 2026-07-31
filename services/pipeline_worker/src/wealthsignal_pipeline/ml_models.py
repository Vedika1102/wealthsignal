from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .baseline_model import FEATURE_COLUMNS
from .models import PersistedFeatureRow

if not os.getenv("MPLCONFIGDIR"):
    matplotlib_cache_dir = Path(tempfile.gettempdir()) / "wealthsignal-matplotlib"
    matplotlib_cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = str(matplotlib_cache_dir)

try:
    from joblib import dump as joblib_dump
    from sklearn.calibration import calibration_curve
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, average_precision_score, f1_score, precision_score, recall_score
    from sklearn.model_selection import GridSearchCV, PredefinedSplit
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.tree import DecisionTreeClassifier
except ImportError:  # pragma: no cover - exercised only when ML deps are installed
    joblib_dump = None
    calibration_curve = None
    RandomForestClassifier = None
    LogisticRegression = None
    accuracy_score = None
    average_precision_score = None
    f1_score = None
    precision_score = None
    recall_score = None
    GridSearchCV = None
    PredefinedSplit = None
    Pipeline = None
    StandardScaler = None
    DecisionTreeClassifier = None

try:
    from xgboost import XGBClassifier
except ImportError:  # pragma: no cover - exercised only when xgboost is installed
    XGBClassifier = None

try:
    import shap
except ImportError:  # pragma: no cover - exercised only when shap is installed
    shap = None

try:
    import mlflow
except ImportError:  # pragma: no cover - exercised only when mlflow is installed
    mlflow = None


@dataclass(slots=True)
class CalibrationPoint:
    predicted_probability: float
    observed_frequency: float


@dataclass(slots=True)
class ModelComparisonResult:
    model_name: str
    metrics: dict[str, float]
    best_params: dict[str, Any]
    feature_names: list[str]
    calibration_curve: list[CalibrationPoint]
    predicted_probabilities: list[float]
    predicted_labels: list[int]
    shap_feature_importance: list[dict[str, float]] | None = None
    estimator: Any | None = None


@dataclass(slots=True)
class ModelTrainingBundle:
    results: list[ModelComparisonResult]
    best_model_name: str
    best_result: ModelComparisonResult
    accession_sequence: list[str]


def train_candidate_models(
    feature_rows: list[PersistedFeatureRow],
    *,
    mlflow_experiment: str | None = None,
) -> ModelTrainingBundle:
    """Train multiple classical ML models on persisted feature rows."""

    _require_ml_dependencies()
    if not feature_rows:
        raise ValueError("No feature rows available for training")

    matrix, labels = build_feature_matrix(feature_rows)
    if len(set(labels.tolist())) < 2:
        raise ValueError("Need both positive and negative weak labels to train candidate models")

    fold_assignments, accession_sequence = build_time_based_fold_assignments(feature_rows, fold_count=3)
    if len(set(fold_assignments)) < 2:
        raise ValueError("Need at least two time-based folds to compare candidate models")

    predefined_split = PredefinedSplit(test_fold=fold_assignments)
    models = _candidate_model_specs()
    results = []

    if mlflow_experiment and mlflow is not None:
        mlflow.set_experiment(mlflow_experiment)

    for model_name, estimator, param_grid in models:
        result = _train_one_model(
            model_name=model_name,
            estimator=estimator,
            param_grid=param_grid,
            matrix=matrix,
            labels=labels,
            predefined_split=predefined_split,
            feature_names=FEATURE_COLUMNS.copy(),
            accession_sequence=accession_sequence,
        )
        results.append(result)

    results.sort(key=lambda item: item.metrics["pr_auc"], reverse=True)
    best = results[0]
    return ModelTrainingBundle(
        results=results,
        best_model_name=best.model_name,
        best_result=best,
        accession_sequence=accession_sequence,
    )


def save_best_model(bundle: ModelTrainingBundle, output_path: str | Path) -> None:
    """Persist the best estimator with joblib."""

    _require_ml_dependencies()
    if bundle.best_result.estimator is None:
        raise ValueError("Best result does not contain a fitted estimator")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib_dump(bundle.best_result.estimator, path)


def build_feature_matrix(feature_rows: list[PersistedFeatureRow]) -> tuple[np.ndarray, np.ndarray]:
    """Convert persisted feature rows into an sklearn-ready matrix and target vector."""

    matrix = np.array([_feature_vector(row) for row in feature_rows], dtype=float)
    labels = np.array([row.weak_label for row in feature_rows], dtype=int)
    return matrix, labels


def build_time_based_fold_assignments(
    feature_rows: list[PersistedFeatureRow],
    *,
    fold_count: int = 3,
) -> tuple[list[int], list[str]]:
    """Assign each row to a time-based fold using accession chronology."""

    if fold_count < 2:
        raise ValueError("fold_count must be at least 2")

    accession_sequence = list(dict.fromkeys(row.current_accession_number for row in feature_rows))
    sorted_accessions = sorted(accession_sequence)
    accession_to_rank = {accession: index for index, accession in enumerate(sorted_accessions)}
    accession_count = len(sorted_accessions)
    if accession_count == 0:
        return [], []

    fold_boundaries = np.array_split(np.arange(accession_count), min(fold_count, accession_count))
    accession_to_fold: dict[str, int] = {}
    for fold_index, boundary in enumerate(fold_boundaries):
        for accession_rank in boundary.tolist():
            accession = sorted_accessions[accession_rank]
            accession_to_fold[accession] = fold_index

    fold_assignments = [accession_to_fold[row.current_accession_number] for row in feature_rows]
    return fold_assignments, sorted_accessions


def _train_one_model(
    *,
    model_name: str,
    estimator: Any,
    param_grid: dict[str, list[Any]],
    matrix: np.ndarray,
    labels: np.ndarray,
    predefined_split: Any,
    feature_names: list[str],
    accession_sequence: list[str],
) -> ModelComparisonResult:
    pipeline = _build_pipeline(model_name, estimator)
    search = GridSearchCV(
        estimator=pipeline,
        param_grid=param_grid,
        scoring="average_precision",
        cv=predefined_split,
        refit=True,
        n_jobs=1,
    )
    search.fit(matrix, labels)

    best_estimator = search.best_estimator_
    probabilities = _probabilities(best_estimator, matrix)
    predicted_labels = (probabilities >= 0.5).astype(int)
    metrics = _classification_metrics(labels, predicted_labels, probabilities)
    calibration = _calibration_curve(labels, probabilities)
    shap_summary = _shap_summary(model_name, best_estimator, matrix, feature_names)

    if mlflow is not None:
        with mlflow.start_run(run_name=f"wealthsignal-{model_name}", nested=True):
            mlflow.log_params(search.best_params_)
            mlflow.log_metrics(metrics)
            mlflow.set_tag("model_name", model_name)
            mlflow.set_tag("accession_count", len(accession_sequence))

    return ModelComparisonResult(
        model_name=model_name,
        metrics=metrics,
        best_params=search.best_params_,
        feature_names=feature_names,
        calibration_curve=calibration,
        predicted_probabilities=probabilities.tolist(),
        predicted_labels=predicted_labels.tolist(),
        shap_feature_importance=shap_summary,
        estimator=best_estimator,
    )


def _candidate_model_specs() -> list[tuple[str, Any, dict[str, list[Any]]]]:
    models = [
        (
            "logistic_regression",
            LogisticRegression(max_iter=500, class_weight="balanced"),
            {
                "model__C": [0.1, 1.0, 5.0],
                "model__solver": ["lbfgs"],
            },
        ),
        (
            "decision_tree",
            DecisionTreeClassifier(random_state=7, class_weight="balanced"),
            {
                "model__max_depth": [3, 5, None],
                "model__min_samples_leaf": [1, 3, 5],
            },
        ),
        (
            "random_forest",
            RandomForestClassifier(random_state=7, class_weight="balanced"),
            {
                "model__n_estimators": [100, 200],
                "model__max_depth": [4, 6, None],
            },
        ),
    ]
    if XGBClassifier is not None:
        models.append(
            (
                "xgboost",
                XGBClassifier(
                    random_state=7,
                    eval_metric="logloss",
                    n_estimators=200,
                    max_depth=4,
                    learning_rate=0.05,
                    subsample=0.9,
                    colsample_bytree=0.9,
                ),
                {
                    "model__n_estimators": [100, 200],
                    "model__max_depth": [3, 4],
                    "model__learning_rate": [0.03, 0.05],
                },
            )
        )
    return models


def _build_pipeline(model_name: str, estimator: Any) -> Any:
    steps = []
    if model_name == "logistic_regression":
        steps.append(("scaler", StandardScaler()))
    steps.append(("model", estimator))
    return Pipeline(steps)


def _probabilities(estimator: Any, matrix: np.ndarray) -> np.ndarray:
    if hasattr(estimator, "predict_proba"):
        return estimator.predict_proba(matrix)[:, 1]
    if hasattr(estimator, "decision_function"):
        scores = estimator.decision_function(matrix)
        return 1.0 / (1.0 + np.exp(-scores))
    raise ValueError("Estimator does not expose probability or decision scores")


def _classification_metrics(labels: np.ndarray, predictions: np.ndarray, probabilities: np.ndarray) -> dict[str, float]:
    return {
        "accuracy": float(accuracy_score(labels, predictions)),
        "precision": float(precision_score(labels, predictions, zero_division=0)),
        "recall": float(recall_score(labels, predictions, zero_division=0)),
        "f1": float(f1_score(labels, predictions, zero_division=0)),
        "pr_auc": float(average_precision_score(labels, probabilities)),
    }


def _calibration_curve(labels: np.ndarray, probabilities: np.ndarray) -> list[CalibrationPoint]:
    curve_true, curve_pred = calibration_curve(labels, probabilities, n_bins=5, strategy="quantile")
    return [
        CalibrationPoint(
            predicted_probability=float(predicted_probability),
            observed_frequency=float(observed_frequency),
        )
        for observed_frequency, predicted_probability in zip(curve_true.tolist(), curve_pred.tolist())
    ]


def _shap_summary(
    model_name: str,
    estimator: Any,
    matrix: np.ndarray,
    feature_names: list[str],
) -> list[dict[str, float]] | None:
    if model_name != "xgboost" or shap is None:
        return None

    model = estimator.named_steps["model"] if hasattr(estimator, "named_steps") else estimator
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(matrix[:25])
    mean_abs = np.abs(shap_values).mean(axis=0)
    ranked = sorted(zip(feature_names, mean_abs.tolist()), key=lambda item: item[1], reverse=True)[:5]
    return [{"feature": feature, "importance": float(importance)} for feature, importance in ranked]


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


def _require_ml_dependencies() -> None:
    missing = []
    if LogisticRegression is None or GridSearchCV is None or PredefinedSplit is None:
        missing.append("scikit-learn")
    if joblib_dump is None:
        missing.append("joblib")
    if missing:
        raise RuntimeError(f"Missing required ML dependencies: {', '.join(sorted(set(missing)))}")
