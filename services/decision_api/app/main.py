from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from wealthsignal_pipeline.materiality import materiality_policy
from wealthsignal_pipeline.persistence import (
    connect,
    get_alert,
    get_latest_prediction_lookup,
    initialize_database,
    list_alert_impacts,
    list_alerts,
    list_filing_summaries,
    list_latest_model_runs,
    list_position_deltas,
    list_recommendations,
)

try:
    from joblib import load as joblib_load
except ImportError:  # pragma: no cover - exercised only when optional ML deps are unavailable
    joblib_load = None


def _default_db_path() -> str:
    repo_root = Path(__file__).resolve().parents[3]
    return str(repo_root / "data" / "wealthsignal.db")


DB_PATH = os.getenv("WEALTHSIGNAL_DB_PATH", _default_db_path())
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="WealthSignal Decision API", version="0.1.0")


def _connection():
    connection = connect(DB_PATH)
    initialize_database(connection)
    return connection


def _resolve_artifact_path(artifact_path: str) -> Path:
    path = Path(artifact_path)
    if path.is_absolute():
        return path
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / path


@lru_cache(maxsize=8)
def _load_model_artifact(artifact_path: str) -> object:
    if joblib_load is None:
        raise RuntimeError("joblib is not available")
    return joblib_load(artifact_path)


def _artifact_loaded(artifact_path: str | None) -> bool:
    if artifact_path is None:
        return False

    resolved_path = _resolve_artifact_path(artifact_path)
    if not resolved_path.exists():
        return False

    try:
        _load_model_artifact(str(resolved_path))
    except Exception:
        return False
    return True


def _serialize_model_run(model_run) -> dict[str, object]:
    return {
        "run_id": model_run.run_id,
        "model_name": model_run.model_name,
        "training_samples": model_run.training_samples,
        "positive_count": model_run.positive_count,
        "feature_names": model_run.feature_names,
        "coefficients": model_run.coefficients,
        "intercept": model_run.intercept,
        "metrics": model_run.metrics,
        "comparison_group_id": model_run.comparison_group_id,
        "best_params": model_run.best_params,
        "calibration_curve": model_run.calibration_curve,
        "shap_feature_importance": model_run.shap_feature_importance,
        "artifact_path": model_run.artifact_path,
        "artifact_loaded": _artifact_loaded(model_run.artifact_path),
        "is_best_model": model_run.is_best_model,
    }


def _serialize_recommendation(recommendation) -> dict[str, object]:
    return {
        "recommendation_id": recommendation.recommendation_id,
        "alert_id": recommendation.alert_id,
        "client_id": recommendation.client_id,
        "client_name": recommendation.client_name,
        "strategy": recommendation.strategy,
        "current_accession_number": recommendation.current_accession_number,
        "issuer_name": recommendation.issuer_name,
        "cusip": recommendation.cusip,
        "sector": recommendation.sector,
        "alert_score": recommendation.alert_score,
        "alert_severity": recommendation.alert_severity,
        "relevance_score": recommendation.relevance_score,
        "content_similarity": recommendation.content_similarity,
        "direct_weight": recommendation.direct_weight,
        "sector_weight": recommendation.sector_weight,
        "precedent_count": len(recommendation.precedents),
        "precedents": recommendation.precedents,
        "rationale": recommendation.rationale,
    }


@app.get("/health")
def healthcheck() -> dict[str, str]:
    backend = "sqlite"
    db_identifier = DB_PATH
    db_exists = str(Path(DB_PATH).exists()).lower()
    if DATABASE_URL:
        if DATABASE_URL.lower().startswith(("postgres://", "postgresql://")):
            backend = "postgres"
            db_identifier = "DATABASE_URL"
            db_exists = "n/a"
        elif DATABASE_URL.lower().startswith("sqlite:///"):
            backend = "sqlite"
            sqlite_path = DATABASE_URL[len("sqlite:///") :]
            db_identifier = sqlite_path
            db_exists = str(Path(sqlite_path).exists()).lower()
    return {"status": "ok", "backend": backend, "db_path": db_identifier, "db_exists": db_exists}


@app.get("/filings")
def filings(limit: int = Query(default=20, ge=1, le=100)) -> list[dict]:
    connection = _connection()
    try:
        return list_filing_summaries(connection, limit=limit)
    finally:
        connection.close()


@app.get("/filings/{accession_number}/changes")
def filing_changes(accession_number: str, limit: int = Query(default=50, ge=1, le=500)) -> list[dict]:
    connection = _connection()
    try:
        return list_position_deltas(connection, accession_number, limit=limit)
    finally:
        connection.close()


@app.get("/alerts")
def alerts(
    limit: int = Query(default=20, ge=1, le=100),
    minimum_score: int = Query(default=40, ge=0, le=100),
    severity: str | None = Query(default=None),
) -> list[dict]:
    connection = _connection()
    try:
        prediction_lookup = get_latest_prediction_lookup(connection)
        return [
            {
                "alert_id": alert.alert_id,
                "current_accession_number": alert.current_accession_number,
                "issuer_name": alert.issuer_name,
                "cusip": alert.cusip,
                "sector": alert.sector,
                "score": alert.score,
                "severity": alert.severity,
                "should_alert": alert.should_alert,
                "reasons": alert.reasons,
                "weight_delta": alert.weight_delta,
                "model_probability": (
                    prediction_lookup[(alert.current_accession_number, alert.holding_key)].probability
                    if (alert.current_accession_number, alert.holding_key) in prediction_lookup
                    else None
                ),
                "weak_label": (
                    prediction_lookup[(alert.current_accession_number, alert.holding_key)].weak_label
                    if (alert.current_accession_number, alert.holding_key) in prediction_lookup
                    else None
                ),
            }
            for alert in list_alerts(connection, limit=limit, minimum_score=minimum_score, severity=severity)
        ]
    finally:
        connection.close()


@app.get("/alerts/{alert_id}")
def alert_detail(alert_id: int) -> dict:
    connection = _connection()
    try:
        alert = get_alert(connection, alert_id)
        if alert is None:
            raise HTTPException(status_code=404, detail="Alert not found")
        prediction_lookup = get_latest_prediction_lookup(connection)
        prediction = prediction_lookup.get((alert.current_accession_number, alert.holding_key))
        return {
            "alert_id": alert.alert_id,
            "current_accession_number": alert.current_accession_number,
            "previous_accession_number": alert.previous_accession_number,
            "issuer_name": alert.issuer_name,
            "cusip": alert.cusip,
            "sector": alert.sector,
            "score": alert.score,
            "severity": alert.severity,
            "reasons": alert.reasons,
            "current_weight": alert.current_weight,
            "previous_weight": alert.previous_weight,
            "weight_delta": alert.weight_delta,
            "current_rank": alert.current_rank,
            "previous_rank": alert.previous_rank,
            "turnover_ratio": alert.turnover_ratio,
            "model_probability": prediction.probability if prediction else None,
            "model_predicted_label": prediction.predicted_label if prediction else None,
            "weak_label": prediction.weak_label if prediction else None,
            "impacts": list_alert_impacts(connection, alert_id),
        }
    finally:
        connection.close()


@app.get("/recommendations/{client_id}")
@app.get("/api/v1/recommendations/{client_id}")
def recommendations(client_id: str, limit: int = Query(default=20, ge=1, le=100)) -> list[dict]:
    connection = _connection()
    try:
        return [
            _serialize_recommendation(recommendation)
            for recommendation in list_recommendations(connection, client_id, limit=limit)
        ]
    finally:
        connection.close()


@app.get("/governance/materiality-policy")
def governance_materiality_policy() -> dict[str, object]:
    return materiality_policy()


@app.get("/models/latest")
def latest_model() -> dict:
    connection = _connection()
    try:
        latest_runs = list_latest_model_runs(connection)
        if not latest_runs:
            raise HTTPException(status_code=404, detail="No model run found")
        best_run = latest_runs[0]
        response = _serialize_model_run(best_run)
        if len(latest_runs) > 1 or best_run.comparison_group_id is not None:
            response["best_model_name"] = best_run.model_name
            response["models"] = [_serialize_model_run(model_run) for model_run in latest_runs]
        return response
    finally:
        connection.close()
