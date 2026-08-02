from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from wealthsignal_pipeline.materiality import materiality_policy
from wealthsignal_pipeline.models import ClientHolding, ClientPortfolio
from wealthsignal_pipeline.persistence import (
    connect,
    count_manager_forecast_predictions,
    get_alert,
    get_client_portfolio,
    get_forecast_run,
    get_latest_prediction_lookup,
    initialize_database,
    list_alert_impacts,
    list_alerts,
    list_client_portfolios,
    list_filing_summaries,
    list_forecast_predictions,
    list_forecast_runs,
    list_latest_model_runs,
    list_position_deltas,
    list_recommendations,
    store_client_portfolio,
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

app = FastAPI(title="WealthSignal Decision API", version="0.2.0")


class ClientHoldingPayload(BaseModel):
    cusip: str = Field(min_length=1)
    issuer_name: str = Field(min_length=1)
    sector: str = Field(min_length=1)
    weight: float = Field(ge=0, le=1)


class ClientPortfolioPayload(BaseModel):
    client_name: str = Field(min_length=1)
    strategy: str = Field(min_length=1)
    holdings: list[ClientHoldingPayload] = Field(min_length=1)


class ForecastRunPayload(BaseModel):
    concept: Literal["predicted_future_holdings"] = "predicted_future_holdings"
    run_id: str
    model_name: str
    model_version: str
    status: str
    dataset_id: str
    dataset_manifest_sha256: str
    protocol_version: str
    protocol_sha256: str
    code_revision: str | None
    implementation_sha256: str
    source_cutoff: str
    target_quarter: str
    generated_at: str
    limitations: list[str]
    source_lineage: list[dict[str, object]]
    prediction_count: int


class ForecastRunPage(BaseModel):
    concept: Literal["predicted_future_holdings"] = "predicted_future_holdings"
    items: list[ForecastRunPayload]
    total: int
    limit: int
    offset: int


class ForecastPredictionPayload(BaseModel):
    example_id: str
    manager_cik: str
    security_key: str
    cusip: str
    issuer_name: str
    feature_report_period: str
    feature_available_at: str
    target_quarter: str
    predicted_weight: float = Field(ge=0)
    predicted_rank: int = Field(ge=1)
    source_accession_numbers: list[str]


class ManagerForecastPage(BaseModel):
    concept: Literal["predicted_future_holdings"] = "predicted_future_holdings"
    observed_holdings_included: Literal[False] = False
    investment_advice: Literal[False] = False
    run_id: str
    manager_cik: str
    target_quarter: str
    source_cutoff: str
    model_name: str
    model_version: str
    dataset_id: str
    protocol_version: str
    generated_at: str
    limitations: list[str]
    items: list[ForecastPredictionPayload]
    total: int
    limit: int
    offset: int


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


def _serialize_forecast_run(run) -> dict[str, object]:
    return {
        "concept": "predicted_future_holdings",
        "run_id": run.run_id, "model_name": run.model_name, "model_version": run.model_version,
        "status": run.status, "dataset_id": run.dataset_id,
        "dataset_manifest_sha256": run.dataset_manifest_sha256,
        "protocol_version": run.protocol_version, "protocol_sha256": run.protocol_sha256,
        "code_revision": run.code_revision, "implementation_sha256": run.implementation_sha256,
        "source_cutoff": run.source_cutoff, "target_quarter": run.target_quarter,
        "generated_at": run.generated_at, "limitations": run.limitations,
        "source_lineage": run.source_lineage, "prediction_count": run.prediction_count,
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


def _serialize_client_portfolio(portfolio: ClientPortfolio) -> dict[str, object]:
    return {
        "client_id": portfolio.client_id,
        "client_name": portfolio.client_name,
        "strategy": portfolio.strategy,
        "holdings": [
            {
                "cusip": holding.cusip,
                "issuer_name": holding.issuer_name,
                "sector": holding.sector,
                "weight": holding.weight,
            }
            for holding in portfolio.holdings
        ],
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


@app.get("/clients")
def clients(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    connection = _connection()
    try:
        return list_client_portfolios(connection, limit=limit)
    finally:
        connection.close()


@app.get("/clients/{client_id}")
def client_detail(client_id: str) -> dict[str, object]:
    connection = _connection()
    try:
        portfolio = get_client_portfolio(connection, client_id)
        if portfolio is None:
            raise HTTPException(status_code=404, detail="Client not found")
        return _serialize_client_portfolio(portfolio)
    finally:
        connection.close()


@app.post("/clients/{client_id}/portfolio")
def upsert_client_portfolio(client_id: str, payload: ClientPortfolioPayload) -> dict[str, object]:
    portfolio = ClientPortfolio(
        client_id=client_id,
        client_name=payload.client_name,
        strategy=payload.strategy,
        holdings=[
            ClientHolding(
                cusip=holding.cusip,
                issuer_name=holding.issuer_name,
                sector=holding.sector,
                weight=holding.weight,
            )
            for holding in payload.holdings
        ],
    )
    connection = _connection()
    try:
        try:
            store_client_portfolio(connection, portfolio)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return _serialize_client_portfolio(portfolio)
    finally:
        connection.close()


@app.get("/governance/materiality-policy")
def governance_materiality_policy() -> dict[str, object]:
    return materiality_policy()


@app.get("/api/v1/forecast-runs", response_model=ForecastRunPage)
def forecast_runs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    connection = _connection()
    try:
        runs, total = list_forecast_runs(connection, limit=limit, offset=offset)
        return {
            "concept": "predicted_future_holdings", "items": [_serialize_forecast_run(run) for run in runs],
            "total": total, "limit": limit, "offset": offset,
        }
    finally:
        connection.close()


@app.get("/api/v1/forecast-runs/{run_id}", response_model=ForecastRunPayload)
def forecast_run_detail(run_id: str) -> dict[str, object]:
    connection = _connection()
    try:
        run = get_forecast_run(connection, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Forecast run not found")
        return _serialize_forecast_run(run)
    finally:
        connection.close()


@app.get(
    "/api/v1/forecast-runs/{run_id}/managers/{manager_cik}",
    response_model=ManagerForecastPage,
)
def manager_forecast(
    run_id: str,
    manager_cik: str,
    target_quarter: str = Query(pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if not manager_cik.isdigit() or len(manager_cik) > 10:
        raise HTTPException(status_code=422, detail="manager_cik must contain at most ten digits")
    normalized_cik = manager_cik.zfill(10)
    connection = _connection()
    try:
        run = get_forecast_run(connection, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Forecast run not found")
        if target_quarter != run.target_quarter:
            raise HTTPException(status_code=404, detail="Target quarter not found for forecast run")
        total = count_manager_forecast_predictions(connection, run_id, normalized_cik, target_quarter)
        if total == 0:
            raise HTTPException(status_code=404, detail="Manager forecast not found")
        predictions = list_forecast_predictions(
            connection, run_id, cik=normalized_cik, target_quarter=target_quarter,
            limit=limit, offset=offset,
        )
        return {
            "concept": "predicted_future_holdings", "observed_holdings_included": False,
            "investment_advice": False, "run_id": run.run_id, "manager_cik": normalized_cik,
            "target_quarter": target_quarter, "source_cutoff": run.source_cutoff,
            "model_name": run.model_name, "model_version": run.model_version,
            "dataset_id": run.dataset_id, "protocol_version": run.protocol_version,
            "generated_at": run.generated_at, "limitations": run.limitations,
            "items": [
                {
                    "example_id": value.example_id, "manager_cik": value.cik,
                    "security_key": value.security_key, "cusip": value.cusip,
                    "issuer_name": value.issuer_name,
                    "feature_report_period": value.feature_report_period,
                    "feature_available_at": value.feature_available_at,
                    "target_quarter": value.target_report_period,
                    "predicted_weight": value.predicted_weight, "predicted_rank": value.predicted_rank,
                    "source_accession_numbers": value.source_accession_numbers,
                }
                for value in predictions
            ],
            "total": total, "limit": limit, "offset": offset,
        }
    finally:
        connection.close()


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
