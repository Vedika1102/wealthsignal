from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from wealthsignal_pipeline.materiality import materiality_policy
from wealthsignal_pipeline.persistence import (
    connect,
    get_alert,
    get_client_portfolio,
    get_latest_model_run,
    get_latest_prediction_lookup,
    initialize_database,
    list_alert_impacts,
    list_alerts,
    list_client_portfolios,
    list_filing_summaries,
    list_position_deltas,
)


def _default_db_path() -> str:
    repo_root = Path(__file__).resolve().parents[3]
    return str(repo_root / "data" / "wealthsignal.db")


DB_PATH = os.getenv("WEALTHSIGNAL_DB_PATH", _default_db_path())

app = FastAPI(title="WealthSignal Decision API", version="0.1.0")


def _connection():
    connection = connect(DB_PATH)
    initialize_database(connection)
    return connection


@app.get("/health")
def healthcheck() -> dict[str, str]:
    db_exists = Path(DB_PATH).exists()
    return {"status": "ok", "db_path": DB_PATH, "db_exists": str(db_exists).lower()}


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


@app.get("/governance/materiality-policy")
def governance_materiality_policy() -> dict[str, object]:
    return materiality_policy()


@app.get("/models/latest")
def latest_model() -> dict:
    connection = _connection()
    try:
        model_run = get_latest_model_run(connection)
        if model_run is None:
            raise HTTPException(status_code=404, detail="No model run found")
        return {
            "run_id": model_run.run_id,
            "model_name": model_run.model_name,
            "training_samples": model_run.training_samples,
            "positive_count": model_run.positive_count,
            "feature_names": model_run.feature_names,
            "coefficients": model_run.coefficients,
            "intercept": model_run.intercept,
            "metrics": model_run.metrics,
        }
    finally:
        connection.close()


@app.get('/clients')
def clients(limit: int = Query(default=100, ge=1, le=500)):
    connection = _connection()
    try:
        return list_client_portfolios(connection, limit)
    finally:
        connection.close()


@app.get('/clients/{client_id}')
def client_detail(client_id: str):
    connection = _connection()
    try:
        portfolio = get_client_portfolio(connection, client_id)
        if portfolio is None:
            raise HTTPException(status_code=404, detail='Client not found')
        return portfolio
    finally:
        connection.close()
