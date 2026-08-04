from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from services.decision_api.app import main as api
from wealthsignal_pipeline.models import ForecastPrediction, ForecastRun
from wealthsignal_pipeline.persistence import connect, initialize_database, store_forecast_run


def _seed_forecast(path: Path) -> str:
    run_id = "forecast-api-fixture"
    run = ForecastRun(
        run_id=run_id, model_name="persistence", model_version="persistence-v1", status="complete",
        dataset_id="dataset-fixture", dataset_manifest_sha256="d" * 64,
        protocol_version="v1", protocol_sha256="p" * 64, code_revision="abc123",
        implementation_sha256="i" * 64, source_cutoff="2024-11-15", target_quarter="2024-12-31",
        generated_at="2025-01-01T00:00:00+00:00",
        limitations=["Delayed public disclosure.", "Not investment advice."],
        source_lineage=[{"package": "2024q3", "sha256": "s" * 64}], prediction_count=3,
    )
    predictions = [
        ForecastPrediction(
            run_id=run_id, example_id=f"example-{rank}", cik="0001067983",
            security_key=f"CUSIP000{rank}|COM||SH|SOLE|", cusip=f"CUSIP000{rank}",
            issuer_name=f"Issuer {rank}", feature_report_period="2024-09-30",
            feature_available_at="2024-11-15", target_report_period="2024-12-31",
            predicted_weight=weight, predicted_rank=rank,
            source_accession_numbers=["0001067983-24-000001"],
        )
        for rank, weight in enumerate((0.5, 0.3, 0.2), start=1)
    ]
    connection = connect(path)
    try:
        initialize_database(connection)
        store_forecast_run(connection, run, predictions)
    finally:
        connection.close()
    return run_id


def test_forecast_api_contract_pagination_and_concept_separation(tmp_path: Path) -> None:
    db_path = tmp_path / "api.db"
    run_id = _seed_forecast(db_path)
    original = api.DB_PATH
    api.DB_PATH = str(db_path)
    try:
        client = TestClient(api.app)
        runs = client.get("/api/v1/forecast-runs", params={"limit": 1, "offset": 0})
        detail = client.get(f"/api/v1/forecast-runs/{run_id}")
        forecast = client.get(
            f"/api/v1/forecast-runs/{run_id}/managers/1067983",
            params={"target_quarter": "2024-12-31", "limit": 1, "offset": 1},
        )

        assert runs.status_code == detail.status_code == forecast.status_code == 200
        assert runs.json()["total"] == 1
        assert detail.json()["protocol_sha256"] == "p" * 64
        payload = forecast.json()
        assert payload["concept"] == "predicted_future_holdings"
        assert payload["observed_holdings_included"] is False
        assert payload["investment_advice"] is False
        assert payload["manager_cik"] == "0001067983"
        assert payload["total"] == 3 and payload["limit"] == 1 and payload["offset"] == 1
        assert payload["items"][0]["predicted_rank"] == 2
        assert payload["items"][0]["source_accession_numbers"]
        serialized = str(payload)
        assert "target_weight" not in serialized
        assert "predicted_is_new" not in serialized
        assert "predicted_is_exit" not in serialized
    finally:
        api.DB_PATH = original


def test_forecast_api_errors_and_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "api.db"
    run_id = _seed_forecast(db_path)
    original = api.DB_PATH
    api.DB_PATH = str(db_path)
    try:
        client = TestClient(api.app)
        assert client.get("/api/v1/forecast-runs/missing").status_code == 404
        assert client.get(
            f"/api/v1/forecast-runs/{run_id}/managers/not-a-cik",
            params={"target_quarter": "2024-12-31"},
        ).status_code == 422
        assert client.get(
            f"/api/v1/forecast-runs/{run_id}/managers/1067983",
            params={"target_quarter": "2025-03-31"},
        ).status_code == 404
        assert client.get(
            f"/api/v1/forecast-runs/{run_id}/managers/9999999999",
            params={"target_quarter": "2024-12-31"},
        ).status_code == 404
        assert client.get(
            f"/api/v1/forecast-runs/{run_id}/managers/1067983",
            params={"target_quarter": "not-a-quarter"},
        ).status_code == 422
        assert client.get("/api/v1/forecast-runs", params={"limit": 0}).status_code == 422
    finally:
        api.DB_PATH = original
