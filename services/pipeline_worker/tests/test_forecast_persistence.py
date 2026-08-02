from __future__ import annotations

from pathlib import Path

import test_temporal_dataset as temporal_fixture

from wealthsignal_pipeline.forecast_materialization import materialize_persistence_forecast
from wealthsignal_pipeline.persistence import (
    connect,
    count_forecast_predictions,
    get_forecast_run,
    initialize_database,
    list_forecast_predictions,
)
from wealthsignal_pipeline.temporal_dataset import build_temporal_dataset


def test_persistence_forecast_round_trip_is_complete_and_idempotent(tmp_path: Path) -> None:
    source = temporal_fixture.TemporalDatasetTests()._write_normalized_dataset(tmp_path / "source")
    temporal = build_temporal_dataset(
        [source], output_root=tmp_path / "temporal", negative_candidate_limit=2,
        minimum_train_target_quarters=2, final_test_quarters=1,
    )
    protocol = tmp_path / "protocol.md"
    protocol.write_text("# Frozen fixture protocol\n", encoding="utf-8")
    connection = connect(tmp_path / "forecast.db")
    try:
        initialize_database(connection)
        first, inserted_first = materialize_persistence_forecast(
            temporal, connection=connection, target_quarter="2024-12-31", protocol_path=protocol,
        )
        second, inserted_second = materialize_persistence_forecast(
            temporal, connection=connection, target_quarter="2024-12-31", protocol_path=protocol,
        )
        stored = get_forecast_run(connection, first.run_id)
        predictions = list_forecast_predictions(connection, first.run_id, limit=100)

        assert inserted_first is True
        assert inserted_second is False
        assert second == first == stored
        assert count_forecast_predictions(connection, first.run_id) == first.prediction_count == len(predictions)
        assert all(row.security_key and row.cusip and row.source_accession_numbers for row in predictions)
        assert all(row.target_report_period == "2024-12-31" for row in predictions)
        for cik in {row.cik for row in predictions}:
            ranks = [row.predicted_rank for row in predictions if row.cik == cik]
            assert ranks == list(range(1, len(ranks) + 1))
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(forecast_predictions)").fetchall()}
        assert "target_weight" not in columns
        assert "predicted_weight" in columns
        plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM forecast_predictions WHERE cik = ? AND target_report_period = ? ORDER BY predicted_rank",
            ("0000000100", "2024-12-31"),
        ).fetchall()
        assert any("idx_forecast_predictions_manager_target" in str(row["detail"]) for row in plan)
    finally:
        connection.close()


def test_materializer_rejects_modified_temporal_csv(tmp_path: Path) -> None:
    source = temporal_fixture.TemporalDatasetTests()._write_normalized_dataset(tmp_path / "source")
    temporal = build_temporal_dataset([source], output_root=tmp_path / "temporal", negative_candidate_limit=2, minimum_train_target_quarters=2, final_test_quarters=1)
    protocol = tmp_path / "protocol.md"; protocol.write_text("frozen", encoding="utf-8")
    with (temporal / "manager_security_quarter.csv").open("a", encoding="utf-8") as handle:
        handle.write("tampered\n")
    connection = connect(tmp_path / "forecast.db")
    try:
        initialize_database(connection)
        try:
            materialize_persistence_forecast(temporal, connection=connection, target_quarter="2024-12-31", protocol_path=protocol)
            raise AssertionError("Expected checksum validation failure")
        except ValueError as error:
            assert "checksum mismatch" in str(error)
    finally:
        connection.close()
