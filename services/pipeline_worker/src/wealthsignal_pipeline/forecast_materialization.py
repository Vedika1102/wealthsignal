from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .models import ForecastPrediction, ForecastRun
from .persistence import (
    DatabaseConnection,
    connect,
    get_forecast_run,
    initialize_database,
    store_forecast_run,
)


FORECAST_MATERIALIZATION_VERSION = 1
LIMITATIONS = [
    "Form 13F is delayed and does not represent a manager's complete economic exposure.",
    "Persistence repeats observed quarter-t weights; it is a reference forecast, not investment advice.",
    "Protocol V1 covers ten managers and six target quarters and does not establish broad external validity.",
]


def materialize_persistence_forecast(
    temporal_dataset: str | Path,
    *,
    connection: DatabaseConnection,
    target_quarter: str,
    protocol_path: str | Path,
    protocol_version: str = "v1",
) -> tuple[ForecastRun, bool]:
    """Persist one historical persistence forecast without reading target truth fields."""

    dataset = Path(temporal_dataset)
    temporal_manifest_path = dataset / "manifest.json"
    temporal_manifest = json.loads(temporal_manifest_path.read_text(encoding="utf-8"))
    temporal_manifest_sha = _sha256_file(temporal_manifest_path)
    source_csv = dataset / "manager_security_quarter.csv"
    expected_csv_sha = temporal_manifest.get("outputs", {}).get("manager_security_quarter.csv", {}).get("sha256")
    if not expected_csv_sha or _sha256_file(source_csv) != expected_csv_sha:
        raise ValueError("Temporal manager_security_quarter.csv checksum mismatch")
    protocol = Path(protocol_path)
    if not protocol.is_file() or not protocol.read_text(encoding="utf-8").strip():
        raise ValueError("protocol_path must identify a non-empty frozen protocol")
    protocol_sha = _sha256_file(protocol)
    implementation_sha = _sha256_file(Path(__file__))
    identity = {
        "contract_version": FORECAST_MATERIALIZATION_VERSION,
        "dataset_id": temporal_manifest["dataset_id"],
        "dataset_manifest_sha256": temporal_manifest_sha,
        "protocol_version": protocol_version,
        "protocol_sha256": protocol_sha,
        "model_name": "persistence",
        "model_version": "persistence-v1",
        "target_quarter": target_quarter,
        "implementation_sha256": implementation_sha,
    }
    run_id = "forecast-" + hashlib.sha256(_canonical_json(identity).encode()).hexdigest()[:20]

    accessions, source_lineage = _source_lineage(temporal_manifest)
    raw_rows: list[dict[str, str]] = []
    with source_csv.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["target_report_period"] == target_quarter:
                raw_rows.append({
                    "example_id": row["example_id"], "cik": row["cik"],
                    "security_key": row["security_key"], "cusip": row["cusip"],
                    "issuer_name": row["issuer_name"], "feature_report_period": row["feature_report_period"],
                    "feature_available_at": row["feature_available_at"],
                    "target_report_period": row["target_report_period"],
                    "current_weight": row["current_weight"],
                })
    if not raw_rows:
        raise ValueError(f"No temporal rows found for target quarter {target_quarter}")

    by_manager: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in raw_rows:
        by_manager[row["cik"]].append(row)
    predictions: list[ForecastPrediction] = []
    for cik, values in sorted(by_manager.items()):
        values.sort(key=lambda row: (-float(row["current_weight"]), row["security_key"]))
        for rank, row in enumerate(values, start=1):
            predictions.append(ForecastPrediction(
                run_id=run_id, example_id=row["example_id"], cik=cik,
                security_key=row["security_key"], cusip=row["cusip"], issuer_name=row["issuer_name"],
                feature_report_period=row["feature_report_period"],
                feature_available_at=row["feature_available_at"], target_report_period=target_quarter,
                predicted_weight=float(row["current_weight"]), predicted_rank=rank,
                source_accession_numbers=accessions.get((cik, row["feature_report_period"]), []),
            ))

    existing = get_forecast_run(connection, run_id)
    if existing is not None:
        inserted = store_forecast_run(connection, existing, predictions)
        return existing, inserted
    run = ForecastRun(
        run_id=run_id, model_name="persistence", model_version="persistence-v1", status="complete",
        dataset_id=str(temporal_manifest["dataset_id"]), dataset_manifest_sha256=temporal_manifest_sha,
        protocol_version=protocol_version, protocol_sha256=protocol_sha, code_revision=_git_revision(),
        implementation_sha256=implementation_sha,
        source_cutoff=max(prediction.feature_available_at for prediction in predictions),
        target_quarter=target_quarter, generated_at=datetime.now(timezone.utc).isoformat(),
        limitations=LIMITATIONS.copy(), source_lineage=source_lineage, prediction_count=len(predictions),
    )
    return run, store_forecast_run(connection, run, predictions)


def _source_lineage(temporal_manifest: dict) -> tuple[dict[tuple[str, str], list[str]], list[dict[str, object]]]:
    accessions: dict[tuple[str, str], list[str]] = {}
    packages: list[dict[str, object]] = []
    for source in temporal_manifest.get("input_sources", []):
        source_dir = Path(str(source["path"]))
        bulk_manifest_path = source_dir / "manifest.json"
        bulk_manifest = json.loads(bulk_manifest_path.read_text(encoding="utf-8"))
        expected = bulk_manifest.get("outputs", {}).get("effective_filings.csv", {}).get("sha256")
        effective_path = source_dir / "effective_filings.csv"
        if not expected or _sha256_file(effective_path) != expected:
            raise ValueError(f"Bulk effective_filings.csv checksum mismatch: {source_dir}")
        packages.extend({key: item.get(key) for key in ("package", "source_url", "sha256", "size_bytes")} for item in bulk_manifest.get("sources", []))
        with effective_path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                values = [value for value in row["source_accession_numbers"].split("|") if value]
                accessions[(row["cik"], row["report_period"])] = values
    packages.sort(key=lambda item: str(item.get("package")))
    return accessions, packages


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _git_revision() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Materialize a lineage-complete persistence forecast")
    parser.add_argument("--temporal-dataset", required=True); parser.add_argument("--target-quarter", required=True)
    parser.add_argument("--protocol", required=True); parser.add_argument("--protocol-version", default="v1")
    parser.add_argument("--db-path", default="data/wealthsignal.db")
    args = parser.parse_args(list(argv) if argv is not None else None)
    connection = connect(args.db_path)
    try:
        initialize_database(connection)
        run, inserted = materialize_persistence_forecast(args.temporal_dataset, connection=connection, target_quarter=args.target_quarter, protocol_path=args.protocol, protocol_version=args.protocol_version)
        print(json.dumps({"run_id": run.run_id, "inserted": inserted, "prediction_count": run.prediction_count, "target_quarter": run.target_quarter}, sort_keys=True))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
