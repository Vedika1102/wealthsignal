from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

from .bulk_dataset import BulkPackageSource, _read_package, _source_manifest
from .cloud2_spark import repository_root
from .temporal_dataset import (
    HoldingSnapshot,
    _leakage_payload,
    _write_json_atomic,
    _write_rows,
    audit_temporal_dataset,
    build_temporal_rows,
    feature_dictionary,
)
from .v2_reference import resolve_manager_records


PROTOCOL_V2_CONFIG_PATH = "docs/ai-governance/forecast-protocol-v2-config.json"
PROTOCOL_V2_CONTRACT_VERSION = 1


def build_v2_split_manifest(
    rows: list[dict[str, object]],
    *,
    validation_start: date,
    validation_end: date,
) -> dict[str, object]:
    target_quarters = sorted(
        {
            str(row["target_report_period"])
            for row in rows
            if validation_start.isoformat() <= str(row["target_report_period"]) <= validation_end.isoformat()
        }
    )
    all_quarters = sorted({str(row["target_report_period"]) for row in rows})
    folds: list[dict[str, object]] = []
    for index, evaluation_quarter in enumerate(target_quarters, start=1):
        train_quarters = [quarter for quarter in all_quarters if quarter < evaluation_quarter]
        if not train_quarters:
            continue
        folds.append(
            {
                "fold_id": f"validation-{index}",
                "role": "validation",
                "train_target_quarters": train_quarters,
                "evaluation_target_quarter": evaluation_quarter,
                "train_example_count": sum(
                    1 for row in rows if str(row["target_report_period"]) in train_quarters
                ),
                "evaluation_example_count": sum(
                    1 for row in rows if str(row["target_report_period"]) == evaluation_quarter
                ),
            }
        )
    return {
        "strategy": "protocol_v2_expanding_window_by_target_report_quarter",
        "protocol_version": "v2-design-2",
        "contract_version": PROTOCOL_V2_CONTRACT_VERSION,
        "validation_window": [validation_start.isoformat(), validation_end.isoformat()],
        "all_target_quarters": all_quarters,
        "status": "ready" if folds else "insufficient_validation_quarters",
        "folds": folds,
    }


def _materialize_audit_manifest(
    rows: list[dict[str, object]],
    split_manifest: dict[str, object],
) -> dict[str, object]:
    by_quarter: dict[str, list[str]] = {}
    for row in rows:
        by_quarter.setdefault(str(row["target_report_period"]), []).append(str(row["example_id"]))
    return {
        "folds": [
            {
                "fold_id": fold["fold_id"],
                "evaluation_target_quarter": fold["evaluation_target_quarter"],
                "train_example_ids": [
                    example_id
                    for quarter in fold["train_target_quarters"]
                    for example_id in by_quarter.get(str(quarter), [])
                ],
                "evaluation_example_ids": by_quarter.get(str(fold["evaluation_target_quarter"]), []),
            }
            for fold in split_manifest["folds"]
        ]
    }


def _load_protocol_v2_defaults() -> tuple[date, date, int]:
    root = repository_root(__file__)
    payload = json.loads((root / PROTOCOL_V2_CONFIG_PATH).read_text(encoding="utf-8"))
    validation_start, validation_end = payload["target_windows"]["validation"]
    default_candidate_cap = min(int(value) for value in payload["candidate_caps_validation_only"])
    return date.fromisoformat(validation_start), date.fromisoformat(validation_end), default_candidate_cap


def build_protocol_v2_temporal_reference(
    sources: list[BulkPackageSource],
    manager_ciks: list[str],
    *,
    output_root: str | Path,
    negative_candidate_limit: int,
    validation_start: date,
    validation_end: date,
    action_tolerance: float = 1e-8,
    chunk_size: int = 5,
) -> Path:
    if not sources:
        raise ValueError("At least one bulk package source is required")
    if not manager_ciks:
        raise ValueError("At least one manager CIK is required")
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")

    identity = {
        "contract_version": PROTOCOL_V2_CONTRACT_VERSION,
        "protocol_version": "v2-design-2",
        "sources": [
            {
                "package": item["package"],
                "source_url": item["source_url"],
                "size_bytes": item["size_bytes"],
                "sha256": item["sha256"],
            }
            for item in (_source_manifest(source) for source in sorted(sources, key=lambda item: item.package))
        ],
        "manager_ciks": manager_ciks,
        "negative_candidate_limit": negative_candidate_limit,
        "validation_window": [validation_start.isoformat(), validation_end.isoformat()],
        "action_tolerance": action_tolerance,
        "chunk_size": chunk_size,
    }
    dataset_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    output_dir = Path(output_root) / f"protocol-v2-temporal-{dataset_id}"
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("identity") != identity:
            raise ValueError(f"Existing temporal reference identity mismatch: {output_dir}")
        return output_dir
    if output_dir.exists():
        raise ValueError(f"Refusing to overwrite incomplete temporal reference output: {output_dir}")

    staging = Path(output_root) / f".protocol-v2-temporal-{dataset_id}.building"
    marker = staging / "identity.json"
    if staging.exists():
        if not marker.exists() or json.loads(marker.read_text(encoding="utf-8")) != identity:
            raise ValueError(f"Refusing to replace unrecognized staging directory: {staging}")
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    _write_json_atomic(marker, identity)

    snapshots: list[HoldingSnapshot] = []
    duplicates_resolved = 0
    selected_filing_rows = 0
    effective_filing_rows: list[dict[str, object]] = []
    for offset in range(0, len(manager_ciks), chunk_size):
        chunk = manager_ciks[offset : offset + chunk_size]
        submissions = []
        holdings_by_accession: dict[str, list] = {}
        for source in sorted(sources, key=lambda item: item.package):
            source_submissions, source_holdings, _, _ = _read_package(
                source, manager_ciks=set(chunk), security_cusips=set()
            )
            submissions.extend(source_submissions)
            for accession, rows in source_holdings.items():
                holdings_by_accession.setdefault(accession, []).extend(rows)

        for cik in chunk:
            manager_submissions = [row for row in submissions if row.cik == cik]
            accessions = {row.accession_number for row in manager_submissions}
            manager_holdings = {
                accession: rows for accession, rows in holdings_by_accession.items() if accession in accessions
            }
            filing_rows, holding_rows, metrics = resolve_manager_records(manager_submissions, manager_holdings)
            duplicates_resolved += metrics["duplicates_resolved"]
            selected_filing_rows += metrics["selected_filing_rows"]
            effective_filing_rows.extend(
                {
                    "cik": row.cik,
                    "report_period": row.report_period.isoformat(),
                    "selected_filing_date": row.selected_filing_date.isoformat(),
                    "filer_name": row.filer_name,
                    "selected_accession_number": row.selected_accession_number,
                    "source_accession_numbers": "|".join(row.source_accession_numbers),
                    "superseded_accession_numbers": "|".join(row.superseded_accession_numbers),
                    "resolution": row.resolution,
                    "package": row.package,
                }
                for row in filing_rows
            )
            snapshots.extend(
                HoldingSnapshot(
                    cik=row.cik,
                    report_period=row.report_period,
                    filing_date=row.available_at,
                    security_key=row.security_key,
                    cusip=row.cusip,
                    issuer_name=row.issuer_name,
                    weight=row.portfolio_weight,
                    rank=row.holding_rank,
                    value_usd=row.value_usd,
                    accession_number=row.effective_accession_number,
                )
                for row in holding_rows
            )

    rows, build_metrics = build_temporal_rows(
        snapshots,
        negative_candidate_limit=negative_candidate_limit,
        action_tolerance=action_tolerance,
    )
    rows = [
        row for row in rows
        if str(row["target_report_period"]) <= validation_end.isoformat()
    ]
    split_manifest = build_v2_split_manifest(
        rows,
        validation_start=validation_start,
        validation_end=validation_end,
    )
    issues = audit_temporal_dataset(rows, _materialize_audit_manifest(rows, split_manifest))

    _write_rows(staging / "manager_security_quarter.csv", rows)
    _write_rows(staging / "effective_filings.csv", effective_filing_rows)
    _write_json_atomic(staging / "feature_dictionary.json", feature_dictionary())
    _write_json_atomic(staging / "split_manifest.json", split_manifest)
    _write_json_atomic(staging / "leakage_report.json", _leakage_payload(issues))
    if issues:
        raise ValueError(
            f"Protocol V2 temporal leakage audit failed with {len(issues)} issue(s); "
            f"see {staging / 'leakage_report.json'}"
        )

    quality = {
        **build_metrics,
        "protocol_version": "v2-design-2",
        "manager_count": len(manager_ciks),
        "input_snapshot_rows": len(snapshots),
        "effective_filing_rows": len(effective_filing_rows),
        "duplicates_resolved": duplicates_resolved,
        "selected_filing_rows": selected_filing_rows,
        "examples": len(rows),
        "target_quarters": sorted({str(row["target_report_period"]) for row in rows}),
        "validation_folds": len(split_manifest["folds"]),
        "leakage_issues": len(issues),
    }
    _write_json_atomic(staging / "quality_report.json", quality)

    marker.unlink()
    manifest = {
        "dataset_id": dataset_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "identity": identity,
        "outputs": {
            path.name: {
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in sorted(staging.iterdir())
            if path.is_file()
        },
    }
    _write_json_atomic(staging / "manifest.json", manifest)
    os.replace(staging, output_dir)
    return output_dir


def build_parser() -> argparse.ArgumentParser:
    validation_start, validation_end, default_candidate_cap = _load_protocol_v2_defaults()
    parser = argparse.ArgumentParser(
        description="Build the Protocol V2 temporal validation reference dataset."
    )
    parser.add_argument("--source", action="append", required=True, help="PACKAGE=path; repeatable.")
    parser.add_argument("--cohort", required=True, help="Path to the frozen Protocol V2 cohort JSON.")
    parser.add_argument("--manager-count", type=int, default=10)
    parser.add_argument("--output-root", default="data/temporal")
    parser.add_argument("--negative-candidate-limit", type=int, default=default_candidate_cap)
    parser.add_argument("--validation-start", default=validation_start.isoformat())
    parser.add_argument("--validation-end", default=validation_end.isoformat())
    parser.add_argument("--chunk-size", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    cohort = json.loads(Path(args.cohort).read_text(encoding="utf-8"))
    sources: list[BulkPackageSource] = []
    for value in args.source:
        if "=" not in value:
            raise SystemExit("Each --source must use PACKAGE=path format")
        package, raw_path = value.split("=", 1)
        sources.append(BulkPackageSource(package.lower(), Path(raw_path), "committed-manifest"))
    output = build_protocol_v2_temporal_reference(
        sources,
        cohort["main_ordered_ciks"][: args.manager_count],
        output_root=args.output_root,
        negative_candidate_limit=args.negative_candidate_limit,
        validation_start=date.fromisoformat(args.validation_start),
        validation_end=date.fromisoformat(args.validation_end),
        chunk_size=args.chunk_size,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
