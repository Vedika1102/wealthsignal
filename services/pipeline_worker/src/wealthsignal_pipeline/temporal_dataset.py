from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping


TEMPORAL_CONTRACT_VERSION = 2


@dataclass(frozen=True, slots=True)
class HoldingSnapshot:
    cik: str
    report_period: date
    filing_date: date
    security_key: str
    cusip: str
    issuer_name: str
    weight: float
    rank: int
    value_usd: int
    accession_number: str


@dataclass(frozen=True, slots=True)
class LeakageIssue:
    code: str
    message: str
    example_id: str | None = None
    fold_id: str | None = None


def build_temporal_dataset(
    input_datasets: Iterable[str | Path],
    *,
    output_root: str | Path,
    negative_candidate_limit: int = 100,
    action_tolerance: float = 1e-8,
    minimum_train_target_quarters: int = 2,
    final_test_quarters: int = 1,
) -> Path:
    """Create immutable manager-security-quarter examples and leakage-safe folds."""

    if negative_candidate_limit < 0:
        raise ValueError("negative_candidate_limit must be non-negative")
    if action_tolerance < 0:
        raise ValueError("action_tolerance must be non-negative")
    started = time.perf_counter()
    dataset_dirs = sorted(Path(value) for value in input_datasets)
    if not dataset_dirs:
        raise ValueError("At least one normalized bulk dataset is required")

    input_manifests = [_load_input_manifest(path) for path in dataset_dirs]
    identity = {
        "contract_version": TEMPORAL_CONTRACT_VERSION,
        "input_datasets": [
            {"dataset_id": manifest["dataset_id"], "manifest_sha256": _sha256_file(path / "manifest.json")}
            for path, manifest in zip(dataset_dirs, input_manifests)
        ],
        "negative_candidate_limit": negative_candidate_limit,
        "action_tolerance": action_tolerance,
        "minimum_train_target_quarters": minimum_train_target_quarters,
        "final_test_quarters": final_test_quarters,
    }
    dataset_id = hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()[:16]
    output_dir = Path(output_root) / f"temporal-{dataset_id}"
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("identity") != identity:
            raise ValueError(f"Existing temporal dataset identity mismatch: {output_dir}")
        return output_dir
    if output_dir.exists():
        raise ValueError(f"Refusing to overwrite incomplete temporal dataset: {output_dir}")

    staging = Path(output_root) / f".temporal-{dataset_id}.building"
    marker = staging / "identity.json"
    if staging.exists():
        if not marker.exists() or json.loads(marker.read_text(encoding="utf-8")) != identity:
            raise ValueError(f"Refusing to replace unrecognized staging directory: {staging}")
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    _write_json_atomic(marker, identity)

    snapshots, duplicate_source_rows = _load_snapshots(dataset_dirs)
    rows, build_metrics = build_temporal_rows(
        snapshots,
        negative_candidate_limit=negative_candidate_limit,
        action_tolerance=action_tolerance,
    )
    split_manifest = build_expanding_window_manifest(
        rows,
        minimum_train_target_quarters=minimum_train_target_quarters,
        final_test_quarters=final_test_quarters,
    )
    issues = audit_temporal_dataset(rows, split_manifest)
    if issues:
        _write_json_atomic(staging / "leakage_report.json", _leakage_payload(issues))
        raise ValueError(f"Temporal leakage audit failed with {len(issues)} issue(s); see {staging / 'leakage_report.json'}")

    _write_rows(staging / "manager_security_quarter.csv", rows)
    _write_json_atomic(staging / "feature_dictionary.json", feature_dictionary())
    _write_json_atomic(staging / "split_manifest.json", split_manifest)
    _write_json_atomic(staging / "leakage_report.json", _leakage_payload(issues))

    target_quarters = sorted({row["target_report_period"] for row in rows})
    quality = {
        **build_metrics,
        "input_snapshot_rows": len(snapshots),
        "duplicate_source_rows_resolved": duplicate_source_rows,
        "examples": len(rows),
        "target_quarters": target_quarters,
        "temporal_folds": len(split_manifest["folds"]),
        "leakage_issues": len(issues),
        "runtime_seconds": round(time.perf_counter() - started, 6),
    }
    _write_json_atomic(staging / "quality_report.json", quality)

    marker.unlink()
    outputs = {
        path.name: {"size_bytes": path.stat().st_size, "sha256": _sha256_file(path)}
        for path in sorted(staging.iterdir())
        if path.is_file()
    }
    manifest = {
        "dataset_id": dataset_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "identity": identity,
        "input_sources": [
            {"path": str(path.resolve()), "dataset_id": manifest["dataset_id"]}
            for path, manifest in zip(dataset_dirs, input_manifests)
        ],
        "outputs": outputs,
    }
    _write_json_atomic(staging / "manifest.json", manifest)
    os.replace(staging, output_dir)
    return output_dir


def build_temporal_rows(
    snapshots: list[HoldingSnapshot], *, negative_candidate_limit: int, action_tolerance: float
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Build objective t+1 rows using only features available by each cutoff."""

    portfolio_rows: dict[tuple[str, date], list[HoldingSnapshot]] = {}
    for row in snapshots:
        portfolio_rows.setdefault((row.cik, row.report_period), []).append(row)
    portfolios: dict[tuple[str, date], dict[str, HoldingSnapshot]] = {
        key: {row.security_key: row for row in values} for key, values in portfolio_rows.items()
    }
    manager_periods: dict[str, list[date]] = {}
    for cik, period in portfolios:
        manager_periods.setdefault(cik, []).append(period)
    for periods in manager_periods.values():
        periods.sort()

    first_seen: dict[str, tuple[date, date]] = {}
    security_reference: dict[str, HoldingSnapshot] = {}
    for row in snapshots:
        seen = first_seen.get(row.security_key)
        candidate = (row.filing_date, row.report_period)
        seen_order = (seen[1], seen[0]) if seen else None
        if seen_order is None or candidate < seen_order:
            first_seen[row.security_key] = (row.report_period, row.filing_date)
            security_reference[row.security_key] = row

    rows: list[dict[str, object]] = []
    eligible_manager_quarters = 0
    skipped_missing_next_quarter = 0
    skipped_target_already_available = 0
    target_holding_count = 0
    covered_target_holding_count = 0
    target_weight_mass_total = 0.0
    covered_target_weight_mass_total = 0.0
    coverage_by_target_quarter: dict[str, dict[str, float | int]] = {}
    coverage_by_manager_quarter: list[dict[str, object]] = []
    for cik in sorted(manager_periods):
        periods = manager_periods[cik]
        for period in periods:
            target_period = _next_quarter(period)
            if target_period not in periods:
                skipped_missing_next_quarter += 1
                continue
            current = portfolios[(cik, period)]
            target = portfolios[(cik, target_period)]
            cutoff = max(row.filing_date for row in current.values())
            target_available_at = max(row.filing_date for row in target.values())
            if target_available_at <= cutoff:
                skipped_target_already_available += 1
                continue
            eligible_manager_quarters += 1
            peer_statistics = _peer_statistics_as_of(
                portfolios, manager_periods, report_period=period, available_at=cutoff
            )
            current_keys = set(current)
            ranked_negatives = [
                key
                for key, _ in sorted(
                    peer_statistics.items(),
                    key=lambda item: (-item[1][0], -item[1][1], item[0]),
                )
                if key not in current_keys
            ][:negative_candidate_limit]
            candidate_keys = sorted(current_keys | set(ranked_negatives))
            target_holding_count += len(target)
            covered_target_holding_count += len(set(target) & set(candidate_keys))
            target_weight_mass = sum(row.weight for row in target.values())
            covered_target_weight_mass = sum(
                row.weight for key, row in target.items() if key in candidate_keys
            )
            target_weight_mass_total += target_weight_mass
            covered_target_weight_mass_total += covered_target_weight_mass
            quarter_metrics = coverage_by_target_quarter.setdefault(
                target_period.isoformat(),
                {
                    "manager_quarter_count": 0,
                    "target_holdings": 0,
                    "target_holdings_in_candidate_universe": 0,
                    "target_weight_mass_total": 0.0,
                    "target_weight_mass_in_candidate_universe": 0.0,
                },
            )
            quarter_metrics["manager_quarter_count"] = int(quarter_metrics["manager_quarter_count"]) + 1
            quarter_metrics["target_holdings"] = int(quarter_metrics["target_holdings"]) + len(target)
            quarter_metrics["target_holdings_in_candidate_universe"] = (
                int(quarter_metrics["target_holdings_in_candidate_universe"])
                + len(set(target) & set(candidate_keys))
            )
            quarter_metrics["target_weight_mass_total"] = (
                float(quarter_metrics["target_weight_mass_total"]) + target_weight_mass
            )
            quarter_metrics["target_weight_mass_in_candidate_universe"] = (
                float(quarter_metrics["target_weight_mass_in_candidate_universe"]) + covered_target_weight_mass
            )
            coverage_by_manager_quarter.append(
                {
                    "cik": cik,
                    "feature_report_period": period.isoformat(),
                    "target_report_period": target_period.isoformat(),
                    "target_holding_count": len(target),
                    "covered_target_holding_count": len(set(target) & set(candidate_keys)),
                    "target_weight_mass_total": target_weight_mass,
                    "target_weight_mass_in_candidate_universe": covered_target_weight_mass,
                    "target_weight_mass_coverage": (
                        covered_target_weight_mass / target_weight_mass if target_weight_mass else 0.0
                    ),
                }
            )

            previous_period = _previous_quarter(period)
            previous = portfolios.get((cik, previous_period), {})
            lag2 = portfolios.get((cik, _previous_quarter(previous_period)), {})
            turnover = _turnover(current, previous) if previous else 0.0
            concentration_hhi = sum(row.weight**2 for row in current.values())
            top10_share = sum(sorted((row.weight for row in current.values()), reverse=True)[:10])
            target_out_rank = len(target) + 1

            for security_key in candidate_keys:
                current_row = current.get(security_key)
                previous_row = previous.get(security_key)
                lag2_row = lag2.get(security_key)
                target_row = target.get(security_key)
                security_period, security_available = first_seen[security_key]
                current_weight = current_row.weight if current_row else 0.0
                previous_weight = previous_row.weight if previous_row else 0.0
                lag2_weight = lag2_row.weight if lag2_row else 0.0
                target_weight = target_row.weight if target_row else 0.0
                owner_count, aggregate_weight = peer_statistics.get(security_key, (0, 0.0))
                last_held = _last_held_period(portfolios, cik, security_key, period)
                action = _holding_action(current_weight, target_weight, action_tolerance)
                reference_row = current_row or previous_row or target_row or security_reference[security_key]
                example_id = hashlib.sha256(
                    f"{cik}|{period.isoformat()}|{security_key}".encode("utf-8")
                ).hexdigest()[:24]
                rows.append(
                    {
                        "example_id": example_id,
                        "cik": cik,
                        "security_key": security_key,
                        "cusip": reference_row.cusip,
                        "issuer_name": reference_row.issuer_name,
                        "feature_report_period": period.isoformat(),
                        "feature_available_at": cutoff.isoformat(),
                        "target_report_period": target_period.isoformat(),
                        "target_available_at": target_available_at.isoformat(),
                        "security_first_seen_period": security_period.isoformat(),
                        "security_first_seen_available_at": security_available.isoformat(),
                        "current_weight": current_weight,
                        "previous_weight": previous_weight,
                        "lag2_weight": lag2_weight,
                        "weight_momentum": current_weight - previous_weight,
                        "current_rank": current_row.rank if current_row else len(current) + 1,
                        "previous_rank": previous_row.rank if previous_row else len(previous) + 1,
                        "rank_momentum": (previous_row.rank if previous_row else len(previous) + 1)
                        - (current_row.rank if current_row else len(current) + 1),
                        "holding_history_quarters": _holding_history_count(portfolios, cik, security_key, period),
                        "quarters_since_last_held": _quarter_distance(last_held, period) if last_held else -1,
                        "manager_turnover": turnover,
                        "manager_concentration_hhi": concentration_hhi,
                        "manager_top10_share": top10_share,
                        "peer_owner_count": owner_count,
                        "peer_aggregate_weight": aggregate_weight,
                        "target_weight": target_weight,
                        "target_rank": target_row.rank if target_row else target_out_rank,
                        "target_is_new": int(action == "new"),
                        "target_is_exit": int(action == "exit"),
                        "target_is_increase": int(action == "increase"),
                        "target_is_decrease": int(action == "decrease"),
                        "target_is_unchanged": int(action == "unchanged"),
                        "target_action": action,
                    }
                )
    rows.sort(key=lambda row: (row["target_report_period"], row["cik"], row["security_key"]))
    mean_target_weight_mass_coverage = (
        sum(float(item["target_weight_mass_coverage"]) for item in coverage_by_manager_quarter)
        / len(coverage_by_manager_quarter)
        if coverage_by_manager_quarter
        else 0.0
    )
    return rows, {
        "eligible_manager_quarters": eligible_manager_quarters,
        "manager_quarters_without_exact_next_quarter": skipped_missing_next_quarter,
        "manager_quarters_target_already_available": skipped_target_already_available,
        "target_holdings": target_holding_count,
        "target_holdings_in_candidate_universe": covered_target_holding_count,
        "target_candidate_coverage": (
            covered_target_holding_count / target_holding_count if target_holding_count else 0.0
        ),
        "target_weight_mass_total": target_weight_mass_total,
        "target_weight_mass_in_candidate_universe": covered_target_weight_mass_total,
        "target_weight_mass_coverage": (
            covered_target_weight_mass_total / target_weight_mass_total if target_weight_mass_total else 0.0
        ),
        "mean_target_weight_mass_coverage": mean_target_weight_mass_coverage,
        "coverage_by_target_quarter": [
            {
                "target_report_period": target_report_period,
                "manager_quarter_count": int(metrics["manager_quarter_count"]),
                "target_holdings": int(metrics["target_holdings"]),
                "target_holdings_in_candidate_universe": int(metrics["target_holdings_in_candidate_universe"]),
                "target_candidate_coverage": (
                    int(metrics["target_holdings_in_candidate_universe"]) / int(metrics["target_holdings"])
                    if int(metrics["target_holdings"]) else 0.0
                ),
                "target_weight_mass_total": float(metrics["target_weight_mass_total"]),
                "target_weight_mass_in_candidate_universe": float(metrics["target_weight_mass_in_candidate_universe"]),
                "target_weight_mass_coverage": (
                    float(metrics["target_weight_mass_in_candidate_universe"])
                    / float(metrics["target_weight_mass_total"])
                    if float(metrics["target_weight_mass_total"]) else 0.0
                ),
            }
            for target_report_period, metrics in sorted(coverage_by_target_quarter.items())
        ],
        "coverage_by_manager_quarter": coverage_by_manager_quarter,
        "negative_candidate_limit": negative_candidate_limit,
        "action_tolerance": action_tolerance,
    }


def build_expanding_window_manifest(
    rows: list[dict[str, object]], *, minimum_train_target_quarters: int, final_test_quarters: int
) -> dict[str, object]:
    if minimum_train_target_quarters < 1:
        raise ValueError("minimum_train_target_quarters must be at least 1")
    if final_test_quarters < 1:
        raise ValueError("final_test_quarters must be at least 1")
    target_quarters = sorted({str(row["target_report_period"]) for row in rows})
    folds: list[dict[str, object]] = []
    for index in range(minimum_train_target_quarters, len(target_quarters)):
        evaluation_quarter = target_quarters[index]
        is_final_test = index >= len(target_quarters) - final_test_quarters
        train_quarters = target_quarters[:index]
        train_ids = [
            str(row["example_id"]) for row in rows if str(row["target_report_period"]) in train_quarters
        ]
        evaluation_ids = [
            str(row["example_id"]) for row in rows if str(row["target_report_period"]) == evaluation_quarter
        ]
        folds.append(
            {
                "fold_id": f"fold-{index - minimum_train_target_quarters + 1}",
                "role": "test" if is_final_test else "validation",
                "train_target_quarters": train_quarters,
                "evaluation_target_quarter": evaluation_quarter,
                "train_example_ids": train_ids,
                "evaluation_example_ids": evaluation_ids,
            }
        )
    return {
        "strategy": "expanding_window_by_target_report_quarter",
        "minimum_train_target_quarters": minimum_train_target_quarters,
        "final_test_quarters": final_test_quarters,
        "all_target_quarters": target_quarters,
        "status": "ready" if folds else "insufficient_target_quarters",
        "folds": folds,
    }


def audit_temporal_dataset(
    rows: list[dict[str, object]], split_manifest: Mapping[str, object]
) -> list[LeakageIssue]:
    issues: list[LeakageIssue] = []
    seen_ids: set[str] = set()
    row_by_id: dict[str, Mapping[str, object]] = {}
    group_by_id: dict[str, tuple[str, str]] = {}
    for row in rows:
        example_id = str(row["example_id"])
        if example_id in seen_ids:
            issues.append(LeakageIssue("duplicate_example", "Example ID appears more than once", example_id))
        seen_ids.add(example_id)
        row_by_id[example_id] = row
        group_by_id[example_id] = (str(row.get("cik", "")), str(row["feature_report_period"]))
        feature_period = date.fromisoformat(str(row["feature_report_period"]))
        target_period = date.fromisoformat(str(row["target_report_period"]))
        feature_available = date.fromisoformat(str(row["feature_available_at"]))
        target_available = date.fromisoformat(str(row["target_available_at"]))
        first_seen_period = date.fromisoformat(str(row["security_first_seen_period"]))
        first_seen_available = date.fromisoformat(str(row["security_first_seen_available_at"]))
        if target_period != _next_quarter(feature_period):
            issues.append(LeakageIssue("invalid_target_horizon", "Target is not the exact next quarter", example_id))
        if target_available <= feature_available:
            issues.append(LeakageIssue("target_available_too_early", "Target filing is not later than feature cutoff", example_id))
        if first_seen_period > feature_period or first_seen_available > feature_available:
            issues.append(LeakageIssue("future_candidate", "Candidate security was first observed after cutoff", example_id))

    for fold in split_manifest.get("folds", []):
        fold_id = str(fold["fold_id"])
        train_ids = set(str(value) for value in fold["train_example_ids"])
        evaluation_ids = set(str(value) for value in fold["evaluation_example_ids"])
        overlap = train_ids & evaluation_ids
        if overlap:
            issues.append(LeakageIssue("split_example_overlap", "Train and evaluation IDs overlap", fold_id=fold_id))
        train_groups = {group_by_id[value] for value in train_ids if value in group_by_id}
        evaluation_groups = {group_by_id[value] for value in evaluation_ids if value in group_by_id}
        if train_groups & evaluation_groups:
            issues.append(
                LeakageIssue(
                    "split_manager_quarter_overlap",
                    "A manager feature-quarter appears in both train and evaluation sets",
                    fold_id=fold_id,
                )
            )
        evaluation_quarter = str(fold["evaluation_target_quarter"])
        for example_id in train_ids:
            row = row_by_id.get(example_id)
            if row is None:
                issues.append(LeakageIssue("unknown_train_example", "Split references an unknown example", example_id, fold_id))
            elif str(row["target_report_period"]) >= evaluation_quarter:
                issues.append(LeakageIssue("train_boundary_violation", "Training target is not earlier than evaluation", example_id, fold_id))
        for example_id in evaluation_ids:
            row = row_by_id.get(example_id)
            if row is None:
                issues.append(LeakageIssue("unknown_evaluation_example", "Split references an unknown example", example_id, fold_id))
            elif str(row["target_report_period"]) != evaluation_quarter:
                issues.append(LeakageIssue("evaluation_boundary_violation", "Evaluation example is in the wrong quarter", example_id, fold_id))
    return issues


def feature_dictionary() -> dict[str, object]:
    return {
        "prediction_unit": "manager-security-quarter",
        "observation_cutoff": "selected manager filing date for feature_report_period",
        "target_horizon": "exact next report-calendar quarter",
        "features": {
            "current_weight": "Manager holding weight in quarter t; available at the selected t filing date.",
            "previous_weight": "Manager holding weight in exact quarter t-1 when available.",
            "lag2_weight": "Manager holding weight in exact quarter t-2 when available.",
            "weight_momentum": "current_weight minus previous_weight.",
            "current_rank": "Weight rank in manager portfolio at t; out-of-portfolio rank for candidates not held.",
            "previous_rank": "Weight rank at t-1; out-of-portfolio rank when absent.",
            "rank_momentum": "previous_rank minus current_rank.",
            "holding_history_quarters": "Count of observed manager quarters through t in which the security was held.",
            "quarters_since_last_held": "Quarter distance to most recent held period through t; -1 if never held by manager.",
            "manager_turnover": "One-half sum of absolute weight changes between exact t-1 and t.",
            "manager_concentration_hhi": "Sum of squared manager weights at t.",
            "manager_top10_share": "Sum of ten largest manager weights at t.",
            "peer_owner_count": "Managers whose latest filing available by cutoff held the security.",
            "peer_aggregate_weight": "Sum of weights across peer latest filings available by cutoff.",
        },
        "targets": {
            "target_weight": "Observed normalized weight in exact quarter t+1, otherwise zero for eligible candidates.",
            "target_rank": "Observed rank in t+1, otherwise one plus target portfolio size.",
            "target_action": "Objective new, exit, increase, decrease, or unchanged action from t to t+1.",
        },
    }


def _load_snapshots(dataset_dirs: list[Path]) -> tuple[list[HoldingSnapshot], int]:
    by_id: dict[str, HoldingSnapshot] = {}
    duplicate_count = 0
    for dataset_dir in dataset_dirs:
        path = dataset_dir / "normalized_holdings.csv"
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                try:
                    snapshot = HoldingSnapshot(
                        cik=row["cik"],
                        report_period=date.fromisoformat(row["report_period"]),
                        filing_date=date.fromisoformat(row["filing_date"]),
                        security_key=row["security_key"],
                        cusip=row["cusip"],
                        issuer_name=row["issuer_name"],
                        weight=float(row["portfolio_weight"]),
                        rank=int(row["holding_rank"]),
                        value_usd=int(row["value_usd"]),
                        accession_number=row["effective_accession_number"],
                    )
                except (KeyError, ValueError) as exc:
                    raise ValueError(f"Invalid normalized holding at {path}:{row_number}: {exc}") from exc
                holding_id = row["holding_id"]
                existing = by_id.get(holding_id)
                if existing is not None:
                    if existing != snapshot:
                        raise ValueError(f"Conflicting duplicate holding_id {holding_id}")
                    duplicate_count += 1
                    continue
                by_id[holding_id] = snapshot
    return sorted(by_id.values(), key=lambda row: (row.report_period, row.cik, row.security_key)), duplicate_count


def _peer_statistics_as_of(
    portfolios: Mapping[tuple[str, date], Mapping[str, HoldingSnapshot]],
    manager_periods: Mapping[str, list[date]],
    *,
    report_period: date,
    available_at: date,
) -> dict[str, tuple[int, float]]:
    owner_count: dict[str, int] = {}
    aggregate_weight: dict[str, float] = {}
    for peer_cik, periods in manager_periods.items():
        eligible = [
            period
            for period in periods
            if period <= report_period
            and max(row.filing_date for row in portfolios[(peer_cik, period)].values()) <= available_at
        ]
        if not eligible:
            continue
        peer = portfolios[(peer_cik, max(eligible))]
        for security_key, row in peer.items():
            owner_count[security_key] = owner_count.get(security_key, 0) + 1
            aggregate_weight[security_key] = aggregate_weight.get(security_key, 0.0) + row.weight
    return {key: (owner_count[key], aggregate_weight[key]) for key in owner_count}


def _holding_action(current: float, target: float, tolerance: float) -> str:
    if current <= tolerance and target > tolerance:
        return "new"
    if current > tolerance and target <= tolerance:
        return "exit"
    difference = target - current
    if difference > tolerance:
        return "increase"
    if difference < -tolerance:
        return "decrease"
    return "unchanged"


def _turnover(current: Mapping[str, HoldingSnapshot], previous: Mapping[str, HoldingSnapshot]) -> float:
    keys = set(current) | set(previous)
    return 0.5 * sum(
        abs((current[key].weight if key in current else 0.0) - (previous[key].weight if key in previous else 0.0))
        for key in keys
    )


def _holding_history_count(
    portfolios: Mapping[tuple[str, date], Mapping[str, HoldingSnapshot]], cik: str, security_key: str, through: date
) -> int:
    return sum(1 for (manager, period), rows in portfolios.items() if manager == cik and period <= through and security_key in rows)


def _last_held_period(
    portfolios: Mapping[tuple[str, date], Mapping[str, HoldingSnapshot]], cik: str, security_key: str, through: date
) -> date | None:
    periods = [period for (manager, period), rows in portfolios.items() if manager == cik and period <= through and security_key in rows]
    return max(periods) if periods else None


def _next_quarter(value: date) -> date:
    month = value.month + 3
    year = value.year
    if month > 12:
        month -= 12
        year += 1
    return date(year, month, _quarter_end_day(year, month))


def _previous_quarter(value: date) -> date:
    month = value.month - 3
    year = value.year
    if month < 1:
        month += 12
        year -= 1
    return date(year, month, _quarter_end_day(year, month))


def _quarter_end_day(year: int, month: int) -> int:
    if month in {3, 12}:
        return 31
    return 30


def _quarter_distance(start: date, end: date) -> int:
    return (end.year - start.year) * 4 + (end.month - start.month) // 3


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not rows:
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _load_input_manifest(path: Path) -> dict[str, object]:
    manifest_path = path / "manifest.json"
    holdings_path = path / "normalized_holdings.csv"
    if not manifest_path.is_file() or not holdings_path.is_file():
        raise ValueError(f"Not a normalized SEC bulk dataset: {path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_checksum = manifest.get("outputs", {}).get("normalized_holdings.csv", {}).get("sha256")
    if not expected_checksum:
        raise ValueError(f"Input manifest lacks normalized_holdings.csv checksum: {manifest_path}")
    if _sha256_file(holdings_path) != expected_checksum:
        raise ValueError(f"Input normalized_holdings.csv checksum mismatch: {holdings_path}")
    return manifest


def _leakage_payload(issues: list[LeakageIssue]) -> dict[str, object]:
    return {
        "status": "PASS" if not issues else "FAIL",
        "issue_count": len(issues),
        "issues": [
            {"code": issue.code, "message": issue.message, "example_id": issue.example_id, "fold_id": issue.fold_id}
            for issue in issues
        ],
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _write_json_atomic(path: Path, payload: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the leakage-audited WealthSignal temporal dataset.")
    parser.add_argument("--input-dataset", action="append", required=True, help="Normalized SEC dataset directory; repeatable.")
    parser.add_argument("--output-root", default="data/temporal")
    parser.add_argument("--negative-candidate-limit", type=int, default=100)
    parser.add_argument("--action-tolerance", type=float, default=1e-8)
    parser.add_argument("--minimum-train-target-quarters", type=int, default=2)
    parser.add_argument("--final-test-quarters", type=int, default=1)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    output = build_temporal_dataset(
        args.input_dataset,
        output_root=args.output_root,
        negative_candidate_limit=args.negative_candidate_limit,
        action_tolerance=args.action_tolerance,
        minimum_train_target_quarters=args.minimum_train_target_quarters,
        final_test_quarters=args.final_test_quarters,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
