"""Memory-bounded pure-Python Protocol V2 reference holdings builder."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Iterable

from .bulk_dataset import (
    BulkHolding,
    BulkPackageSource,
    BulkSubmission,
    _read_package,
    _submission_order,
)


V2_START = date(2019, 3, 31)
V2_END = date(2026, 3, 31)
FIELDS = (
    "cik", "report_period", "security_key", "value_usd", "shares_or_principal",
    "portfolio_weight", "holding_rank", "available_at", "source_accession_numbers",
)


def v2_security_key(holding: BulkHolding) -> str:
    side = holding.put_call.upper() if holding.put_call.upper() in {"PUT", "CALL"} else "LONG"
    return f"{holding.cusip}|{side}"


def _aggregate(rows: Iterable[BulkHolding]) -> tuple[dict[str, BulkHolding], int]:
    grouped: dict[str, BulkHolding] = {}
    duplicates = 0
    for row in sorted(rows, key=lambda item: (item.info_table_key, v2_security_key(item))):
        # Match the frozen Spark V2 parser: normalized CUSIPs must be exactly
        # nine permitted characters before they can enter amendment resolution.
        if len(row.cusip) != 9:
            continue
        key = v2_security_key(row)
        previous = grouped.get(key)
        if previous is None:
            grouped[key] = row
        else:
            duplicates += 1
            grouped[key] = replace(
                previous,
                value_usd=previous.value_usd + row.value_usd,
                shares_or_principal=previous.shares_or_principal + row.shares_or_principal,
            )
    return grouped, duplicates


def resolve_manager(
    submissions: list[BulkSubmission], holdings_by_accession: dict[str, list[BulkHolding]]
) -> tuple[list[dict[str, object]], dict[str, int]]:
    groups: dict[date, list[BulkSubmission]] = {}
    for submission in submissions:
        if V2_START <= submission.report_period <= V2_END:
            groups.setdefault(submission.report_period, []).append(submission)
    output: list[dict[str, object]] = []
    duplicate_count = 0
    selected_filing_count = 0
    for report_period, group in sorted(groups.items()):
        ordered = sorted(group, key=_submission_order)
        ordinary = [row for row in ordered if row.submission_type == "13F-HR"]
        base = ordinary[-1] if ordinary else ordered[0]
        applicable = [row for row in ordered if _submission_order(row) >= _submission_order(base)]
        effective: dict[str, tuple[BulkHolding, set[str], date]] = {}
        accessions: list[str] = []
        for submission in applicable:
            rows, duplicates = _aggregate(holdings_by_accession.get(submission.accession_number, []))
            duplicate_count += duplicates
            amendment = submission.amendment_type.replace("_", " ")
            additive = submission.submission_type != "13F-HR" and (
                "NEW HOLDING" in amendment or "ADD" in amendment
            )
            if additive:
                for key, row in rows.items():
                    previous = effective.get(key)
                    if previous is None:
                        effective[key] = (row, {submission.accession_number}, submission.filing_date)
                    else:
                        duplicate_count += 1
                        previous_row, previous_accessions, previous_available_at = previous
                        effective[key] = (
                            replace(
                                previous_row,
                                value_usd=previous_row.value_usd + row.value_usd,
                                shares_or_principal=previous_row.shares_or_principal + row.shares_or_principal,
                            ),
                            previous_accessions | {submission.accession_number},
                            max(previous_available_at, submission.filing_date),
                        )
                accessions.append(submission.accession_number)
            else:
                effective = {
                    key: (row, {submission.accession_number}, submission.filing_date)
                    for key, row in rows.items()
                }
                accessions = [submission.accession_number]
        selected_filing_count += len(accessions)
        total = sum(item[0].value_usd for item in effective.values())
        ranked = sorted(effective.items(), key=lambda item: (-item[1][0].value_usd, item[0]))
        for rank, (security_key, (holding, security_accessions, available_at)) in enumerate(ranked, start=1):
            output.append(
                {
                    "cik": base.cik,
                    "report_period": report_period.isoformat(),
                    "security_key": security_key,
                    "value_usd": holding.value_usd,
                    "shares_or_principal": format(holding.shares_or_principal, ".17g"),
                    "portfolio_weight": format(holding.value_usd / total if total else 0.0, ".17g"),
                    "holding_rank": rank,
                    "available_at": available_at.isoformat(),
                    "source_accession_numbers": json.dumps(sorted(security_accessions), separators=(",", ":")),
                }
            )
    return output, {"duplicates_resolved": duplicate_count, "selected_filing_rows": selected_filing_count}


def build_reference(
    sources: list[BulkPackageSource], manager_ciks: list[str], output_path: str | Path,
    *, chunk_size: int = 5,
) -> dict[str, object]:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    row_count = 0
    duplicates = 0
    selected_filings = 0
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for offset in range(0, len(manager_ciks), chunk_size):
            chunk = manager_ciks[offset : offset + chunk_size]
            submissions: list[BulkSubmission] = []
            holdings: dict[str, list[BulkHolding]] = {}
            for source in sources:
                source_submissions, source_holdings, _, _ = _read_package(
                    source, manager_ciks=set(chunk), security_cusips=set()
                )
                submissions.extend(source_submissions)
                for accession, rows in source_holdings.items():
                    holdings.setdefault(accession, []).extend(rows)
            for cik in chunk:
                manager_submissions = [row for row in submissions if row.cik == cik]
                accessions = {row.accession_number for row in manager_submissions}
                manager_holdings = {key: value for key, value in holdings.items() if key in accessions}
                rows, metrics = resolve_manager(manager_submissions, manager_holdings)
                for row in rows:
                    writer.writerow(row)
                row_count += len(rows)
                duplicates += metrics["duplicates_resolved"]
                selected_filings += metrics["selected_filing_rows"]
    with destination.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return {
        "row_count": row_count,
        "manager_count": len(manager_ciks),
        "duplicates_resolved": duplicates,
        "selected_filing_rows": selected_filings,
        "sha256": digest.hexdigest(),
        "output_path": str(destination),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--cohort", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--manager-count", type=int, default=10)
    parser.add_argument("--chunk-size", type=int, default=5)
    args = parser.parse_args()
    cohort = json.loads(Path(args.cohort).read_text(encoding="utf-8"))
    source_dir = Path(args.source_dir)
    sources = [
        BulkPackageSource(path.name.split("_", 1)[0], path, "committed-manifest")
        for path in sorted(source_dir.glob("*_form13f.zip"))
    ]
    result = build_reference(
        sources,
        cohort["main_ordered_ciks"][: args.manager_count],
        args.output,
        chunk_size=args.chunk_size,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
