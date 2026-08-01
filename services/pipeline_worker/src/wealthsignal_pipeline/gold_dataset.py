from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from .persistence import connect, initialize_database, load_feature_rows


REVIEW_COLUMNS = ("manual_label", "review_reason", "reviewer_id", "reviewed_at")
EXPORT_COLUMNS = (
    "event_id",
    "current_accession_number",
    "previous_accession_number",
    "holding_key",
    "issuer_name",
    "cusip",
    "sector",
    "current_weight",
    "previous_weight",
    "weight_delta",
    "value_delta_thousands",
    "is_new_position",
    "is_exited_position",
    "current_rank",
    "previous_rank",
    "turnover_ratio",
    "rule_score",
    "weak_label",
    *REVIEW_COLUMNS,
)


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    row_count: int
    positive_count: int
    negative_count: int


def export_labeling_candidates(db_path: str | Path, output_path: str | Path, *, limit: int = 300) -> int:
    """Export a deterministic, weak-label-balanced review queue from persisted features."""

    if limit < 1:
        raise ValueError("Export limit must be at least 1")

    connection = connect(db_path)
    try:
        initialize_database(connection)
        rows = load_feature_rows(connection)
    finally:
        connection.close()

    positives = sorted((row for row in rows if row.weak_label == 1), key=_candidate_sort_key)
    negatives = sorted((row for row in rows if row.weak_label == 0), key=_candidate_sort_key)
    selected = _interleave(positives, negatives, limit)

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        for row in selected:
            writer.writerow(
                {
                    "event_id": _event_id(row.current_accession_number, row.holding_key),
                    "current_accession_number": row.current_accession_number,
                    "previous_accession_number": row.previous_accession_number,
                    "holding_key": row.holding_key,
                    "issuer_name": row.issuer_name,
                    "cusip": row.cusip,
                    "sector": row.sector,
                    "current_weight": row.current_weight,
                    "previous_weight": row.previous_weight,
                    "weight_delta": row.weight_delta,
                    "value_delta_thousands": row.value_delta_thousands,
                    "is_new_position": int(row.is_new_position),
                    "is_exited_position": int(row.is_exited_position),
                    "current_rank": row.current_rank if row.current_rank is not None else "",
                    "previous_rank": row.previous_rank if row.previous_rank is not None else "",
                    "turnover_ratio": row.turnover_ratio,
                    "rule_score": row.rule_score,
                    "weak_label": row.weak_label,
                    "manual_label": "",
                    "review_reason": "",
                    "reviewer_id": "",
                    "reviewed_at": "",
                }
            )
    return len(selected)


def validate_labeled_dataset(input_path: str | Path) -> ValidationSummary:
    """Validate that a reviewed CSV is complete, unique, and ready for evaluation."""

    source = Path(input_path)
    with source.open(newline="", encoding="utf-8-sig") as input_file:
        reader = csv.DictReader(input_file)
        missing_columns = [column for column in EXPORT_COLUMNS if column not in (reader.fieldnames or [])]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        rows = list(reader)

    if not rows:
        raise ValueError("Labeled dataset must contain at least one event")

    seen_event_ids: set[str] = set()
    positive_count = 0
    for row_number, row in enumerate(rows, start=2):
        event_id = row["event_id"].strip()
        if not event_id:
            raise ValueError(f"Row {row_number}: event_id is required")
        if event_id in seen_event_ids:
            raise ValueError(f"Row {row_number}: duplicate event_id {event_id}")
        seen_event_ids.add(event_id)

        label = row["manual_label"].strip()
        if label not in {"0", "1"}:
            raise ValueError(f"Row {row_number}: manual_label must be 0 or 1")
        if not row["review_reason"].strip():
            raise ValueError(f"Row {row_number}: review_reason is required")
        if not row["reviewer_id"].strip():
            raise ValueError(f"Row {row_number}: reviewer_id is required")
        if not row["reviewed_at"].strip():
            raise ValueError(f"Row {row_number}: reviewed_at is required")
        positive_count += int(label)

    return ValidationSummary(
        row_count=len(rows),
        positive_count=positive_count,
        negative_count=len(rows) - positive_count,
    )


def _candidate_sort_key(row) -> tuple:
    return (-row.rule_score, row.current_accession_number, row.holding_key)


def _event_id(current_accession_number: str, holding_key: str) -> str:
    return f"{current_accession_number}:{holding_key}"


def _interleave(positives: list, negatives: list, limit: int) -> list:
    selected = []
    positive_index = 0
    negative_index = 0
    while len(selected) < limit and (positive_index < len(positives) or negative_index < len(negatives)):
        if positive_index < len(positives):
            selected.append(positives[positive_index])
            positive_index += 1
            if len(selected) == limit:
                break
        if negative_index < len(negatives):
            selected.append(negatives[negative_index])
            negative_index += 1
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export or validate the WealthSignal manual gold dataset.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export candidate events for manual labeling.")
    export_parser.add_argument("--db-path", default="data/wealthsignal.db")
    export_parser.add_argument("--output", default="data/evaluation/materiality_gold.csv")
    export_parser.add_argument("--limit", type=int, default=300)

    validate_parser = subparsers.add_parser("validate", help="Validate a completed labeling CSV.")
    validate_parser.add_argument("--input", default="data/evaluation/materiality_gold.csv")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "export":
        count = export_labeling_candidates(args.db_path, args.output, limit=args.limit)
        print(f"Exported {count} candidate events to {args.output}")
        return 0

    summary = validate_labeled_dataset(args.input)
    print(
        f"Validated {summary.row_count} events: "
        f"{summary.positive_count} advisor-worthy, {summary.negative_count} routine"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
