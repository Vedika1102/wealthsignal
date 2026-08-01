from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .persistence import connect, get_latest_prediction_lookup, initialize_database, load_feature_rows


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


def evaluate_labeled_dataset(
    input_path: str | Path,
    output_path: str | Path,
    *,
    db_path: str | Path | None = None,
    rule_threshold: int = 40,
) -> dict[str, object]:
    """Evaluate rules and available stored predictions against manual gold labels."""

    validation = validate_labeled_dataset(input_path)
    source = Path(input_path)
    with source.open(newline="", encoding="utf-8-sig") as input_file:
        rows = list(csv.DictReader(input_file))

    labels = [int(row["manual_label"]) for row in rows]
    rule_scores = [float(row["rule_score"]) / 100.0 for row in rows]
    rule_predictions = [int(float(row["rule_score"]) >= rule_threshold) for row in rows]
    report: dict[str, object] = {
        "dataset": {
            "version_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "path": str(source),
            "row_count": validation.row_count,
            "positive_count": validation.positive_count,
            "negative_count": validation.negative_count,
        },
        "generated_at": datetime.now(UTC).isoformat(),
        "rule_engine": {
            "threshold": rule_threshold,
            "metrics": _binary_metrics(labels, rule_predictions, rule_scores),
        },
        "slices": {
            "sector": _slice_metrics(rows, labels, rule_predictions, rule_scores, "sector"),
            "event_type": _event_type_slices(rows, labels, rule_predictions, rule_scores),
        },
    }

    if db_path is not None:
        connection = connect(db_path)
        try:
            initialize_database(connection)
            prediction_lookup = get_latest_prediction_lookup(connection)
        finally:
            connection.close()

        matched_rows = []
        model_labels = []
        model_predictions = []
        model_scores = []
        for row, label in zip(rows, labels):
            prediction = prediction_lookup.get((row["current_accession_number"], row["holding_key"]))
            if prediction is None:
                continue
            matched_rows.append(row)
            model_labels.append(label)
            model_predictions.append(prediction.predicted_label)
            model_scores.append(prediction.probability)
        report["stored_model"] = {
            "status": "diagnostic_in_sample",
            "warning": (
                "Stored predictions may come from models trained on weak labels for these same events; "
                "exclude gold-set event IDs from training before treating these metrics as holdout results."
            ),
            "coverage": len(matched_rows) / len(rows),
            "matched_events": len(matched_rows),
            "metrics": (
                _binary_metrics(model_labels, model_predictions, model_scores) if matched_rows else None
            ),
        }

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


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


def _binary_metrics(labels: list[int], predictions: list[int], scores: list[float]) -> dict[str, float | int]:
    true_positive = sum(label == 1 and prediction == 1 for label, prediction in zip(labels, predictions))
    true_negative = sum(label == 0 and prediction == 0 for label, prediction in zip(labels, predictions))
    false_positive = sum(label == 0 and prediction == 1 for label, prediction in zip(labels, predictions))
    false_negative = sum(label == 1 and prediction == 0 for label, prediction in zip(labels, predictions))
    count = len(labels)
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
    return {
        "sample_count": count,
        "true_positive": true_positive,
        "true_negative": true_negative,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "accuracy": (true_positive + true_negative) / count if count else 0.0,
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
        "pr_auc": _average_precision(labels, scores),
        "brier_score": sum((score - label) ** 2 for label, score in zip(labels, scores)) / count if count else 0.0,
        "precision_at_top_10_percent": _precision_at_fraction(labels, scores, 0.1),
    }


def _average_precision(labels: list[int], scores: list[float]) -> float:
    positive_count = sum(labels)
    if positive_count == 0:
        return 0.0
    ranked = sorted(zip(scores, labels), key=lambda item: item[0], reverse=True)
    true_positive = 0
    precision_sum = 0.0
    for rank, (_, label) in enumerate(ranked, start=1):
        if label == 1:
            true_positive += 1
            precision_sum += true_positive / rank
    return precision_sum / positive_count


def _precision_at_fraction(labels: list[int], scores: list[float], fraction: float) -> float:
    if not labels:
        return 0.0
    count = max(1, round(len(labels) * fraction))
    ranked_labels = [label for _, label in sorted(zip(scores, labels), reverse=True)[:count]]
    return sum(ranked_labels) / count


def _slice_metrics(
    rows: list[dict[str, str]],
    labels: list[int],
    predictions: list[int],
    scores: list[float],
    column: str,
) -> dict[str, dict[str, float | int]]:
    groups: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
        groups.setdefault(row[column] or "Unknown", []).append(index)
    return {
        group: _binary_metrics(
            [labels[index] for index in indices],
            [predictions[index] for index in indices],
            [scores[index] for index in indices],
        )
        for group, indices in sorted(groups.items())
    }


def _event_type_slices(
    rows: list[dict[str, str]],
    labels: list[int],
    predictions: list[int],
    scores: list[float],
) -> dict[str, dict[str, float | int]]:
    typed_rows = []
    for row in rows:
        if row["is_new_position"] == "1":
            event_type = "new_position"
        elif row["is_exited_position"] == "1":
            event_type = "exited_position"
        elif float(row["weight_delta"]) > 0:
            event_type = "increased_position"
        else:
            event_type = "decreased_position"
        typed_rows.append({**row, "event_type": event_type})
    return _slice_metrics(typed_rows, labels, predictions, scores, "event_type")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export or validate the WealthSignal manual gold dataset.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export candidate events for manual labeling.")
    export_parser.add_argument("--db-path", default="data/wealthsignal.db")
    export_parser.add_argument("--output", default="data/evaluation/materiality_gold.csv")
    export_parser.add_argument("--limit", type=int, default=300)

    validate_parser = subparsers.add_parser("validate", help="Validate a completed labeling CSV.")
    validate_parser.add_argument("--input", default="data/evaluation/materiality_gold.csv")

    evaluate_parser = subparsers.add_parser("evaluate", help="Evaluate rules and stored predictions.")
    evaluate_parser.add_argument("--input", default="data/evaluation/materiality_gold.csv")
    evaluate_parser.add_argument("--output", default="data/evaluation/materiality_evaluation.json")
    evaluate_parser.add_argument("--db-path")
    evaluate_parser.add_argument("--rule-threshold", type=int, default=40)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "export":
        count = export_labeling_candidates(args.db_path, args.output, limit=args.limit)
        print(f"Exported {count} candidate events to {args.output}")
        return 0

    if args.command == "validate":
        summary = validate_labeled_dataset(args.input)
        print(
            f"Validated {summary.row_count} events: "
            f"{summary.positive_count} advisor-worthy, {summary.negative_count} routine"
        )
        return 0

    report = evaluate_labeled_dataset(
        args.input,
        args.output,
        db_path=args.db_path,
        rule_threshold=args.rule_threshold,
    )
    print(f"Wrote evaluation report for {report['dataset']['row_count']} events to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
