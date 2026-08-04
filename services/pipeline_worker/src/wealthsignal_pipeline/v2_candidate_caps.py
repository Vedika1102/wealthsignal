from __future__ import annotations

import argparse
import json
from pathlib import Path


def select_candidate_cap(
    candidate_reports: list[dict[str, object]],
    *,
    tolerance_percentage_points: float = 0.25,
) -> dict[str, object]:
    if not candidate_reports:
        raise ValueError("At least one candidate report is required")
    if tolerance_percentage_points < 0:
        raise ValueError("tolerance_percentage_points must be non-negative")

    normalized = sorted(
        (
            {
                "negative_candidate_limit": int(report["negative_candidate_limit"]),
                "mean_target_weight_mass_coverage": float(report["mean_target_weight_mass_coverage"]),
                "dataset_id": str(report.get("dataset_id", "")) or None,
            }
            for report in candidate_reports
        ),
        key=lambda item: item["negative_candidate_limit"],
    )
    best_coverage = max(item["mean_target_weight_mass_coverage"] for item in normalized)
    tolerance = tolerance_percentage_points / 100.0
    eligible = [
        item
        for item in normalized
        if best_coverage - item["mean_target_weight_mass_coverage"] <= tolerance
    ]
    selected = min(eligible, key=lambda item: item["negative_candidate_limit"])
    return {
        "candidate_reports": normalized,
        "best_mean_target_weight_mass_coverage": best_coverage,
        "tolerance_percentage_points": tolerance_percentage_points,
        "selected_negative_candidate_limit": selected["negative_candidate_limit"],
        "selected_dataset_id": selected["dataset_id"],
    }


def summarize_candidate_cap_study(dataset_dirs: list[str | Path]) -> dict[str, object]:
    reports = []
    for dataset_dir in dataset_dirs:
        root = Path(dataset_dir)
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        quality = json.loads((root / "quality_report.json").read_text(encoding="utf-8"))
        reports.append(
            {
                "dataset_id": manifest["dataset_id"],
                "negative_candidate_limit": manifest["identity"]["negative_candidate_limit"],
                "mean_target_weight_mass_coverage": quality["mean_target_weight_mass_coverage"],
            }
        )
    return select_candidate_cap(reports)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Select the frozen Protocol V2 negative candidate cap from saved reference datasets."
    )
    parser.add_argument("--dataset-dir", action="append", required=True, help="Protocol V2 reference dataset directory; repeatable.")
    parser.add_argument("--output")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = summarize_candidate_cap_study(args.dataset_dir)
    if args.output:
        Path(args.output).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
