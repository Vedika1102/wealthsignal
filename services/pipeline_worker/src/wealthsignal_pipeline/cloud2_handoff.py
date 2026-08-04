"""Build and validate the Databricks Cloud 2/3 handoff contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_GIT_URL = "https://github.com/Vedika1102/wealthsignal.git"
DEFAULT_GIT_PROVIDER = "gitHub"
DEFAULT_SOURCE_VOLUME = "/Volumes/workspace/bronze/sec_13f_development"
DEFAULT_REPORT_ROOT = "/Volumes/workspace/silver/cloud2_reports"
DEFAULT_COHORT_PATH = "docs/ai-governance/forecast-protocol-v2-manager-cohort.json"


def build_cloud2_handoff_payload(
    *,
    git_commit: str,
    manager_count: int = 10,
    git_url: str = DEFAULT_GIT_URL,
    git_provider: str = DEFAULT_GIT_PROVIDER,
    source_volume: str = DEFAULT_SOURCE_VOLUME,
    report_root: str = DEFAULT_REPORT_ROOT,
    cohort_path: str = DEFAULT_COHORT_PATH,
) -> dict[str, object]:
    """Return the exact multi-task Databricks payload for a Cloud 2/3 handoff."""

    if manager_count not in {10, 25, 50}:
        raise ValueError("manager_count must be one of 10, 25, or 50")
    reference_path = f"{report_root}/protocol-v2-python-reference-{manager_count}.csv"
    task_suffix = str(manager_count)
    return {
        "run_name": f"wealthsignal-cloud2-{task_suffix}-manager-handoff",
        "timeout_seconds": 14400,
        "git_source": {
            "git_url": git_url,
            "git_provider": git_provider,
            "git_commit": git_commit,
        },
        "tasks": [
            {
                "task_key": f"bronze_to_silver_{task_suffix}",
                "spark_python_task": {
                    "python_file": "services/pipeline_worker/src/wealthsignal_pipeline/cloud2_spark.py",
                    "parameters": ["--manager-count", task_suffix],
                    "source": "GIT",
                },
                "environment_key": "default",
            },
            {
                "task_key": f"python_reference_{task_suffix}",
                "spark_python_task": {
                    "python_file": "services/pipeline_worker/src/wealthsignal_pipeline/v2_reference.py",
                    "parameters": [
                        "--source-dir",
                        source_volume,
                        "--cohort",
                        cohort_path,
                        "--output",
                        reference_path,
                        "--manager-count",
                        task_suffix,
                    ],
                    "source": "GIT",
                },
                "environment_key": "default",
            },
            {
                "task_key": f"reconcile_{task_suffix}",
                "depends_on": [
                    {"task_key": f"bronze_to_silver_{task_suffix}"},
                    {"task_key": f"python_reference_{task_suffix}"},
                ],
                "spark_python_task": {
                    "python_file": "services/pipeline_worker/src/wealthsignal_pipeline/cloud2_reconcile.py",
                    "parameters": [
                        "--manager-count",
                        task_suffix,
                        "--reference-path",
                        reference_path,
                    ],
                    "source": "GIT",
                },
                "environment_key": "default",
            },
        ],
        "environments": [{"environment_key": "default", "spec": {"client": "2"}}],
    }


def validate_cloud2_handoff_reports(
    cloud2_report_path: str | Path,
    reconciliation_report_path: str | Path,
) -> dict[str, object]:
    """Validate the acceptance gate for scaling beyond the current Cloud 2 run."""

    cloud2 = json.loads(Path(cloud2_report_path).read_text(encoding="utf-8"))
    reconciliation = json.loads(Path(reconciliation_report_path).read_text(encoding="utf-8"))
    manager_count = int(cloud2["inputs"]["manager_count"])
    if manager_count != int(reconciliation["manager_count"]):
        raise ValueError("manager_count mismatch between Cloud 2 and reconciliation reports")

    required_cloud2_flags = {
        "distributed_pipeline_passed": bool(cloud2["acceptance"]["distributed_pipeline_passed"]),
        "frozen_window_passed": bool(cloud2["acceptance"]["frozen_window_passed"]),
        "portfolio_reconciliation_passed": bool(cloud2["acceptance"]["portfolio_reconciliation_passed"]),
        "prospective_guard_passed": bool(cloud2["acceptance"]["prospective_guard_passed"]),
    }
    reconciliation_passed = bool(reconciliation["passed"])
    blocking_reasons = [
        key for key, value in required_cloud2_flags.items() if not value
    ]
    if not reconciliation_passed:
        blocking_reasons.append("python_reference_reconciliation_failed")

    return {
        "cloud_milestone": "Cloud 2/3 handoff",
        "manager_count": manager_count,
        "cloud2_report_path": str(cloud2_report_path),
        "reconciliation_report_path": str(reconciliation_report_path),
        "required_cloud2_flags": required_cloud2_flags,
        "python_reference_reconciliation_passed": reconciliation_passed,
        "scale_up_authorized": not blocking_reasons,
        "next_allowed_manager_counts": [25, 50] if not blocking_reasons and manager_count == 10 else [],
        "blocking_reasons": blocking_reasons,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    payload = subparsers.add_parser("payload", help="Build the Cloud 2/3 Databricks submit payload.")
    payload.add_argument("--git-commit", required=True)
    payload.add_argument("--manager-count", type=int, default=10, choices=(10, 25, 50))
    payload.add_argument("--git-url", default=DEFAULT_GIT_URL)
    payload.add_argument("--git-provider", default=DEFAULT_GIT_PROVIDER)
    payload.add_argument("--source-volume", default=DEFAULT_SOURCE_VOLUME)
    payload.add_argument("--report-root", default=DEFAULT_REPORT_ROOT)
    payload.add_argument("--cohort-path", default=DEFAULT_COHORT_PATH)
    payload.add_argument("--output")

    gate = subparsers.add_parser("gate-report", help="Validate Cloud 2 plus reconciliation reports.")
    gate.add_argument("--cloud2-report", required=True)
    gate.add_argument("--reconciliation-report", required=True)
    gate.add_argument("--output")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "payload":
        result = build_cloud2_handoff_payload(
            git_commit=args.git_commit,
            manager_count=args.manager_count,
            git_url=args.git_url,
            git_provider=args.git_provider,
            source_volume=args.source_volume,
            report_root=args.report_root,
            cohort_path=args.cohort_path,
        )
    else:
        result = validate_cloud2_handoff_reports(
            args.cloud2_report,
            args.reconciliation_report,
        )

    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
