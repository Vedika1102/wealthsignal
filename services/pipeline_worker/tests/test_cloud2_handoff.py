from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from wealthsignal_pipeline.cloud2_handoff import (
    build_cloud2_handoff_payload,
    validate_cloud2_handoff_reports,
)


class Cloud2HandoffTests(unittest.TestCase):
    def test_build_payload_creates_three_stage_databricks_handoff(self) -> None:
        payload = build_cloud2_handoff_payload(git_commit="abc123", manager_count=10)

        self.assertEqual(payload["run_name"], "wealthsignal-cloud2-10-manager-handoff")
        self.assertEqual(len(payload["tasks"]), 3)
        bronze, reference, reconcile = payload["tasks"]
        self.assertEqual(bronze["task_key"], "bronze_to_silver_10")
        self.assertEqual(reference["task_key"], "python_reference_10")
        self.assertEqual(reconcile["task_key"], "reconcile_10")
        self.assertEqual(
            reconcile["depends_on"],
            [{"task_key": "bronze_to_silver_10"}, {"task_key": "python_reference_10"}],
        )
        self.assertTrue(
            any(
                value.endswith("protocol-v2-python-reference-10.csv")
                for value in reference["spark_python_task"]["parameters"]
            )
        )

    def test_gate_report_authorizes_scale_up_only_when_reconciliation_passes(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            cloud2_path = root / "cloud2.json"
            reconciliation_path = root / "reconciliation.json"
            cloud2_path.write_text(
                json.dumps(
                    {
                        "inputs": {"manager_count": 10},
                        "acceptance": {
                            "distributed_pipeline_passed": True,
                            "frozen_window_passed": True,
                            "portfolio_reconciliation_passed": True,
                            "prospective_guard_passed": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            reconciliation_path.write_text(
                json.dumps({"manager_count": 10, "passed": True}),
                encoding="utf-8",
            )

            result = validate_cloud2_handoff_reports(cloud2_path, reconciliation_path)

            self.assertTrue(result["scale_up_authorized"])
            self.assertEqual(result["next_allowed_manager_counts"], [25, 50])
            self.assertEqual(result["blocking_reasons"], [])


if __name__ == "__main__":
    unittest.main()
