from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from wealthsignal_pipeline.v2_candidate_caps import (
    select_candidate_cap,
    summarize_candidate_cap_study,
)


class ProtocolV2CandidateCapTests(unittest.TestCase):
    def test_selects_smallest_cap_within_protocol_tolerance(self) -> None:
        summary = select_candidate_cap(
            [
                {"negative_candidate_limit": 100, "mean_target_weight_mass_coverage": 0.7930, "dataset_id": "a"},
                {"negative_candidate_limit": 250, "mean_target_weight_mass_coverage": 0.7949, "dataset_id": "b"},
                {"negative_candidate_limit": 500, "mean_target_weight_mass_coverage": 0.7952, "dataset_id": "c"},
            ]
        )

        self.assertEqual(summary["selected_negative_candidate_limit"], 100)
        self.assertEqual(summary["selected_dataset_id"], "a")

    def test_summarizes_saved_reference_datasets(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self._write_dataset(root / "cap-100", dataset_id="cap100", cap=100, coverage=0.80)
            second = self._write_dataset(root / "cap-250", dataset_id="cap250", cap=250, coverage=0.805)
            summary = summarize_candidate_cap_study([first, second])

            self.assertEqual(summary["selected_negative_candidate_limit"], 250)
            self.assertEqual(summary["selected_dataset_id"], "cap250")

    @staticmethod
    def _write_dataset(path: Path, *, dataset_id: str, cap: int, coverage: float) -> Path:
        path.mkdir(parents=True)
        (path / "manifest.json").write_text(
            json.dumps(
                {
                    "dataset_id": dataset_id,
                    "identity": {"negative_candidate_limit": cap},
                }
            ),
            encoding="utf-8",
        )
        (path / "quality_report.json").write_text(
            json.dumps({"mean_target_weight_mass_coverage": coverage}),
            encoding="utf-8",
        )
        return path


if __name__ == "__main__":
    unittest.main()
