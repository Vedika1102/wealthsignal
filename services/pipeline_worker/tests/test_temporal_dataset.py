from __future__ import annotations

import copy
import csv
import hashlib
import json
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from wealthsignal_pipeline.temporal_dataset import (
    audit_temporal_dataset,
    build_expanding_window_manifest,
    build_temporal_dataset,
)


class TemporalDatasetTests(unittest.TestCase):
    def test_builds_objective_targets_and_expanding_window_folds(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self._write_normalized_dataset(root / "source")
            output = build_temporal_dataset(
                [source],
                output_root=root / "output",
                negative_candidate_limit=2,
                minimum_train_target_quarters=2,
                final_test_quarters=1,
            )

            with (output / "manager_security_quarter.csv").open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            splits = json.loads((output / "split_manifest.json").read_text(encoding="utf-8"))
            leakage = json.loads((output / "leakage_report.json").read_text(encoding="utf-8"))
            quality = json.loads((output / "quality_report.json").read_text(encoding="utf-8"))

            new_position = next(
                row
                for row in rows
                if row["cik"] == "0000000100"
                and row["feature_report_period"] == "2024-06-30"
                and row["cusip"] == "333333333"
            )
            self.assertEqual(new_position["target_report_period"], "2024-09-30")
            self.assertEqual(new_position["target_action"], "new")
            self.assertEqual(new_position["target_is_new"], "1")
            self.assertGreater(float(new_position["target_weight"]), 0.0)
            self.assertLessEqual(new_position["security_first_seen_available_at"], new_position["feature_available_at"])

            self.assertEqual(splits["status"], "ready")
            self.assertEqual([fold["role"] for fold in splits["folds"]], ["validation", "test"])
            self.assertLess(
                max(splits["folds"][0]["train_target_quarters"]),
                splits["folds"][0]["evaluation_target_quarter"],
            )
            self.assertEqual(leakage["status"], "PASS")
            self.assertEqual(leakage["issue_count"], 0)
            self.assertEqual(quality["eligible_manager_quarters"], 8)
            self.assertGreater(quality["target_candidate_coverage"], 0.8)

            reused = build_temporal_dataset(
                [source],
                output_root=root / "output",
                negative_candidate_limit=2,
                minimum_train_target_quarters=2,
                final_test_quarters=1,
            )
            self.assertEqual(reused, output)

    def test_leakage_audit_rejects_future_candidate_and_invalid_horizon(self) -> None:
        row = self._audit_row()
        leaked = copy.deepcopy(row)
        leaked["security_first_seen_available_at"] = "2024-06-01"
        leaked["target_report_period"] = "2024-12-31"
        manifest = build_expanding_window_manifest(
            [leaked], minimum_train_target_quarters=1, final_test_quarters=1
        )
        issues = audit_temporal_dataset([leaked], manifest)
        self.assertIn("future_candidate", {issue.code for issue in issues})
        self.assertIn("invalid_target_horizon", {issue.code for issue in issues})

    def test_leakage_audit_rejects_split_overlap(self) -> None:
        row = self._audit_row()
        manifest = {
            "folds": [
                {
                    "fold_id": "fold-leaked",
                    "evaluation_target_quarter": "2024-06-30",
                    "train_example_ids": [row["example_id"]],
                    "evaluation_example_ids": [row["example_id"]],
                }
            ]
        }
        issues = audit_temporal_dataset([row], manifest)
        self.assertIn("split_example_overlap", {issue.code for issue in issues})
        self.assertIn("split_manager_quarter_overlap", {issue.code for issue in issues})
        self.assertIn("train_boundary_violation", {issue.code for issue in issues})

    @staticmethod
    def _audit_row() -> dict[str, object]:
        return {
            "example_id": "example-1",
            "cik": "0000000100",
            "feature_report_period": "2024-03-31",
            "feature_available_at": "2024-05-15",
            "target_report_period": "2024-06-30",
            "target_available_at": "2024-08-15",
            "security_first_seen_period": "2024-03-31",
            "security_first_seen_available_at": "2024-05-10",
        }

    @staticmethod
    def _write_normalized_dataset(path: Path) -> Path:
        path.mkdir(parents=True)
        fieldnames = [
            "holding_id",
            "cik",
            "report_period",
            "filing_date",
            "effective_accession_number",
            "security_key",
            "issuer_name",
            "cusip",
            "value_usd",
            "portfolio_weight",
            "holding_rank",
        ]
        rows: list[dict[str, object]] = []
        quarters = [
            (date(2024, 3, 31), date(2024, 5, 15)),
            (date(2024, 6, 30), date(2024, 8, 15)),
            (date(2024, 9, 30), date(2024, 11, 15)),
            (date(2024, 12, 31), date(2025, 2, 15)),
            (date(2025, 3, 31), date(2025, 5, 15)),
        ]
        manager_holdings = {
            "0000000100": [
                [("111111111", 0.7), ("222222222", 0.3)],
                [("111111111", 0.6), ("222222222", 0.4)],
                [("111111111", 0.5), ("222222222", 0.3), ("333333333", 0.2)],
                [("111111111", 0.45), ("333333333", 0.35), ("222222222", 0.2)],
                [("111111111", 0.4), ("333333333", 0.4), ("222222222", 0.2)],
            ],
            "0000000200": [
                [("333333333", 0.8), ("222222222", 0.2)],
                [("333333333", 0.7), ("222222222", 0.3)],
                [("333333333", 0.6), ("222222222", 0.4)],
                [("333333333", 0.55), ("222222222", 0.45)],
                [("333333333", 0.5), ("222222222", 0.5)],
            ],
        }
        for cik, histories in manager_holdings.items():
            for quarter_index, ((report_period, filing_date), holdings) in enumerate(zip(quarters, histories)):
                peer_filing_date = filing_date if cik == "0000000100" else filing_date.replace(day=10)
                for rank, (cusip, weight) in enumerate(sorted(holdings, key=lambda value: -value[1]), start=1):
                    security_key = f"{cusip}|COM||SH|SOLE|"
                    rows.append(
                        {
                            "holding_id": hashlib.sha256(
                                f"{cik}|{report_period.isoformat()}|{security_key}".encode()
                            ).hexdigest()[:24],
                            "cik": cik,
                            "report_period": report_period.isoformat(),
                            "filing_date": peer_filing_date.isoformat(),
                            "effective_accession_number": f"{cik}-{quarter_index}",
                            "security_key": security_key,
                            "issuer_name": f"ISSUER {cusip}",
                            "cusip": cusip,
                            "value_usd": int(weight * 1_000_000),
                            "portfolio_weight": weight,
                            "holding_rank": rank,
                        }
                    )
        holdings_path = path / "normalized_holdings.csv"
        with holdings_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        checksum = hashlib.sha256(holdings_path.read_bytes()).hexdigest()
        effective_path = path / "effective_filings.csv"
        with effective_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["cik", "report_period", "source_accession_numbers"])
            writer.writeheader()
            for cik, histories in manager_holdings.items():
                for quarter_index, ((report_period, _), _) in enumerate(zip(quarters, histories)):
                    writer.writerow({"cik": cik, "report_period": report_period.isoformat(), "source_accession_numbers": f"{cik}-{quarter_index}"})
        manifest = {
            "dataset_id": "synthetic-temporal-fixture",
            "outputs": {
                "normalized_holdings.csv": {"sha256": checksum},
                "effective_filings.csv": {"sha256": hashlib.sha256(effective_path.read_bytes()).hexdigest()},
            },
            "sources": [{"package": "fixture", "source_url": "fixture://13f", "sha256": "fixture", "size_bytes": 1}],
        }
        (path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return path


if __name__ == "__main__":
    unittest.main()
