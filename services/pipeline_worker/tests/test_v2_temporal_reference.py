from __future__ import annotations

import csv
import json
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from wealthsignal_pipeline.bulk_dataset import BulkPackageSource
from wealthsignal_pipeline.v2_temporal_reference import (
    build_protocol_v2_temporal_reference,
    build_v2_split_manifest,
)


class ProtocolV2TemporalReferenceTests(unittest.TestCase):
    def test_split_manifest_uses_quarter_predicates_instead_of_example_ids(self) -> None:
        rows = [
            {"example_id": "a", "target_report_period": "2024-06-30"},
            {"example_id": "b", "target_report_period": "2024-09-30"},
            {"example_id": "c", "target_report_period": "2024-12-31"},
        ]
        manifest = build_v2_split_manifest(
            rows,
            validation_start=date(2024, 9, 30),
            validation_end=date(2024, 12, 31),
        )

        self.assertEqual(manifest["status"], "ready")
        self.assertEqual(
            [fold["evaluation_target_quarter"] for fold in manifest["folds"]],
            ["2024-09-30", "2024-12-31"],
        )
        self.assertNotIn("train_example_ids", manifest["folds"][0])
        self.assertEqual(manifest["folds"][0]["train_target_quarters"], ["2024-06-30"])

    def test_builds_leakage_audited_v2_temporal_reference(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_dir = self._write_source_dir(root / "fixture")
            output = build_protocol_v2_temporal_reference(
                [BulkPackageSource("fixture", source_dir, "fixture://sec-13f")],
                ["0000000100", "0000000200"],
                output_root=root / "output",
                negative_candidate_limit=2,
                validation_start=date(2024, 9, 30),
                validation_end=date(2024, 12, 31),
                chunk_size=1,
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
            self.assertEqual(leakage["status"], "PASS")
            self.assertEqual(leakage["issue_count"], 0)
            self.assertEqual(
                [fold["evaluation_target_quarter"] for fold in splits["folds"]],
                ["2024-09-30", "2024-12-31"],
            )
            self.assertEqual(quality["validation_folds"], 2)
            self.assertEqual(quality["manager_count"], 2)
            self.assertGreater(quality["target_candidate_coverage"], 0.8)
            self.assertGreater(quality["mean_target_weight_mass_coverage"], 0.8)

    @staticmethod
    def _write_source_dir(path: Path) -> Path:
        path.mkdir(parents=True)
        submission_fields = ["ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"]
        cover_fields = [
            "ACCESSION_NUMBER",
            "FILINGMANAGER_NAME",
            "ISAMENDMENT",
            "AMENDMENTNO",
            "AMENDMENTTYPE",
        ]
        info_fields = [
            "ACCESSION_NUMBER",
            "INFOTABLE_SK",
            "NAMEOFISSUER",
            "TITLEOFCLASS",
            "CUSIP",
            "VALUE",
            "SSHPRNAMT",
            "SSHPRNAMTTYPE",
            "PUTCALL",
            "INVESTMENTDISCRETION",
            "OTHERMANAGER",
            "VOTING_AUTH_SOLE",
            "VOTING_AUTH_SHARED",
            "VOTING_AUTH_NONE",
        ]
        quarters = [
            (date(2024, 3, 31), date(2024, 5, 15)),
            (date(2024, 6, 30), date(2024, 8, 15)),
            (date(2024, 9, 30), date(2024, 11, 15)),
            (date(2024, 12, 31), date(2025, 2, 15)),
            (date(2025, 3, 31), date(2025, 5, 15)),
        ]
        histories = {
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

        submission_rows: list[dict[str, object]] = []
        cover_rows: list[dict[str, object]] = []
        info_rows: list[dict[str, object]] = []
        for cik, manager_history in histories.items():
            for quarter_index, ((report_period, filing_date), holdings) in enumerate(zip(quarters, manager_history)):
                accession = f"{cik}-{quarter_index:04d}"
                submission_rows.append(
                    {
                        "ACCESSION_NUMBER": accession,
                        "FILING_DATE": filing_date.isoformat(),
                        "SUBMISSIONTYPE": "13F-HR",
                        "CIK": cik,
                        "PERIODOFREPORT": report_period.isoformat(),
                    }
                )
                cover_rows.append(
                    {
                        "ACCESSION_NUMBER": accession,
                        "FILINGMANAGER_NAME": f"Manager {cik}",
                        "ISAMENDMENT": "",
                        "AMENDMENTNO": "",
                        "AMENDMENTTYPE": "",
                    }
                )
                portfolio_value = 1_000_000
                for row_index, (cusip, weight) in enumerate(holdings, start=1):
                    info_rows.append(
                        {
                            "ACCESSION_NUMBER": accession,
                            "INFOTABLE_SK": str(row_index),
                            "NAMEOFISSUER": f"Issuer {cusip}",
                            "TITLEOFCLASS": "COM",
                            "CUSIP": cusip,
                            "VALUE": str(int(portfolio_value * weight)),
                            "SSHPRNAMT": str(int(portfolio_value * weight / 10)),
                            "SSHPRNAMTTYPE": "SH",
                            "PUTCALL": "",
                            "INVESTMENTDISCRETION": "SOLE",
                            "OTHERMANAGER": "",
                            "VOTING_AUTH_SOLE": "0",
                            "VOTING_AUTH_SHARED": "0",
                            "VOTING_AUTH_NONE": "0",
                        }
                    )

        ProtocolV2TemporalReferenceTests._write_tsv(path / "SUBMISSION.tsv", submission_fields, submission_rows)
        ProtocolV2TemporalReferenceTests._write_tsv(path / "COVERPAGE.tsv", cover_fields, cover_rows)
        ProtocolV2TemporalReferenceTests._write_tsv(path / "INFOTABLE.tsv", info_fields, info_rows)
        return path

    @staticmethod
    def _write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
