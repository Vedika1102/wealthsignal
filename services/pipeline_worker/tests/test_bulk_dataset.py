from __future__ import annotations

import csv
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from wealthsignal_pipeline.bulk_dataset import (
    BulkPackageSource,
    build_historical_dataset,
    download_quarter_packages,
    quarter_range,
    sec_bulk_url,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sec_13f_bulk_sample"


class BulkDatasetTests(unittest.TestCase):
    def test_quarter_range_and_url(self) -> None:
        self.assertEqual(quarter_range("2023q4", "2024q2"), ["2023q4", "2024q1", "2024q2"])
        self.assertEqual(
            sec_bulk_url("2024Q1"),
            "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2024q1_form13f.zip",
        )
        with self.assertRaises(ValueError):
            quarter_range("2024q2", "2024q1")

    def test_build_sample_dataset_resolves_amendments_and_units(self) -> None:
        with TemporaryDirectory() as temporary:
            source = BulkPackageSource(
                package="2024q2",
                path=FIXTURE_DIR,
                source_url="https://www.sec.gov/example/2024q2_form13f.zip",
                retrieved_at="2024-06-01T00:00:00+00:00",
            )
            output = build_historical_dataset([source], output_root=temporary)

            with (output / "effective_filings.csv").open(encoding="utf-8", newline="") as handle:
                filings = list(csv.DictReader(handle))
            with (output / "normalized_holdings.csv").open(encoding="utf-8", newline="") as handle:
                holdings = list(csv.DictReader(handle))
            report = json.loads((output / "quality_report.json").read_text(encoding="utf-8"))
            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(len(filings), 3)
            restatement = next(row for row in filings if row["cik"] == "0000000100")
            self.assertEqual(restatement["resolution"], "restatement_amendment")
            self.assertEqual(restatement["source_accession_numbers"], "0000000100-24-000002")
            self.assertIn("0000000100-24-000001", restatement["superseded_accession_numbers"])

            additive = next(row for row in filings if row["cik"] == "0000000200")
            self.assertEqual(additive["resolution"], "initial_plus_additive_amendment")
            additive_holdings = [row for row in holdings if row["cik"] == "0000000200"]
            self.assertEqual({row["cusip"] for row in additive_holdings}, {"037833100", "67066G104"})
            self.assertEqual(next(row for row in additive_holdings if row["cusip"] == "67066G104")["value_usd"], "450000")

            historical = next(row for row in holdings if row["cik"] == "0000000300")
            self.assertEqual(historical["value_usd"], "100000")
            self.assertEqual(report["invalid_rows_rejected"], 2)
            self.assertEqual(report["duplicates_resolved"], 1)
            self.assertEqual(report["amendments_resolved"], 2)
            self.assertEqual(report["identifier_coverage"], 1.0)
            self.assertEqual(manifest["dataset_id"], output.name.removeprefix("sec-13f-"))

            reused = build_historical_dataset([source], output_root=temporary)
            self.assertEqual(reused, output)

    def test_filters_manager_universe(self) -> None:
        with TemporaryDirectory() as temporary:
            source = BulkPackageSource("2024q2", FIXTURE_DIR, "https://www.sec.gov/example.zip")
            output = build_historical_dataset(
                [source],
                output_root=temporary,
                manager_ciks={"200"},
                security_cusips={"037833100"},
            )
            with (output / "normalized_holdings.csv").open(encoding="utf-8", newline="") as handle:
                holdings = list(csv.DictReader(handle))
            self.assertEqual(len(holdings), 1)
            self.assertEqual(holdings[0]["cik"], "0000000200")
            self.assertEqual(holdings[0]["cusip"], "037833100")

    def test_download_requires_contact_email_without_network(self) -> None:
        with TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                download_quarter_packages(
                    start="2024q1",
                    end="2024q1",
                    raw_dir=temporary,
                    user_agent="anonymous",
                )

    def test_existing_download_is_verified_and_reused(self) -> None:
        with TemporaryDirectory() as temporary:
            raw_dir = Path(temporary)
            target = raw_dir / "2024q1_form13f.zip"
            target.write_bytes(b"fixture")
            import hashlib

            metadata = {
                "sha256": hashlib.sha256(b"fixture").hexdigest(),
                "retrieved_at": "2024-01-01T00:00:00+00:00",
            }
            target.with_suffix(".download.json").write_text(json.dumps(metadata), encoding="utf-8")
            with patch("wealthsignal_pipeline.bulk_dataset.urlopen") as mocked:
                sources = download_quarter_packages(
                    start="2024q1",
                    end="2024q1",
                    raw_dir=raw_dir,
                    user_agent="WealthSignal test@example.com",
                )
            mocked.assert_not_called()
            self.assertEqual(sources[0].path, target)


if __name__ == "__main__":
    unittest.main()
