import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile

from wealthsignal_pipeline.delta_engine import compute_filing_delta
from wealthsignal_pipeline.feature_engineering import build_position_features
from wealthsignal_pipeline.edgar_client import (
    discover_filing_artifacts,
    filing_index_url,
    normalize_cik,
    recent_filings_from_submissions,
    select_information_table_filename,
    submissions_url,
)
from wealthsignal_pipeline.materiality import score_materiality_batch
from wealthsignal_pipeline.portfolios import generate_synthetic_client_portfolios, score_client_impacts
from wealthsignal_pipeline.persistence import connect, initialize_database, load_latest_filing_accessions, load_parsed_filing, store_filing_delta, store_parsed_filing
from wealthsignal_pipeline.parser import parse_information_table
from wealthsignal_pipeline.parser import parse_primary_document_metadata


SAMPLE_INFORMATION_TABLE = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>125000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>500000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <otherManager></otherManager>
    <votingAuthority>
      <Sole>500000</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>NVIDIA CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>67066G104</cusip>
    <value>98000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>250000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <putCall>CALL</putCall>
    <investmentDiscretion>SHARED</investmentDiscretion>
    <votingAuthority>
      <Sole>0</Sole>
      <Shared>250000</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
</informationTable>
"""

PREVIOUS_INFORMATION_TABLE = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>100000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>400000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority>
      <Sole>400000</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>CHEVRON CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>166764100</cusip>
    <value>50000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>200000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority>
      <Sole>200000</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
</informationTable>
"""

SAMPLE_SUBMISSIONS = {
    "cik": "1067983",
    "filings": {
        "recent": {
            "accessionNumber": [
                "0001193125-26-226661",
                "0001193125-26-100000",
                "0001193125-25-226661",
            ],
            "filingDate": ["2026-05-15", "2026-03-01", "2025-11-14"],
            "reportDate": ["2026-03-31", "2026-02-15", "2025-09-30"],
            "form": ["13F-HR", "10-K", "13F-HR/A"],
            "primaryDocument": ["xslForm13F_X02/primary_doc.xml", "brka10k.htm", "primary_doc.xml"],
            "primaryDocDescription": ["", "", ""],
        }
    },
}

SAMPLE_INDEX_JSON = {
    "directory": {
        "item": [
            {"name": "0001193125-26-226661.txt", "size": ""},
            {"name": "53405.xml", "size": "45259"},
            {"name": "primary_doc.xml", "size": "5555"},
        ]
    }
}

SAMPLE_PRIMARY_DOCUMENT = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler" xmlns:ns1="http://www.sec.gov/edgar/common">
  <headerData>
    <submissionType>13F-HR</submissionType>
    <filerInfo>
      <filer>
        <credentials>
          <cik>0001067983</cik>
        </credentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>03-31-2026</reportCalendarOrQuarter>
      <filingManager>
        <name>Berkshire Hathaway Inc</name>
      </filingManager>
    </coverPage>
  </formData>
</edgarSubmission>
"""


class ParseInformationTableTests(unittest.TestCase):
    def test_parse_information_table_extracts_holdings(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000001",
            filer_name="Example Capital Management",
        )

        self.assertEqual(parsed.filing.cik, "0001067983")
        self.assertEqual(parsed.filing.accession_number, "0001067983-24-000001")
        self.assertEqual(len(parsed.holdings), 2)

        apple = parsed.holdings[0]
        self.assertEqual(apple.issuer_name, "APPLE INC")
        self.assertEqual(apple.cusip, "037833100")
        self.assertEqual(apple.value_thousands, 125000)
        self.assertEqual(apple.market_value_usd, 125000000)
        self.assertEqual(apple.shares_or_principal, 500000)
        self.assertEqual(apple.put_call, "")

    def test_parse_information_table_handles_optional_tags(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000001",
        )

        nvidia = parsed.holdings[1]
        self.assertEqual(nvidia.issuer_name, "NVIDIA CORP")
        self.assertEqual(nvidia.put_call, "CALL")
        self.assertEqual(nvidia.investment_discretion, "SHARED")
        self.assertEqual(nvidia.voting_authority_shared, 250000)

    def test_parse_primary_document_metadata_extracts_cover_page_fields(self) -> None:
        metadata = parse_primary_document_metadata(SAMPLE_PRIMARY_DOCUMENT)
        self.assertEqual(metadata.cik, "0001067983")
        self.assertEqual(metadata.form_type, "13F-HR")
        self.assertEqual(str(metadata.report_period), "2026-03-31")
        self.assertEqual(metadata.filer_name, "Berkshire Hathaway Inc")


class EdgarClientUtilityTests(unittest.TestCase):
    def test_normalize_cik_zero_pads_to_ten_digits(self) -> None:
        self.assertEqual(normalize_cik("1067983"), "0001067983")

    def test_submissions_url_uses_zero_padded_cik(self) -> None:
        self.assertEqual(
            submissions_url("1067983"),
            "https://data.sec.gov/submissions/CIK0001067983.json",
        )

    def test_filing_index_url_removes_accession_hyphens(self) -> None:
        self.assertEqual(
            filing_index_url("1067983", "0001067983-24-000001"),
            "https://www.sec.gov/Archives/edgar/data/1067983/000106798324000001",
        )

    def test_recent_filings_extracts_only_13f_rows(self) -> None:
        filings = recent_filings_from_submissions(SAMPLE_SUBMISSIONS)
        self.assertEqual(len(filings), 2)
        self.assertEqual(filings[0].accession_number, "0001193125-26-226661")
        self.assertEqual(filings[0].cik, "0001067983")
        self.assertEqual(filings[1].form_type, "13F-HR/A")

    def test_select_information_table_filename_prefers_non_primary_xml(self) -> None:
        self.assertEqual(
            select_information_table_filename(SAMPLE_INDEX_JSON, "xslForm13F_X02/primary_doc.xml"),
            "53405.xml",
        )

    def test_discover_filing_artifacts_builds_information_table_url(self) -> None:
        filing = recent_filings_from_submissions(SAMPLE_SUBMISSIONS)[0]
        artifacts = discover_filing_artifacts(filing, SAMPLE_INDEX_JSON)

        self.assertEqual(
            artifacts.primary_document_url,
            "https://www.sec.gov/Archives/edgar/data/1067983/000119312526226661/primary_doc.xml",
        )
        self.assertEqual(
            artifacts.information_table_url,
            "https://www.sec.gov/Archives/edgar/data/1067983/000119312526226661/53405.xml",
        )


class DeltaEngineTests(unittest.TestCase):
    def test_compute_filing_delta_flags_new_and_exited_positions(self) -> None:
        current = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )
        previous = parse_information_table(
            PREVIOUS_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-23-000999",
        )

        delta = compute_filing_delta(current, previous)
        positions = {position.cusip: position for position in delta.positions}

        apple = positions["037833100"]
        self.assertFalse(apple.is_new_position)
        self.assertFalse(apple.is_exited_position)
        self.assertEqual(apple.value_delta_thousands, 25000)
        self.assertAlmostEqual(apple.shares_pct_change, 0.25)

        nvidia = positions["67066G104"]
        self.assertTrue(nvidia.is_new_position)
        self.assertFalse(nvidia.is_exited_position)
        self.assertIsNone(nvidia.value_pct_change)

        chevron = positions["166764100"]
        self.assertFalse(chevron.is_new_position)
        self.assertTrue(chevron.is_exited_position)
        self.assertEqual(chevron.new_value_thousands, 0)

    def test_build_position_features_marks_top_rank_entry(self) -> None:
        current = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )
        previous = parse_information_table(
            PREVIOUS_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-23-000999",
        )
        delta = compute_filing_delta(current, previous)
        features = {item.cusip: item for item in build_position_features(delta)}

        nvidia = features["67066G104"]
        self.assertTrue(nvidia.is_new_position)
        self.assertTrue(nvidia.entered_top10)
        self.assertGreater(nvidia.turnover_ratio, 0)

    def test_materiality_scoring_generates_alert_candidates(self) -> None:
        current = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )
        previous = parse_information_table(
            PREVIOUS_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-23-000999",
        )
        delta = compute_filing_delta(current, previous)
        assessments = score_materiality_batch(build_position_features(delta))

        top = assessments[0]
        self.assertTrue(top.should_alert)
        self.assertGreaterEqual(top.score, 40)
        self.assertGreater(len(top.reasons), 0)

    def test_client_impact_scores_overlap_on_alert_candidate(self) -> None:
        current = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )
        previous = parse_information_table(
            PREVIOUS_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-23-000999",
        )
        delta = compute_filing_delta(current, previous)
        assessments = [item for item in score_materiality_batch(build_position_features(delta)) if item.should_alert]
        portfolios = generate_synthetic_client_portfolios(current.holdings, seed=11)

        assessment = next(item for item in assessments if item.cusip == "67066G104")
        impacts = score_client_impacts(assessment, portfolios)
        self.assertGreater(len(impacts), 0)
        self.assertGreater(impacts[0].impact_score, 0)


class PersistenceTests(unittest.TestCase):
    def test_store_and_load_parsed_filing_round_trip(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000001",
            filer_name="Example Capital Management",
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_parsed_filing(connection, parsed)

            loaded = load_parsed_filing(connection, "0001067983-24-000001")
            self.assertIsNotNone(loaded)
            self.assertEqual(len(loaded.holdings), 2)
            self.assertEqual(loaded.filing.cik, "0001067983")
            self.assertEqual(loaded.holdings[0].issuer_name, "APPLE INC")
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_store_delta_and_list_latest_accessions(self) -> None:
        current = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )
        previous = parse_information_table(
            PREVIOUS_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-23-000999",
        )
        delta = compute_filing_delta(current, previous)

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_parsed_filing(connection, previous)
            store_parsed_filing(connection, current)
            store_filing_delta(connection, delta)

            latest = load_latest_filing_accessions(connection, "0001067983", limit=2)
            self.assertEqual(latest, ["0001067983-24-000002", "0001067983-23-000999"])
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
