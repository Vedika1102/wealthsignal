import unittest
import os
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import patch
from zipfile import ZipFile

from wealthsignal_pipeline import cli as pipeline_cli
from wealthsignal_pipeline import ingest as pipeline_ingest
from wealthsignal_pipeline import ml_models
from wealthsignal_pipeline.baseline_model import assign_weak_label, train_logistic_baseline
from wealthsignal_pipeline.delta_engine import compute_filing_delta
from wealthsignal_pipeline.feature_engineering import build_persisted_feature_rows, build_position_features
from wealthsignal_pipeline.models import (
    FilingArtifacts,
    IngestBatchResult,
    IngestFailure,
    PersistedFeatureRow,
    SubmissionFiling,
)
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
from wealthsignal_pipeline.persistence import (
    connect,
    get_alert,
    get_latest_model_run,
    initialize_database,
    list_alert_impacts,
    list_alerts,
    load_feature_rows,
    load_latest_filing_accessions,
    load_parsed_filing,
    store_feature_rows,
    store_model_run,
    store_alerts,
    store_filing_delta,
    store_parsed_filing,
)
from wealthsignal_pipeline.parser import parse_information_table
from wealthsignal_pipeline.parser import parse_primary_document_metadata
from wealthsignal_pipeline.reference_data import (
    OFFICIAL_13F_LIST_SOURCE_LABEL,
    Section13FListSnapshot,
    Section13FSecurity,
    build_security_lookup,
    discover_latest_official_list_url,
    enrich_parsed_filing_with_official_list,
    normalize_cusip,
    parse_official_list_xlsx,
)
from wealthsignal_pipeline.sector_enrichment import infer_sector
from wealthsignal_pipeline.storage import load_storage_from_env
from wealthsignal_pipeline.worker_health import build_health_payload


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

SAMPLE_PRIMARY_ONLY_INDEX_JSON = {
    "directory": {
        "item": [
            {"name": "0001193125-26-226661.txt", "size": ""},
            {"name": "primary_doc.xml", "size": "45555"},
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

SAMPLE_13F_LIST_HTML = """
<html>
  <body>
    <a href="/files/quarterly/13f/13flist2026q2.xlsx">Quarter 2 2026</a>
    <a href="/files/quarterly/13f/13flist2026q1.xlsx">Quarter 1 2026</a>
  </body>
</html>
"""


def build_sample_13f_list_xlsx() -> bytes:
    workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
              xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
      <sheets>
        <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
      </sheets>
    </workbook>
    """
    relationships_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
      <Relationship Id="rId1"
        Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
        Target="worksheets/sheet1.xml"/>
    </Relationships>
    """
    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheetData>
        <row r="1">
          <c r="A1" t="inlineStr"><is><t>CUSIP</t></is></c>
          <c r="B1" t="inlineStr"><is><t>Issuer Description</t></is></c>
          <c r="C1" t="inlineStr"><is><t>Class Title</t></is></c>
          <c r="D1" t="inlineStr"><is><t>Ticker</t></is></c>
          <c r="E1" t="inlineStr"><is><t>Status</t></is></c>
        </row>
        <row r="2">
          <c r="A2" t="inlineStr"><is><t>037833100</t></is></c>
          <c r="B2" t="inlineStr"><is><t>APPLE INC</t></is></c>
          <c r="C2" t="inlineStr"><is><t>COM</t></is></c>
          <c r="D2" t="inlineStr"><is><t>AAPL</t></is></c>
          <c r="E2" t="inlineStr"><is><t>ACTIVE</t></is></c>
        </row>
        <row r="3">
          <c r="A3" t="inlineStr"><is><t>67066G104</t></is></c>
          <c r="B3" t="inlineStr"><is><t>NVIDIA CORP</t></is></c>
          <c r="C3" t="inlineStr"><is><t>COM</t></is></c>
          <c r="D3" t="inlineStr"><is><t>NVDA</t></is></c>
          <c r="E3" t="inlineStr"><is><t>ACTIVE</t></is></c>
        </row>
      </sheetData>
    </worksheet>
    """
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", relationships_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


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

    def test_select_information_table_filename_falls_back_to_primary_xml(self) -> None:
        self.assertEqual(
            select_information_table_filename(SAMPLE_PRIMARY_ONLY_INDEX_JSON, "primary_doc.xml"),
            "primary_doc.xml",
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

    def test_infer_sector_maps_known_issuer_names(self) -> None:
        self.assertEqual(infer_sector("ALPHABET INC"), "Communication Services")
        self.assertEqual(infer_sector("VISA INC"), "Financials")


class ReferenceDataTests(unittest.TestCase):
    def test_discover_latest_official_list_url_reads_first_matching_link(self) -> None:
        self.assertEqual(
            discover_latest_official_list_url(SAMPLE_13F_LIST_HTML),
            "https://www.sec.gov/files/quarterly/13f/13flist2026q2.xlsx",
        )

    def test_parse_official_list_xlsx_extracts_security_rows(self) -> None:
        securities = parse_official_list_xlsx(build_sample_13f_list_xlsx())
        self.assertEqual(len(securities), 2)
        self.assertEqual(securities[0].cusip, "037833100")
        self.assertEqual(securities[0].ticker, "AAPL")
        self.assertEqual(securities[1].issuer_name, "NVIDIA CORP")

    def test_enrich_parsed_filing_with_official_list_matches_by_cusip(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000001",
        )
        snapshot = Section13FListSnapshot(
            source_url="https://www.sec.gov/files/quarterly/13f/13flist2026q2.xlsx",
            securities=[
                Section13FSecurity(
                    cusip=normalize_cusip("037833100"),
                    issuer_name="APPLE INC",
                    class_title="COM",
                    ticker="AAPL",
                    status="ACTIVE",
                )
            ],
        )
        enriched = enrich_parsed_filing_with_official_list(parsed, build_security_lookup(snapshot))

        apple = enriched.holdings[0]
        self.assertTrue(apple.official_list_match)
        self.assertEqual(apple.official_issuer_name, "APPLE INC")
        self.assertEqual(apple.official_class_title, "COM")
        self.assertEqual(apple.ticker, "AAPL")
        self.assertEqual(apple.official_list_source, OFFICIAL_13F_LIST_SOURCE_LABEL)


class StorageTests(unittest.TestCase):
    def test_load_storage_from_env_returns_none_when_unconfigured(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(load_storage_from_env())

    def test_download_and_parse_filing_uploads_raw_artifacts_when_storage_is_provided(self) -> None:
        filing = SubmissionFiling(
            cik="0001067983",
            accession_number="0001193125-26-226661",
            form_type="13F-HR",
            primary_document="primary_doc.xml",
        )
        artifacts = FilingArtifacts(
            filing=filing,
            filing_index_json_url="https://example.com/index.json",
            filing_folder_url="https://example.com/folder",
            primary_document_url="https://example.com/primary_doc.xml",
            information_table_url="https://example.com/53405.xml",
            information_table_filename="53405.xml",
        )

        class FakeStorage:
            def __init__(self) -> None:
                self.calls: list[dict[str, str]] = []

            def upload_raw_filing_artifacts(self, **kwargs):
                self.calls.append(kwargs)
                return {
                    "information_table": "wealthsignal-filings/0001067983/0001193125-26-226661/information_table.xml"
                }

        storage = FakeStorage()
        with patch(
            "wealthsignal_pipeline.ingest.fetch_text",
            side_effect=[SAMPLE_PRIMARY_DOCUMENT, SAMPLE_INFORMATION_TABLE],
        ):
            parsed = pipeline_ingest.download_and_parse_filing(
                artifacts,
                "Test Agent",
                artifact_storage=storage,
            )

        self.assertEqual(parsed.filing.accession_number, "0001193125-26-226661")
        self.assertEqual(len(storage.calls), 1)
        self.assertEqual(storage.calls[0]["cik"], "0001067983")
        self.assertEqual(storage.calls[0]["accession_number"], "0001193125-26-226661")
        self.assertIn("informationTable", storage.calls[0]["information_table_text"])


class WorkerHealthTests(unittest.TestCase):
    def test_build_health_payload_reflects_database_backend(self) -> None:
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://postgres:postgres@db:5432/wealthsignal"}, clear=False):
            payload = build_health_payload()

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "pipeline-worker")
        self.assertEqual(payload["backend"], "postgres")


class MlModelPrepTests(unittest.TestCase):
    def test_build_feature_matrix_outputs_expected_shape(self) -> None:
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
        features = build_position_features(delta)
        assessments = score_materiality_batch(features)
        rows = build_persisted_feature_rows(delta, features, assessments)

        matrix, labels = ml_models.build_feature_matrix(rows)

        self.assertEqual(matrix.shape[0], len(rows))
        self.assertEqual(matrix.shape[1], len(ml_models.FEATURE_COLUMNS))
        self.assertEqual(labels.shape[0], len(rows))

    def test_build_time_based_fold_assignments_orders_by_accession(self) -> None:
        rows = [
            PersistedFeatureRow(
                current_accession_number="0002",
                previous_accession_number="0001",
                holding_key="b",
                issuer_name="B",
                cusip="2",
                sector="Unknown",
                current_value_thousands=2,
                previous_value_thousands=1,
                current_weight=0.2,
                previous_weight=0.1,
                weight_delta=0.1,
                abs_weight_delta=0.1,
                value_delta_thousands=1,
                abs_value_delta_thousands=1,
                value_pct_change=1.0,
                shares_pct_change=1.0,
                is_new_position=False,
                is_exited_position=False,
                current_rank=1,
                previous_rank=2,
                entered_top10=True,
                exited_top10=False,
                entered_top20=True,
                exited_top20=False,
                turnover_ratio=0.1,
                change_share_of_turnover=0.1,
                rule_score=60,
                weak_label=1,
            ),
            PersistedFeatureRow(
                current_accession_number="0003",
                previous_accession_number="0002",
                holding_key="c",
                issuer_name="C",
                cusip="3",
                sector="Unknown",
                current_value_thousands=3,
                previous_value_thousands=2,
                current_weight=0.3,
                previous_weight=0.2,
                weight_delta=0.1,
                abs_weight_delta=0.1,
                value_delta_thousands=1,
                abs_value_delta_thousands=1,
                value_pct_change=0.5,
                shares_pct_change=0.5,
                is_new_position=False,
                is_exited_position=False,
                current_rank=1,
                previous_rank=2,
                entered_top10=True,
                exited_top10=False,
                entered_top20=True,
                exited_top20=False,
                turnover_ratio=0.1,
                change_share_of_turnover=0.1,
                rule_score=60,
                weak_label=1,
            ),
            PersistedFeatureRow(
                current_accession_number="0001",
                previous_accession_number="0000",
                holding_key="a",
                issuer_name="A",
                cusip="1",
                sector="Unknown",
                current_value_thousands=1,
                previous_value_thousands=0,
                current_weight=0.1,
                previous_weight=0.0,
                weight_delta=0.1,
                abs_weight_delta=0.1,
                value_delta_thousands=1,
                abs_value_delta_thousands=1,
                value_pct_change=None,
                shares_pct_change=None,
                is_new_position=True,
                is_exited_position=False,
                current_rank=1,
                previous_rank=None,
                entered_top10=True,
                exited_top10=False,
                entered_top20=True,
                exited_top20=False,
                turnover_ratio=0.1,
                change_share_of_turnover=0.1,
                rule_score=70,
                weak_label=1,
            ),
        ]

        assignments, accession_sequence = ml_models.build_time_based_fold_assignments(rows, fold_count=3)

        self.assertEqual(accession_sequence, ["0001", "0002", "0003"])
        self.assertEqual(len(assignments), len(rows))
        self.assertEqual(assignments[0], 1)
        self.assertEqual(assignments[1], 2)
        self.assertEqual(assignments[2], 0)


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

    def test_assign_weak_label_separates_stronger_events(self) -> None:
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
        assessment_map = {item.cusip: item for item in assessments}
        feature_map = {item.cusip: item for item in build_position_features(delta)}

        nvidia_label = assign_weak_label(feature_map["67066G104"], rule_score=assessment_map["67066G104"].score)
        chevron_label = assign_weak_label(feature_map["166764100"], rule_score=assessment_map["166764100"].score)
        self.assertEqual(nvidia_label, 1)
        self.assertIn(chevron_label, {0, 1})

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
    def test_connect_supports_sqlite_database_url(self) -> None:
        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            with patch.dict(os.environ, {"DATABASE_URL": f"sqlite:///{db_path}"}, clear=False):
                connection = connect()
                self.assertEqual(connection.backend, "sqlite")
                initialize_database(connection)
                connection.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_store_and_load_parsed_filing_round_trip(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000001",
            filer_name="Example Capital Management",
        )
        parsed = enrich_parsed_filing_with_official_list(
            parsed,
            {
                "037833100": Section13FSecurity(
                    cusip="037833100",
                    issuer_name="APPLE INC",
                    class_title="COM",
                    ticker="AAPL",
                    status="ACTIVE",
                )
            },
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
            self.assertTrue(loaded.holdings[0].official_list_match)
            self.assertEqual(loaded.holdings[0].ticker, "AAPL")
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)


class CliTests(unittest.TestCase):
    def test_main_returns_cleanly_when_no_recent_filings_are_found(self) -> None:
        with (
            patch("sys.argv", ["wealthsignal-cli", "--cik", "1067983", "--user-agent", "Test Agent"]),
            patch("sys.stdout", new_callable=StringIO) as stdout,
            patch(
                "wealthsignal_pipeline.cli.ingest_recent_filings_batch_for_cik",
                return_value=IngestBatchResult(cik="0001067983"),
            ),
            patch("wealthsignal_pipeline.cli.connect") as connect_mock,
        ):
            exit_code = pipeline_cli.main()

        self.assertEqual(exit_code, 0)
        self.assertIn("No recent 13F filings were ingested successfully", stdout.getvalue())
        connect_mock.assert_not_called()

    def test_main_skips_delta_generation_when_only_one_filing_is_available(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )

        with (
            patch("sys.argv", ["wealthsignal-cli", "--cik", "1067983", "--user-agent", "Test Agent"]),
            patch("sys.stdout", new_callable=StringIO) as stdout,
            patch(
                "wealthsignal_pipeline.cli.ingest_recent_filings_batch_for_cik",
                return_value=IngestBatchResult(cik="0001067983", parsed_filings=[parsed]),
            ),
            patch("wealthsignal_pipeline.cli.connect") as connect_mock,
            patch("wealthsignal_pipeline.cli.initialize_database"),
            patch("wealthsignal_pipeline.cli.load_latest_filing_accessions", return_value=[parsed.filing.accession_number]),
        ):
            exit_code = pipeline_cli.main()

        self.assertEqual(exit_code, 0)
        self.assertIn("Only one filing is available", stdout.getvalue())
        connect_mock.return_value.close.assert_called_once()

    def test_main_reports_skipped_filings_before_successful_ingest_output(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )
        batch_result = IngestBatchResult(
            cik="0001067983",
            parsed_filings=[parsed],
            failures=[],
        )
        batch_result.failures.append(
            IngestFailure(
                accession_number="0001067983-24-000001",
                stage="download-parse",
                message="bad xml",
            )
        )

        with (
            patch("sys.argv", ["wealthsignal-cli", "--cik", "1067983", "--user-agent", "Test Agent"]),
            patch("sys.stdout", new_callable=StringIO) as stdout,
            patch("wealthsignal_pipeline.cli.ingest_recent_filings_batch_for_cik", return_value=batch_result),
            patch("wealthsignal_pipeline.cli.connect") as connect_mock,
            patch("wealthsignal_pipeline.cli.initialize_database"),
            patch("wealthsignal_pipeline.cli.load_latest_filing_accessions", return_value=[parsed.filing.accession_number]),
        ):
            exit_code = pipeline_cli.main()

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("Skipped 1 filing(s) during ingest", output)
        self.assertIn("stage=download-parse", output)
        connect_mock.return_value.close.assert_called_once()


class IngestHardeningTests(unittest.TestCase):
    def test_download_and_parse_filing_rejects_empty_holdings(self) -> None:
        filing = SubmissionFiling(
            cik="0001067983",
            accession_number="0001193125-26-226661",
            form_type="13F-HR",
            primary_document="primary_doc.xml",
        )
        artifacts = FilingArtifacts(
            filing=filing,
            filing_index_json_url="https://example.com/index.json",
            filing_folder_url="https://example.com/folder",
            primary_document_url="https://example.com/primary_doc.xml",
            information_table_url="https://example.com/primary_doc.xml",
            information_table_filename="primary_doc.xml",
        )

        with patch(
            "wealthsignal_pipeline.ingest.fetch_text",
            side_effect=[SAMPLE_PRIMARY_DOCUMENT, "<informationTable></informationTable>"],
        ):
            with self.assertRaisesRegex(ValueError, "No holdings were parsed"):
                pipeline_ingest.download_and_parse_filing(artifacts, "Test Agent")

    def test_ingest_batch_continues_after_one_filing_fails(self) -> None:
        failing_filing = SubmissionFiling(
            cik="0001067983",
            accession_number="0001193125-26-111111",
            form_type="13F-HR",
            primary_document="primary_doc.xml",
        )
        successful_filing = SubmissionFiling(
            cik="0001067983",
            accession_number="0001193125-26-222222",
            form_type="13F-HR",
            primary_document="primary_doc.xml",
        )
        failing_artifacts = FilingArtifacts(
            filing=failing_filing,
            filing_index_json_url="https://example.com/1/index.json",
            filing_folder_url="https://example.com/1",
            primary_document_url="https://example.com/1/primary_doc.xml",
        )
        successful_artifacts = FilingArtifacts(
            filing=successful_filing,
            filing_index_json_url="https://example.com/2/index.json",
            filing_folder_url="https://example.com/2",
            primary_document_url="https://example.com/2/primary_doc.xml",
            information_table_url="https://example.com/2/53405.xml",
            information_table_filename="53405.xml",
        )
        parsed_success = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001193125-26-222222",
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            with (
                patch("wealthsignal_pipeline.ingest.fetch_json", return_value={"unused": True}),
                patch(
                    "wealthsignal_pipeline.ingest.recent_filings_from_submissions",
                    return_value=[failing_filing, successful_filing],
                ),
                patch(
                    "wealthsignal_pipeline.ingest._resolve_filing_artifacts",
                    side_effect=[failing_artifacts, successful_artifacts],
                ),
                patch(
                    "wealthsignal_pipeline.ingest.download_and_parse_filing",
                    side_effect=[ValueError("broken xml"), parsed_success],
                ),
            ):
                result = pipeline_ingest.ingest_recent_filings_batch_for_cik(
                    "1067983",
                    "Test Agent",
                    db_path=db_path,
                    limit=2,
                )

            self.assertEqual(result.cik, "0001067983")
            self.assertEqual(len(result.parsed_filings), 1)
            self.assertEqual(result.parsed_filings[0].filing.accession_number, "0001193125-26-222222")
            self.assertEqual(len(result.failures), 1)
            self.assertEqual(result.failures[0].accession_number, "0001193125-26-111111")
            self.assertEqual(result.failures[0].stage, "download-parse")

            connection = connect(db_path)
            try:
                stored = load_parsed_filing(connection, "0001193125-26-222222")
                self.assertIsNotNone(stored)
            finally:
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

    def test_store_alerts_and_impacts_round_trip(self) -> None:
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
        impacts_by_holding_key = {
            assessment.holding_key: score_client_impacts(assessment, portfolios)
            for assessment in assessments
        }

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_alerts(
                connection,
                "0001067983-24-000002",
                "0001067983-23-000999",
                assessments,
                impacts_by_holding_key,
            )

            alerts = list_alerts(connection, limit=10, minimum_score=40)
            self.assertGreater(len(alerts), 0)
            alert = get_alert(connection, alerts[0].alert_id)
            self.assertIsNotNone(alert)
            self.assertGreaterEqual(alert.score, 40)
            impacts = list_alert_impacts(connection, alerts[0].alert_id)
            self.assertIsInstance(impacts, list)
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_store_feature_rows_and_train_baseline(self) -> None:
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
        features = build_position_features(delta)
        assessments = score_materiality_batch(features)
        rows = build_persisted_feature_rows(delta, features, assessments)

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_feature_rows(connection, rows)
            loaded_rows = load_feature_rows(connection)
            self.assertEqual(len(loaded_rows), len(rows))

            fit = train_logistic_baseline(loaded_rows)
            run_id = store_model_run(
                connection,
                model_name="numpy-logistic-baseline",
                training_samples=len(loaded_rows),
                positive_count=sum(row.weak_label for row in loaded_rows),
                feature_names=fit.feature_names,
                coefficients=fit.coefficients,
                intercept=fit.intercept,
                metrics=fit.metrics,
                predictions=fit.predictions,
            )
            model_run = get_latest_model_run(connection)
            self.assertEqual(model_run.run_id, run_id)
            self.assertGreaterEqual(model_run.training_samples, 1)
            self.assertIn("accuracy", model_run.metrics)
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
