import csv
import unittest
import os
from datetime import date
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
from wealthsignal_pipeline.gold_dataset import export_labeling_candidates, validate_labeled_dataset
from wealthsignal_pipeline.models import (
    ClientHolding,
    ClientPortfolio,
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
    get_client_portfolio,
    get_latest_model_run,
    get_latest_prediction_lookup,
    initialize_database,
    list_alert_impacts,
    list_alerts,
    list_client_portfolios,
    load_feature_rows,
    load_latest_filing_accessions,
    load_parsed_filing,
    list_latest_model_runs,
    list_recommendations,
    store_feature_rows,
    store_model_comparison_bundle,
    store_model_run,
    store_recommendations,
    store_alerts,
    store_client_portfolio,
    store_filing_delta,
    store_parsed_filing,
)
from wealthsignal_pipeline.parser import parse_information_table
from wealthsignal_pipeline.parser import parse_primary_document_metadata
from wealthsignal_pipeline.recommendation import build_client_recommendations
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


def build_sample_feature_rows() -> list[PersistedFeatureRow]:
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
    return build_persisted_feature_rows(delta, features, assessments)


def build_historical_feature_rows() -> list[PersistedFeatureRow]:
    rows = build_sample_feature_rows()
    historical_rows: list[PersistedFeatureRow] = []
    for index, row in enumerate(rows):
        historical_rows.append(
            PersistedFeatureRow(
                current_accession_number="0001067983-24-000001",
                previous_accession_number="0001067983-23-000998",
                holding_key=f"{row.holding_key}-hist-{index}",
                issuer_name=row.issuer_name,
                cusip=row.cusip,
                sector=row.sector,
                current_value_thousands=row.current_value_thousands,
                previous_value_thousands=row.previous_value_thousands,
                current_weight=row.current_weight,
                previous_weight=row.previous_weight,
                weight_delta=row.weight_delta,
                abs_weight_delta=row.abs_weight_delta,
                value_delta_thousands=row.value_delta_thousands,
                abs_value_delta_thousands=row.abs_value_delta_thousands,
                value_pct_change=row.value_pct_change,
                shares_pct_change=row.shares_pct_change,
                is_new_position=row.is_new_position,
                is_exited_position=row.is_exited_position,
                current_rank=row.current_rank,
                previous_rank=row.previous_rank,
                entered_top10=row.entered_top10,
                exited_top10=row.exited_top10,
                entered_top20=row.entered_top20,
                exited_top20=row.exited_top20,
                turnover_ratio=row.turnover_ratio,
                change_share_of_turnover=row.change_share_of_turnover,
                rule_score=row.rule_score,
                weak_label=row.weak_label,
            )
        )
    return historical_rows


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

    def test_synthetic_portfolios_consolidate_duplicate_cusips(self) -> None:
        current = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000002",
        )
        holdings_with_duplicate = [current.holdings[0], current.holdings[0], *current.holdings[1:]]

        portfolios = generate_synthetic_client_portfolios(holdings_with_duplicate, seed=11)

        for portfolio in portfolios:
            cusips = [holding.cusip for holding in portfolio.holdings]
            self.assertEqual(len(cusips), len(set(cusips)))
            self.assertAlmostEqual(sum(holding.weight for holding in portfolio.holdings), 1.0)


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

    def test_latest_accessions_prefer_original_filing_for_each_report_period(self) -> None:
        original = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="original-2024-q1",
            form_type="13F-HR",
            report_period=date(2024, 3, 31),
        )
        amendment = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="amendment-2024-q1",
            form_type="13F-HR/A",
            report_period=date(2024, 3, 31),
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_parsed_filing(connection, original)
            store_parsed_filing(connection, amendment)

            accessions = load_latest_filing_accessions(connection, "0001067983", limit=2)

            self.assertEqual(accessions, ["original-2024-q1"])
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_store_client_portfolio_round_trip_and_replace(self) -> None:
        portfolio = ClientPortfolio(
            client_id="client-001",
            client_name="Balanced Household",
            strategy="balanced",
            holdings=[
                ClientHolding("037833100", "Apple Inc", "Technology", 0.6),
                ClientHolding("166764100", "Chevron Corp", "Energy", 0.4),
            ],
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_client_portfolio(connection, portfolio)

            summaries = list_client_portfolios(connection)
            loaded = get_client_portfolio(connection, "client-001")
            self.assertEqual(summaries[0]["holding_count"], 2)
            self.assertEqual(loaded, portfolio)

            replacement = ClientPortfolio(
                client_id="client-001",
                client_name="Income Household",
                strategy="income",
                holdings=[ClientHolding("166764100", "Chevron Corp", "Energy", 1.0)],
            )
            store_client_portfolio(connection, replacement)
            self.assertEqual(get_client_portfolio(connection, "client-001"), replacement)
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_store_client_portfolio_rejects_invalid_weights(self) -> None:
        portfolio = ClientPortfolio(
            client_id="client-001",
            client_name="Invalid Household",
            strategy="balanced",
            holdings=[ClientHolding("037833100", "Apple Inc", "Technology", 0.8)],
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            with self.assertRaisesRegex(ValueError, "sum to 1"):
                store_client_portfolio(connection, portfolio)
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


class AdvancedModelPersistenceTests(unittest.TestCase):
    def test_store_model_comparison_bundle_persists_latest_comparison_group(self) -> None:
        feature_rows = build_sample_feature_rows()
        bundle = ml_models.ModelTrainingBundle(
            results=[
                ml_models.ModelComparisonResult(
                    model_name="logistic_regression",
                    metrics={"accuracy": 0.8, "precision": 0.75, "recall": 0.7, "f1": 0.72, "pr_auc": 0.78},
                    best_params={"model__C": 1.0},
                    feature_names=ml_models.FEATURE_COLUMNS.copy(),
                    calibration_curve=[
                        ml_models.CalibrationPoint(predicted_probability=0.3, observed_frequency=0.25),
                    ],
                    predicted_probabilities=[0.42, 0.81, 0.36],
                    predicted_labels=[0, 1, 0],
                ),
                ml_models.ModelComparisonResult(
                    model_name="random_forest",
                    metrics={"accuracy": 0.86, "precision": 0.8, "recall": 0.8, "f1": 0.8, "pr_auc": 0.88},
                    best_params={"model__n_estimators": 100},
                    feature_names=ml_models.FEATURE_COLUMNS.copy(),
                    calibration_curve=[
                        ml_models.CalibrationPoint(predicted_probability=0.6, observed_frequency=0.66),
                    ],
                    predicted_probabilities=[0.21, 0.92, 0.12],
                    predicted_labels=[0, 1, 0],
                    shap_feature_importance=[{"feature": "weight_delta", "importance": 0.37}],
                ),
            ],
            best_model_name="random_forest",
            best_result=ml_models.ModelComparisonResult(
                model_name="random_forest",
                metrics={"accuracy": 0.86, "precision": 0.8, "recall": 0.8, "f1": 0.8, "pr_auc": 0.88},
                best_params={"model__n_estimators": 100},
                feature_names=ml_models.FEATURE_COLUMNS.copy(),
                calibration_curve=[
                    ml_models.CalibrationPoint(predicted_probability=0.6, observed_frequency=0.66),
                ],
                predicted_probabilities=[0.21, 0.92, 0.12],
                predicted_labels=[0, 1, 0],
                shap_feature_importance=[{"feature": "weight_delta", "importance": 0.37}],
            ),
            accession_sequence=["0001067983-24-000002"],
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            comparison_group_id, run_ids = store_model_comparison_bundle(
                connection,
                bundle=bundle,
                feature_rows=feature_rows,
                artifact_path="data/models/missing-best-model.joblib",
            )

            latest_runs = list_latest_model_runs(connection)
            latest_run = get_latest_model_run(connection)
            prediction_lookup = get_latest_prediction_lookup(connection)

            self.assertEqual(len(run_ids), 2)
            self.assertIsNotNone(comparison_group_id)
            self.assertEqual(len(latest_runs), 2)
            self.assertEqual(latest_runs[0].comparison_group_id, comparison_group_id)
            self.assertEqual(latest_runs[0].model_name, "random_forest")
            self.assertTrue(latest_runs[0].is_best_model)
            self.assertEqual(latest_runs[0].artifact_path, "data/models/missing-best-model.joblib")
            self.assertEqual(latest_runs[1].model_name, "logistic_regression")
            self.assertFalse(latest_runs[1].is_best_model)
            self.assertIsNone(latest_runs[1].artifact_path)
            self.assertEqual(latest_run.model_name, "random_forest")
            self.assertEqual(len(prediction_lookup), len(feature_rows))
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)


class RecommendationTests(unittest.TestCase):
    def test_build_and_store_recommendations_round_trip(self) -> None:
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
        recommendations = build_client_recommendations(
            assessments,
            portfolios,
            impacts_by_holding_key,
            build_historical_feature_rows(),
            current_accession_number=current.filing.accession_number,
        )

        self.assertGreater(len(recommendations), 0)
        self.assertGreaterEqual(recommendations[0].relevance_score, 1)
        self.assertGreaterEqual(len(recommendations[0].precedents), 1)

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            alert_ids = store_alerts(
                connection,
                current.filing.accession_number,
                previous.filing.accession_number,
                assessments,
                impacts_by_holding_key,
            )
            stored_ids = store_recommendations(
                connection,
                current_accession_number=current.filing.accession_number,
                recommendations=recommendations,
                alert_ids_by_holding_key={
                    assessment.holding_key: alert_id for assessment, alert_id in zip(assessments, alert_ids)
                },
            )
            persisted = list_recommendations(connection, recommendations[0].client_id)

            self.assertGreater(len(stored_ids), 0)
            self.assertGreater(len(persisted), 0)
            self.assertEqual(persisted[0].client_id, recommendations[0].client_id)
            self.assertGreaterEqual(len(persisted[0].precedents), 1)
            connection.close()
        finally:
            db_path.unlink(missing_ok=True)


class GoldDatasetTests(unittest.TestCase):
    def test_export_and_validate_completed_gold_dataset(self) -> None:
        with NamedTemporaryFile(suffix=".db", delete=False) as db_file:
            db_path = Path(db_file.name)
        with NamedTemporaryFile(suffix=".csv", delete=False) as csv_file:
            csv_path = Path(csv_file.name)

        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_feature_rows(connection, build_sample_feature_rows())
            connection.close()

            exported_count = export_labeling_candidates(db_path, csv_path, limit=3)
            with csv_path.open(newline="", encoding="utf-8") as input_file:
                reader = csv.DictReader(input_file)
                fieldnames = reader.fieldnames
                rows = list(reader)
            for index, row in enumerate(rows):
                row["manual_label"] = str(index % 2)
                row["review_reason"] = "Independent rubric-based review."
                row["reviewer_id"] = "reviewer-01"
                row["reviewed_at"] = "2026-08-01"
            with csv_path.open("w", newline="", encoding="utf-8") as output_file:
                writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            summary = validate_labeled_dataset(csv_path)
            self.assertEqual(exported_count, 3)
            self.assertEqual(summary.row_count, 3)
            self.assertEqual(summary.positive_count + summary.negative_count, 3)
        finally:
            db_path.unlink(missing_ok=True)
            csv_path.unlink(missing_ok=True)

    def test_validation_rejects_incomplete_review(self) -> None:
        with NamedTemporaryFile(suffix=".csv", delete=False, mode="w", newline="", encoding="utf-8") as csv_file:
            csv_path = Path(csv_file.name)
            writer = csv.DictWriter(csv_file, fieldnames=[
                "event_id", "current_accession_number", "previous_accession_number", "holding_key",
                "issuer_name", "cusip", "sector", "current_weight", "previous_weight", "weight_delta",
                "value_delta_thousands", "is_new_position", "is_exited_position", "current_rank",
                "previous_rank", "turnover_ratio", "rule_score", "weak_label", "manual_label",
                "review_reason", "reviewer_id", "reviewed_at",
            ])
            writer.writeheader()
            writer.writerow({"event_id": "event-1", "manual_label": "1"})

        try:
            with self.assertRaisesRegex(ValueError, "review_reason is required"):
                validate_labeled_dataset(csv_path)
        finally:
            csv_path.unlink(missing_ok=True)


class DecisionApiTests(unittest.TestCase):
    def test_client_portfolio_endpoints_upsert_and_read(self) -> None:
        from services.decision_api.app import main as decision_api_main

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        original_db_path = decision_api_main.DB_PATH
        try:
            decision_api_main.DB_PATH = str(db_path)
            payload = decision_api_main.ClientPortfolioPayload(
                client_name="Growth Household",
                strategy="growth",
                holdings=[
                    decision_api_main.ClientHoldingPayload(
                        cusip="037833100",
                        issuer_name="Apple Inc",
                        sector="Technology",
                        weight=1.0,
                    )
                ],
            )
            created = decision_api_main.upsert_client_portfolio("client-api-001", payload)
            detail = decision_api_main.client_detail("client-api-001")
            summaries = decision_api_main.clients(limit=100)

            self.assertEqual(created["client_id"], "client-api-001")
            self.assertEqual(detail["holdings"][0]["weight"], 1.0)
            self.assertEqual(summaries[0]["holding_count"], 1)
        finally:
            decision_api_main.DB_PATH = original_db_path
            db_path.unlink(missing_ok=True)

    def test_latest_model_returns_comparison_models_when_available(self) -> None:
        try:
            from services.decision_api.app import main as decision_api_main
        except ModuleNotFoundError as exc:
            if exc.name == "fastapi":
                self.skipTest("fastapi is not installed in the current test environment")
            raise

        feature_rows = build_sample_feature_rows()
        bundle = ml_models.ModelTrainingBundle(
            results=[
                ml_models.ModelComparisonResult(
                    model_name="decision_tree",
                    metrics={"accuracy": 0.75, "precision": 0.7, "recall": 0.7, "f1": 0.7, "pr_auc": 0.74},
                    best_params={"model__max_depth": 3},
                    feature_names=ml_models.FEATURE_COLUMNS.copy(),
                    calibration_curve=[
                        ml_models.CalibrationPoint(predicted_probability=0.5, observed_frequency=0.5),
                    ],
                    predicted_probabilities=[0.18, 0.73, 0.22],
                    predicted_labels=[0, 1, 0],
                ),
                ml_models.ModelComparisonResult(
                    model_name="xgboost",
                    metrics={"accuracy": 0.89, "precision": 0.85, "recall": 0.8, "f1": 0.82, "pr_auc": 0.91},
                    best_params={"model__max_depth": 4},
                    feature_names=ml_models.FEATURE_COLUMNS.copy(),
                    calibration_curve=[
                        ml_models.CalibrationPoint(predicted_probability=0.7, observed_frequency=0.75),
                    ],
                    predicted_probabilities=[0.11, 0.95, 0.09],
                    predicted_labels=[0, 1, 0],
                    shap_feature_importance=[{"feature": "abs_weight_delta", "importance": 0.41}],
                ),
            ],
            best_model_name="xgboost",
            best_result=ml_models.ModelComparisonResult(
                model_name="xgboost",
                metrics={"accuracy": 0.89, "precision": 0.85, "recall": 0.8, "f1": 0.82, "pr_auc": 0.91},
                best_params={"model__max_depth": 4},
                feature_names=ml_models.FEATURE_COLUMNS.copy(),
                calibration_curve=[
                    ml_models.CalibrationPoint(predicted_probability=0.7, observed_frequency=0.75),
                ],
                predicted_probabilities=[0.11, 0.95, 0.09],
                predicted_labels=[0, 1, 0],
                shap_feature_importance=[{"feature": "abs_weight_delta", "importance": 0.41}],
            ),
            accession_sequence=["0001067983-24-000002"],
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        original_db_path = decision_api_main.DB_PATH
        try:
            connection = connect(db_path)
            initialize_database(connection)
            store_model_comparison_bundle(
                connection,
                bundle=bundle,
                feature_rows=feature_rows,
                artifact_path="data/models/does-not-exist.joblib",
            )
            connection.close()

            decision_api_main.DB_PATH = str(db_path)
            response = decision_api_main.latest_model()

            self.assertEqual(response["model_name"], "xgboost")
            self.assertEqual(response["best_model_name"], "xgboost")
            self.assertFalse(response["artifact_loaded"])
            self.assertEqual(len(response["models"]), 2)
            self.assertEqual(response["models"][0]["model_name"], "xgboost")
            self.assertTrue(response["models"][0]["is_best_model"])
            self.assertEqual(response["models"][1]["model_name"], "decision_tree")
        finally:
            decision_api_main.DB_PATH = original_db_path
            db_path.unlink(missing_ok=True)

    def test_recommendations_endpoint_returns_ranked_recommendations(self) -> None:
        from services.decision_api.app import main as decision_api_main

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
        recommendations = build_client_recommendations(
            assessments,
            portfolios,
            impacts_by_holding_key,
            build_historical_feature_rows(),
            current_accession_number=current.filing.accession_number,
        )

        with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = Path(temp_file.name)

        original_db_path = decision_api_main.DB_PATH
        try:
            connection = connect(db_path)
            initialize_database(connection)
            alert_ids = store_alerts(
                connection,
                current.filing.accession_number,
                previous.filing.accession_number,
                assessments,
                impacts_by_holding_key,
            )
            store_recommendations(
                connection,
                current_accession_number=current.filing.accession_number,
                recommendations=recommendations,
                alert_ids_by_holding_key={
                    assessment.holding_key: alert_id for assessment, alert_id in zip(assessments, alert_ids)
                },
            )
            connection.close()

            decision_api_main.DB_PATH = str(db_path)
            payload = decision_api_main.recommendations(recommendations[0].client_id, limit=10)

            self.assertGreater(len(payload), 0)
            self.assertEqual(payload[0]["client_id"], recommendations[0].client_id)
            self.assertIn("precedent_count", payload[0])
            self.assertIn("rationale", payload[0])
        finally:
            decision_api_main.DB_PATH = original_db_path
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
