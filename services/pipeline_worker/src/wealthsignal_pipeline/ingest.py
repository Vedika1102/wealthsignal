from __future__ import annotations

from pathlib import Path

from .delta_engine import compute_filing_delta
from .edgar_client import (
    discover_filing_artifacts,
    fetch_json,
    fetch_text,
    filing_index_json_url,
    normalize_cik,
    recent_filings_from_submissions,
    submissions_url,
)
from .models import FilingArtifacts, IngestBatchResult, IngestFailure, ParsedInformationTable, SubmissionFiling
from .parser import parse_information_table, parse_primary_document_metadata
from .persistence import (
    connect,
    initialize_database,
    load_latest_filing_accessions,
    load_parsed_filing,
    store_filing_delta,
    store_parsed_filing,
)
from .reference_data import Section13FSecurity, enrich_parsed_filing_with_official_list
from .storage import ObjectStorage, load_storage_from_env


def download_and_parse_filing(
    artifacts: FilingArtifacts,
    user_agent: str,
    *,
    artifact_storage: ObjectStorage | None = None,
    official_list_lookup: dict[str, Section13FSecurity] | None = None,
) -> ParsedInformationTable:
    """Download SEC filing artifacts and parse them into normalized holdings."""

    if not artifacts.information_table_url:
        raise ValueError(f"No information table XML found for {artifacts.filing.accession_number}")

    primary_document_text = fetch_text(artifacts.primary_document_url, user_agent)
    information_table_text = fetch_text(artifacts.information_table_url, user_agent)
    if artifact_storage is not None:
        artifact_storage.upload_raw_filing_artifacts(
            cik=artifacts.filing.cik,
            accession_number=artifacts.filing.accession_number,
            information_table_text=information_table_text,
            primary_document_text=primary_document_text,
        )

    primary_metadata = parse_primary_document_metadata(primary_document_text)
    filing = artifacts.filing
    parsed = parse_information_table(
        information_table_text,
        cik=primary_metadata.cik or filing.cik,
        accession_number=filing.accession_number,
        form_type=primary_metadata.form_type or filing.form_type,
        filer_name=primary_metadata.filer_name,
        filing_date=filing.filing_date,
        report_period=primary_metadata.report_period or filing.report_period,
    )
    if official_list_lookup:
        parsed = enrich_parsed_filing_with_official_list(parsed, official_list_lookup)
    if not parsed.holdings:
        raise ValueError(f"No holdings were parsed from {artifacts.information_table_filename or artifacts.information_table_url}")
    return parsed


def _resolve_filing_artifacts(filing: SubmissionFiling, user_agent: str) -> FilingArtifacts:
    """Fetch index metadata and resolve artifact URLs for one filing."""

    index_payload = fetch_json(filing_index_json_url(filing.cik, filing.accession_number), user_agent)
    return discover_filing_artifacts(filing, index_payload)


def fetch_recent_filing_artifacts(cik: str | int, user_agent: str, *, limit: int = 2) -> list[FilingArtifacts]:
    """Resolve the latest 13F filings and their artifact URLs for a filer."""

    submissions_payload = fetch_json(submissions_url(cik), user_agent)
    recent_filings = recent_filings_from_submissions(submissions_payload)[:limit]

    artifacts: list[FilingArtifacts] = []
    for filing in recent_filings:
        artifacts.append(_resolve_filing_artifacts(filing, user_agent))
    return artifacts


def ingest_recent_filings_batch_for_cik(
    cik: str | int,
    user_agent: str,
    *,
    artifact_storage: ObjectStorage | None = None,
    db_path: str | Path,
    limit: int = 2,
    official_list_lookup: dict[str, Section13FSecurity] | None = None,
) -> IngestBatchResult:
    """Ingest recent 13F filings and continue past per-filing failures."""

    normalized_cik = normalize_cik(cik)
    submissions_payload = fetch_json(submissions_url(cik), user_agent)
    recent_filings = recent_filings_from_submissions(submissions_payload)[:limit]
    if recent_filings:
        normalized_cik = recent_filings[0].cik
    if artifact_storage is None:
        artifact_storage = load_storage_from_env()

    connection = connect(db_path)
    try:
        initialize_database(connection)
        result = IngestBatchResult(cik=normalized_cik)
        for filing in recent_filings:
            try:
                artifacts = _resolve_filing_artifacts(filing, user_agent)
            except Exception as exc:
                result.failures.append(
                    IngestFailure(
                        accession_number=filing.accession_number,
                        stage="artifact-discovery",
                        message=str(exc),
                    )
                )
                continue

            try:
                parsed = download_and_parse_filing(
                    artifacts,
                    user_agent,
                    artifact_storage=artifact_storage,
                    official_list_lookup=official_list_lookup,
                )
                store_parsed_filing(connection, parsed, artifacts=artifacts)
                result.parsed_filings.append(parsed)
            except Exception as exc:
                result.failures.append(
                    IngestFailure(
                        accession_number=filing.accession_number,
                        stage="download-parse",
                        message=str(exc),
                    )
                )
        return result
    finally:
        connection.close()


def ingest_recent_filings_for_cik(
    cik: str | int,
    user_agent: str,
    *,
    artifact_storage: ObjectStorage | None = None,
    db_path: str | Path,
    limit: int = 2,
    official_list_lookup: dict[str, Section13FSecurity] | None = None,
) -> list[ParsedInformationTable]:
    """Ingest recent 13F filings for a CIK and persist them locally."""
    result = ingest_recent_filings_batch_for_cik(
        cik,
        user_agent,
        artifact_storage=artifact_storage,
        db_path=db_path,
        limit=limit,
        official_list_lookup=official_list_lookup,
    )
    return result.parsed_filings


def ingest_and_store_latest_delta(
    cik: str | int,
    user_agent: str,
    *,
    db_path: str | Path,
) -> ParsedInformationTable | None:
    """Ingest recent filings and persist the latest available quarter delta."""

    ingest_recent_filings_for_cik(cik, user_agent, db_path=db_path, limit=2)

    connection = connect(db_path)
    try:
        initialize_database(connection)
        accessions = load_latest_filing_accessions(connection, str(cik).zfill(10), limit=2)
        if len(accessions) < 2:
            return None

        current = load_parsed_filing(connection, accessions[0])
        previous = load_parsed_filing(connection, accessions[1])
        if current is None or previous is None:
            return None

        delta = compute_filing_delta(current, previous)
        store_filing_delta(connection, delta)
        return current
    finally:
        connection.close()
