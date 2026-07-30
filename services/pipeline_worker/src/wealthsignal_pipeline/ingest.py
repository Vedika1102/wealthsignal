from __future__ import annotations

from pathlib import Path

from .delta_engine import compute_filing_delta
from .edgar_client import (
    discover_filing_artifacts,
    fetch_json,
    fetch_text,
    filing_index_json_url,
    recent_filings_from_submissions,
    submissions_url,
)
from .models import FilingArtifacts, ParsedInformationTable
from .parser import parse_information_table, parse_primary_document_metadata
from .persistence import (
    connect,
    initialize_database,
    load_latest_filing_accessions,
    load_parsed_filing,
    store_filing_delta,
    store_parsed_filing,
)


def download_and_parse_filing(artifacts: FilingArtifacts, user_agent: str) -> ParsedInformationTable:
    """Download SEC filing artifacts and parse them into normalized holdings."""

    if not artifacts.information_table_url:
        raise ValueError(f"No information table XML found for {artifacts.filing.accession_number}")

    primary_document_text = fetch_text(artifacts.primary_document_url, user_agent)
    information_table_text = fetch_text(artifacts.information_table_url, user_agent)

    primary_metadata = parse_primary_document_metadata(primary_document_text)
    filing = artifacts.filing
    return parse_information_table(
        information_table_text,
        cik=primary_metadata.cik or filing.cik,
        accession_number=filing.accession_number,
        form_type=primary_metadata.form_type or filing.form_type,
        filer_name=primary_metadata.filer_name,
        filing_date=filing.filing_date,
        report_period=primary_metadata.report_period or filing.report_period,
    )


def fetch_recent_filing_artifacts(cik: str | int, user_agent: str, *, limit: int = 2) -> list[FilingArtifacts]:
    """Resolve the latest 13F filings and their artifact URLs for a filer."""

    submissions_payload = fetch_json(submissions_url(cik), user_agent)
    recent_filings = recent_filings_from_submissions(submissions_payload)[:limit]

    artifacts: list[FilingArtifacts] = []
    for filing in recent_filings:
        index_payload = fetch_json(filing_index_json_url(filing.cik, filing.accession_number), user_agent)
        artifacts.append(discover_filing_artifacts(filing, index_payload))
    return artifacts


def ingest_recent_filings_for_cik(
    cik: str | int,
    user_agent: str,
    *,
    db_path: str | Path,
    limit: int = 2,
) -> list[ParsedInformationTable]:
    """Ingest recent 13F filings for a CIK and persist them locally."""

    connection = connect(db_path)
    try:
        initialize_database(connection)
        parsed_filings: list[ParsedInformationTable] = []
        for artifacts in fetch_recent_filing_artifacts(cik, user_agent, limit=limit):
            parsed = download_and_parse_filing(artifacts, user_agent)
            store_parsed_filing(connection, parsed, artifacts=artifacts)
            parsed_filings.append(parsed)
        return parsed_filings
    finally:
        connection.close()


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
