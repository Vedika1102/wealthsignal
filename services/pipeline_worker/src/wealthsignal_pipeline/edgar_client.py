from __future__ import annotations

import json
from datetime import date
from pathlib import PurePosixPath
from urllib.request import Request, urlopen

from .models import FilingArtifacts, SubmissionFiling

SEC_SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"


def normalize_cik(cik: str | int) -> str:
    """Return a zero-padded 10-digit CIK string."""

    digits = str(cik).strip()
    digits = digits.replace("CIK", "").replace("cik", "").strip()
    return digits.zfill(10)


def sec_headers(user_agent: str) -> dict[str, str]:
    """Build request headers for SEC access.

    The SEC expects a descriptive user agent that includes contact information.
    """

    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "identity",
    }


def submissions_url(cik: str | int) -> str:
    """Return the SEC company submissions JSON URL for a filer."""

    return f"{SEC_SUBMISSIONS_BASE}/CIK{normalize_cik(cik)}.json"


def filing_index_path(cik: str | int, accession_number: str) -> str:
    """Return the archive path stem for a filing.

    Example:
    cik=1067983, accession=0001067983-24-000001
    -> /Archives/edgar/data/1067983/000106798324000001
    """

    cik_no_padding = str(int(normalize_cik(cik)))
    accession_compact = accession_number.replace("-", "")
    return f"/edgar/data/{cik_no_padding}/{accession_compact}"


def filing_index_url(cik: str | int, accession_number: str) -> str:
    """Return the SEC archive folder URL for a filing."""

    return f"{SEC_ARCHIVES_BASE}{filing_index_path(cik, accession_number)}"


def filing_index_json_url(cik: str | int, accession_number: str) -> str:
    """Return the SEC archive index.json URL for a filing folder."""

    return f"{filing_index_url(cik, accession_number)}/index.json"


def filing_file_url(cik: str | int, accession_number: str, filename: str) -> str:
    """Return the SEC archive URL for a specific filing artifact."""

    return f"{filing_index_url(cik, accession_number)}/{filename}"


def fetch_json(url: str, user_agent: str) -> dict:
    """Fetch and decode a JSON payload from the SEC."""

    request = Request(url, headers=sec_headers(user_agent))
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8", errors="ignore"))


def fetch_text(url: str, user_agent: str) -> str:
    """Fetch a text artifact from the SEC."""

    request = Request(url, headers=sec_headers(user_agent))
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def recent_filings_from_submissions(
    submissions_payload: dict,
    *,
    allowed_forms: set[str] | None = None,
) -> list[SubmissionFiling]:
    """Extract recent filing records from the SEC submissions payload.

    The SEC exposes recent filings as a columnar structure under
    `filings.recent`. This function converts that structure into typed rows.
    """

    allowed = allowed_forms or {"13F-HR", "13F-HR/A"}
    recent = submissions_payload.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])

    records: list[SubmissionFiling] = []
    cik = normalize_cik(submissions_payload.get("cik", ""))
    for index, form in enumerate(forms):
        if form not in allowed:
            continue
        record = SubmissionFiling(
            cik=cik,
            accession_number=recent.get("accessionNumber", [""])[index],
            form_type=form,
            filing_date=_parse_date(recent.get("filingDate", [""])[index]),
            report_period=_parse_date(recent.get("reportDate", [""])[index]),
            primary_document=recent.get("primaryDocument", [""])[index],
            primary_doc_description=recent.get("primaryDocDescription", [""])[index],
        )
        records.append(record)

    return records


def select_information_table_filename(index_payload: dict, primary_document: str = "") -> str | None:
    """Choose the best candidate information table XML from a filing folder.

    For many 13F filings the `primary_doc.xml` file contains the cover/header
    data, while a second XML file contains the `informationTable`.
    """

    items = index_payload.get("directory", {}).get("item", [])
    primary_basename = PurePosixPath(primary_document).name.lower()

    xml_candidates = [
        item
        for item in items
        if item.get("name", "").lower().endswith(".xml")
        and item.get("name", "").lower() != primary_basename
        and "primary_doc" not in item.get("name", "").lower()
    ]
    if not xml_candidates:
        return None

    xml_candidates.sort(key=lambda item: int(item.get("size") or 0), reverse=True)
    return xml_candidates[0].get("name") or None


def discover_filing_artifacts(filing: SubmissionFiling, index_payload: dict) -> FilingArtifacts:
    """Resolve the primary document and information table URLs for a filing."""

    primary_filename = PurePosixPath(filing.primary_document).name
    info_table_filename = select_information_table_filename(index_payload, filing.primary_document)

    return FilingArtifacts(
        filing=filing,
        filing_index_json_url=filing_index_json_url(filing.cik, filing.accession_number),
        filing_folder_url=filing_index_url(filing.cik, filing.accession_number),
        primary_document_url=filing_file_url(filing.cik, filing.accession_number, primary_filename),
        information_table_url=(
            filing_file_url(filing.cik, filing.accession_number, info_table_filename)
            if info_table_filename
            else None
        ),
        information_table_filename=info_table_filename,
    )
