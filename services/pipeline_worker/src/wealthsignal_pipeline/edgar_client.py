from __future__ import annotations


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
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
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
