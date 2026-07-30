from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal


@dataclass(slots=True)
class FilingReference:
    """Minimal metadata used to identify a specific SEC filing."""

    cik: str
    accession_number: str
    filing_date: date | None = None
    report_period: date | None = None
    form_type: str = "13F-HR"
    filer_name: str | None = None


@dataclass(slots=True)
class Holding:
    """Normalized holding extracted from the 13F information table."""

    issuer_name: str
    title_of_class: str
    cusip: str
    value_thousands: int
    shares_or_principal: float
    share_type: str = ""
    put_call: Literal["PUT", "CALL", ""] = ""
    investment_discretion: str = ""
    other_manager: str | None = None
    voting_authority_sole: int = 0
    voting_authority_shared: int = 0
    voting_authority_none: int = 0

    @property
    def market_value_usd(self) -> int:
        return self.value_thousands * 1000


@dataclass(slots=True)
class ParsedInformationTable:
    """Structured representation of a 13F filing's holdings table."""

    filing: FilingReference
    holdings: list[Holding]


@dataclass(slots=True)
class SubmissionFiling:
    """A single filing entry extracted from the SEC submissions JSON feed."""

    cik: str
    accession_number: str
    form_type: str
    filing_date: date | None = None
    report_period: date | None = None
    primary_document: str = ""
    primary_doc_description: str = ""


@dataclass(slots=True)
class FilingArtifacts:
    """Resolved file locations for a filing inside the SEC archive folder."""

    filing: SubmissionFiling
    filing_index_json_url: str
    filing_folder_url: str
    primary_document_url: str
    information_table_url: str | None = None
    information_table_filename: str | None = None


@dataclass(slots=True)
class PositionDelta:
    """Quarter-over-quarter change for a single holding key."""

    holding_key: str
    issuer_name: str
    cusip: str
    old_value_thousands: int
    new_value_thousands: int
    old_shares: float
    new_shares: float
    old_weight: float
    new_weight: float
    is_new_position: bool
    is_exited_position: bool
    share_type: str = ""
    value_delta_thousands: int = 0
    shares_delta: float = 0.0
    value_pct_change: float | None = None
    shares_pct_change: float | None = None
    rank_delta: int | None = None


@dataclass(slots=True)
class FilingDelta:
    """Collection of deltas between two quarters for the same filer."""

    current_filing: FilingReference
    previous_filing: FilingReference
    positions: list[PositionDelta] = field(default_factory=list)
