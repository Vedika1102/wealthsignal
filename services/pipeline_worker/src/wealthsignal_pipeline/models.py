from __future__ import annotations

from dataclasses import dataclass
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
