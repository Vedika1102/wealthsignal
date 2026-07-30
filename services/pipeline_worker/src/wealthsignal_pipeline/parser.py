from __future__ import annotations

from datetime import date
from xml.etree import ElementTree as ET

from .models import FilingReference, Holding, ParsedInformationTable


def _strip_namespaces(xml_text: str) -> str:
    """Remove XML namespace declarations to simplify XPath access."""

    return xml_text.replace(' xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable"', "")


def _text(element: ET.Element, tag: str) -> str:
    node = element.find(tag)
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def _int_text(element: ET.Element, tag: str) -> int:
    value = _text(element, tag)
    if not value:
        return 0
    return int(float(value))


def _float_text(element: ET.Element, tag: str) -> float:
    value = _text(element, tag)
    if not value:
        return 0.0
    return float(value.replace(",", ""))


def parse_information_table(
    xml_text: str,
    *,
    cik: str,
    accession_number: str,
    form_type: str = "13F-HR",
    filer_name: str | None = None,
    filing_date: date | None = None,
    report_period: date | None = None,
) -> ParsedInformationTable:
    """Parse a 13F information table XML payload into normalized holdings.

    The SEC information table commonly uses a default namespace. For a
    lightweight first pass, this parser strips the namespace and reads the
    canonical tags used by `infoTable` records.
    """

    cleaned_xml = _strip_namespaces(xml_text)
    root = ET.fromstring(cleaned_xml)

    holdings: list[Holding] = []

    for info_table in root.findall(".//infoTable"):
        shares_block = info_table.find("shrsOrPrnAmt")
        voting_block = info_table.find("votingAuthority")

        holding = Holding(
            issuer_name=_text(info_table, "nameOfIssuer"),
            title_of_class=_text(info_table, "titleOfClass"),
            cusip=_text(info_table, "cusip"),
            value_thousands=_int_text(info_table, "value"),
            shares_or_principal=_float_text(shares_block, "sshPrnamt") if shares_block is not None else 0.0,
            share_type=_text(shares_block, "sshPrnamtType") if shares_block is not None else "",
            put_call=_text(info_table, "putCall").upper(),
            investment_discretion=_text(info_table, "investmentDiscretion"),
            other_manager=_text(info_table, "otherManager") or None,
            voting_authority_sole=_int_text(voting_block, "Sole") if voting_block is not None else 0,
            voting_authority_shared=_int_text(voting_block, "Shared") if voting_block is not None else 0,
            voting_authority_none=_int_text(voting_block, "None") if voting_block is not None else 0,
        )
        holdings.append(holding)

    filing = FilingReference(
        cik=cik,
        accession_number=accession_number,
        filing_date=filing_date,
        report_period=report_period,
        form_type=form_type,
        filer_name=filer_name,
    )

    return ParsedInformationTable(filing=filing, holdings=holdings)
