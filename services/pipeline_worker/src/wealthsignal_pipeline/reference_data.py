from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass, field
from io import BytesIO, StringIO
from pathlib import Path, PurePosixPath
from urllib.parse import urljoin
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from .edgar_client import fetch_bytes, fetch_text
from .models import Holding, ParsedInformationTable


OFFICIAL_13F_LIST_PAGE_URL = "https://www.sec.gov/divisions/investment/13flists.htm"
OFFICIAL_13F_LIST_SOURCE_LABEL = "SEC Official 13F Securities List"


@dataclass(slots=True)
class Section13FSecurity:
    """One security row from the official SEC 13F securities list."""

    cusip: str
    issuer_name: str
    class_title: str | None = None
    ticker: str | None = None
    status: str | None = None


@dataclass(slots=True)
class Section13FListSnapshot:
    """Local cached snapshot of the official 13F list."""

    source_url: str
    securities: list[Section13FSecurity] = field(default_factory=list)


def normalize_cusip(cusip: str) -> str:
    """Normalize a CUSIP-like identifier for lookup matching."""

    return re.sub(r"[^A-Za-z0-9]", "", cusip or "").upper()


def discover_latest_official_list_url(page_html: str, *, base_url: str = OFFICIAL_13F_LIST_PAGE_URL) -> str | None:
    """Extract the latest downloadable 13F list link from the SEC page."""

    candidates: list[str] = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', page_html, flags=re.IGNORECASE):
        lowered = href.lower()
        if "13f" not in lowered:
            continue
        if not lowered.endswith((".xlsx", ".csv", ".txt")):
            continue
        candidates.append(urljoin(base_url, href))
    return candidates[0] if candidates else None


def parse_official_list_payload(source_url: str, payload: bytes) -> Section13FListSnapshot:
    """Parse one official 13F list payload into a normalized snapshot."""

    lowered = source_url.lower()
    if lowered.endswith(".xlsx"):
        securities = parse_official_list_xlsx(payload)
    elif lowered.endswith(".xls"):
        raise ValueError("Legacy .xls official lists are not supported by the current parser")
    else:
        text = payload.decode("utf-8-sig", errors="ignore")
        securities = parse_official_list_delimited(text)
    return Section13FListSnapshot(source_url=source_url, securities=securities)


def refresh_official_list_snapshot(
    user_agent: str,
    output_path: str | Path,
    *,
    page_url: str = OFFICIAL_13F_LIST_PAGE_URL,
) -> Section13FListSnapshot:
    """Fetch the latest official 13F list from the SEC and cache it locally."""

    page_html = fetch_text(page_url, user_agent)
    source_url = discover_latest_official_list_url(page_html, base_url=page_url)
    if not source_url:
        raise ValueError("Could not locate a downloadable 13F securities list on the SEC page")
    snapshot = parse_official_list_payload(source_url, fetch_bytes(source_url, user_agent))
    save_official_list_snapshot(output_path, snapshot)
    return snapshot


def load_official_list_snapshot(path: str | Path) -> Section13FListSnapshot:
    """Load a cached JSON snapshot from disk."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return Section13FListSnapshot(
        source_url=payload["source_url"],
        securities=[Section13FSecurity(**item) for item in payload.get("securities", [])],
    )


def save_official_list_snapshot(path: str | Path, snapshot: Section13FListSnapshot) -> None:
    """Persist a cached JSON snapshot to disk."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(snapshot), indent=2), encoding="utf-8")


def build_security_lookup(snapshot: Section13FListSnapshot) -> dict[str, Section13FSecurity]:
    """Build a normalized CUSIP lookup from a snapshot."""

    return {
        normalized: security
        for security in snapshot.securities
        if (normalized := normalize_cusip(security.cusip))
    }


def enrich_parsed_filing_with_official_list(
    parsed: ParsedInformationTable,
    lookup: dict[str, Section13FSecurity],
) -> ParsedInformationTable:
    """Attach official-list issuer metadata to parsed holdings when available."""

    enriched_holdings: list[Holding] = []
    for holding in parsed.holdings:
        security = lookup.get(normalize_cusip(holding.cusip))
        enriched_holdings.append(
            Holding(
                issuer_name=holding.issuer_name,
                title_of_class=holding.title_of_class,
                cusip=holding.cusip,
                value_thousands=holding.value_thousands,
                shares_or_principal=holding.shares_or_principal,
                share_type=holding.share_type,
                put_call=holding.put_call,
                investment_discretion=holding.investment_discretion,
                other_manager=holding.other_manager,
                voting_authority_sole=holding.voting_authority_sole,
                voting_authority_shared=holding.voting_authority_shared,
                voting_authority_none=holding.voting_authority_none,
                official_issuer_name=security.issuer_name if security else holding.official_issuer_name,
                official_class_title=security.class_title if security else holding.official_class_title,
                ticker=security.ticker if security and security.ticker else holding.ticker,
                official_list_match=security is not None,
                official_list_source=OFFICIAL_13F_LIST_SOURCE_LABEL if security else holding.official_list_source,
            )
        )
    return ParsedInformationTable(filing=parsed.filing, holdings=enriched_holdings)


def parse_official_list_delimited(text: str) -> list[Section13FSecurity]:
    """Parse a CSV or TSV variant of the official list."""

    sample = text[:2048]
    delimiter = ","
    if "\t" in sample and sample.count("\t") >= sample.count(","):
        delimiter = "\t"
    reader = csv.reader(StringIO(text), delimiter=delimiter)
    rows = [row for row in reader]
    return _rows_to_securities(rows)


def parse_official_list_xlsx(payload: bytes) -> list[Section13FSecurity]:
    """Parse the first worksheet of an XLSX official list export."""

    with ZipFile(BytesIO(payload)) as archive:
        shared_strings = _load_shared_strings(archive)
        sheet_path = _first_sheet_path(archive)
        rows = list(_iter_sheet_rows(archive.read(sheet_path), shared_strings))
    return _rows_to_securities(rows)


def _rows_to_securities(rows: list[list[str]]) -> list[Section13FSecurity]:
    header_index = None
    normalized_headers: list[str] = []
    for index, row in enumerate(rows):
        normalized = [_normalize_header(cell) for cell in row]
        if any("cusip" in cell for cell in normalized):
            header_index = index
            normalized_headers = normalized
            break
    if header_index is None:
        raise ValueError("Could not identify the header row in the official 13F list")

    cusip_index = _find_header_index(normalized_headers, ("cusip",))
    issuer_index = _find_header_index(normalized_headers, ("issuer description", "issuer", "name of issuer"))
    class_index = _find_header_index(normalized_headers, ("class", "title of class"))
    ticker_index = _find_header_index(normalized_headers, ("ticker", "symbol"))
    status_index = _find_header_index(normalized_headers, ("status",))
    if cusip_index is None or issuer_index is None:
        raise ValueError("The official 13F list is missing required CUSIP or issuer columns")

    securities: list[Section13FSecurity] = []
    seen_cusips: set[str] = set()
    for row in rows[header_index + 1 :]:
        cusip = normalize_cusip(_safe_cell(row, cusip_index))
        issuer_name = _safe_cell(row, issuer_index).strip()
        if not cusip or not issuer_name:
            continue
        if cusip in seen_cusips:
            continue
        seen_cusips.add(cusip)
        securities.append(
            Section13FSecurity(
                cusip=cusip,
                issuer_name=issuer_name,
                class_title=_blank_to_none(_safe_cell(row, class_index).strip()) if class_index is not None else None,
                ticker=_blank_to_none(_safe_cell(row, ticker_index).strip()) if ticker_index is not None else None,
                status=_blank_to_none(_safe_cell(row, status_index).strip()) if status_index is not None else None,
            )
        )
    return securities


def _blank_to_none(value: str) -> str | None:
    return value or None


def _safe_cell(row: list[str], index: int | None) -> str:
    if index is None or index >= len(row):
        return ""
    return row[index]


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _find_header_index(headers: list[str], candidates: tuple[str, ...]) -> int | None:
    for candidate in candidates:
        for index, header in enumerate(headers):
            if candidate in header:
                return index
    return None


def _load_shared_strings(archive: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for string_item in _find_all_by_local_name(root, "si"):
        parts: list[str] = []
        for text_node in _find_all_by_local_name(string_item, "t"):
            parts.append(text_node.text or "")
        strings.append("".join(parts))
    return strings


def _first_sheet_path(archive: ZipFile) -> str:
    workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
    sheets = _find_first_by_local_name(workbook_root, "sheets")
    if sheets is None:
        raise ValueError("Workbook does not contain any sheets")
    first_sheet = _find_first_by_local_name(sheets, "sheet")
    if first_sheet is None:
        raise ValueError("Workbook does not contain any sheet entries")

    relationship_id = ""
    for key, value in first_sheet.attrib.items():
        if key.endswith("}id") or key == "id":
            relationship_id = value
            break
    if not relationship_id:
        raise ValueError("Could not resolve the first worksheet relationship")

    relationships_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    for relationship in _find_all_by_local_name(relationships_root, "Relationship"):
        if relationship.attrib.get("Id") != relationship_id:
            continue
        target = relationship.attrib.get("Target", "")
        return str(PurePosixPath("xl") / PurePosixPath(target))
    raise ValueError("Could not locate the first worksheet target")


def _iter_sheet_rows(sheet_payload: bytes, shared_strings: list[str]) -> list[list[str]]:
    root = ET.fromstring(sheet_payload)
    rows: list[list[str]] = []
    for row in _find_all_by_local_name(root, "row"):
        values_by_index: dict[int, str] = {}
        max_index = -1
        for cell in _find_all_by_local_name(row, "c"):
            cell_ref = cell.attrib.get("r", "")
            column_index = _column_index_from_ref(cell_ref)
            values_by_index[column_index] = _cell_text(cell, shared_strings)
            max_index = max(max_index, column_index)
        if max_index < 0:
            rows.append([])
            continue
        rows.append([values_by_index.get(index, "").strip() for index in range(max_index + 1)])
    return rows


def _cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        inline_string = _find_first_by_local_name(cell, "is")
        if inline_string is None:
            return ""
        return "".join(node.text or "" for node in _find_all_by_local_name(inline_string, "t"))

    value_node = _find_first_by_local_name(cell, "v")
    if value_node is None or value_node.text is None:
        return ""
    raw_value = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw_value)]
        except (IndexError, ValueError):
            return ""
    return raw_value


def _column_index_from_ref(cell_ref: str) -> int:
    letters = "".join(char for char in cell_ref if char.isalpha()).upper()
    if not letters:
        return 0
    index = 0
    for letter in letters:
        index = (index * 26) + (ord(letter) - 64)
    return index - 1


def _find_first_by_local_name(element: ET.Element, name: str) -> ET.Element | None:
    for child in element.iter():
        if child is element:
            continue
        if child.tag.split("}", 1)[-1] == name:
            return child
    return None


def _find_all_by_local_name(element: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in element.iter() if child.tag.split("}", 1)[-1] == name]
