from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping
from urllib.request import Request, urlopen
from zipfile import ZipFile


SEC_BULK_BASE_URL = "https://www.sec.gov/files/structureddata/data/form-13f-data-sets"
VALUE_UNIT_CHANGE_DATE = date(2023, 1, 3)
MODERN_SEC_BULK_FILENAMES = {
    "2024q1": "01jan2024-29feb2024_form13f.zip",
    "2024q2": "01mar2024-31may2024_form13f.zip",
    "2024q3": "01jun2024-31aug2024_form13f.zip",
    "2024q4": "01sep2024-30nov2024_form13f.zip",
    "2025q1": "01dec2024-28feb2025_form13f.zip",
    "2025q2": "01mar2025-31may2025_form13f.zip",
    "2025q3": "01jun2025-31aug2025_form13f.zip",
    "2025q4": "01sep2025-30nov2025_form13f.zip",
    "2026q1": "01dec2025-28feb2026_form13f.zip",
    "2026q2": "01mar2026-31may2026_form13f.zip",
}


@dataclass(frozen=True, slots=True)
class BulkPackageSource:
    """One SEC quarterly package and its immutable source identity."""

    package: str
    path: Path
    source_url: str
    retrieved_at: str | None = None


@dataclass(frozen=True, slots=True)
class BulkSubmission:
    accession_number: str
    filing_date: date
    submission_type: str
    cik: str
    report_period: date
    filer_name: str
    is_amendment: bool
    amendment_number: int | None
    amendment_type: str
    package: str


@dataclass(frozen=True, slots=True)
class BulkHolding:
    source_accession_number: str
    info_table_key: str
    issuer_name: str
    title_of_class: str
    cusip: str
    figi: str
    value_usd: int
    shares_or_principal: float
    share_type: str
    put_call: str
    investment_discretion: str
    other_manager: str
    voting_authority_sole: int
    voting_authority_shared: int
    voting_authority_none: int

    @property
    def security_key(self) -> str:
        return "|".join(
            (
                self.cusip,
                self.title_of_class.upper(),
                self.put_call.upper(),
                self.share_type.upper(),
                self.investment_discretion.upper(),
                self.other_manager.upper(),
            )
        )


@dataclass(frozen=True, slots=True)
class EffectiveFiling:
    cik: str
    report_period: date
    selected_filing_date: date
    filer_name: str
    selected_accession_number: str
    source_accession_numbers: tuple[str, ...]
    superseded_accession_numbers: tuple[str, ...]
    resolution: str
    package: str


def quarter_range(start: str, end: str) -> list[str]:
    """Return inclusive SEC package labels in YYYYqQ form."""

    start_year, start_quarter = _parse_quarter(start)
    end_year, end_quarter = _parse_quarter(end)
    if (start_year, start_quarter) > (end_year, end_quarter):
        raise ValueError("start quarter must not be after end quarter")
    values: list[str] = []
    year, quarter = start_year, start_quarter
    while (year, quarter) <= (end_year, end_quarter):
        values.append(f"{year}q{quarter}")
        quarter += 1
        if quarter == 5:
            year += 1
            quarter = 1
    return values


def sec_bulk_url(package: str) -> str:
    _parse_quarter(package)
    normalized = package.lower()
    filename = MODERN_SEC_BULK_FILENAMES.get(normalized, f"{normalized}_form13f.zip")
    return f"{SEC_BULK_BASE_URL}/{filename}"


def download_quarter_packages(
    *,
    start: str,
    end: str,
    raw_dir: str | Path,
    user_agent: str,
    request_interval_seconds: float = 0.2,
) -> list[BulkPackageSource]:
    """Download missing SEC packages, verifying reusable files by checksum."""

    if not user_agent.strip() or "@" not in user_agent:
        raise ValueError("user_agent must identify the requester and include a contact email")
    if request_interval_seconds < 0.1:
        raise ValueError("request_interval_seconds must be at least 0.1")

    destination = Path(raw_dir)
    destination.mkdir(parents=True, exist_ok=True)
    sources: list[BulkPackageSource] = []
    for index, package in enumerate(quarter_range(start, end)):
        url = sec_bulk_url(package)
        target = destination / f"{package}_form13f.zip"
        metadata_path = target.with_suffix(".download.json")
        if target.exists() and metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            digest = _sha256_file(target)
            if digest != metadata.get("sha256"):
                raise ValueError(f"Existing download checksum mismatch: {target}")
            sources.append(
                BulkPackageSource(
                    package=package,
                    path=target,
                    source_url=url,
                    retrieved_at=metadata.get("retrieved_at"),
                )
            )
            continue

        if index:
            time.sleep(request_interval_seconds)
        request = Request(url, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"})
        temporary = target.with_suffix(".zip.part")
        try:
            with urlopen(request, timeout=120) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
            with ZipFile(temporary) as archive:
                if archive.testzip() is not None:
                    raise ValueError(f"Downloaded ZIP failed integrity check: {url}")
            _replace_with_retry(temporary, target)
        finally:
            if temporary.exists():
                _unlink_with_retry(temporary)

        retrieved_at = datetime.now(timezone.utc).isoformat()
        metadata = {
            "package": package,
            "source_url": url,
            "retrieved_at": retrieved_at,
            "size_bytes": target.stat().st_size,
            "sha256": _sha256_file(target),
        }
        _write_json_atomic(metadata_path, metadata)
        sources.append(BulkPackageSource(package, target, url, retrieved_at))
    return sources


def build_historical_dataset(
    sources: Iterable[BulkPackageSource],
    *,
    output_root: str | Path,
    manager_ciks: set[str] | None = None,
    security_cusips: set[str] | None = None,
) -> Path:
    """Build an immutable normalized dataset from SEC bulk packages."""

    started = time.perf_counter()
    source_list = sorted(sources, key=lambda item: item.package)
    if not source_list:
        raise ValueError("At least one bulk package source is required")
    normalized_managers = {_normalize_cik(value) for value in manager_ciks or set()}
    normalized_securities = {_normalize_cusip(value) for value in security_cusips or set()}

    source_manifest = [_source_manifest(source) for source in source_list]
    identity_payload = {
        "contract_version": 2,
        "sources": [
            {
                "package": item["package"],
                "source_url": item["source_url"],
                "size_bytes": item["size_bytes"],
                "sha256": item["sha256"],
            }
            for item in source_manifest
        ],
        "manager_ciks": sorted(normalized_managers),
        "security_cusips": sorted(normalized_securities),
    }
    dataset_id = hashlib.sha256(_canonical_json(identity_payload).encode("utf-8")).hexdigest()[:16]
    output_dir = Path(output_root) / f"sec-13f-{dataset_id}"
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("identity") != identity_payload:
            raise ValueError(f"Existing dataset manifest does not match requested identity: {output_dir}")
        return output_dir
    if output_dir.exists():
        raise ValueError(f"Refusing to overwrite incomplete immutable dataset directory: {output_dir}")
    staging_dir = Path(output_root) / f".sec-13f-{dataset_id}.building"
    staging_marker = staging_dir / "identity.json"
    if staging_dir.exists():
        if not staging_marker.exists() or json.loads(staging_marker.read_text(encoding="utf-8")) != identity_payload:
            raise ValueError(f"Refusing to replace unrecognized staging directory: {staging_dir}")
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)
    _write_json_atomic(staging_marker, identity_payload)

    submissions: list[BulkSubmission] = []
    holdings_by_accession: dict[str, list[BulkHolding]] = {}
    invalid_rows: list[dict[str, str]] = []
    raw_holding_count = 0
    for source in source_list:
        package_submissions, package_holdings, package_invalid, package_raw_count = _read_package(
            source,
            manager_ciks=normalized_managers,
            security_cusips=normalized_securities,
        )
        submissions.extend(package_submissions)
        for accession, rows in package_holdings.items():
            holdings_by_accession.setdefault(accession, []).extend(rows)
        invalid_rows.extend(package_invalid)
        raw_holding_count += package_raw_count

    eligible_accessions = {row.accession_number for row in submissions}
    holdings_by_accession = {
        accession: rows for accession, rows in holdings_by_accession.items() if accession in eligible_accessions
    }
    effective_rows, normalized_holdings, duplicate_count, amendment_count = _resolve_effective_filings(
        submissions, holdings_by_accession
    )
    if not effective_rows:
        raise ValueError("No eligible 13F-HR filings remained after filtering and validation")

    _write_effective_filings(staging_dir / "effective_filings.csv", effective_rows)
    _write_normalized_holdings(staging_dir / "normalized_holdings.csv", normalized_holdings)
    _write_invalid_rows(staging_dir / "invalid_rows.csv", invalid_rows)

    normalized_security_count = len({row["security_key"] for row in normalized_holdings})
    identified_count = sum(bool(row["cusip"]) for row in normalized_holdings)
    report = {
        "packages_requested": len(source_list),
        "packages_processed": len(source_list),
        "package_labels": [source.package for source in source_list],
        "submissions_processed": len(submissions),
        "effective_filings": len(effective_rows),
        "raw_holdings": raw_holding_count,
        "normalized_holdings": len(normalized_holdings),
        "unique_managers": len({row.cik for row in effective_rows}),
        "unique_securities": normalized_security_count,
        "duplicates_resolved": duplicate_count,
        "amendments_resolved": amendment_count,
        "invalid_rows_rejected": len(invalid_rows),
        "identifier_coverage": identified_count / len(normalized_holdings) if normalized_holdings else 0.0,
        "runtime_seconds": round(time.perf_counter() - started, 6),
    }
    output_size = sum(
        (staging_dir / name).stat().st_size
        for name in ("effective_filings.csv", "normalized_holdings.csv", "invalid_rows.csv")
    )
    report["output_size_bytes"] = output_size
    _write_json_atomic(staging_dir / "quality_report.json", report)

    manifest = {
        "dataset_id": dataset_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "identity": identity_payload,
        "sources": source_manifest,
        "outputs": {
            path.name: {"size_bytes": path.stat().st_size, "sha256": _sha256_file(path)}
            for path in sorted(staging_dir.iterdir())
            if path.is_file() and path.name not in {"identity.json", "manifest.json"}
        },
    }
    staging_marker.unlink()
    _write_json_atomic(staging_dir / "manifest.json", manifest)
    os.replace(staging_dir, output_dir)
    return output_dir


def materialize_manager_cohort(
    sources: Iterable[BulkPackageSource],
    *,
    output_path: str | Path,
    target_count: int = 50,
    optional_scale_count: int = 99,
    required_quarters: int = 16,
    must_include_report_quarter: date = date(2023, 12, 31),
) -> Path:
    """Select managers from filing-level totals without retaining holding rows."""

    source_list = sorted(sources, key=lambda item: item.package)
    submissions: list[BulkSubmission] = []
    totals_by_accession: dict[str, int] = {}
    skipped_missing_summary_total = 0
    selection_start = date(2019, 3, 31)
    selection_end = date(2023, 12, 31)
    for source in source_list:
        covers = {
            row.get("ACCESSION_NUMBER", "").strip(): row
            for row in _iter_table_rows(source.path, "COVERPAGE")
        }
        summaries = {
            row.get("ACCESSION_NUMBER", "").strip(): row
            for row in _iter_table_rows(source.path, "SUMMARYPAGE")
        }
        for row in _iter_table_rows(source.path, "SUBMISSION"):
            submission_type = row.get("SUBMISSIONTYPE", "").strip().upper()
            if submission_type not in {"13F-HR", "13F-HR/A"}:
                continue
            accession = _required(row, "ACCESSION_NUMBER")
            report_period = _parse_sec_date(_required(row, "PERIODOFREPORT"))
            if not selection_start <= report_period <= selection_end:
                continue
            filing_date = _parse_sec_date(_required(row, "FILING_DATE"))
            cover = covers.get(accession, {})
            summary = summaries.get(accession, {})
            if not summary.get("TABLEVALUETOTAL", "").strip():
                skipped_missing_summary_total += 1
                continue
            raw_total = _required_int(summary, "TABLEVALUETOTAL")
            totals_by_accession[accession] = (
                raw_total if filing_date >= VALUE_UNIT_CHANGE_DATE else raw_total * 1000
            )
            submissions.append(
                BulkSubmission(
                    accession_number=accession,
                    filing_date=filing_date,
                    submission_type=submission_type,
                    cik=_normalize_cik(_required(row, "CIK")),
                    report_period=report_period,
                    filer_name=cover.get("FILINGMANAGER_NAME", "").strip(),
                    is_amendment=submission_type.endswith("/A") or _truthy(cover.get("ISAMENDMENT", "")),
                    amendment_number=_optional_int(cover.get("AMENDMENTNO", "")),
                    amendment_type=cover.get("AMENDMENTTYPE", "").strip().upper(),
                    package=source.package,
                )
            )

    values_by_manager: dict[str, dict[date, int]] = {}
    names_by_manager: dict[str, str] = {}
    grouped: dict[tuple[str, date], list[BulkSubmission]] = {}
    for submission in submissions:
        grouped.setdefault((submission.cik, submission.report_period), []).append(submission)
        if submission.filer_name:
            names_by_manager[submission.cik] = submission.filer_name
    for (cik, report_period), group in grouped.items():
        ordered = sorted(group, key=_submission_order)
        ordinary = [row for row in ordered if row.submission_type == "13F-HR"]
        base = ordinary[-1] if ordinary else ordered[0]
        total = 0
        for submission in (row for row in ordered if _submission_order(row) >= _submission_order(base)):
            value = totals_by_accession[submission.accession_number]
            amendment_type = submission.amendment_type.replace("_", " ")
            if submission.submission_type == "13F-HR" or not (
                "NEW HOLDING" in amendment_type or "ADD" in amendment_type
            ):
                total = value
            else:
                total += value
        values_by_manager.setdefault(cik, {})[report_period] = total

    eligible = []
    for cik, quarter_values in values_by_manager.items():
        if len(quarter_values) < required_quarters or must_include_report_quarter not in quarter_values:
            continue
        eligible.append(
            {
                "cik": cik,
                "filer_name": names_by_manager.get(cik, ""),
                "eligible_quarter_count": len(quarter_values),
                "median_reported_long_market_value_usd": int(statistics.median(quarter_values.values())),
            }
        )
    eligible.sort(key=lambda row: (-row["median_reported_long_market_value_usd"], row["cik"]))
    ordered = eligible[:optional_scale_count]
    main_ciks = [row["cik"] for row in ordered[:target_count]]
    payload = {
        "protocol_version": "v2-design-2",
        "selection_window": [selection_start.isoformat(), selection_end.isoformat()],
        "selection_rule": {
            "required_quarters": required_quarters,
            "must_include_report_quarter": must_include_report_quarter.isoformat(),
            "ranking": "median reported long-market value descending, CIK ascending",
        },
        "eligible_manager_count": len(eligible),
        "skipped_filings_missing_summary_total": skipped_missing_summary_total,
        "main_target_count": target_count,
        "main_actual_count": len(main_ciks),
        "main_ordered_ciks": main_ciks,
        "main_ordered_ciks_sha256": hashlib.sha256(_canonical_json(main_ciks).encode("utf-8")).hexdigest(),
        "engineering_subsets": {"10": main_ciks[:10], "25": main_ciks[:25]},
        "optional_scale_target_count": optional_scale_count,
        "ordered_manager_records": ordered,
        "sources": [
            {key: value for key, value in _source_manifest(source).items() if key != "source_path"}
            for source in source_list
        ],
    }
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(destination, payload)
    return destination


def materialize_source_manifest(sources: Iterable[BulkPackageSource], *, output_path: str | Path) -> Path:
    """Persist portable checksums for an ordered set of downloaded SEC packages."""

    records = [
        {key: value for key, value in _source_manifest(source).items() if key != "source_path"}
        for source in sorted(sources, key=lambda item: item.package)
    ]
    payload = {
        "protocol_version": "v2-design-2",
        "package_count": len(records),
        "first_package": records[0]["package"] if records else None,
        "latest_package": records[-1]["package"] if records else None,
        "prospective_2026q3_included": any(row["package"] == "2026q3" for row in records),
        "packages": records,
    }
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(destination, payload)
    return destination


def _read_package(
    source: BulkPackageSource,
    *,
    manager_ciks: set[str],
    security_cusips: set[str],
) -> tuple[list[BulkSubmission], dict[str, list[BulkHolding]], list[dict[str, str]], int]:
    cover_by_accession = {
        row.get("ACCESSION_NUMBER", "").strip(): row for row in _iter_table_rows(source.path, "COVERPAGE")
    }
    submissions: list[BulkSubmission] = []
    invalid: list[dict[str, str]] = []
    submission_lookup: dict[str, BulkSubmission] = {}
    for row_number, row in enumerate(_iter_table_rows(source.path, "SUBMISSION"), start=2):
        try:
            submission_type = row.get("SUBMISSIONTYPE", "").strip().upper()
            if submission_type not in {"13F-HR", "13F-HR/A"}:
                continue
            accession = _required(row, "ACCESSION_NUMBER")
            cover = cover_by_accession.get(accession, {})
            submission = BulkSubmission(
                accession_number=accession,
                filing_date=_parse_sec_date(_required(row, "FILING_DATE")),
                submission_type=submission_type,
                cik=_normalize_cik(_required(row, "CIK")),
                report_period=_parse_sec_date(_required(row, "PERIODOFREPORT")),
                filer_name=cover.get("FILINGMANAGER_NAME", "").strip(),
                is_amendment=submission_type.endswith("/A") or _truthy(cover.get("ISAMENDMENT", "")),
                amendment_number=_optional_int(cover.get("AMENDMENTNO", "")),
                amendment_type=cover.get("AMENDMENTTYPE", "").strip().upper(),
                package=source.package,
            )
            if manager_ciks and submission.cik not in manager_ciks:
                continue
            submissions.append(submission)
            submission_lookup[accession] = submission
        except (ValueError, KeyError) as exc:
            invalid.append(_invalid(source.package, "SUBMISSION", row_number, str(exc)))

    holdings: dict[str, list[BulkHolding]] = {}
    raw_count = 0
    for row_number, row in enumerate(_iter_table_rows(source.path, "INFOTABLE"), start=2):
        raw_count += 1
        accession = row.get("ACCESSION_NUMBER", "").strip()
        submission = submission_lookup.get(accession)
        if submission is None:
            continue
        try:
            raw_value = _required_int(row, "VALUE")
            value_usd = raw_value if submission.filing_date >= VALUE_UNIT_CHANGE_DATE else raw_value * 1000
            holding = BulkHolding(
                source_accession_number=accession,
                info_table_key=_required(row, "INFOTABLE_SK"),
                issuer_name=_required(row, "NAMEOFISSUER"),
                title_of_class=_required(row, "TITLEOFCLASS"),
                cusip=_normalize_cusip(_required(row, "CUSIP")),
                figi=row.get("FIGI", "").strip().upper(),
                value_usd=value_usd,
                shares_or_principal=_required_float(row, "SSHPRNAMT"),
                share_type=_required(row, "SSHPRNAMTTYPE").upper(),
                put_call=row.get("PUTCALL", "").strip().upper(),
                investment_discretion=_required(row, "INVESTMENTDISCRETION").upper(),
                other_manager=row.get("OTHERMANAGER", "").strip(),
                voting_authority_sole=_optional_int(row.get("VOTING_AUTH_SOLE", "")) or 0,
                voting_authority_shared=_optional_int(row.get("VOTING_AUTH_SHARED", "")) or 0,
                voting_authority_none=_optional_int(row.get("VOTING_AUTH_NONE", "")) or 0,
            )
            if security_cusips and holding.cusip not in security_cusips:
                continue
            if holding.value_usd < 0 or holding.shares_or_principal < 0:
                raise ValueError("VALUE and SSHPRNAMT must be non-negative")
            holdings.setdefault(accession, []).append(holding)
        except (ValueError, KeyError) as exc:
            invalid.append(_invalid(source.package, "INFOTABLE", row_number, str(exc)))
    return submissions, holdings, invalid, raw_count


def _resolve_effective_filings(
    submissions: list[BulkSubmission], holdings_by_accession: Mapping[str, list[BulkHolding]]
) -> tuple[list[EffectiveFiling], list[dict[str, object]], int, int]:
    groups: dict[tuple[str, date], list[BulkSubmission]] = {}
    for submission in submissions:
        groups.setdefault((submission.cik, submission.report_period), []).append(submission)

    effective_filings: list[EffectiveFiling] = []
    normalized_rows: list[dict[str, object]] = []
    duplicates_resolved = 0
    amendments_resolved = 0
    for (cik, report_period), group in sorted(groups.items()):
        ordered = sorted(group, key=_submission_order)
        initial_filings = [row for row in ordered if row.submission_type == "13F-HR"]
        base = initial_filings[-1] if initial_filings else ordered[0]
        effective: dict[str, BulkHolding] = {}
        source_accessions: list[str] = []
        resolution = "initial"

        applicable = [row for row in ordered if _submission_order(row) >= _submission_order(base)]
        for submission in applicable:
            rows, duplicate_delta = _deduplicate_holdings(holdings_by_accession.get(submission.accession_number, []))
            duplicates_resolved += duplicate_delta
            if submission.submission_type == "13F-HR":
                effective = rows
                source_accessions = [submission.accession_number]
                resolution = "initial"
                continue
            amendments_resolved += 1
            amendment_type = submission.amendment_type.replace("_", " ")
            if "NEW HOLDING" in amendment_type or "ADD" in amendment_type:
                for key, holding in rows.items():
                    if key in effective:
                        duplicates_resolved += 1
                    effective[key] = holding
                source_accessions.append(submission.accession_number)
                resolution = "initial_plus_additive_amendment"
            else:
                effective = rows
                source_accessions = [submission.accession_number]
                resolution = "restatement_amendment" if submission.amendment_type else "unspecified_amendment_replacement"

        selected = applicable[-1]
        all_accessions = [row.accession_number for row in ordered]
        effective_filing = EffectiveFiling(
            cik=cik,
            report_period=report_period,
            selected_filing_date=selected.filing_date,
            filer_name=selected.filer_name or base.filer_name,
            selected_accession_number=selected.accession_number,
            source_accession_numbers=tuple(source_accessions),
            superseded_accession_numbers=tuple(value for value in all_accessions if value not in source_accessions),
            resolution=resolution,
            package=selected.package,
        )
        effective_filings.append(effective_filing)
        total_value = sum(row.value_usd for row in effective.values())
        for rank, holding in enumerate(
            sorted(effective.values(), key=lambda row: (-row.value_usd, row.security_key, row.info_table_key)), start=1
        ):
            holding_id = hashlib.sha256(
                f"{cik}|{report_period.isoformat()}|{holding.security_key}".encode("utf-8")
            ).hexdigest()[:24]
            normalized_rows.append(
                {
                    "holding_id": holding_id,
                    "cik": cik,
                    "report_period": report_period.isoformat(),
                    "filing_date": selected.filing_date.isoformat(),
                    "effective_accession_number": selected.accession_number,
                    "source_accession_number": holding.source_accession_number,
                    "security_key": holding.security_key,
                    "issuer_name": holding.issuer_name,
                    "title_of_class": holding.title_of_class,
                    "cusip": holding.cusip,
                    "figi": holding.figi,
                    "value_usd": holding.value_usd,
                    "portfolio_weight": holding.value_usd / total_value if total_value else 0.0,
                    "holding_rank": rank,
                    "shares_or_principal": holding.shares_or_principal,
                    "share_type": holding.share_type,
                    "put_call": holding.put_call,
                    "investment_discretion": holding.investment_discretion,
                    "other_manager": holding.other_manager,
                    "voting_authority_sole": holding.voting_authority_sole,
                    "voting_authority_shared": holding.voting_authority_shared,
                    "voting_authority_none": holding.voting_authority_none,
                }
            )
    return effective_filings, normalized_rows, duplicates_resolved, amendments_resolved


def _deduplicate_holdings(rows: Iterable[BulkHolding]) -> tuple[dict[str, BulkHolding], int]:
    ordered = sorted(rows, key=lambda row: (_numeric_key(row.info_table_key), row.security_key))
    result: dict[str, BulkHolding] = {}
    duplicate_count = 0
    for row in ordered:
        if row.security_key in result:
            duplicate_count += 1
        result[row.security_key] = row
    return result, duplicate_count


def _iter_table_rows(path: Path, table_name: str):
    if path.is_dir():
        files = {item.name.upper(): item for item in path.iterdir() if item.is_file()}
        table_path = _find_table_file(files, table_name)
        with table_path.open("r", encoding="utf-8-sig", newline="") as handle:
            yield from csv.DictReader(handle, delimiter="\t")
        return
    if not path.is_file():
        raise FileNotFoundError(path)
    with ZipFile(path) as archive:
        members = {Path(name).name.upper(): name for name in archive.namelist() if not name.endswith("/")}
        member = _find_table_file(members, table_name)
        with archive.open(member) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text:
            yield from csv.DictReader(text, delimiter="\t")


def _find_table_file(files: Mapping[str, object], table_name: str):
    matches = [value for key, value in files.items() if table_name in key and key.endswith((".TSV", ".TXT"))]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one {table_name} TSV/TXT table, found {len(matches)}")
    return matches[0]


def _write_effective_filings(path: Path, rows: list[EffectiveFiling]) -> None:
    fieldnames = [
        "cik",
        "report_period",
        "selected_filing_date",
        "filer_name",
        "selected_accession_number",
        "source_accession_numbers",
        "superseded_accession_numbers",
        "resolution",
        "package",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            payload["report_period"] = row.report_period.isoformat()
            payload["selected_filing_date"] = row.selected_filing_date.isoformat()
            payload["source_accession_numbers"] = "|".join(row.source_accession_numbers)
            payload["superseded_accession_numbers"] = "|".join(row.superseded_accession_numbers)
            writer.writerow(payload)


def _write_normalized_holdings(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = list(rows[0]) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not fieldnames:
            return
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_invalid_rows(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = ["package", "table", "row_number", "reason"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _source_manifest(source: BulkPackageSource) -> dict[str, object]:
    return {
        "package": source.package,
        "source_url": source.source_url,
        "retrieved_at": source.retrieved_at,
        "source_path": str(source.path.resolve()),
        "size_bytes": _source_size(source.path),
        "sha256": _sha256_source(source.path),
    }


def _source_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _sha256_source(path: Path) -> str:
    if path.is_file():
        return _sha256_file(path)
    digest = hashlib.sha256()
    for item in sorted(value for value in path.rglob("*") if value.is_file()):
        digest.update(item.relative_to(path).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(item.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_atomic(path: Path, payload: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _replace_with_retry(source: Path, target: Path, *, attempts: int = 20, delay_seconds: float = 0.1) -> None:
    """Promote a download despite short-lived Windows file-indexing locks."""

    for attempt in range(attempts):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(delay_seconds)


def _unlink_with_retry(path: Path, *, attempts: int = 20, delay_seconds: float = 0.1) -> None:
    """Remove a temporary download despite short-lived Windows file-indexing locks."""

    for attempt in range(attempts):
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(delay_seconds)


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _parse_quarter(value: str) -> tuple[int, int]:
    normalized = value.strip().lower()
    if len(normalized) != 6 or normalized[4] != "q" or not normalized[:4].isdigit() or normalized[5] not in "1234":
        raise ValueError(f"Invalid quarter {value!r}; expected YYYYqQ")
    return int(normalized[:4]), int(normalized[5])


def _parse_sec_date(value: str) -> date:
    normalized = value.strip()
    for format_string in ("%d-%b-%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(normalized, format_string).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid SEC date: {value!r}")


def _normalize_cik(value: str) -> str:
    digits = "".join(character for character in value if character.isdigit())
    if not digits:
        raise ValueError("CIK is required")
    return digits.zfill(10)


def _normalize_cusip(value: str) -> str:
    normalized = "".join(character for character in value.upper() if character.isalnum() or character in "*@#")
    if len(normalized) != 9:
        raise ValueError(f"CUSIP must contain 9 valid characters: {value!r}")
    return normalized


def _required(row: Mapping[str, str], field: str) -> str:
    value = row.get(field, "").strip()
    if not value:
        raise ValueError(f"{field} is required")
    return value


def _required_int(row: Mapping[str, str], field: str) -> int:
    return int(_required(row, field).replace(",", ""))


def _required_float(row: Mapping[str, str], field: str) -> float:
    return float(_required(row, field).replace(",", ""))


def _optional_int(value: str) -> int | None:
    normalized = value.strip().replace(",", "")
    return int(normalized) if normalized else None


def _truthy(value: str) -> bool:
    return value.strip().upper() in {"1", "TRUE", "Y", "YES", "X"}


def _numeric_key(value: str) -> tuple[int, str]:
    try:
        return int(value), value
    except ValueError:
        return 0, value


def _submission_order(row: BulkSubmission) -> tuple[date, int, str]:
    return row.filing_date, row.amendment_number or 0, row.accession_number


def _invalid(package: str, table: str, row_number: int, reason: str) -> dict[str, str]:
    return {"package": package, "table": table, "row_number": str(row_number), "reason": reason}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download or normalize official SEC Form 13F bulk packages.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download", help="Download an inclusive range of quarterly SEC packages.")
    download.add_argument("--start", required=True, help="First package in YYYYqQ form.")
    download.add_argument("--end", required=True, help="Last package in YYYYqQ form.")
    download.add_argument("--raw-dir", default="data/raw/sec-13f", help="Directory for immutable ZIP downloads.")
    download.add_argument("--user-agent", required=True, help="Requester identity and contact email for SEC access.")
    download.add_argument("--request-interval-seconds", type=float, default=0.2)

    build = subparsers.add_parser("build", help="Build an immutable normalized dataset from local packages.")
    build.add_argument("--source", action="append", required=True, help="PACKAGE=path to a ZIP or extracted directory; repeatable.")
    build.add_argument("--output-root", default="data/historical", help="Root for immutable versioned dataset directories.")
    build.add_argument("--manager-cik", action="append", default=[], help="Optional manager CIK filter; repeatable.")
    build.add_argument("--security-cusip", action="append", default=[], help="Optional CUSIP filter; repeatable.")

    cohort = subparsers.add_parser("cohort", help="Materialize the deterministic Protocol V2 manager cohort.")
    cohort.add_argument("--source", action="append", required=True, help="PACKAGE=path; repeatable.")
    cohort.add_argument("--output", required=True, help="Output JSON path.")

    source_manifest = subparsers.add_parser("source-manifest", help="Persist portable SEC package checksums.")
    source_manifest.add_argument("--source", action="append", required=True, help="PACKAGE=path; repeatable.")
    source_manifest.add_argument("--output", required=True, help="Output JSON path.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "download":
        sources = download_quarter_packages(
            start=args.start,
            end=args.end,
            raw_dir=args.raw_dir,
            user_agent=args.user_agent,
            request_interval_seconds=args.request_interval_seconds,
        )
        for source in sources:
            print(f"{source.package}={source.path}")
        return 0

    sources: list[BulkPackageSource] = []
    for value in args.source:
        if "=" not in value:
            raise SystemExit("Each --source must use PACKAGE=path format")
        package, raw_path = value.split("=", 1)
        sources.append(BulkPackageSource(package.lower(), Path(raw_path), sec_bulk_url(package)))
    if args.command == "cohort":
        output = materialize_manager_cohort(sources, output_path=args.output)
        print(output)
        return 0
    if args.command == "source-manifest":
        output = materialize_source_manifest(sources, output_path=args.output)
        print(output)
        return 0

    output = build_historical_dataset(
        sources,
        output_root=args.output_root,
        manager_ciks=set(args.manager_cik),
        security_cusips=set(args.security_cusip),
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
