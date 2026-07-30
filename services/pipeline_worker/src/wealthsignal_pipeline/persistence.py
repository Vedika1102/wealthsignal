from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from .delta_engine import holding_key
from .models import FilingArtifacts, FilingDelta, FilingReference, Holding, ParsedInformationTable


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection for WealthSignal local development."""

    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    """Create the local SQLite schema used by the pipeline."""

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS filings (
            accession_number TEXT PRIMARY KEY,
            cik TEXT NOT NULL,
            filing_date TEXT,
            report_period TEXT,
            form_type TEXT NOT NULL,
            filer_name TEXT,
            primary_document_url TEXT,
            information_table_url TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS holdings (
            accession_number TEXT NOT NULL,
            holding_key TEXT NOT NULL,
            issuer_name TEXT NOT NULL,
            title_of_class TEXT NOT NULL,
            cusip TEXT NOT NULL,
            value_thousands INTEGER NOT NULL,
            shares_or_principal REAL NOT NULL,
            share_type TEXT NOT NULL,
            put_call TEXT NOT NULL,
            investment_discretion TEXT NOT NULL,
            other_manager TEXT,
            voting_authority_sole INTEGER NOT NULL,
            voting_authority_shared INTEGER NOT NULL,
            voting_authority_none INTEGER NOT NULL,
            PRIMARY KEY (accession_number, holding_key),
            FOREIGN KEY (accession_number) REFERENCES filings(accession_number)
        );

        CREATE TABLE IF NOT EXISTS position_deltas (
            current_accession_number TEXT NOT NULL,
            previous_accession_number TEXT NOT NULL,
            holding_key TEXT NOT NULL,
            issuer_name TEXT NOT NULL,
            cusip TEXT NOT NULL,
            old_value_thousands INTEGER NOT NULL,
            new_value_thousands INTEGER NOT NULL,
            old_shares REAL NOT NULL,
            new_shares REAL NOT NULL,
            old_weight REAL NOT NULL,
            new_weight REAL NOT NULL,
            is_new_position INTEGER NOT NULL,
            is_exited_position INTEGER NOT NULL,
            share_type TEXT NOT NULL,
            value_delta_thousands INTEGER NOT NULL,
            shares_delta REAL NOT NULL,
            value_pct_change REAL,
            shares_pct_change REAL,
            rank_delta INTEGER,
            PRIMARY KEY (current_accession_number, previous_accession_number, holding_key)
        );

        CREATE INDEX IF NOT EXISTS idx_filings_cik_period
            ON filings(cik, report_period DESC, filing_date DESC);
        """
    )
    connection.commit()


def store_parsed_filing(
    connection: sqlite3.Connection,
    parsed: ParsedInformationTable,
    *,
    artifacts: FilingArtifacts | None = None,
) -> None:
    """Persist a parsed filing and its normalized holdings."""

    filing = parsed.filing
    connection.execute(
        """
        INSERT OR REPLACE INTO filings (
            accession_number, cik, filing_date, report_period, form_type, filer_name,
            primary_document_url, information_table_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filing.accession_number,
            filing.cik,
            filing.filing_date.isoformat() if filing.filing_date else None,
            filing.report_period.isoformat() if filing.report_period else None,
            filing.form_type,
            filing.filer_name,
            artifacts.primary_document_url if artifacts else None,
            artifacts.information_table_url if artifacts else None,
        ),
    )

    connection.execute("DELETE FROM holdings WHERE accession_number = ?", (filing.accession_number,))
    connection.executemany(
        """
        INSERT INTO holdings (
            accession_number, holding_key, issuer_name, title_of_class, cusip, value_thousands,
            shares_or_principal, share_type, put_call, investment_discretion, other_manager,
            voting_authority_sole, voting_authority_shared, voting_authority_none
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                filing.accession_number,
                holding_key(holding),
                holding.issuer_name,
                holding.title_of_class,
                holding.cusip,
                holding.value_thousands,
                holding.shares_or_principal,
                holding.share_type,
                holding.put_call,
                holding.investment_discretion,
                holding.other_manager,
                holding.voting_authority_sole,
                holding.voting_authority_shared,
                holding.voting_authority_none,
            )
            for holding in parsed.holdings
        ],
    )
    connection.commit()


def store_filing_delta(connection: sqlite3.Connection, delta: FilingDelta) -> None:
    """Persist the quarter-over-quarter delta output."""

    connection.execute(
        """
        DELETE FROM position_deltas
        WHERE current_accession_number = ? AND previous_accession_number = ?
        """,
        (delta.current_filing.accession_number, delta.previous_filing.accession_number),
    )
    connection.executemany(
        """
        INSERT INTO position_deltas (
            current_accession_number, previous_accession_number, holding_key, issuer_name, cusip,
            old_value_thousands, new_value_thousands, old_shares, new_shares, old_weight, new_weight,
            is_new_position, is_exited_position, share_type, value_delta_thousands, shares_delta,
            value_pct_change, shares_pct_change, rank_delta
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                delta.current_filing.accession_number,
                delta.previous_filing.accession_number,
                position.holding_key,
                position.issuer_name,
                position.cusip,
                position.old_value_thousands,
                position.new_value_thousands,
                position.old_shares,
                position.new_shares,
                position.old_weight,
                position.new_weight,
                int(position.is_new_position),
                int(position.is_exited_position),
                position.share_type,
                position.value_delta_thousands,
                position.shares_delta,
                position.value_pct_change,
                position.shares_pct_change,
                position.rank_delta,
            )
            for position in delta.positions
        ],
    )
    connection.commit()


def load_parsed_filing(connection: sqlite3.Connection, accession_number: str) -> ParsedInformationTable | None:
    """Reconstruct a parsed filing from local SQLite storage."""

    filing_row = connection.execute(
        """
        SELECT accession_number, cik, filing_date, report_period, form_type, filer_name
        FROM filings
        WHERE accession_number = ?
        """,
        (accession_number,),
    ).fetchone()
    if filing_row is None:
        return None

    holding_rows = connection.execute(
        """
        SELECT issuer_name, title_of_class, cusip, value_thousands, shares_or_principal,
               share_type, put_call, investment_discretion, other_manager,
               voting_authority_sole, voting_authority_shared, voting_authority_none
        FROM holdings
        WHERE accession_number = ?
        ORDER BY value_thousands DESC, issuer_name ASC
        """,
        (accession_number,),
    ).fetchall()

    filing = FilingReference(
        cik=filing_row["cik"],
        accession_number=filing_row["accession_number"],
        filing_date=_parse_iso_date(filing_row["filing_date"]),
        report_period=_parse_iso_date(filing_row["report_period"]),
        form_type=filing_row["form_type"],
        filer_name=filing_row["filer_name"],
    )
    holdings = [
        Holding(
            issuer_name=row["issuer_name"],
            title_of_class=row["title_of_class"],
            cusip=row["cusip"],
            value_thousands=row["value_thousands"],
            shares_or_principal=row["shares_or_principal"],
            share_type=row["share_type"],
            put_call=row["put_call"],
            investment_discretion=row["investment_discretion"],
            other_manager=row["other_manager"],
            voting_authority_sole=row["voting_authority_sole"],
            voting_authority_shared=row["voting_authority_shared"],
            voting_authority_none=row["voting_authority_none"],
        )
        for row in holding_rows
    ]
    return ParsedInformationTable(filing=filing, holdings=holdings)


def load_latest_filing_accessions(
    connection: sqlite3.Connection,
    cik: str,
    *,
    limit: int = 2,
) -> list[str]:
    """Return the most recent locally stored filing accessions for a CIK."""

    rows = connection.execute(
        """
        SELECT accession_number
        FROM filings
        WHERE cik = ?
        ORDER BY report_period DESC, filing_date DESC, accession_number DESC
        LIMIT ?
        """,
        (cik, limit),
    ).fetchall()
    return [row["accession_number"] for row in rows]


def _parse_iso_date(value: str | None):
    if not value:
        return None
    return date.fromisoformat(value)
