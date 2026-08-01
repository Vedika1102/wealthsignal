from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:  # pragma: no cover - exercised only when PostgreSQL deps are installed
    psycopg2 = None
    RealDictCursor = None

from .delta_engine import holding_key
from .models import (
    ClientHolding,
    ClientImpact,
    ClientPortfolio,
    ClientRecommendation,
    FilingArtifacts,
    FilingDelta,
    FilingReference,
    Holding,
    MaterialityAssessment,
    ModelPrediction,
    ModelRunSummary,
    ParsedInformationTable,
    PersistedAlert,
    PersistedFeatureRow,
    PersistedRecommendation,
)


SQLITE_SCHEMA = """
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
    official_issuer_name TEXT,
    official_class_title TEXT,
    ticker TEXT,
    official_list_match INTEGER NOT NULL DEFAULT 0,
    official_list_source TEXT,
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

CREATE TABLE IF NOT EXISTS alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
    current_accession_number TEXT NOT NULL,
    previous_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    sector TEXT NOT NULL,
    score INTEGER NOT NULL,
    severity TEXT NOT NULL,
    should_alert INTEGER NOT NULL,
    reasons_json TEXT NOT NULL,
    current_weight REAL NOT NULL,
    previous_weight REAL NOT NULL,
    weight_delta REAL NOT NULL,
    current_rank INTEGER,
    previous_rank INTEGER,
    turnover_ratio REAL NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alert_impacts (
    alert_id INTEGER NOT NULL,
    client_id TEXT NOT NULL,
    client_name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    cusip TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    sector TEXT NOT NULL,
    direct_weight REAL NOT NULL,
    sector_weight REAL NOT NULL,
    impact_score INTEGER NOT NULL,
    impact_label TEXT NOT NULL,
    PRIMARY KEY (alert_id, client_id),
    FOREIGN KEY (alert_id) REFERENCES alerts(alert_id)
);

CREATE TABLE IF NOT EXISTS client_portfolios (
    client_id TEXT PRIMARY KEY,
    client_name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_holdings (
    client_id TEXT NOT NULL,
    cusip TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    sector TEXT NOT NULL,
    weight REAL NOT NULL CHECK (weight >= 0 AND weight <= 1),
    PRIMARY KEY (client_id, cusip),
    FOREIGN KEY (client_id) REFERENCES client_portfolios(client_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id INTEGER NOT NULL,
    client_id TEXT NOT NULL,
    client_name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    current_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    sector TEXT NOT NULL,
    alert_score INTEGER NOT NULL,
    alert_severity TEXT NOT NULL,
    relevance_score INTEGER NOT NULL,
    content_similarity REAL NOT NULL,
    direct_weight REAL NOT NULL,
    sector_weight REAL NOT NULL,
    precedents_json TEXT NOT NULL,
    rationale_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (alert_id) REFERENCES alerts(alert_id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_current_accession
    ON alerts(current_accession_number, score DESC);

CREATE INDEX IF NOT EXISTS idx_recommendations_client
    ON recommendations(client_id, relevance_score DESC, recommendation_id DESC);

CREATE TABLE IF NOT EXISTS position_features (
    current_accession_number TEXT NOT NULL,
    previous_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    sector TEXT NOT NULL,
    current_value_thousands INTEGER NOT NULL,
    previous_value_thousands INTEGER NOT NULL,
    current_weight REAL NOT NULL,
    previous_weight REAL NOT NULL,
    weight_delta REAL NOT NULL,
    abs_weight_delta REAL NOT NULL,
    value_delta_thousands INTEGER NOT NULL,
    abs_value_delta_thousands INTEGER NOT NULL,
    value_pct_change REAL,
    shares_pct_change REAL,
    is_new_position INTEGER NOT NULL,
    is_exited_position INTEGER NOT NULL,
    current_rank INTEGER,
    previous_rank INTEGER,
    entered_top10 INTEGER NOT NULL,
    exited_top10 INTEGER NOT NULL,
    entered_top20 INTEGER NOT NULL,
    exited_top20 INTEGER NOT NULL,
    turnover_ratio REAL NOT NULL,
    change_share_of_turnover REAL NOT NULL,
    rule_score INTEGER NOT NULL,
    weak_label INTEGER NOT NULL,
    PRIMARY KEY (current_accession_number, previous_accession_number, holding_key)
);

CREATE TABLE IF NOT EXISTS model_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name TEXT NOT NULL,
    training_samples INTEGER NOT NULL,
    positive_count INTEGER NOT NULL,
    feature_names_json TEXT NOT NULL,
    coefficients_json TEXT NOT NULL,
    intercept REAL NOT NULL,
    metrics_json TEXT NOT NULL,
    comparison_group_id TEXT,
    best_params_json TEXT,
    calibration_curve_json TEXT,
    shap_feature_importance_json TEXT,
    artifact_path TEXT,
    is_best_model INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_predictions (
    run_id INTEGER NOT NULL,
    current_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    probability REAL NOT NULL,
    predicted_label INTEGER NOT NULL,
    weak_label INTEGER NOT NULL,
    rule_score INTEGER NOT NULL,
    PRIMARY KEY (run_id, current_accession_number, holding_key),
    FOREIGN KEY (run_id) REFERENCES model_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_model_predictions_current
    ON model_predictions(current_accession_number, probability DESC);
"""


POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS filings (
    accession_number TEXT PRIMARY KEY,
    cik TEXT NOT NULL,
    filing_date TEXT,
    report_period TEXT,
    form_type TEXT NOT NULL,
    filer_name TEXT,
    primary_document_url TEXT,
    information_table_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    official_issuer_name TEXT,
    official_class_title TEXT,
    ticker TEXT,
    official_list_match INTEGER NOT NULL DEFAULT 0,
    official_list_source TEXT,
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

CREATE TABLE IF NOT EXISTS alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    current_accession_number TEXT NOT NULL,
    previous_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    sector TEXT NOT NULL,
    score INTEGER NOT NULL,
    severity TEXT NOT NULL,
    should_alert INTEGER NOT NULL,
    reasons_json TEXT NOT NULL,
    current_weight REAL NOT NULL,
    previous_weight REAL NOT NULL,
    weight_delta REAL NOT NULL,
    current_rank INTEGER,
    previous_rank INTEGER,
    turnover_ratio REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alert_impacts (
    alert_id BIGINT NOT NULL,
    client_id TEXT NOT NULL,
    client_name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    cusip TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    sector TEXT NOT NULL,
    direct_weight REAL NOT NULL,
    sector_weight REAL NOT NULL,
    impact_score INTEGER NOT NULL,
    impact_label TEXT NOT NULL,
    PRIMARY KEY (alert_id, client_id),
    FOREIGN KEY (alert_id) REFERENCES alerts(alert_id)
);

CREATE TABLE IF NOT EXISTS client_portfolios (
    client_id TEXT PRIMARY KEY,
    client_name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_holdings (
    client_id TEXT NOT NULL,
    cusip TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    sector TEXT NOT NULL,
    weight DOUBLE PRECISION NOT NULL CHECK (weight >= 0 AND weight <= 1),
    PRIMARY KEY (client_id, cusip),
    FOREIGN KEY (client_id) REFERENCES client_portfolios(client_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id BIGSERIAL PRIMARY KEY,
    alert_id BIGINT NOT NULL,
    client_id TEXT NOT NULL,
    client_name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    current_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    sector TEXT NOT NULL,
    alert_score INTEGER NOT NULL,
    alert_severity TEXT NOT NULL,
    relevance_score INTEGER NOT NULL,
    content_similarity REAL NOT NULL,
    direct_weight REAL NOT NULL,
    sector_weight REAL NOT NULL,
    precedents_json TEXT NOT NULL,
    rationale_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (alert_id) REFERENCES alerts(alert_id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_current_accession
    ON alerts(current_accession_number, score DESC);

CREATE INDEX IF NOT EXISTS idx_recommendations_client
    ON recommendations(client_id, relevance_score DESC, recommendation_id DESC);

CREATE TABLE IF NOT EXISTS position_features (
    current_accession_number TEXT NOT NULL,
    previous_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    sector TEXT NOT NULL,
    current_value_thousands INTEGER NOT NULL,
    previous_value_thousands INTEGER NOT NULL,
    current_weight REAL NOT NULL,
    previous_weight REAL NOT NULL,
    weight_delta REAL NOT NULL,
    abs_weight_delta REAL NOT NULL,
    value_delta_thousands INTEGER NOT NULL,
    abs_value_delta_thousands INTEGER NOT NULL,
    value_pct_change REAL,
    shares_pct_change REAL,
    is_new_position INTEGER NOT NULL,
    is_exited_position INTEGER NOT NULL,
    current_rank INTEGER,
    previous_rank INTEGER,
    entered_top10 INTEGER NOT NULL,
    exited_top10 INTEGER NOT NULL,
    entered_top20 INTEGER NOT NULL,
    exited_top20 INTEGER NOT NULL,
    turnover_ratio REAL NOT NULL,
    change_share_of_turnover REAL NOT NULL,
    rule_score INTEGER NOT NULL,
    weak_label INTEGER NOT NULL,
    PRIMARY KEY (current_accession_number, previous_accession_number, holding_key)
);

CREATE TABLE IF NOT EXISTS model_runs (
    run_id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    training_samples INTEGER NOT NULL,
    positive_count INTEGER NOT NULL,
    feature_names_json TEXT NOT NULL,
    coefficients_json TEXT NOT NULL,
    intercept REAL NOT NULL,
    metrics_json TEXT NOT NULL,
    comparison_group_id TEXT,
    best_params_json TEXT,
    calibration_curve_json TEXT,
    shap_feature_importance_json TEXT,
    artifact_path TEXT,
    is_best_model INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_predictions (
    run_id BIGINT NOT NULL,
    current_accession_number TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    probability REAL NOT NULL,
    predicted_label INTEGER NOT NULL,
    weak_label INTEGER NOT NULL,
    rule_score INTEGER NOT NULL,
    PRIMARY KEY (run_id, current_accession_number, holding_key),
    FOREIGN KEY (run_id) REFERENCES model_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_model_predictions_current
    ON model_predictions(current_accession_number, probability DESC);
"""


class DatabaseCursor:
    """Small compatibility wrapper over sqlite3 and psycopg2 cursors."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def lastrowid(self) -> int | None:
        value = getattr(self._cursor, "lastrowid", None)
        return int(value) if value is not None else None

    def fetchone(self) -> Mapping[str, Any] | None:
        row = self._cursor.fetchone()
        if row is None:
            return None
        return row

    def fetchall(self) -> list[Mapping[str, Any]]:
        return list(self._cursor.fetchall())


class DatabaseConnection:
    """Backend-neutral connection wrapper for SQLite and PostgreSQL."""

    def __init__(self, raw_connection: Any, *, backend: str) -> None:
        self._raw_connection = raw_connection
        self.backend = backend

    def execute(self, sql: str, parameters: tuple[object, ...] | list[object] = ()) -> DatabaseCursor:
        if self.backend == "sqlite":
            return DatabaseCursor(self._raw_connection.execute(sql, parameters))

        cursor = self._raw_connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(_prepare_sql_for_backend(sql, backend=self.backend), parameters)
        return DatabaseCursor(cursor)

    def executemany(self, sql: str, parameter_sets: list[tuple[object, ...]]) -> DatabaseCursor:
        if self.backend == "sqlite":
            return DatabaseCursor(self._raw_connection.executemany(sql, parameter_sets))

        cursor = self._raw_connection.cursor(cursor_factory=RealDictCursor)
        cursor.executemany(_prepare_sql_for_backend(sql, backend=self.backend), parameter_sets)
        return DatabaseCursor(cursor)

    def executescript(self, sql_script: str) -> None:
        if self.backend == "sqlite":
            self._raw_connection.executescript(sql_script)
            return

        for statement in _split_sql_script(sql_script):
            self.execute(statement)

    def commit(self) -> None:
        self._raw_connection.commit()

    def close(self) -> None:
        self._raw_connection.close()


def connect(db_path: str | Path | None = None) -> DatabaseConnection:
    """Open a database connection driven by DATABASE_URL when available."""

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        sqlite_path = str(db_path) if db_path is not None else "data/wealthsignal.db"
        sqlite_connection = sqlite3.connect(sqlite_path)
        sqlite_connection.row_factory = sqlite3.Row
        return DatabaseConnection(sqlite_connection, backend="sqlite")

    backend, target = _resolve_database_target(database_url)
    if backend == "sqlite":
        sqlite_connection = sqlite3.connect(target)
        sqlite_connection.row_factory = sqlite3.Row
        return DatabaseConnection(sqlite_connection, backend="sqlite")

    if psycopg2 is None or RealDictCursor is None:
        raise RuntimeError("psycopg2-binary is required when DATABASE_URL points to PostgreSQL")

    postgres_connection = psycopg2.connect(target)
    return DatabaseConnection(postgres_connection, backend="postgres")


def initialize_database(connection: DatabaseConnection) -> None:
    """Create the local database schema used by the pipeline."""

    schema = POSTGRES_SCHEMA if connection.backend == "postgres" else SQLITE_SCHEMA
    connection.executescript(schema)
    _ensure_column(connection, "holdings", "official_issuer_name", "TEXT")
    _ensure_column(connection, "holdings", "official_class_title", "TEXT")
    _ensure_column(connection, "holdings", "ticker", "TEXT")
    _ensure_column(connection, "holdings", "official_list_match", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "holdings", "official_list_source", "TEXT")
    _ensure_column(connection, "model_runs", "comparison_group_id", "TEXT")
    _ensure_column(connection, "model_runs", "best_params_json", "TEXT")
    _ensure_column(connection, "model_runs", "calibration_curve_json", "TEXT")
    _ensure_column(connection, "model_runs", "shap_feature_importance_json", "TEXT")
    _ensure_column(connection, "model_runs", "artifact_path", "TEXT")
    _ensure_column(connection, "model_runs", "is_best_model", "INTEGER NOT NULL DEFAULT 0")
    connection.commit()


def store_parsed_filing(
    connection: DatabaseConnection,
    parsed: ParsedInformationTable,
    *,
    artifacts: FilingArtifacts | None = None,
) -> None:
    """Persist a parsed filing and its normalized holdings."""

    filing = parsed.filing
    filing_values = (
        filing.accession_number,
        filing.cik,
        filing.filing_date.isoformat() if filing.filing_date else None,
        filing.report_period.isoformat() if filing.report_period else None,
        filing.form_type,
        filing.filer_name,
        artifacts.primary_document_url if artifacts else None,
        artifacts.information_table_url if artifacts else None,
    )

    if connection.backend == "postgres":
        connection.execute(
            """
            INSERT INTO filings (
                accession_number, cik, filing_date, report_period, form_type, filer_name,
                primary_document_url, information_table_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (accession_number) DO UPDATE SET
                cik = EXCLUDED.cik,
                filing_date = EXCLUDED.filing_date,
                report_period = EXCLUDED.report_period,
                form_type = EXCLUDED.form_type,
                filer_name = EXCLUDED.filer_name,
                primary_document_url = EXCLUDED.primary_document_url,
                information_table_url = EXCLUDED.information_table_url
            """,
            filing_values,
        )
    else:
        connection.execute(
            """
            INSERT OR REPLACE INTO filings (
                accession_number, cik, filing_date, report_period, form_type, filer_name,
                primary_document_url, information_table_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            filing_values,
        )

    connection.execute("DELETE FROM holdings WHERE accession_number = ?", (filing.accession_number,))
    connection.executemany(
        """
        INSERT INTO holdings (
            accession_number, holding_key, issuer_name, title_of_class, cusip, value_thousands,
            shares_or_principal, share_type, put_call, investment_discretion, other_manager,
            voting_authority_sole, voting_authority_shared, voting_authority_none,
            official_issuer_name, official_class_title, ticker, official_list_match, official_list_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                holding.official_issuer_name,
                holding.official_class_title,
                holding.ticker,
                int(holding.official_list_match),
                holding.official_list_source,
            )
            for holding in parsed.holdings
        ],
    )
    connection.commit()


def store_filing_delta(connection: DatabaseConnection, delta: FilingDelta) -> None:
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


def load_parsed_filing(connection: DatabaseConnection, accession_number: str) -> ParsedInformationTable | None:
    """Reconstruct a parsed filing from local storage."""

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
               voting_authority_sole, voting_authority_shared, voting_authority_none,
               official_issuer_name, official_class_title, ticker, official_list_match, official_list_source
        FROM holdings
        WHERE accession_number = ?
        ORDER BY value_thousands DESC, issuer_name ASC
        """,
        (accession_number,),
    ).fetchall()

    filing = FilingReference(
        cik=str(filing_row["cik"]),
        accession_number=str(filing_row["accession_number"]),
        filing_date=_parse_iso_date(filing_row["filing_date"]),
        report_period=_parse_iso_date(filing_row["report_period"]),
        form_type=str(filing_row["form_type"]),
        filer_name=filing_row["filer_name"],
    )
    holdings = [
        Holding(
            issuer_name=str(row["issuer_name"]),
            title_of_class=str(row["title_of_class"]),
            cusip=str(row["cusip"]),
            value_thousands=int(row["value_thousands"]),
            shares_or_principal=float(row["shares_or_principal"]),
            share_type=str(row["share_type"]),
            put_call=str(row["put_call"]),
            investment_discretion=str(row["investment_discretion"]),
            other_manager=row["other_manager"],
            voting_authority_sole=int(row["voting_authority_sole"]),
            voting_authority_shared=int(row["voting_authority_shared"]),
            voting_authority_none=int(row["voting_authority_none"]),
            official_issuer_name=row["official_issuer_name"],
            official_class_title=row["official_class_title"],
            ticker=row["ticker"],
            official_list_match=bool(row["official_list_match"]),
            official_list_source=row["official_list_source"],
        )
        for row in holding_rows
    ]
    return ParsedInformationTable(filing=filing, holdings=holdings)


def load_latest_filing_accessions(
    connection: DatabaseConnection,
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
    return [str(row["accession_number"]) for row in rows]


def store_feature_rows(
    connection: DatabaseConnection,
    feature_rows: list[PersistedFeatureRow],
) -> None:
    """Persist feature rows used for weak labeling and model training."""

    if not feature_rows:
        return

    current_accession_number = feature_rows[0].current_accession_number
    previous_accession_number = feature_rows[0].previous_accession_number
    connection.execute(
        """
        DELETE FROM position_features
        WHERE current_accession_number = ? AND previous_accession_number = ?
        """,
        (current_accession_number, previous_accession_number),
    )
    connection.executemany(
        """
        INSERT INTO position_features (
            current_accession_number, previous_accession_number, holding_key, issuer_name, cusip, sector,
            current_value_thousands, previous_value_thousands, current_weight, previous_weight, weight_delta,
            abs_weight_delta, value_delta_thousands, abs_value_delta_thousands, value_pct_change,
            shares_pct_change, is_new_position, is_exited_position, current_rank, previous_rank,
            entered_top10, exited_top10, entered_top20, exited_top20, turnover_ratio,
            change_share_of_turnover, rule_score, weak_label
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.current_accession_number,
                row.previous_accession_number,
                row.holding_key,
                row.issuer_name,
                row.cusip,
                row.sector,
                row.current_value_thousands,
                row.previous_value_thousands,
                row.current_weight,
                row.previous_weight,
                row.weight_delta,
                row.abs_weight_delta,
                row.value_delta_thousands,
                row.abs_value_delta_thousands,
                row.value_pct_change,
                row.shares_pct_change,
                int(row.is_new_position),
                int(row.is_exited_position),
                row.current_rank,
                row.previous_rank,
                int(row.entered_top10),
                int(row.exited_top10),
                int(row.entered_top20),
                int(row.exited_top20),
                row.turnover_ratio,
                row.change_share_of_turnover,
                row.rule_score,
                row.weak_label,
            )
            for row in feature_rows
        ],
    )
    connection.commit()


def load_feature_rows(connection: DatabaseConnection, *, limit: int | None = None) -> list[PersistedFeatureRow]:
    """Load feature rows for model training."""

    query = """
        SELECT *
        FROM position_features
        ORDER BY current_accession_number DESC, abs_value_delta_thousands DESC
    """
    parameters: tuple[object, ...] = ()
    if limit is not None:
        query += " LIMIT ?"
        parameters = (limit,)
    rows = connection.execute(query, parameters).fetchall()
    return [_row_to_feature_row(row) for row in rows]


def store_model_run(
    connection: DatabaseConnection,
    *,
    model_name: str,
    training_samples: int,
    positive_count: int,
    feature_names: list[str],
    coefficients: list[float],
    intercept: float,
    metrics: dict[str, float],
    predictions: list[ModelPrediction],
    comparison_group_id: str | None = None,
    best_params: dict[str, object] | None = None,
    calibration_curve: list[dict[str, float]] | None = None,
    shap_feature_importance: list[dict[str, float]] | None = None,
    artifact_path: str | None = None,
    is_best_model: bool = False,
) -> int:
    """Persist one model run and its predictions."""

    insert_sql = """
        INSERT INTO model_runs (
            model_name, training_samples, positive_count, feature_names_json,
            coefficients_json, intercept, metrics_json, comparison_group_id,
            best_params_json, calibration_curve_json, shap_feature_importance_json,
            artifact_path, is_best_model
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    if connection.backend == "postgres":
        insert_sql += " RETURNING run_id"

    cursor = connection.execute(
        insert_sql,
        (
            model_name,
            training_samples,
            positive_count,
            json.dumps(feature_names),
            json.dumps(coefficients),
            intercept,
            json.dumps(metrics),
            comparison_group_id,
            json.dumps(best_params) if best_params is not None else None,
            json.dumps(calibration_curve) if calibration_curve is not None else None,
            json.dumps(shap_feature_importance) if shap_feature_importance is not None else None,
            artifact_path,
            int(is_best_model),
        ),
    )
    if connection.backend == "postgres":
        inserted_row = cursor.fetchone()
        if inserted_row is None:
            raise RuntimeError("PostgreSQL did not return a model run id")
        run_id = int(inserted_row["run_id"])
    else:
        run_id = int(cursor.lastrowid or 0)

    if predictions:
        connection.executemany(
            """
            INSERT INTO model_predictions (
                run_id, current_accession_number, holding_key, issuer_name, cusip,
                probability, predicted_label, weak_label, rule_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    prediction.current_accession_number,
                    prediction.holding_key,
                    prediction.issuer_name,
                    prediction.cusip,
                    prediction.probability,
                    prediction.predicted_label,
                    prediction.weak_label,
                    prediction.rule_score,
                )
                for prediction in predictions
            ],
        )
    connection.commit()
    return run_id


def get_latest_model_run(connection: DatabaseConnection) -> ModelRunSummary | None:
    """Return the latest effective model run, preferring the best model in the newest comparison set."""

    latest_runs = list_latest_model_runs(connection)
    if not latest_runs:
        return None
    return latest_runs[0]


def list_latest_model_runs(connection: DatabaseConnection) -> list[ModelRunSummary]:
    """Return the latest model comparison set, or the latest single model run."""

    latest_row = connection.execute(
        """
        SELECT comparison_group_id, run_id
        FROM model_runs
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if latest_row is None:
        return []

    comparison_group_id = latest_row["comparison_group_id"]
    if comparison_group_id:
        rows = connection.execute(
            """
            SELECT run_id, model_name, training_samples, positive_count, feature_names_json,
                   coefficients_json, intercept, metrics_json, comparison_group_id,
                   best_params_json, calibration_curve_json, shap_feature_importance_json,
                   artifact_path, is_best_model
            FROM model_runs
            WHERE comparison_group_id = ?
            ORDER BY is_best_model DESC, run_id DESC
            """,
            (comparison_group_id,),
        ).fetchall()
    else:
        rows = connection.execute(
            """
            SELECT run_id, model_name, training_samples, positive_count, feature_names_json,
                   coefficients_json, intercept, metrics_json, comparison_group_id,
                   best_params_json, calibration_curve_json, shap_feature_importance_json,
                   artifact_path, is_best_model
            FROM model_runs
            WHERE run_id = ?
            """,
            (latest_row["run_id"],),
        ).fetchall()

    return [_row_to_model_run(row) for row in rows]


def get_latest_prediction_lookup(connection: DatabaseConnection) -> dict[tuple[str, str], ModelPrediction]:
    """Return the latest model predictions keyed by accession and holding key."""

    latest_run = get_latest_model_run(connection)
    if latest_run is None:
        return {}

    rows = connection.execute(
        """
        SELECT run_id, current_accession_number, holding_key, issuer_name, cusip,
               probability, predicted_label, weak_label, rule_score
        FROM model_predictions
        WHERE run_id = ?
        """,
        (latest_run.run_id,),
    ).fetchall()

    return {
        (str(row["current_accession_number"]), str(row["holding_key"])): ModelPrediction(
            run_id=int(row["run_id"]),
            current_accession_number=str(row["current_accession_number"]),
            holding_key=str(row["holding_key"]),
            issuer_name=str(row["issuer_name"]),
            cusip=str(row["cusip"]),
            probability=float(row["probability"]),
            predicted_label=int(row["predicted_label"]),
            weak_label=int(row["weak_label"]),
            rule_score=int(row["rule_score"]),
        )
        for row in rows
    }


def store_model_comparison_bundle(
    connection: DatabaseConnection,
    *,
    bundle: object,
    feature_rows: list[PersistedFeatureRow],
    artifact_path: str | None = None,
) -> tuple[str, list[int]]:
    """Persist a multi-model training bundle under one comparison group id."""

    comparison_group_id = str(uuid4())
    training_samples = len(feature_rows)
    positive_count = sum(row.weak_label for row in feature_rows)
    run_ids: list[int] = []
    for result in bundle.results:
        if len(result.predicted_probabilities) != len(feature_rows) or len(result.predicted_labels) != len(feature_rows):
            raise ValueError("Model comparison result predictions do not align with the feature rows used for training")

        coefficients, intercept = _extract_estimator_parameters(result.estimator)
        predictions = [
            ModelPrediction(
                run_id=0,
                current_accession_number=row.current_accession_number,
                holding_key=row.holding_key,
                issuer_name=row.issuer_name,
                cusip=row.cusip,
                probability=float(probability),
                predicted_label=int(predicted_label),
                weak_label=row.weak_label,
                rule_score=row.rule_score,
            )
            for row, probability, predicted_label in zip(
                feature_rows,
                result.predicted_probabilities,
                result.predicted_labels,
            )
        ]
        run_ids.append(
            store_model_run(
                connection,
                model_name=result.model_name,
                training_samples=training_samples,
                positive_count=positive_count,
                feature_names=result.feature_names,
                coefficients=coefficients,
                intercept=intercept,
                metrics=result.metrics,
                predictions=predictions,
                comparison_group_id=comparison_group_id,
                best_params=result.best_params,
                calibration_curve=[
                    {
                        "predicted_probability": point.predicted_probability,
                        "observed_frequency": point.observed_frequency,
                    }
                    for point in result.calibration_curve
                ],
                shap_feature_importance=result.shap_feature_importance,
                artifact_path=artifact_path if result.model_name == bundle.best_model_name else None,
                is_best_model=result.model_name == bundle.best_model_name,
            )
        )
    return comparison_group_id, run_ids


def store_client_portfolio(connection: DatabaseConnection, portfolio: ClientPortfolio) -> None:
    """Create or replace a normalized client portfolio as one transaction."""

    if not portfolio.client_id.strip():
        raise ValueError("Client ID is required")
    if not portfolio.client_name.strip():
        raise ValueError("Client name is required")
    if not portfolio.strategy.strip():
        raise ValueError("Portfolio strategy is required")
    if not portfolio.holdings:
        raise ValueError("Portfolio holdings are required")
    if len({holding.cusip for holding in portfolio.holdings}) != len(portfolio.holdings):
        raise ValueError("Portfolio holdings must have unique CUSIPs")

    total_weight = sum(holding.weight for holding in portfolio.holdings)
    if abs(total_weight - 1.0) > 0.001:
        raise ValueError("Portfolio weights must sum to 1")
    if any(holding.weight < 0 or holding.weight > 1 for holding in portfolio.holdings):
        raise ValueError("Portfolio weights must be between 0 and 1")

    values = (portfolio.client_id, portfolio.client_name, portfolio.strategy)
    if connection.backend == "postgres":
        connection.execute(
            """
            INSERT INTO client_portfolios (client_id, client_name, strategy, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (client_id) DO UPDATE SET
                client_name = EXCLUDED.client_name,
                strategy = EXCLUDED.strategy,
                updated_at = CURRENT_TIMESTAMP
            """,
            values,
        )
    else:
        connection.execute(
            """
            INSERT INTO client_portfolios (client_id, client_name, strategy, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (client_id) DO UPDATE SET
                client_name = excluded.client_name,
                strategy = excluded.strategy,
                updated_at = CURRENT_TIMESTAMP
            """,
            values,
        )

    connection.execute("DELETE FROM client_holdings WHERE client_id = ?", (portfolio.client_id,))
    connection.executemany(
        """
        INSERT INTO client_holdings (client_id, cusip, issuer_name, sector, weight)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (portfolio.client_id, holding.cusip, holding.issuer_name, holding.sector, holding.weight)
            for holding in portfolio.holdings
        ],
    )
    connection.commit()


def store_client_portfolios(connection: DatabaseConnection, portfolios: list[ClientPortfolio]) -> None:
    """Persist a collection of client portfolios."""

    for portfolio in portfolios:
        store_client_portfolio(connection, portfolio)


def list_client_portfolios(connection: DatabaseConnection, *, limit: int = 100) -> list[dict]:
    rows = connection.execute(
        """
        SELECT p.client_id, p.client_name, p.strategy, p.updated_at,
               COUNT(h.cusip) AS holding_count
        FROM client_portfolios p
        LEFT JOIN client_holdings h ON h.client_id = p.client_id
        GROUP BY p.client_id, p.client_name, p.strategy, p.updated_at
        ORDER BY p.client_name
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_client_portfolio(connection: DatabaseConnection, client_id: str) -> ClientPortfolio | None:
    row = connection.execute(
        "SELECT client_id, client_name, strategy FROM client_portfolios WHERE client_id = ?",
        (client_id,),
    ).fetchone()
    if row is None:
        return None

    holdings = connection.execute(
        """
        SELECT cusip, issuer_name, sector, weight
        FROM client_holdings
        WHERE client_id = ?
        ORDER BY weight DESC, issuer_name
        """,
        (client_id,),
    ).fetchall()
    return ClientPortfolio(
        client_id=str(row["client_id"]),
        client_name=str(row["client_name"]),
        strategy=str(row["strategy"]),
        holdings=[
            ClientHolding(
                cusip=str(holding["cusip"]),
                issuer_name=str(holding["issuer_name"]),
                sector=str(holding["sector"]),
                weight=float(holding["weight"]),
            )
            for holding in holdings
        ],
    )


def store_alerts(
    connection: DatabaseConnection,
    current_accession_number: str,
    previous_accession_number: str,
    assessments: list[MaterialityAssessment],
    impacts_by_holding_key: dict[str, list[ClientImpact]],
) -> list[int]:
    """Persist alert candidates and related client impacts."""

    connection.execute(
        "DELETE FROM alert_impacts WHERE alert_id IN (SELECT alert_id FROM alerts WHERE current_accession_number = ?)",
        (current_accession_number,),
    )
    connection.execute("DELETE FROM alerts WHERE current_accession_number = ?", (current_accession_number,))

    alert_ids: list[int] = []
    for assessment in assessments:
        feature = assessment.feature_snapshot
        insert_sql = """
            INSERT INTO alerts (
                current_accession_number, previous_accession_number, holding_key, issuer_name, cusip, sector,
                score, severity, should_alert, reasons_json, current_weight, previous_weight, weight_delta,
                current_rank, previous_rank, turnover_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        if connection.backend == "postgres":
            insert_sql += " RETURNING alert_id"

        cursor = connection.execute(
            insert_sql,
            (
                current_accession_number,
                previous_accession_number,
                assessment.holding_key,
                assessment.issuer_name,
                assessment.cusip,
                assessment.sector,
                assessment.score,
                assessment.severity,
                int(assessment.should_alert),
                json.dumps(assessment.reasons),
                feature.current_weight,
                feature.previous_weight,
                feature.weight_delta,
                feature.current_rank,
                feature.previous_rank,
                feature.turnover_ratio,
            ),
        )
        if connection.backend == "postgres":
            inserted_row = cursor.fetchone()
            if inserted_row is None:
                raise RuntimeError("PostgreSQL did not return an alert id")
            alert_id = int(inserted_row["alert_id"])
        else:
            alert_id = int(cursor.lastrowid or 0)
        alert_ids.append(alert_id)

        impacts = impacts_by_holding_key.get(assessment.holding_key, [])
        connection.executemany(
            """
            INSERT INTO alert_impacts (
                alert_id, client_id, client_name, strategy, cusip, issuer_name, sector,
                direct_weight, sector_weight, impact_score, impact_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    alert_id,
                    impact.client_id,
                    impact.client_name,
                    impact.strategy,
                    impact.cusip,
                    impact.issuer_name,
                    impact.sector,
                    impact.direct_weight,
                    impact.sector_weight,
                    impact.impact_score,
                    impact.impact_label,
                )
                for impact in impacts
            ],
        )

    connection.commit()
    return alert_ids


def list_filing_summaries(connection: DatabaseConnection, *, limit: int = 20) -> list[dict]:
    """List recently stored filing summaries for API and operator views."""

    rows = connection.execute(
        """
        SELECT accession_number, cik, filing_date, report_period, form_type, filer_name
        FROM filings
        ORDER BY report_period DESC, filing_date DESC, accession_number DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def list_position_deltas(connection: DatabaseConnection, accession_number: str, *, limit: int = 50) -> list[dict]:
    """List stored position deltas for a filing accession."""

    rows = connection.execute(
        """
        SELECT *
        FROM position_deltas
        WHERE current_accession_number = ?
        ORDER BY ABS(value_delta_thousands) DESC, issuer_name ASC
        LIMIT ?
        """,
        (accession_number, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def list_alerts(
    connection: DatabaseConnection,
    *,
    limit: int = 20,
    minimum_score: int = 0,
    severity: str | None = None,
) -> list[PersistedAlert]:
    """List persisted alerts ordered by score."""

    query = """
        SELECT *
        FROM alerts
        WHERE score >= ?
    """
    parameters: list[object] = [minimum_score]
    if severity:
        query += " AND severity = ?"
        parameters.append(severity)
    query += " ORDER BY score DESC, alert_id DESC LIMIT ?"
    parameters.append(limit)

    rows = connection.execute(query, tuple(parameters)).fetchall()
    return [_row_to_alert(row) for row in rows]


def get_alert(connection: DatabaseConnection, alert_id: int) -> PersistedAlert | None:
    """Fetch one persisted alert by ID."""

    row = connection.execute("SELECT * FROM alerts WHERE alert_id = ?", (alert_id,)).fetchone()
    if row is None:
        return None
    return _row_to_alert(row)


def list_alert_impacts(connection: DatabaseConnection, alert_id: int) -> list[dict]:
    """List persisted client impacts for a single alert."""

    rows = connection.execute(
        """
        SELECT client_id, client_name, strategy, cusip, issuer_name, sector,
               direct_weight, sector_weight, impact_score, impact_label
        FROM alert_impacts
        WHERE alert_id = ?
        ORDER BY impact_score DESC, client_name ASC
        """,
        (alert_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def store_recommendations(
    connection: DatabaseConnection,
    *,
    current_accession_number: str,
    recommendations: list[ClientRecommendation],
    alert_ids_by_holding_key: dict[str, int],
) -> list[int]:
    """Persist ranked client recommendations for one filing window."""

    connection.execute(
        "DELETE FROM recommendations WHERE current_accession_number = ?",
        (current_accession_number,),
    )

    recommendation_ids: list[int] = []
    for recommendation in recommendations:
        alert_id = alert_ids_by_holding_key.get(recommendation.holding_key)
        if alert_id is None:
            continue

        insert_sql = """
            INSERT INTO recommendations (
                alert_id, client_id, client_name, strategy, current_accession_number,
                holding_key, issuer_name, cusip, sector, alert_score, alert_severity,
                relevance_score, content_similarity, direct_weight, sector_weight,
                precedents_json, rationale_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        if connection.backend == "postgres":
            insert_sql += " RETURNING recommendation_id"

        cursor = connection.execute(
            insert_sql,
            (
                alert_id,
                recommendation.client_id,
                recommendation.client_name,
                recommendation.strategy,
                recommendation.current_accession_number,
                recommendation.holding_key,
                recommendation.issuer_name,
                recommendation.cusip,
                recommendation.sector,
                recommendation.alert_score,
                recommendation.alert_severity,
                recommendation.relevance_score,
                recommendation.content_similarity,
                recommendation.direct_weight,
                recommendation.sector_weight,
                json.dumps(
                    [
                        {
                            "current_accession_number": precedent.current_accession_number,
                            "previous_accession_number": precedent.previous_accession_number,
                            "issuer_name": precedent.issuer_name,
                            "cusip": precedent.cusip,
                            "sector": precedent.sector,
                            "abs_weight_delta": precedent.abs_weight_delta,
                            "value_delta_thousands": precedent.value_delta_thousands,
                            "rule_score": precedent.rule_score,
                            "weak_label": precedent.weak_label,
                            "similarity": precedent.similarity,
                        }
                        for precedent in recommendation.precedents
                    ]
                ),
                json.dumps(recommendation.rationale),
            ),
        )
        if connection.backend == "postgres":
            inserted_row = cursor.fetchone()
            if inserted_row is None:
                raise RuntimeError("PostgreSQL did not return a recommendation id")
            recommendation_ids.append(int(inserted_row["recommendation_id"]))
        else:
            recommendation_ids.append(int(cursor.lastrowid or 0))

    connection.commit()
    return recommendation_ids


def list_recommendations(
    connection: DatabaseConnection,
    client_id: str,
    *,
    limit: int = 20,
) -> list[PersistedRecommendation]:
    """List ranked recommendations for one client."""

    rows = connection.execute(
        """
        SELECT recommendation_id, alert_id, client_id, client_name, strategy,
               current_accession_number, holding_key, issuer_name, cusip, sector,
               alert_score, alert_severity, relevance_score, content_similarity,
               direct_weight, sector_weight, precedents_json, rationale_json
        FROM recommendations
        WHERE client_id = ?
        ORDER BY relevance_score DESC, alert_score DESC, recommendation_id DESC
        LIMIT ?
        """,
        (client_id, limit),
    ).fetchall()
    return [_row_to_recommendation(row) for row in rows]


def _row_to_alert(row: Mapping[str, Any]) -> PersistedAlert:
    return PersistedAlert(
        alert_id=int(row["alert_id"]),
        current_accession_number=str(row["current_accession_number"]),
        previous_accession_number=str(row["previous_accession_number"]),
        holding_key=str(row["holding_key"]),
        issuer_name=str(row["issuer_name"]),
        cusip=str(row["cusip"]),
        sector=str(row["sector"]),
        score=int(row["score"]),
        severity=str(row["severity"]),
        should_alert=bool(row["should_alert"]),
        reasons=json.loads(str(row["reasons_json"])),
        current_weight=float(row["current_weight"]),
        previous_weight=float(row["previous_weight"]),
        weight_delta=float(row["weight_delta"]),
        current_rank=int(row["current_rank"]) if row["current_rank"] is not None else None,
        previous_rank=int(row["previous_rank"]) if row["previous_rank"] is not None else None,
        turnover_ratio=float(row["turnover_ratio"]),
    )


def _row_to_recommendation(row: Mapping[str, Any]) -> PersistedRecommendation:
    return PersistedRecommendation(
        recommendation_id=int(row["recommendation_id"]),
        alert_id=int(row["alert_id"]),
        client_id=str(row["client_id"]),
        client_name=str(row["client_name"]),
        strategy=str(row["strategy"]),
        current_accession_number=str(row["current_accession_number"]),
        holding_key=str(row["holding_key"]),
        issuer_name=str(row["issuer_name"]),
        cusip=str(row["cusip"]),
        sector=str(row["sector"]),
        alert_score=int(row["alert_score"]),
        alert_severity=str(row["alert_severity"]),
        relevance_score=int(row["relevance_score"]),
        content_similarity=float(row["content_similarity"]),
        direct_weight=float(row["direct_weight"]),
        sector_weight=float(row["sector_weight"]),
        precedents=json.loads(str(row["precedents_json"])),
        rationale=json.loads(str(row["rationale_json"])),
    )


def _row_to_model_run(row: Mapping[str, Any]) -> ModelRunSummary:
    return ModelRunSummary(
        run_id=int(row["run_id"]),
        model_name=str(row["model_name"]),
        training_samples=int(row["training_samples"]),
        positive_count=int(row["positive_count"]),
        feature_names=json.loads(str(row["feature_names_json"])),
        coefficients=json.loads(str(row["coefficients_json"])),
        intercept=float(row["intercept"]),
        metrics=json.loads(str(row["metrics_json"])),
        comparison_group_id=str(row["comparison_group_id"]) if row["comparison_group_id"] is not None else None,
        best_params=json.loads(str(row["best_params_json"])) if row["best_params_json"] else None,
        calibration_curve=json.loads(str(row["calibration_curve_json"])) if row["calibration_curve_json"] else None,
        shap_feature_importance=(
            json.loads(str(row["shap_feature_importance_json"])) if row["shap_feature_importance_json"] else None
        ),
        artifact_path=str(row["artifact_path"]) if row["artifact_path"] is not None else None,
        is_best_model=bool(row["is_best_model"]),
    )


def _row_to_feature_row(row: Mapping[str, Any]) -> PersistedFeatureRow:
    return PersistedFeatureRow(
        current_accession_number=str(row["current_accession_number"]),
        previous_accession_number=str(row["previous_accession_number"]),
        holding_key=str(row["holding_key"]),
        issuer_name=str(row["issuer_name"]),
        cusip=str(row["cusip"]),
        sector=str(row["sector"]),
        current_value_thousands=int(row["current_value_thousands"]),
        previous_value_thousands=int(row["previous_value_thousands"]),
        current_weight=float(row["current_weight"]),
        previous_weight=float(row["previous_weight"]),
        weight_delta=float(row["weight_delta"]),
        abs_weight_delta=float(row["abs_weight_delta"]),
        value_delta_thousands=int(row["value_delta_thousands"]),
        abs_value_delta_thousands=int(row["abs_value_delta_thousands"]),
        value_pct_change=float(row["value_pct_change"]) if row["value_pct_change"] is not None else None,
        shares_pct_change=float(row["shares_pct_change"]) if row["shares_pct_change"] is not None else None,
        is_new_position=bool(row["is_new_position"]),
        is_exited_position=bool(row["is_exited_position"]),
        current_rank=int(row["current_rank"]) if row["current_rank"] is not None else None,
        previous_rank=int(row["previous_rank"]) if row["previous_rank"] is not None else None,
        entered_top10=bool(row["entered_top10"]),
        exited_top10=bool(row["exited_top10"]),
        entered_top20=bool(row["entered_top20"]),
        exited_top20=bool(row["exited_top20"]),
        turnover_ratio=float(row["turnover_ratio"]),
        change_share_of_turnover=float(row["change_share_of_turnover"]),
        rule_score=int(row["rule_score"]),
        weak_label=int(row["weak_label"]),
    )


def _extract_estimator_parameters(estimator: Any) -> tuple[list[float], float]:
    if estimator is None:
        return [], 0.0

    model = estimator.named_steps["model"] if hasattr(estimator, "named_steps") else estimator
    raw_coefficients = getattr(model, "coef_", None)
    raw_intercept = getattr(model, "intercept_", None)
    if raw_coefficients is None or raw_intercept is None:
        return [], 0.0

    coefficients = raw_coefficients.tolist()
    if coefficients and isinstance(coefficients[0], list):
        coefficients = coefficients[0]

    intercept_values = raw_intercept.tolist() if hasattr(raw_intercept, "tolist") else raw_intercept
    if isinstance(intercept_values, list):
        intercept = float(intercept_values[0]) if intercept_values else 0.0
    else:
        intercept = float(intercept_values)

    return [float(value) for value in coefficients], intercept


def _parse_iso_date(value: str | date | None) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _ensure_column(connection: DatabaseConnection, table_name: str, column_name: str, column_definition: str) -> None:
    if connection.backend == "sqlite":
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
    else:
        rows = connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ? AND table_schema = current_schema()
            """,
            (table_name,),
        ).fetchall()
        existing_columns = {str(row["column_name"]) for row in rows}

    if column_name not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")


def _resolve_database_target(database_url: str) -> tuple[str, str]:
    lowered = database_url.lower()
    if lowered.startswith("sqlite:///"):
        return "sqlite", database_url[len("sqlite:///") :]
    if lowered.startswith("postgres://") or lowered.startswith("postgresql://"):
        return "postgres", database_url
    raise ValueError("DATABASE_URL must start with sqlite:/// or postgresql://")


def _prepare_sql_for_backend(sql: str, *, backend: str) -> str:
    if backend != "postgres":
        return sql
    return re.sub(r"\?", "%s", sql)


def _split_sql_script(sql_script: str) -> list[str]:
    return [statement.strip() for statement in sql_script.split(";") if statement.strip()]
