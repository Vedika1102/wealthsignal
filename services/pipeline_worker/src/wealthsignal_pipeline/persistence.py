from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

from .delta_engine import holding_key
from .models import (
    ClientImpact,
    FilingArtifacts,
    FilingDelta,
    FilingReference,
    Holding,
    MaterialityAssessment,
    ModelPrediction,
    ModelRunSummary,
    ParsedInformationTable,
    PersistedFeatureRow,
    PersistedAlert,
)


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

        CREATE INDEX IF NOT EXISTS idx_alerts_current_accession
            ON alerts(current_accession_number, score DESC);

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


def store_feature_rows(
    connection: sqlite3.Connection,
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


def load_feature_rows(connection: sqlite3.Connection, *, limit: int | None = None) -> list[PersistedFeatureRow]:
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
    connection: sqlite3.Connection,
    *,
    model_name: str,
    training_samples: int,
    positive_count: int,
    feature_names: list[str],
    coefficients: list[float],
    intercept: float,
    metrics: dict[str, float],
    predictions: list[ModelPrediction],
) -> int:
    """Persist one model run and its predictions."""

    cursor = connection.execute(
        """
        INSERT INTO model_runs (
            model_name, training_samples, positive_count, feature_names_json,
            coefficients_json, intercept, metrics_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            model_name,
            training_samples,
            positive_count,
            json.dumps(feature_names),
            json.dumps(coefficients),
            intercept,
            json.dumps(metrics),
        ),
    )
    run_id = int(cursor.lastrowid)
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


def get_latest_model_run(connection: sqlite3.Connection) -> ModelRunSummary | None:
    """Return the most recent stored model run."""

    row = connection.execute(
        """
        SELECT run_id, model_name, training_samples, positive_count, feature_names_json,
               coefficients_json, intercept, metrics_json
        FROM model_runs
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return ModelRunSummary(
        run_id=row["run_id"],
        model_name=row["model_name"],
        training_samples=row["training_samples"],
        positive_count=row["positive_count"],
        feature_names=json.loads(row["feature_names_json"]),
        coefficients=json.loads(row["coefficients_json"]),
        intercept=row["intercept"],
        metrics=json.loads(row["metrics_json"]),
    )


def get_latest_prediction_lookup(connection: sqlite3.Connection) -> dict[tuple[str, str], ModelPrediction]:
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
        (row["current_accession_number"], row["holding_key"]): ModelPrediction(
            run_id=row["run_id"],
            current_accession_number=row["current_accession_number"],
            holding_key=row["holding_key"],
            issuer_name=row["issuer_name"],
            cusip=row["cusip"],
            probability=row["probability"],
            predicted_label=row["predicted_label"],
            weak_label=row["weak_label"],
            rule_score=row["rule_score"],
        )
        for row in rows
    }


def store_alerts(
    connection: sqlite3.Connection,
    current_accession_number: str,
    previous_accession_number: str,
    assessments: list[MaterialityAssessment],
    impacts_by_holding_key: dict[str, list[ClientImpact]],
) -> list[int]:
    """Persist alert candidates and related client impacts."""

    connection.execute("DELETE FROM alert_impacts WHERE alert_id IN (SELECT alert_id FROM alerts WHERE current_accession_number = ?)", (current_accession_number,))
    connection.execute("DELETE FROM alerts WHERE current_accession_number = ?", (current_accession_number,))

    alert_ids: list[int] = []
    for assessment in assessments:
        feature = assessment.feature_snapshot
        cursor = connection.execute(
            """
            INSERT INTO alerts (
                current_accession_number, previous_accession_number, holding_key, issuer_name, cusip, sector,
                score, severity, should_alert, reasons_json, current_weight, previous_weight, weight_delta,
                current_rank, previous_rank, turnover_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
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
        alert_id = int(cursor.lastrowid)
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


def list_filing_summaries(connection: sqlite3.Connection, *, limit: int = 20) -> list[dict]:
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


def list_position_deltas(connection: sqlite3.Connection, accession_number: str, *, limit: int = 50) -> list[dict]:
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
    connection: sqlite3.Connection,
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


def get_alert(connection: sqlite3.Connection, alert_id: int) -> PersistedAlert | None:
    """Fetch one persisted alert by ID."""

    row = connection.execute("SELECT * FROM alerts WHERE alert_id = ?", (alert_id,)).fetchone()
    if row is None:
        return None
    return _row_to_alert(row)


def list_alert_impacts(connection: sqlite3.Connection, alert_id: int) -> list[dict]:
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


def _row_to_alert(row: sqlite3.Row) -> PersistedAlert:
    return PersistedAlert(
        alert_id=row["alert_id"],
        current_accession_number=row["current_accession_number"],
        previous_accession_number=row["previous_accession_number"],
        holding_key=row["holding_key"],
        issuer_name=row["issuer_name"],
        cusip=row["cusip"],
        sector=row["sector"],
        score=row["score"],
        severity=row["severity"],
        should_alert=bool(row["should_alert"]),
        reasons=json.loads(row["reasons_json"]),
        current_weight=row["current_weight"],
        previous_weight=row["previous_weight"],
        weight_delta=row["weight_delta"],
        current_rank=row["current_rank"],
        previous_rank=row["previous_rank"],
        turnover_ratio=row["turnover_ratio"],
    )


def _row_to_feature_row(row: sqlite3.Row) -> PersistedFeatureRow:
    return PersistedFeatureRow(
        current_accession_number=row["current_accession_number"],
        previous_accession_number=row["previous_accession_number"],
        holding_key=row["holding_key"],
        issuer_name=row["issuer_name"],
        cusip=row["cusip"],
        sector=row["sector"],
        current_value_thousands=row["current_value_thousands"],
        previous_value_thousands=row["previous_value_thousands"],
        current_weight=row["current_weight"],
        previous_weight=row["previous_weight"],
        weight_delta=row["weight_delta"],
        abs_weight_delta=row["abs_weight_delta"],
        value_delta_thousands=row["value_delta_thousands"],
        abs_value_delta_thousands=row["abs_value_delta_thousands"],
        value_pct_change=row["value_pct_change"],
        shares_pct_change=row["shares_pct_change"],
        is_new_position=bool(row["is_new_position"]),
        is_exited_position=bool(row["is_exited_position"]),
        current_rank=row["current_rank"],
        previous_rank=row["previous_rank"],
        entered_top10=bool(row["entered_top10"]),
        exited_top10=bool(row["exited_top10"]),
        entered_top20=bool(row["entered_top20"]),
        exited_top20=bool(row["exited_top20"]),
        turnover_ratio=row["turnover_ratio"],
        change_share_of_turnover=row["change_share_of_turnover"],
        rule_score=row["rule_score"],
        weak_label=row["weak_label"],
    )


def _parse_iso_date(value: str | None):
    if not value:
        return None
    return date.fromisoformat(value)
