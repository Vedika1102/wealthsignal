from __future__ import annotations

import argparse
from pathlib import Path

from .alerting import generate_alert_candidates, generate_demo_portfolios_for_delta
from .baseline_model import train_logistic_baseline
from .delta_engine import compute_filing_delta
from .feature_engineering import build_persisted_feature_rows, build_position_features
from .ingest import ingest_recent_filings_for_cik
from .persistence import (
    connect,
    load_feature_rows,
    initialize_database,
    load_latest_filing_accessions,
    load_parsed_filing,
    store_feature_rows,
    store_model_run,
    store_client_portfolios,
    store_alerts,
    store_filing_delta,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest recent SEC 13F filings into WealthSignal.")
    parser.add_argument("--cik", required=True, help="SEC CIK for the filer, for example 1067983.")
    parser.add_argument(
        "--user-agent",
        required=True,
        help="Descriptive SEC User-Agent including contact information.",
    )
    parser.add_argument(
        "--db-path",
        default="data/wealthsignal.db",
        help="SQLite database path for local storage.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2,
        help="Number of recent 13F filings to ingest.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    parsed_filings = ingest_recent_filings_for_cik(
        args.cik,
        args.user_agent,
        db_path=db_path,
        limit=args.limit,
    )

    connection = connect(db_path)
    try:
        initialize_database(connection)
        accessions = load_latest_filing_accessions(connection, parsed_filings[0].filing.cik, limit=2)
        if len(accessions) >= 2:
            current = load_parsed_filing(connection, accessions[0])
            previous = load_parsed_filing(connection, accessions[1])
            if current is not None and previous is not None:
                delta = compute_filing_delta(current, previous)
                store_filing_delta(connection, delta)
                features = build_position_features(delta)
                portfolios = generate_demo_portfolios_for_delta(delta)
                store_client_portfolios(connection, portfolios)
                all_assessments, assessments, impacts_by_holding_key = generate_alert_candidates(delta, portfolios)
                feature_rows = build_persisted_feature_rows(delta, features, all_assessments)
                store_feature_rows(connection, feature_rows)
                store_alerts(
                    connection,
                    current.filing.accession_number,
                    previous.filing.accession_number,
                    assessments,
                    impacts_by_holding_key,
                )

                model_probabilities: dict[str, float] = {}
                training_rows = load_feature_rows(connection)
                try:
                    fit = train_logistic_baseline(training_rows)
                    store_model_run(
                        connection,
                        model_name="numpy-logistic-baseline",
                        training_samples=len(training_rows),
                        positive_count=sum(row.weak_label for row in training_rows),
                        feature_names=fit.feature_names,
                        coefficients=fit.coefficients,
                        intercept=fit.intercept,
                        metrics=fit.metrics,
                        predictions=fit.predictions,
                    )
                    model_probabilities = {
                        prediction.holding_key: prediction.probability
                        for prediction in fit.predictions
                        if prediction.current_accession_number == current.filing.accession_number
                    }
                    print(
                        "Baseline model metrics: "
                        f"accuracy={fit.metrics['accuracy']:.2f} "
                        f"precision={fit.metrics['precision']:.2f} "
                        f"recall={fit.metrics['recall']:.2f} "
                        f"f1={fit.metrics['f1']:.2f}"
                    )
                except ValueError as exc:
                    print(f"Baseline model skipped: {exc}")

                print(
                    f"Stored delta for {current.filing.filer_name or current.filing.cik}: "
                    f"{len(delta.positions)} position changes"
                )
                for assessment in assessments[:5]:
                    model_probability = model_probabilities.get(assessment.holding_key)
                    model_text = (
                        f" | model_prob={model_probability:.2f}"
                        if model_probability is not None
                        else ""
                    )
                    print(
                        f"Alert: {assessment.issuer_name} | score={assessment.score} | "
                        f"severity={assessment.severity} | sector={assessment.sector} | "
                        f"reasons={'; '.join(assessment.reasons[:3])}{model_text}"
                    )
                    impacts = impacts_by_holding_key.get(assessment.holding_key, [])
                    if impacts:
                        top_impact = impacts[0]
                        print(
                            f"  Top client impact: {top_impact.client_name} "
                            f"({top_impact.strategy}) score={top_impact.impact_score} "
                            f"direct={top_impact.direct_weight:.2%} sector={top_impact.sector_weight:.2%}"
                        )
        for parsed in parsed_filings:
            print(
                f"Ingested {parsed.filing.accession_number} "
                f"({parsed.filing.report_period}) with {len(parsed.holdings)} holdings"
            )
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
