from __future__ import annotations

import argparse
from pathlib import Path

from .alerting import generate_alert_candidates, generate_demo_portfolios_for_delta
from .baseline_model import train_logistic_baseline
from .delta_engine import compute_filing_delta
from .feature_engineering import build_persisted_feature_rows, build_position_features
from .ingest import ingest_recent_filings_batch_for_cik
from .ml_models import save_best_model, train_candidate_models
from .persistence import (
    connect,
    load_feature_rows,
    initialize_database,
    load_latest_filing_accessions,
    load_parsed_filing,
    store_feature_rows,
    store_model_comparison_bundle,
    store_model_run,
    store_recommendations,
    store_alerts,
    store_client_portfolios,
    store_filing_delta,
)
from .recommendation import build_client_recommendations
from .reference_data import (
    build_security_lookup,
    load_official_list_snapshot,
    refresh_official_list_snapshot,
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
    parser.add_argument(
        "--reference-data-path",
        default="data/reference/sec_official_13f_list.json",
        help="Local cache path for the official SEC 13F securities list snapshot.",
    )
    parser.add_argument(
        "--refresh-official-13f-list",
        action="store_true",
        help="Fetch and cache the latest official SEC 13F securities list before ingest.",
    )
    parser.add_argument(
        "--train-advanced-models",
        action="store_true",
        help="Train scikit-learn/XGBoost candidate models and persist the best estimator artifact.",
    )
    parser.add_argument(
        "--model-output-path",
        default="data/models/materiality-best.joblib",
        help="Output path for the persisted best advanced model artifact.",
    )
    parser.add_argument(
        "--mlflow-experiment",
        default=None,
        help="Optional MLflow experiment name for advanced model comparison tracking.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    reference_data_path = Path(args.reference_data_path)

    official_list_lookup = None
    if args.refresh_official_13f_list:
        try:
            snapshot = refresh_official_list_snapshot(args.user_agent, reference_data_path)
            official_list_lookup = build_security_lookup(snapshot)
            print(
                f"Refreshed official 13F list: {len(snapshot.securities)} securities "
                f"from {snapshot.source_url}"
            )
        except Exception as exc:
            print(f"Official 13F list refresh skipped: {exc}")
    elif reference_data_path.exists():
        snapshot = load_official_list_snapshot(reference_data_path)
        official_list_lookup = build_security_lookup(snapshot)
        print(
            f"Loaded official 13F list cache: {len(snapshot.securities)} securities "
            f"from {snapshot.source_url}"
        )

    batch_result = ingest_recent_filings_batch_for_cik(
        args.cik,
        args.user_agent,
        db_path=db_path,
        limit=args.limit,
        official_list_lookup=official_list_lookup,
    )
    parsed_filings = batch_result.parsed_filings
    if batch_result.failures:
        print(f"Skipped {len(batch_result.failures)} filing(s) during ingest:")
        for failure in batch_result.failures[:5]:
            accession_label = failure.accession_number or batch_result.cik
            print(f"  {accession_label} | stage={failure.stage} | error={failure.message}")
    if not parsed_filings:
        print(f"No recent 13F filings were ingested successfully for CIK {args.cik}.")
        return 0

    if official_list_lookup is not None:
        total_holdings = sum(len(parsed.holdings) for parsed in parsed_filings)
        matched_holdings = sum(
            1
            for parsed in parsed_filings
            for holding in parsed.holdings
            if holding.official_list_match
        )
        print(f"Official 13F reference coverage: {matched_holdings}/{total_holdings} holdings matched")

    connection = connect(db_path)
    try:
        initialize_database(connection)
        accessions = load_latest_filing_accessions(
            connection,
            parsed_filings[0].filing.cik,
            limit=max(args.limit, len(parsed_filings), 2),
        )
        if len(accessions) >= 2:
            latest_context: tuple | None = None
            training_pair_count = 0
            for pair_index, (current_accession, previous_accession) in enumerate(zip(accessions, accessions[1:])):
                current = load_parsed_filing(connection, current_accession)
                previous = load_parsed_filing(connection, previous_accession)
                if current is None or previous is None:
                    continue

                delta = compute_filing_delta(current, previous)
                store_filing_delta(connection, delta)
                features = build_position_features(delta)
                portfolios = generate_demo_portfolios_for_delta(delta)
                store_client_portfolios(connection, portfolios)
                all_assessments, assessments, impacts_by_holding_key = generate_alert_candidates(delta, portfolios)
                feature_rows = build_persisted_feature_rows(delta, features, all_assessments)
                store_feature_rows(connection, feature_rows)
                training_pair_count += 1

                if pair_index == 0:
                    alert_ids = store_alerts(
                        connection,
                        current.filing.accession_number,
                        previous.filing.accession_number,
                        assessments,
                        impacts_by_holding_key,
                    )
                    latest_context = (current, delta, assessments, impacts_by_holding_key, alert_ids, portfolios)

            if latest_context is not None:
                current, delta, assessments, impacts_by_holding_key, alert_ids, portfolios = latest_context
                model_probabilities: dict[str, float] = {}
                training_rows = load_feature_rows(connection)
                recommendations = build_client_recommendations(
                    assessments,
                    portfolios,
                    impacts_by_holding_key,
                    training_rows,
                    current_accession_number=current.filing.accession_number,
                )
                store_recommendations(
                    connection,
                    current_accession_number=current.filing.accession_number,
                    recommendations=recommendations,
                    alert_ids_by_holding_key={
                        assessment.holding_key: alert_id for assessment, alert_id in zip(assessments, alert_ids)
                    },
                )
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

                if args.train_advanced_models:
                    try:
                        bundle = train_candidate_models(
                            training_rows,
                            mlflow_experiment=args.mlflow_experiment,
                        )
                        model_output_path = Path(args.model_output_path)
                        model_output_path.parent.mkdir(parents=True, exist_ok=True)
                        save_best_model(bundle, model_output_path)
                        store_model_comparison_bundle(
                            connection,
                            bundle=bundle,
                            feature_rows=training_rows,
                            artifact_path=str(model_output_path),
                        )
                        model_probabilities = {
                            row.holding_key: probability
                            for row, probability in zip(
                                training_rows,
                                bundle.best_result.predicted_probabilities,
                            )
                            if row.current_accession_number == current.filing.accession_number
                        }
                        print(
                            "Advanced model comparison: "
                            f"best={bundle.best_model_name} "
                            f"pr_auc={bundle.best_result.metrics['pr_auc']:.2f} "
                            f"models={len(bundle.results)} "
                            f"artifact={model_output_path}"
                        )
                    except (RuntimeError, ValueError) as exc:
                        print(f"Advanced model training skipped: {exc}")

                print(
                    f"Prepared model training history from {training_pair_count} adjacent filing window(s)"
                )
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
                for recommendation in recommendations[:5]:
                    print(
                        f"Recommended review for {recommendation.client_name} "
                        f"based on content similarity={recommendation.content_similarity:.2f} "
                        f"and {len(recommendation.precedents)} historical precedents: "
                        f"{recommendation.issuer_name} relevance={recommendation.relevance_score}"
                    )
        elif len(accessions) == 1:
            print(
                f"Only one filing is available for {parsed_filings[0].filing.cik}; "
                "skipping delta, alert, and model generation."
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
