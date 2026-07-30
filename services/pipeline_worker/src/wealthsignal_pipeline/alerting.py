from __future__ import annotations

from .feature_engineering import build_position_features
from .materiality import score_materiality_batch
from .models import ClientImpact, ClientPortfolio, FilingDelta, Holding, MaterialityAssessment
from .portfolios import generate_synthetic_client_portfolios, score_client_impacts


def generate_alert_candidates(
    delta: FilingDelta,
    portfolios: list[ClientPortfolio],
) -> tuple[list[MaterialityAssessment], list[MaterialityAssessment], dict[str, list[ClientImpact]]]:
    """Generate explainable alert candidates and client impact mappings."""

    features = build_position_features(delta)
    all_assessments = score_materiality_batch(features)
    assessments = [item for item in all_assessments if item.should_alert]
    impacts_by_holding_key = {
        assessment.holding_key: score_client_impacts(assessment, portfolios)
        for assessment in assessments
    }
    return all_assessments, assessments, impacts_by_holding_key


def generate_demo_portfolios_for_delta(delta: FilingDelta) -> list[ClientPortfolio]:
    """Create synthetic client portfolios from the current filing names and sizes."""

    holdings = []
    for position in delta.positions:
        if position.new_value_thousands > 0:
            holdings.append(
                Holding(
                    issuer_name=position.issuer_name,
                    title_of_class="",
                    cusip=position.cusip,
                    value_thousands=position.new_value_thousands,
                    shares_or_principal=position.new_shares,
                    share_type=position.share_type,
                )
            )
    return generate_synthetic_client_portfolios(holdings)
