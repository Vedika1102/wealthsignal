from __future__ import annotations

import math
from dataclasses import asdict

from .models import (
    ClientImpact,
    ClientPortfolio,
    ClientRecommendation,
    MaterialityAssessment,
    PersistedFeatureRow,
    RecommendationPrecedent,
)


def build_client_recommendations(
    assessments: list[MaterialityAssessment],
    portfolios: list[ClientPortfolio],
    impacts_by_holding_key: dict[str, list[ClientImpact]],
    historical_feature_rows: list[PersistedFeatureRow],
    *,
    current_accession_number: str,
    max_precedents: int = 3,
) -> list[ClientRecommendation]:
    """Rank current material alerts for each synthetic client portfolio."""

    if not assessments or not portfolios:
        return []

    sector_order = _sector_order(assessments, portfolios)
    recommendations: list[ClientRecommendation] = []
    for assessment in assessments:
        impacts = impacts_by_holding_key.get(assessment.holding_key, [])
        impact_by_client = {impact.client_id: impact for impact in impacts}
        precedents = find_historical_precedents(
            assessment,
            historical_feature_rows,
            current_accession_number=current_accession_number,
            limit=max_precedents,
        )
        for portfolio in portfolios:
            impact = impact_by_client.get(portfolio.client_id)
            if impact is None:
                continue

            content_similarity = compute_content_similarity(assessment, portfolio, sector_order)
            relevance_score = _relevance_score(content_similarity, impact)
            recommendations.append(
                ClientRecommendation(
                    client_id=portfolio.client_id,
                    client_name=portfolio.client_name,
                    strategy=portfolio.strategy,
                    current_accession_number=current_accession_number,
                    holding_key=assessment.holding_key,
                    issuer_name=assessment.issuer_name,
                    cusip=assessment.cusip,
                    sector=assessment.sector,
                    alert_score=assessment.score,
                    alert_severity=assessment.severity,
                    relevance_score=relevance_score,
                    content_similarity=content_similarity,
                    direct_weight=impact.direct_weight,
                    sector_weight=impact.sector_weight,
                    precedents=precedents,
                    rationale=_recommendation_rationale(assessment, impact, content_similarity, precedents),
                )
            )

    recommendations.sort(
        key=lambda item: (
            item.relevance_score,
            item.content_similarity,
            item.alert_score,
            len(item.precedents),
        ),
        reverse=True,
    )
    return recommendations


def compute_content_similarity(
    assessment: MaterialityAssessment,
    portfolio: ClientPortfolio,
    sector_order: list[str],
) -> float:
    """Measure how closely a client portfolio lines up with one alert context."""

    alert_vector = _alert_context_vector(assessment, sector_order)
    portfolio_vector = _portfolio_context_vector(portfolio, assessment, sector_order)
    return _cosine_similarity(alert_vector, portfolio_vector)


def find_historical_precedents(
    assessment: MaterialityAssessment,
    historical_feature_rows: list[PersistedFeatureRow],
    *,
    current_accession_number: str,
    limit: int = 3,
) -> list[RecommendationPrecedent]:
    """Find earlier moves with similar sector and magnitude as fallback precedents."""

    target = assessment.feature_snapshot
    candidates: list[RecommendationPrecedent] = []
    for row in historical_feature_rows:
        if row.current_accession_number == current_accession_number:
            continue
        if row.sector != assessment.sector:
            continue

        similarity = _feature_row_similarity(target, row)
        if similarity < 0.45:
            continue

        candidates.append(
            RecommendationPrecedent(
                current_accession_number=row.current_accession_number,
                previous_accession_number=row.previous_accession_number,
                issuer_name=row.issuer_name,
                cusip=row.cusip,
                sector=row.sector,
                abs_weight_delta=row.abs_weight_delta,
                value_delta_thousands=row.value_delta_thousands,
                rule_score=row.rule_score,
                weak_label=row.weak_label,
                similarity=similarity,
            )
        )

    candidates.sort(key=lambda item: (item.similarity, item.rule_score, abs(item.value_delta_thousands)), reverse=True)
    return candidates[:limit]


def precedents_to_payload(precedents: list[RecommendationPrecedent]) -> list[dict[str, object]]:
    return [asdict(precedent) for precedent in precedents]


def _sector_order(
    assessments: list[MaterialityAssessment],
    portfolios: list[ClientPortfolio],
) -> list[str]:
    sectors = {
        assessment.sector
        for assessment in assessments
        if assessment.sector != "Unknown"
    }
    sectors.update(
        holding.sector
        for portfolio in portfolios
        for holding in portfolio.holdings
        if holding.sector != "Unknown"
    )
    return sorted(sectors)


def _alert_context_vector(assessment: MaterialityAssessment, sector_order: list[str]) -> list[float]:
    feature = assessment.feature_snapshot
    vector = [0.0 for _ in sector_order]
    if assessment.sector in sector_order:
        vector[sector_order.index(assessment.sector)] = max(feature.abs_weight_delta + feature.current_weight, 0.01)
    vector.extend(
        [
            1.0,
            max(feature.abs_weight_delta, 0.001),
            max(feature.change_share_of_turnover, 0.001),
        ]
    )
    return vector


def _portfolio_context_vector(
    portfolio: ClientPortfolio,
    assessment: MaterialityAssessment,
    sector_order: list[str],
) -> list[float]:
    sector_weights = {sector: 0.0 for sector in sector_order}
    direct_weight = 0.0
    for holding in portfolio.holdings:
        if holding.sector in sector_weights:
            sector_weights[holding.sector] += holding.weight
        if holding.cusip == assessment.cusip:
            direct_weight += holding.weight

    sector_weight = sector_weights.get(assessment.sector, 0.0)
    vector = [sector_weights[sector] for sector in sector_order]
    vector.extend(
        [
            max(direct_weight, 0.001),
            max(sector_weight, 0.001),
            max(sum(holding.weight for holding in portfolio.holdings[:5]), 0.001),
        ]
    )
    return vector


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    numerator = sum(lhs * rhs for lhs, rhs in zip(left, right))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


def _feature_row_similarity(assessment_feature, historical_row: PersistedFeatureRow) -> float:
    target_vector = [
        assessment_feature.abs_weight_delta,
        abs(assessment_feature.value_delta_thousands) / 1_000_000,
        assessment_feature.turnover_ratio,
        assessment_feature.change_share_of_turnover,
        float(assessment_feature.is_new_position),
        float(assessment_feature.is_exited_position),
    ]
    candidate_vector = [
        historical_row.abs_weight_delta,
        abs(historical_row.value_delta_thousands) / 1_000_000,
        historical_row.turnover_ratio,
        historical_row.change_share_of_turnover,
        float(historical_row.is_new_position),
        float(historical_row.is_exited_position),
    ]
    return _cosine_similarity(target_vector, candidate_vector)


def _relevance_score(content_similarity: float, impact: ClientImpact) -> int:
    similarity_score = content_similarity * 100
    return min(
        100,
        round((similarity_score * 0.55) + (impact.impact_score * 0.45)),
    )


def _recommendation_rationale(
    assessment: MaterialityAssessment,
    impact: ClientImpact,
    content_similarity: float,
    precedents: list[RecommendationPrecedent],
) -> list[str]:
    reasons = [
        f"content similarity={content_similarity:.2f} for {assessment.sector} exposure",
        f"client sector overlap={impact.sector_weight:.2%}",
    ]
    if impact.direct_weight > 0:
        reasons.append(f"direct holding overlap={impact.direct_weight:.2%}")
    if precedents:
        reasons.append(f"{len(precedents)} historical precedent(s) with similar magnitude")
    return reasons
