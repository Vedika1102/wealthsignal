from __future__ import annotations

from random import Random

from .models import ClientHolding, ClientImpact, ClientPortfolio, Holding, MaterialityAssessment
from .sector_enrichment import infer_sector


def _normalize_weights(weights: list[float]) -> list[float]:
    total = sum(weights)
    if total == 0:
        return [0.0 for _ in weights]
    return [weight / total for weight in weights]


def _client_holdings_from_selection(selected: list[Holding], *, randomizer: Random) -> list[ClientHolding]:
    raw_weights = []
    for index, holding in enumerate(selected):
        base = max(holding.value_thousands, 1)
        tilt = 1.15 - (index * 0.03)
        noise = 0.85 + (randomizer.random() * 0.3)
        raw_weights.append(base * tilt * noise)
    normalized = _normalize_weights(raw_weights)
    return [
        ClientHolding(
            cusip=holding.cusip,
            issuer_name=holding.issuer_name,
            sector=infer_sector(holding.issuer_name),
            weight=weight,
        )
        for holding, weight in zip(selected, normalized)
    ]


def generate_synthetic_client_portfolios(
    holdings_universe: list[Holding],
    *,
    seed: int = 7,
) -> list[ClientPortfolio]:
    """Generate a small set of realistic synthetic client portfolios.

    The portfolios are derived from the live holdings universe so they create
    plausible overlap for the alert-ranking demo.
    """

    if not holdings_universe:
        return []

    ranked = sorted(holdings_universe, key=lambda item: item.value_thousands, reverse=True)
    randomizer = Random(seed)

    templates = [
        ("client-001", "Avery Capital Family", "Large Cap Core", ranked[:12]),
        ("client-002", "North Harbor Trust", "Concentrated Quality", ranked[:8]),
        ("client-003", "Summit Legacy Account", "Balanced Compounders", ranked[4:18]),
        ("client-004", "Oak Ridge Advisory", "Diversified Equity", ranked[10:26]),
        ("client-005", "Bluecrest Private Client", "Opportunistic Growth", ranked[:5] + ranked[12:20]),
    ]

    portfolios: list[ClientPortfolio] = []
    for client_id, client_name, strategy, selected in templates:
        if not selected:
            continue
        portfolios.append(
            ClientPortfolio(
                client_id=client_id,
                client_name=client_name,
                strategy=strategy,
                holdings=_client_holdings_from_selection(selected, randomizer=randomizer),
            )
        )
    return portfolios


def score_client_impacts(
    assessment: MaterialityAssessment,
    portfolios: list[ClientPortfolio],
) -> list[ClientImpact]:
    """Score how relevant one alert candidate is to each client portfolio."""

    impacts: list[ClientImpact] = []
    for portfolio in portfolios:
        direct_weight = sum(
            holding.weight
            for holding in portfolio.holdings
            if holding.cusip == assessment.cusip
        )
        sector_weight = sum(
            holding.weight
            for holding in portfolio.holdings
            if holding.sector == assessment.sector and assessment.sector != "Unknown"
        )
        if direct_weight <= 0 and sector_weight <= 0:
            continue

        impact_score = min(
            100,
            round((direct_weight * 450) + (sector_weight * 120) + (assessment.score * 0.55)),
        )
        if impact_score >= 80:
            impact_label = "high"
        elif impact_score >= 55:
            impact_label = "medium"
        else:
            impact_label = "low"

        impacts.append(
            ClientImpact(
                client_id=portfolio.client_id,
                client_name=portfolio.client_name,
                strategy=portfolio.strategy,
                cusip=assessment.cusip,
                issuer_name=assessment.issuer_name,
                sector=assessment.sector,
                direct_weight=direct_weight,
                sector_weight=sector_weight,
                impact_score=impact_score,
                impact_label=impact_label,
            )
        )

    impacts.sort(key=lambda item: item.impact_score, reverse=True)
    return impacts
