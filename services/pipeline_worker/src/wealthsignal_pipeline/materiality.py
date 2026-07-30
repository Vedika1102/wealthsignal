from __future__ import annotations

from .models import MaterialityAssessment, PositionFeatures


def materiality_policy() -> dict[str, object]:
    """Return the human-readable scoring policy for governance and API use."""

    return {
        "severity_bands": {
            "urgent": "score >= 80",
            "review": "60 <= score < 80",
            "informational": "40 <= score < 60",
            "ignore": "score < 40",
        },
        "rule_summary": [
            "entered top 10 or top 20 holdings",
            "exited prior top 10 or top 20 holdings",
            "new position or full exit",
            "large weight change",
            "large absolute value move",
            "large percentage move",
            "filing-level turnover contribution",
        ],
    }


def _severity_from_score(score: int) -> str:
    if score >= 80:
        return "urgent"
    if score >= 60:
        return "review"
    if score >= 40:
        return "informational"
    return "ignore"


def score_materiality(feature: PositionFeatures) -> MaterialityAssessment:
    """Apply an explainable rules-based materiality scoring policy."""

    score = 0
    reasons: list[str] = []

    if feature.entered_top10:
        score += 30
        reasons.append("entered top 10 holdings")
    elif feature.entered_top20:
        score += 20
        reasons.append("entered top 20 holdings")

    if feature.exited_top10:
        score += 28
        reasons.append("exited prior top 10 holding")
    elif feature.exited_top20:
        score += 18
        reasons.append("exited prior top 20 holding")

    if feature.is_new_position:
        score += 12
        reasons.append("new reported position")

    if feature.is_exited_position:
        score += 12
        reasons.append("fully exited position")

    if feature.abs_weight_delta >= 0.03:
        score += 20
        reasons.append("weight changed by at least 3 percentage points")
    elif feature.abs_weight_delta >= 0.015:
        score += 12
        reasons.append("weight changed by at least 1.5 percentage points")
    elif feature.abs_weight_delta >= 0.0075:
        score += 6
        reasons.append("weight changed by at least 0.75 percentage points")

    if feature.abs_value_delta_thousands >= 1_000_000:
        score += 15
        reasons.append("absolute reported value moved by at least $1B")
    elif feature.abs_value_delta_thousands >= 500_000:
        score += 10
        reasons.append("absolute reported value moved by at least $500M")
    elif feature.abs_value_delta_thousands >= 100_000:
        score += 5
        reasons.append("absolute reported value moved by at least $100M")

    if feature.value_pct_change is not None:
        if abs(feature.value_pct_change) >= 1.0:
            score += 10
            reasons.append("position value at least doubled or halved")
        elif abs(feature.value_pct_change) >= 0.5:
            score += 6
            reasons.append("position value changed by at least 50%")

    if feature.turnover_ratio >= 0.20:
        score += 8
        reasons.append("filing shows elevated portfolio turnover")
    elif feature.turnover_ratio >= 0.10:
        score += 4
        reasons.append("filing shows moderate portfolio turnover")

    if feature.change_share_of_turnover >= 0.15:
        score += 8
        reasons.append("position explains a large share of filing turnover")
    elif feature.change_share_of_turnover >= 0.08:
        score += 4
        reasons.append("position is a meaningful contributor to turnover")

    score = min(score, 100)
    severity = _severity_from_score(score)

    return MaterialityAssessment(
        holding_key=feature.holding_key,
        issuer_name=feature.issuer_name,
        cusip=feature.cusip,
        sector=feature.sector,
        score=score,
        severity=severity,
        should_alert=score >= 40,
        reasons=reasons,
        feature_snapshot=feature,
    )


def score_materiality_batch(features: list[PositionFeatures]) -> list[MaterialityAssessment]:
    """Score and rank multiple position-change events."""

    assessments = [score_materiality(feature) for feature in features]
    assessments.sort(key=lambda item: item.score, reverse=True)
    return assessments
