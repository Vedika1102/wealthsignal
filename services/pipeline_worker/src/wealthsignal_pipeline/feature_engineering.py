from __future__ import annotations

from .baseline_model import assign_weak_label
from .models import FilingDelta, MaterialityAssessment, PersistedFeatureRow, PositionFeatures
from .sector_enrichment import infer_sector


def compute_turnover_ratio(delta: FilingDelta) -> float:
    """Approximate portfolio turnover using weight changes.

    We use half the sum of absolute weight changes, which is a standard
    portfolio turnover approximation for weight-based holdings snapshots.
    """

    return sum(abs(position.new_weight - position.old_weight) for position in delta.positions) / 2.0


def build_position_features(delta: FilingDelta) -> list[PositionFeatures]:
    """Convert raw position deltas into first-pass materiality features."""

    turnover_ratio = compute_turnover_ratio(delta)
    total_abs_weight_change = sum(abs(position.new_weight - position.old_weight) for position in delta.positions)

    features: list[PositionFeatures] = []
    for position in delta.positions:
        weight_delta = position.new_weight - position.old_weight
        abs_weight_delta = abs(weight_delta)
        entered_top10 = position.current_rank is not None and position.current_rank <= 10 and (
            position.previous_rank is None or position.previous_rank > 10
        )
        exited_top10 = position.previous_rank is not None and position.previous_rank <= 10 and (
            position.current_rank is None or position.current_rank > 10
        )
        entered_top20 = position.current_rank is not None and position.current_rank <= 20 and (
            position.previous_rank is None or position.previous_rank > 20
        )
        exited_top20 = position.previous_rank is not None and position.previous_rank <= 20 and (
            position.current_rank is None or position.current_rank > 20
        )
        change_share_of_turnover = 0.0
        if total_abs_weight_change > 0:
            change_share_of_turnover = abs_weight_delta / total_abs_weight_change
        reference_name = position.official_issuer_name or position.issuer_name

        features.append(
            PositionFeatures(
                holding_key=position.holding_key,
                issuer_name=position.issuer_name,
                cusip=position.cusip,
                sector=infer_sector(reference_name),
                current_value_thousands=position.new_value_thousands,
                previous_value_thousands=position.old_value_thousands,
                current_weight=position.new_weight,
                previous_weight=position.old_weight,
                weight_delta=weight_delta,
                abs_weight_delta=abs_weight_delta,
                value_delta_thousands=position.value_delta_thousands,
                abs_value_delta_thousands=abs(position.value_delta_thousands),
                value_pct_change=position.value_pct_change,
                shares_pct_change=position.shares_pct_change,
                is_new_position=position.is_new_position,
                is_exited_position=position.is_exited_position,
                current_rank=position.current_rank,
                previous_rank=position.previous_rank,
                entered_top10=entered_top10,
                exited_top10=exited_top10,
                entered_top20=entered_top20,
                exited_top20=exited_top20,
                turnover_ratio=turnover_ratio,
                change_share_of_turnover=change_share_of_turnover,
            )
        )

    return features


def build_persisted_feature_rows(
    delta: FilingDelta,
    features: list[PositionFeatures],
    assessments: list[MaterialityAssessment],
) -> list[PersistedFeatureRow]:
    """Combine features and rule assessments into stored training rows."""

    assessment_by_key = {assessment.holding_key: assessment for assessment in assessments}
    rows: list[PersistedFeatureRow] = []
    for feature in features:
        assessment = assessment_by_key.get(feature.holding_key)
        rule_score = assessment.score if assessment is not None else 0
        rows.append(
            PersistedFeatureRow(
                current_accession_number=delta.current_filing.accession_number,
                previous_accession_number=delta.previous_filing.accession_number,
                holding_key=feature.holding_key,
                issuer_name=feature.issuer_name,
                cusip=feature.cusip,
                sector=feature.sector,
                current_value_thousands=feature.current_value_thousands,
                previous_value_thousands=feature.previous_value_thousands,
                current_weight=feature.current_weight,
                previous_weight=feature.previous_weight,
                weight_delta=feature.weight_delta,
                abs_weight_delta=feature.abs_weight_delta,
                value_delta_thousands=feature.value_delta_thousands,
                abs_value_delta_thousands=feature.abs_value_delta_thousands,
                value_pct_change=feature.value_pct_change,
                shares_pct_change=feature.shares_pct_change,
                is_new_position=feature.is_new_position,
                is_exited_position=feature.is_exited_position,
                current_rank=feature.current_rank,
                previous_rank=feature.previous_rank,
                entered_top10=feature.entered_top10,
                exited_top10=feature.exited_top10,
                entered_top20=feature.entered_top20,
                exited_top20=feature.exited_top20,
                turnover_ratio=feature.turnover_ratio,
                change_share_of_turnover=feature.change_share_of_turnover,
                rule_score=rule_score,
                weak_label=assign_weak_label(feature, rule_score=rule_score),
            )
        )
    return rows
