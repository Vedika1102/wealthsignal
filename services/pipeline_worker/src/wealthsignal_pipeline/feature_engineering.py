from __future__ import annotations

from .models import FilingDelta, PositionFeatures


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

        features.append(
            PositionFeatures(
                holding_key=position.holding_key,
                issuer_name=position.issuer_name,
                cusip=position.cusip,
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
