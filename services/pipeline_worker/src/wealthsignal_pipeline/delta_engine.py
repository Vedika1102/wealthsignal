from __future__ import annotations

from .models import FilingDelta, Holding, ParsedInformationTable, PositionDelta


def holding_key(holding: Holding) -> str:
    """Return a stable join key for quarter-over-quarter comparisons."""

    parts = [
        holding.cusip or holding.issuer_name,
        holding.title_of_class,
        holding.share_type,
        holding.put_call,
        holding.investment_discretion,
        holding.other_manager or "",
    ]
    return "|".join(parts)


def _weight_map(holdings: list[Holding]) -> dict[str, float]:
    total_value = sum(holding.value_thousands for holding in holdings)
    if total_value <= 0:
        return {holding_key(holding): 0.0 for holding in holdings}
    return {holding_key(holding): holding.value_thousands / total_value for holding in holdings}


def _rank_map(holdings: list[Holding]) -> dict[str, int]:
    ranked = sorted(holdings, key=lambda item: item.value_thousands, reverse=True)
    return {holding_key(holding): rank for rank, holding in enumerate(ranked, start=1)}


def _pct_change(old_value: float, new_value: float) -> float | None:
    if old_value == 0:
        return None
    return (new_value - old_value) / old_value


def compute_filing_delta(current: ParsedInformationTable, previous: ParsedInformationTable) -> FilingDelta:
    """Compute position-level deltas between two parsed 13F quarters."""

    current_map = {holding_key(holding): holding for holding in current.holdings}
    previous_map = {holding_key(holding): holding for holding in previous.holdings}
    keys = sorted(set(current_map) | set(previous_map))

    current_weights = _weight_map(current.holdings)
    previous_weights = _weight_map(previous.holdings)
    current_ranks = _rank_map(current.holdings)
    previous_ranks = _rank_map(previous.holdings)

    deltas: list[PositionDelta] = []
    for key in keys:
        current_holding = current_map.get(key)
        previous_holding = previous_map.get(key)

        issuer_name = current_holding.issuer_name if current_holding else previous_holding.issuer_name
        cusip = current_holding.cusip if current_holding else previous_holding.cusip
        share_type = current_holding.share_type if current_holding else previous_holding.share_type

        old_value = previous_holding.value_thousands if previous_holding else 0
        new_value = current_holding.value_thousands if current_holding else 0
        old_shares = previous_holding.shares_or_principal if previous_holding else 0.0
        new_shares = current_holding.shares_or_principal if current_holding else 0.0
        old_rank = previous_ranks.get(key)
        new_rank = current_ranks.get(key)

        delta = PositionDelta(
            holding_key=key,
            issuer_name=issuer_name,
            cusip=cusip,
            old_value_thousands=old_value,
            new_value_thousands=new_value,
            old_shares=old_shares,
            new_shares=new_shares,
            old_weight=previous_weights.get(key, 0.0),
            new_weight=current_weights.get(key, 0.0),
            is_new_position=previous_holding is None and current_holding is not None,
            is_exited_position=previous_holding is not None and current_holding is None,
            share_type=share_type,
            value_delta_thousands=new_value - old_value,
            shares_delta=new_shares - old_shares,
            value_pct_change=_pct_change(old_value, new_value),
            shares_pct_change=_pct_change(old_shares, new_shares),
            rank_delta=(old_rank - new_rank) if old_rank is not None and new_rank is not None else None,
            previous_rank=old_rank,
            current_rank=new_rank,
            official_issuer_name=(
                current_holding.official_issuer_name
                if current_holding and current_holding.official_issuer_name
                else previous_holding.official_issuer_name
                if previous_holding
                else None
            ),
        )
        deltas.append(delta)

    deltas.sort(key=lambda item: abs(item.value_delta_thousands), reverse=True)
    return FilingDelta(current_filing=current.filing, previous_filing=previous.filing, positions=deltas)
