from dataclasses import replace

from wealthsignal_pipeline.bulk_dataset import BulkHolding
from wealthsignal_pipeline.v2_reference import _aggregate, v2_security_key


def _holding(*, value: int, put_call: str = "", key: str = "1") -> BulkHolding:
    return BulkHolding(
        source_accession_number="a", info_table_key=key, issuer_name="Issuer",
        title_of_class="COM", cusip="037833100", figi="", value_usd=value,
        shares_or_principal=float(value), share_type="SH", put_call=put_call,
        investment_discretion="SOLE", other_manager="", voting_authority_sole=0,
        voting_authority_shared=0, voting_authority_none=0,
    )


def test_v2_security_identity_is_cusip_plus_side() -> None:
    assert v2_security_key(_holding(value=1)) == "037833100|LONG"
    assert v2_security_key(_holding(value=1, put_call="put")) == "037833100|PUT"


def test_v2_duplicate_rows_are_summed() -> None:
    rows, duplicates = _aggregate([_holding(value=2), _holding(value=3, key="2")])
    assert duplicates == 1
    assert rows["037833100|LONG"].value_usd == 5
    assert rows["037833100|LONG"].shares_or_principal == 5.0


def test_v2_invalid_cusips_are_excluded() -> None:
    invalid = replace(_holding(value=2), cusip="BAD")
    rows, duplicates = _aggregate([invalid])
    assert rows == {}
    assert duplicates == 0
