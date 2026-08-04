import pytest

from wealthsignal_pipeline.cloud2_spark import security_key_expression


def test_security_key_expression_uses_frozen_cusip_side_identity() -> None:
    expression = security_key_expression("normalized_cusip", "option_side")
    assert expression == (
        "concat(normalized_cusip, '|', case when upper(trim(coalesce(option_side, ''))) "
        "in ('PUT','CALL') then upper(trim(option_side)) else 'LONG' end)"
    )


def test_cloud2_module_does_not_require_pyspark_for_contract_import() -> None:
    assert "LONG" in security_key_expression()
