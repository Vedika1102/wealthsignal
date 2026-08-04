import pytest

from wealthsignal_pipeline.identifiers import cusip_normalization_sql, normalize_cusip, validated_cusip


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (" 06738c778 ", "06738C778"),
        ("81369y209", "81369Y209"),
        ("AB-12.3@#*4", "AB123@#*4"),
        ("", ""),
        ("é12345678", "12345678"),
    ],
)
def test_normalize_cusip_contract(raw: str, expected: str) -> None:
    assert normalize_cusip(raw) == expected


def test_validated_cusip_requires_nine_post_normalization_characters() -> None:
    assert validated_cusip(" 06738c778 ") == "06738C778"
    with pytest.raises(ValueError, match="after normalization"):
        validated_cusip("BAD")


def test_spark_contract_uppercases_before_filtering() -> None:
    assert cusip_normalization_sql("CUSIP") == (
        "regexp_replace(upper(trim(CUSIP)), '[^A-Z0-9*@#]', '')"
    )
