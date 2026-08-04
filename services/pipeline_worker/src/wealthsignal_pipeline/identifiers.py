"""Frozen Protocol V2 identifier normalization contracts."""

from __future__ import annotations


CUSIP_ALLOWED_CHARACTERS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789*@#")


def normalize_cusip(value: str) -> str:
    """Trim, uppercase, then retain only ASCII CUSIP-permitted characters."""
    return "".join(character for character in value.strip().upper() if character in CUSIP_ALLOWED_CHARACTERS)


def validated_cusip(value: str) -> str:
    """Return a normalized nine-character CUSIP or raise a stable error."""
    normalized = normalize_cusip(value)
    if len(normalized) != 9:
        raise ValueError(f"CUSIP must contain 9 valid characters after normalization: {value!r}")
    return normalized


def cusip_normalization_sql(column: str) -> str:
    """Return Spark SQL implementing the identical normalization order."""
    return f"regexp_replace(upper(trim({column})), '[^A-Z0-9*@#]', '')"
