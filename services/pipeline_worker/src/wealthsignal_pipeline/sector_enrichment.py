from __future__ import annotations


SECTOR_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    (
        "Technology",
        (
            "APPLE",
            "MICROSOFT",
            "NVIDIA",
            "ORACLE",
            "SNOWFLAKE",
            "INTERNATIONAL BUSINESS MACHINES",
            "TAIWAN SEMICONDUCTOR",
            "HEWLETT PACKARD",
        ),
    ),
    (
        "Communication Services",
        (
            "ALPHABET",
            "META",
            "NETFLIX",
            "DISNEY",
            "SPOTIFY",
            "COMCAST",
            "CHARTER COMMUNICATIONS",
        ),
    ),
    (
        "Financials",
        (
            "VISA",
            "MASTERCARD",
            "AMERICAN EXPRESS",
            "BANK",
            "CAPITAL ONE",
            "JPMORGAN",
            "GOLDMAN",
            "MORGAN STANLEY",
            "PAYPAL",
            "DISCOVER",
            "MOODY",
            "S&P GLOBAL",
        ),
    ),
    (
        "Consumer Staples",
        (
            "KRAFT HEINZ",
            "COCA COLA",
            "PEPSICO",
            "PROCTER",
            "COSTCO",
            "MONDELEZ",
            "KROGER",
            "WALMART",
        ),
    ),
    (
        "Consumer Discretionary",
        (
            "AMAZON",
            "TESLA",
            "HOME DEPOT",
            "LOWE",
            "MCDONALD",
            "STARBUCKS",
            "NIKE",
            "BOOKING",
        ),
    ),
    (
        "Healthcare",
        (
            "HEALTH",
            "PFIZER",
            "MERCK",
            "ABBVIE",
            "ELI LILLY",
            "JOHNSON & JOHNSON",
            "UNITEDHEALTH",
            "DANAHER",
            "THERMO FISHER",
        ),
    ),
    (
        "Energy",
        (
            "CHEVRON",
            "OCCIDENTAL",
            "EXXON",
            "CONOCOPHILLIPS",
            "SCHLUMBERGER",
            "EOG",
            "ENERGY",
        ),
    ),
    (
        "Industrials",
        (
            "DELTA AIR",
            "BOEING",
            "CATERPILLAR",
            "UNION PACIFIC",
            "LOCKHEED",
            "GENERAL ELECTRIC",
            "3M",
        ),
    ),
    (
        "Utilities",
        (
            "DUKE ENERGY",
            "NEXTERA",
            "SOUTHERN CO",
            "DOMINION ENERGY",
        ),
    ),
    (
        "Real Estate",
        (
            "REALTY INCOME",
            "PROLOGIS",
            "SIMON PROPERTY",
            "WELLTOWER",
        ),
    ),
]


def infer_sector(issuer_name: str) -> str:
    """Infer a coarse sector label from issuer name patterns.

    This is a first-pass enrichment layer for the demo. A later version should
    replace it with a dedicated reference-data service.
    """

    normalized = issuer_name.upper()
    for sector, patterns in SECTOR_PATTERNS:
        if any(pattern in normalized for pattern in patterns):
            return sector
    return "Unknown"
