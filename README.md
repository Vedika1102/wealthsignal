# WealthSignal

WealthSignal is an applied AI/ML project that ingests real SEC `13F-HR` filings, computes institutional portfolio changes, scores advisor-worthy materiality, and ranks downstream client portfolio impact.

This repository starts with the `13F ingestion foundation`, because a credible platform depends on:

1. reliable parsing of real filings,
2. clean normalized holdings data,
3. quarter-over-quarter change computation,
4. explainable downstream scoring.

## V1 Focus

The initial build targets:

- SEC filing ingestion
- XML holdings parsing
- normalized holdings models
- delta computation
- materiality feature generation
- client impact scoring
- alert APIs

## Repository Layout

```text
wealthsignal/
├── README.md
├── pyproject.toml
└── services/
    ├── decision_api/
    │   └── app/
    │       └── main.py
    └── pipeline_worker/
        ├── src/
        │   └── wealthsignal_pipeline/
        │       ├── __init__.py
        │       ├── models.py
        │       └── parser.py
        └── tests/
            └── test_parser.py
```

## Current State

Implemented:

- base project scaffold
- holdings data models
- first-pass 13F information table parser
- SEC client URL and header utilities
- parser unit tests

Next:

- SEC client for index and filing downloads
- normalized persistence layer
- prior-quarter delta engine
- feature engineering for materiality classification
