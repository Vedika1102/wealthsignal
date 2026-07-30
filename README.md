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
        │       ├── delta_engine.py
        │       ├── edgar_client.py
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
- SEC submissions and filing artifact discovery utilities
- quarter-over-quarter position delta engine
- live SEC artifact resolution verified against a real 13F filing
- unit tests for parser, SEC utilities, and delta computation

Next:

- real filing downloader and end-to-end parser orchestration
- normalized persistence layer
- feature engineering for materiality classification
- synthetic client portfolio generation
