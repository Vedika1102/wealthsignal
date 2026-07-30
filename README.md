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
        │       ├── feature_engineering.py
        │       ├── ingest.py
        │       ├── materiality.py
        │       ├── models.py
        │       ├── portfolios.py
        │       ├── persistence.py
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
- primary document metadata parsing
- SQLite persistence for filings, holdings, and deltas
- ingest CLI for pulling recent 13F filings end to end
- first-pass materiality feature generation
- explainable rule-based alert scoring
- synthetic client portfolio impact scoring
- live SEC artifact resolution verified against a real 13F filing
- unit tests for parser, SEC utilities, and delta computation

Next:

- persist alert candidates and client impact results
- sector enrichment and richer portfolio context
- train the first baseline materiality classifier

## Local Ingest Example

```bash
set PYTHONPATH=services/pipeline_worker/src
python -m wealthsignal_pipeline.cli ^
  --cik 1067983 ^
  --user-agent "Vedika Shinde vedikashinde11feb@gmail.com" ^
  --db-path data/wealthsignal.db
```

This prints:

- latest ingested 13F filings,
- stored quarter-over-quarter delta size,
- top explainable alert candidates,
- top impacted synthetic client for each alert.
