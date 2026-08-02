# SEC Form 13F Historical Bulk Dataset

## Purpose

This milestone creates an immutable, auditable historical holdings dataset from the SEC's official quarterly Form 13F bulk ZIP files. It is the source-data foundation for the manager-security-quarter forecasting dataset described in `docs/WealthSignal_Forecasting_Spec.md`.

The implementation is `wealthsignal_pipeline.bulk_dataset`. It uses only the Python standard library and supports an offline fixture path for tests and CI.

## Official source contract

- Dataset page: `https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets`
- Table documentation: `https://www.sec.gov/files/form_13f_readme.pdf`
- ZIP pattern: `https://www.sec.gov/files/structureddata/data/form-13f-data-sets/YYYYqQ_form13f.zip`

The SEC publishes the data as filed, including amendments and possible inconsistencies. Each package contains up to seven UTF-8, tab-delimited tables. This pipeline currently requires:

- `SUBMISSION`: accession number, filing date, form type, CIK, and report period;
- `COVERPAGE`: manager name and amendment metadata;
- `INFOTABLE`: reported security holdings.

The SEC documentation states that `INFOTABLE.VALUE` was reported in thousands of dollars before January 3, 2023 and in dollars beginning on that date. WealthSignal normalizes both eras to integer `value_usd`.

## Download packages

Use a descriptive user agent containing a contact email. The downloader enforces at least 0.1 seconds between new requests, writes through a temporary file, validates ZIP integrity, records URL/time/size/SHA-256 metadata, and reuses an existing file only after its checksum passes.

```powershell
$env:PYTHONPATH="services/pipeline_worker/src"
python -m wealthsignal_pipeline.bulk_dataset download `
  --start 2023q1 `
  --end 2024q4 `
  --raw-dir data/raw/sec-13f `
  --user-agent "Vedika Shinde Research wealthsignal@example.com"
```

This command can download large public files. Do not commit ZIPs or generated datasets; `data/` is ignored by Git.

## Build a normalized dataset

Pass every local package explicitly. This makes the source set reviewable and prevents the build step from performing hidden network access.

```powershell
python -m wealthsignal_pipeline.bulk_dataset build `
  --source 2023q1=data/raw/sec-13f/2023q1_form13f.zip `
  --source 2023q2=data/raw/sec-13f/2023q2_form13f.zip `
  --output-root data/historical
```

Optional repeatable filters:

```text
--manager-cik 1067983
--security-cusip 037833100
```

For the committed offline sample:

```powershell
python -m wealthsignal_pipeline.bulk_dataset build `
  --source 2024q2=services/pipeline_worker/tests/fixtures/sec_13f_bulk_sample `
  --output-root data/historical-sample
```

## Immutable outputs

The build identity hashes the contract version, package labels, source URLs, source sizes and checksums, and normalized universe filters. Output is written to:

```text
<output-root>/sec-13f-<dataset-id>/
```

Files:

- `manifest.json`: dataset identity, creation time, complete source metadata, and output checksums;
- `effective_filings.csv`: one resolved filing per manager-report-quarter plus contributing and superseded accessions;
- `normalized_holdings.csv`: typed effective holdings with stable IDs, dollar values, weights, and ranks;
- `invalid_rows.csv`: rejected input rows and reasons;
- `quality_report.json`: measured counts, coverage, runtime, and output size.

If a matching final manifest already exists, the build returns it without rewriting files. Processing uses a dataset-specific staging directory; after an interrupted run, retrying recognizes that staging identity and rebuilds safely from the checksum-verified source packages. A completed dataset directory is never overwritten.

## Amendment policy

The policy is deterministic and deliberately visible:

1. Group holdings reports by normalized CIK and report period.
2. Order filings by filing date, amendment number, and accession number.
3. Use the latest ordinary `13F-HR` as the base filing when one exists.
4. Treat an amendment identified as new/additional holdings as additive.
5. Treat a restatement or an amendment without a recognized additive type as a replacement.
6. Deduplicate holdings by the stable security/discretion key; the last information-table key wins deterministically.
7. Record all contributing and superseded accession numbers.

This is a conservative initial policy. Before large-scale forecasting, it must be tested against real amendment examples and extended when metadata indicates partial additions or confidential-treatment behavior that cannot be represented safely by this rule.

## Normalized holding identity

The security/discretion key combines:

- CUSIP;
- title of class;
- put/call indicator;
- shares/principal type;
- investment discretion;
- other-manager reference.

The stable `holding_id` hashes normalized CIK, report period, and that key. CUSIPs must contain exactly nine permitted characters. Invalid identifiers are rejected and counted rather than silently repaired.

## Quality measurements

Each build reports:

- packages requested and processed;
- submissions processed;
- effective filings;
- raw and normalized holdings;
- unique managers and securities;
- duplicates and amendments resolved;
- invalid rows rejected;
- identifier coverage;
- runtime;
- core CSV output size.

These values are measured from execution. They are not estimates and should be cited together with the dataset ID.

## Current boundary

This component produces normalized historical source data. Downstream temporal construction and baseline evaluation are now implemented separately. This component itself does not:

- load the result into PostgreSQL;
- create manager-security-quarter forecasting examples or targets directly;
- create temporal split manifests directly;
- claim complete corporate-action identity resolution.

The first two downstream responsibilities are implemented by `wealthsignal_pipeline.temporal_dataset`; complete corporate-action identity resolution remains open work.
