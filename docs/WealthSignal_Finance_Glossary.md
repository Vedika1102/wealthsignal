# WealthSignal Finance Glossary

This glossary is written for the WealthSignal project. Each term includes:

- what it means,
- why it matters here,
- what mistake to avoid.

---

## 13F

### Form 13F

What it means:

- A regulatory filing submitted by certain institutional investment managers.

Why it matters here:

- It is the core source of holdings data for WealthSignal.

Mistake to avoid:

- Do not treat it as real-time trade data. It is a delayed disclosure.

### 13F-HR

What it means:

- The regular quarterly `Holding Report` version of Form 13F.

Why it matters here:

- Most of the ingestion pipeline will focus on these filings.

### 13F-HR/A

What it means:

- An amended 13F filing.

Why it matters here:

- Your ingestion logic must detect and handle amendments correctly.

Mistake to avoid:

- Do not assume the first filing is the final truth for that quarter.

### Section 13(f) Securities

What it means:

- A defined list of reportable securities maintained by the SEC.

Why it matters here:

- Not every security is reportable on Form 13F.

Mistake to avoid:

- Do not assume a 13F shows a fund’s entire portfolio.

### Institutional Investment Manager

What it means:

- An entity or person that exercises investment discretion over securities accounts.

Why it matters here:

- These are the filers whose behavior WealthSignal tracks.

Examples:

- hedge funds
- banks
- insurance firms
- investment advisers
- pension managers

### $100 Million Threshold

What it means:

- Managers with investment discretion over `$100 million or more` in Section 13(f) securities must file Form 13F.

Why it matters here:

- This is the filing threshold behind the dataset.

Mistake to avoid:

- Do not confuse this with total firm AUM. It is tied to reportable 13(f) securities.

### 45-Day Delay

What it means:

- Form 13F is generally due within `45 days after quarter-end`.

Why it matters here:

- This defines the timeliness limit of the product.

Mistake to avoid:

- Do not claim the platform detects trades as they happen.

---

## Filing Entities and Identifiers

### Filer

What it means:

- The institution submitting the 13F.

Why it matters here:

- WealthSignal groups filings and behavior history by filer.

### CIK

What it means:

- Central Index Key, a unique SEC identifier for the filer.

Why it matters here:

- It is the most reliable key for linking filings across quarters.

### Information Table

What it means:

- The detailed holdings table within a 13F filing.

Why it matters here:

- Most of your parsing work happens here.

Common fields:

- issuer name
- CUSIP
- value
- shares or principal amount
- put/call
- discretion

### CUSIP

What it means:

- A security identifier used in U.S. markets.

Why it matters here:

- 13F filings often report `CUSIP`, not ticker, so you need mapping logic.

Mistake to avoid:

- Do not assume ticker is always directly available or stable over time.

### Ticker

What it means:

- The stock symbol used in trading venues, like `AAPL`.

Why it matters here:

- Easier for portfolio overlap, charting, and user-facing explanations.

Mistake to avoid:

- Ticker mapping can be messy after corporate actions or stale mappings.

---

## Portfolio and Holdings Terms

### Holding

What it means:

- A reported position in a security.

Why it matters here:

- The platform compares holdings quarter over quarter.

### Position

What it means:

- A stake in a given security, usually measured by shares and market value.

Why it matters here:

- Position changes are the atomic events for your model.

### Long Position

What it means:

- Ownership that benefits if the asset price rises.

Why it matters here:

- Most visible 13F holdings are long positions.

Mistake to avoid:

- 13F does not fully reveal short exposure or hedges.

### Market Value

What it means:

- Dollar value of the holding as of quarter-end.

Why it matters here:

- A large dollar move is often more meaningful than a small one.

### Position Weight

What it means:

- The holding’s share of the total reported portfolio value.

Why it matters here:

- Weight change is usually more informative than raw shares alone.

### New Position

What it means:

- A security that appears this quarter but was absent last quarter.

Why it matters here:

- Often a candidate signal for a strategic change.

### Full Exit

What it means:

- A security present last quarter but absent this quarter.

Why it matters here:

- Exits can be highly informative, especially for large holdings.

### Top Holdings

What it means:

- Largest positions in the reported portfolio by market value or weight.

Why it matters here:

- A move in a top holding is more likely to matter than a move in a tiny tail position.

### Concentration

What it means:

- How much of a portfolio is concentrated in a few names or sectors.

Why it matters here:

- High-conviction concentrated moves should score higher in WealthSignal.

### Turnover

What it means:

- How much a portfolio changes over time.

Why it matters here:

- Some funds naturally churn more than others; your model should account for that.

### Rebalancing

What it means:

- Routine adjustments to restore target exposures.

Why it matters here:

- This is the main negative class for your classifier.

Mistake to avoid:

- Not every large numeric change is a strategic signal.

### Sector Allocation

What it means:

- The proportion of portfolio value assigned to sectors like technology, healthcare, or financials.

Why it matters here:

- Sector rotation is often more interpretable than single-name movement.

### Sector Rotation

What it means:

- Shifting portfolio exposure from one sector to another.

Why it matters here:

- WealthSignal should detect these higher-level strategic moves.

---

## Wealth Management Context

### Advisor

What it means:

- The relationship manager or wealth advisor serving end clients.

Why it matters here:

- The system is not built for hedge fund PMs. It is built for advisors deciding what to review.

### Client Portfolio

What it means:

- A client’s investable holdings across securities and sectors.

Why it matters here:

- WealthSignal ranks alerts based on relevance to actual client exposure.

### Exposure

What it means:

- How much a portfolio is affected by a security, sector, theme, or risk factor.

Why it matters here:

- This is the basis for impact scoring.

### Overlap

What it means:

- The degree to which a client portfolio shares names or sectors with a detected institutional change.

Why it matters here:

- High overlap means the alert is more relevant.

### Recommendation

What it means:

- The system’s suggested review action, such as informational, review, or urgent.

Why it matters here:

- Keep this explainable and policy-driven in V1.

Mistake to avoid:

- Do not position this as autonomous financial advice.

---

## Modeling Terms

### Materiality

What it means:

- Whether a filing change is important enough to warrant advisor attention.

Why it matters here:

- This should be the central ML target.

Mistake to avoid:

- Do not define materiality only by future stock performance.

### Material Shift

What it means:

- A change that looks strategic, high-conviction, or relevant to client exposure.

Examples:

- new top-10 position
- full exit of a large legacy holding
- major weight increase
- meaningful sector reallocation

### Routine Rebalance

What it means:

- A normal adjustment that is not especially advisor-worthy.

Why it matters here:

- This is your main contrast class.

### Conviction

What it means:

- Strength of the manager’s apparent belief in a position, often inferred from size and persistence.

Why it matters here:

- Useful as a feature, but it is inferred rather than directly observed.

### Event Study

What it means:

- Analysis of what happened in the market after a disclosed event.

Why it matters here:

- Helpful as secondary validation, not as the primary truth label.

### Calibration

What it means:

- Whether predicted probabilities correspond to actual observed frequencies.

Why it matters here:

- If the system says `0.85 materiality`, that number should be trustworthy.

### Drift

What it means:

- Change over time in data distributions or model behavior.

Why it matters here:

- Filing patterns and market regimes change, so monitoring is necessary.

### Explainability

What it means:

- The ability to explain why the model made a decision.

Why it matters here:

- In a financial setting, black-box alerts are weak and hard to trust.

---

## Governance and Compliance Terms

### Model Card

What it means:

- A document describing model purpose, data, metrics, limitations, and risks.

Why it matters here:

- It makes the project look like enterprise applied ML rather than a hobby model.

### Audit Trail

What it means:

- A record of what the system did, when, and why.

Why it matters here:

- Important for debugging, governance, and reviewer confidence.

### Data Lineage

What it means:

- The trace from raw source data to final features and outputs.

Why it matters here:

- Useful for governance and reproducibility.

### Right to Explanation

What it means:

- In practice here, the ability to justify why an alert was generated.

Why it matters here:

- Financial users need evidence, not just a score.

---

## Things 13F Does Not Tell You

This is one of the most important sections to remember.

13F data is useful, but incomplete.

It does not fully tell you:

- intraday or real-time trades,
- exact execution timing,
- full short exposure,
- all derivatives exposure,
- complete hedge structure,
- rationale of the manager.

WealthSignal should be honest about that.

---

## Terms You Should Learn First While Building

Start with these before moving into modeling:

1. `13F`
2. `institutional investment manager`
3. `CIK`
4. `CUSIP`
5. `position weight`
6. `new position`
7. `full exit`
8. `sector allocation`
9. `portfolio overlap`
10. `materiality`

These ten terms are enough to understand the first build phases.
