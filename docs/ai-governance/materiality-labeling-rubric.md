# Materiality Gold-Set Labeling Rubric

## Purpose

The gold set measures whether WealthSignal prioritizes events that deserve an advisor's review. It must remain independent of the rule-generated `weak_label` used for model training.

Label the filing event using only information available at or before the filing date. Do not use subsequent price performance to decide the label.

## Unit of Review

Each row is one fund-quarter-position-change event, uniquely identified by the current filing accession and holding key.

## Labels

- `1 — advisor-worthy`: A reasonable wealth advisor should review the change because it appears strategic, materially changes an important exposure, or is relevant to concentrated client holdings.
- `0 — routine`: The change is small, mechanical, duplicative, or unlikely to affect an advisor's review priorities.

## Positive Indicators

Assign `1` when the available evidence supports one or more of these conditions:

- a new position enters the fund's top 10 holdings;
- a prior top-20 position is fully exited;
- the position weight changes materially relative to the rest of the filing;
- the event contributes substantially to quarterly turnover;
- the change is part of a meaningful sector allocation shift;
- the name or sector has concentrated exposure in plausible wealth portfolios.

These are evidence, not automatic rules. Reviewers must apply judgment and explain why the event merits attention.

## Negative Indicators

Assign `0` when the change is best explained by:

- a small weight change with little effect on portfolio composition;
- broad proportional rebalancing;
- a low-ranked position with limited client relevance;
- identifier or amendment noise;
- a change that looks large in percentage terms only because the prior position was tiny.

## Required Review Fields

- `manual_label`: `0` or `1`.
- `review_reason`: one or two sentences explaining the decision without copying the rule score.
- `reviewer_id`: stable reviewer identifier, not necessarily a personal name.
- `reviewed_at`: ISO 8601 date or timestamp.

The `weak_label` and `rule_score` columns provide traceability but must not substitute for independent judgment.

## Quality Control

Target 150–300 events spanning multiple filers, quarters, sectors, event types, and score ranges. At least 20% should be independently reviewed by a second reviewer. Resolve disagreements through documented adjudication before using the rows for final evaluation.

Do not change labels after seeing model predictions on the evaluation set. Create a new version when corrections are necessary, and retain the prior version for traceability.

## Acceptance Criteria

A release-ready gold set has:

- no duplicate event IDs;
- every required review field completed;
- both positive and negative examples;
- representation across more than one filer and reporting quarter;
- a documented dataset version and adjudication date.

## Evaluation and Leakage Control

Run the validator before any evaluation. The evaluation report identifies the dataset by the SHA-256 digest of the reviewed CSV so results remain tied to the exact labels used.

Rule-engine metrics can be treated as holdout results because manual labels are independent of the rules. A trained model must exclude all gold-set event IDs from training before its metrics are described as holdout performance. Reports generated from existing stored predictions label those results `diagnostic_in_sample` until that exclusion is implemented and verified.
