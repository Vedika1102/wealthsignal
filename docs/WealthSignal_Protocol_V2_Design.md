# Protocol V2 Design Summary

Protocol V2 is frozen at the design level before new data download or evaluation. Its authoritative rules are in `docs/ai-governance/forecast-comparison-protocol-v2.md`, with machine-readable parameters in `docs/ai-governance/forecast-protocol-v2-config.json`.

The official SEC inventory available on 2026-08-02 ends with the March–May 2026 filing package, which is sufficient for report quarter 2026 Q1. Report quarter 2026 Q2 is due on August 14, 2026 and is deliberately reserved as a prospective untouched test.

V2 expands from 10 to a deterministic main-study target of 50 managers selected only from 2019–2023 history. The first 10 and first 25 managers in the same frozen ordering are engineering subsets for smoke tests and graph debugging, not the main reported comparison. An optional 99-manager scale study may follow the stable 50-manager NAVIS reproduction to approach the paper's scale. V2 uses 2024 Q1 through 2026 Q1 for walk-forward validation and does not condition cohort membership on future continuity. Security identity is revised to aggregate filing rows at normalized CUSIP plus long/put/call side, while unsupported CUSIP changes remain explicit breaks. Current index membership is never used.

No V2 SEC package was downloaded and no V2 model or holdout was evaluated during protocol design. After the design checkpoint, the 30 declared development packages through May 2026 were acquired and checksum-manifested, and the deterministic 50-manager cohort was selected from 2019 Q1–2023 Q4 report periods. The prospective 2026 Q2 source remains unopened and must stay so until the full release conditions are satisfied. The next execution step is leakage-audited validation-dataset construction.
