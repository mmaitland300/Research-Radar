# Learned Scorer Holdout Assignment (ml-learned-scorer-holdout-assignment-v1)

## Executive Summary

This materializes the holdout policy into deterministic per-row and per-work assignments. It does not train or refit a scorer, rerun product scoring, or authorize shadow or production use.

- **Strategy:** `product_candidate_snapshot_holdout`
- **Eval work count:** 217
- **Eval work-set SHA256:** `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a`
- **Train/eval work overlap:** 0
- **Global zero leakage assertion:** True

## Strategy And Eval Work-Set Source

The eval set is all unique canonical works from `ml-offline-production-candidate-scoring-v2` candidate rows for ranking run `rank-ee2ba6c816` and family `emerging`.

## Leakage Report

| Measure | Value |
| --- | ---: |
| Train unique works | 125 |
| Eval unique works | 217 |
| Assigned eval unique works | 217 |
| Train/eval work overlap | 0 |
| Train/eval row_id overlap | 0 |
| Per-work violations | 0 |

## Class Balance

### Observation Level

| Assignment | Count | Positive | Negative | Positive Rate |
| --- | ---: | ---: | ---: | ---: |
| Train | 141 | 54 | 87 | 0.383 |
| Eval | 286 | 255 | 31 | 0.892 |

### Work Level Any-Positive

| Assignment | Works | Positive Works | Negative Works | Conflicting Works |
| --- | ---: | ---: | ---: | ---: |
| Train | 125 | 38 | 87 | 0 |
| Eval | 217 | 190 | 27 | 3 |

## Duplicate/Conflict Summary

| Scope | Duplicate Work Groups | Duplicate Pressure | Conflicting Work Groups |
| --- | ---: | ---: | ---: |
| Global | 55 | 85 | 3 |
| Train | 13 | 16 | 0 |
| Eval | 42 | 69 | 3 |

## Train Negative Advisory

- **Threshold:** 10 train negative works
- **Triggered:** False

## Assignments Overview

- **Eligible assigned rows:** 427
- **Train rows:** 141
- **Eval rows:** 286
- **Train works:** 125
- **Assigned eval works:** 217
- **Unlabeled eval works reserved from training:** 0

## Next Step

`ml-offline-audit-embedding-scorer-export-v2`

## Not Validation / Not Shadow / Not Production

- Not validation.
- Assignment materializes policy only; no model fit.
- Eval work set is full product snapshot; label metrics use labeled eval observations only.
- Train arm may be small; report class balance before export v2.
- Observation-level conflicts preserved.
- No shadow/production authorization.
- Shadow scoring authorized: False
- Production default authorized: False
