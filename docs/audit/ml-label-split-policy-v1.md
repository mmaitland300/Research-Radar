# ML Label Split Policy (ml-label-split-policy-v1)

## Executive Summary

This artifact defines the v1 split contract for future offline ranker research. It does not assign folds, train a model, or authorize production ranking changes.

- **Allowed v1 target:** `good_or_acceptable`
- **Forbidden v1 target:** `surprising_or_useful`
- **Grouping unit:** canonical OpenAlex work identity, not row_id
- **Production status:** blocked; offline research infrastructure only

## Target Policy

| Target | Status | Production Eligible | Reason |
| --- | --- | --- | --- |
| `good_or_acceptable` | eligible_for_offline_ranker_research | False | Current evidence supports treating good_or_acceptable as the only v1 offline research target. |
| `surprising_or_useful` | excluded_from_v1_split | False | Hard exclusion from v1 split eligibility because current evidence shows weak/inconsistent transfer and rubric instability. |

## Grouping And Leakage Policy

Future split generation must normalize `work_id`, `openalex_work_id`, then `paper_id` to an uppercase OpenAlex W token. All observations for the same canonical work must share one split assignment.

- No canonical work may appear in both train and eval.
- No row_id-level random splitting without work grouping.
- No target may use labels from its eval group in feature construction or sampling.

## Dataset Inventory

| Metric | Count |
| --- | ---: |
| Total observation rows | 427 |
| Explicit labeled rows | 427 |
| Explicit audit labeled rows | 427 |
| Unique canonical work groups | 342 |
| Duplicate canonical work groups | 55 |
| Rows missing canonical work id | 0 |
| v1 good_or_acceptable eligible observations | 427 |

### Rows By Review Pool Variant

| Review Pool Variant | Rows |
| --- | ---: |
| `(null)` | 65 |
| `bridge_eligible_only` | 20 |
| `full_family_top_k` | 40 |
| `ml_blind_snapshot_audit` | 120 |
| `ml_contrastive_offline_audit` | 45 |
| `ml_emerging_target_gap_audit:good_or_acceptable` | 25 |
| `ml_external_near_miss_audit` | 60 |
| `ml_hard_negative_audit` | 7 |
| `ml_transfer_gap_audit` | 45 |

### Target Counts

| Target | True | False | Null | Total |
| --- | ---: | ---: | ---: | ---: |
| `good_or_acceptable` | 309 | 118 | 0 | 427 |
| `surprising_or_useful` | 326 | 101 | 0 | 427 |

## Duplicate And Conflict Policy

Duplicate and conflicting observations are preserved. Duplicate work groups must be assigned as a unit, and downstream experiments must cite `ml-label-conflict-policy.md` when reporting conflicts or exclusions.

| Rollup | Count |
| --- | ---: |
| Duplicate paper IDs in dataset metadata | 55 |
| Raw label conflicts in dataset metadata | 83 |
| Derived target conflicts in dataset metadata | 11 |

## Transfer-Readiness Note

Transfer-readiness is evidence-only in this policy; it is not used as a hard gate except where policy metadata explicitly states target eligibility.

## Caveats

- Not validation.
- Not production.
- Single-reviewer audit labels.
- No fold assignment yet.
- No ranking/API/web changes.
- No production ranking change implied.

No production ranking, API, web, or default behavior change is implied by this policy.
