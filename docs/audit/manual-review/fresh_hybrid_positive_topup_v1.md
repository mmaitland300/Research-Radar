# Fresh Hybrid Positive Top-Up Worksheet (ml-fresh-eval-positive-topup-worksheet-hybrid-v1)

## Executive Summary

This reviewer-blank worksheet targets the remaining positive-label shortfall on the fresh hybrid eval surface. It does not ingest labels, run validation, or authorize shadow/production use.

- Ranking run: `rank-9f4b2a2084`
- Family: `emerging`
- Snapshot: `source-snapshot-fresh-hybrid-v1-20260518`
- Confirmatory eligible works: 143
- Labeled / unlabeled works before top-up: 121 / 22
- Positive / negative / distinct negative works before top-up: 39 / 82 / 82
- Positive work threshold: 39 / 50 (deficit 11)
- Requested / generated worksheet rows: 0 / 22

## Why This Worksheet Exists

The only positive threshold short by policy is `minimum_confirmatory_positive_work_count`.
All policy thresholds except `minimum_confirmatory_positive_work_count` pass. The current surface needs at least 11 additional `good` or `acceptable` works to reach the 50 work-level positive threshold.

## Thresholds Before Top-Up

| Threshold | Observed | Required | Deficit | Passed |
| --- | ---: | ---: | ---: | --- |
| `minimum_candidate_work_count` | 143 | 100 | 0 | True |
| `minimum_confirmatory_label_coverage_rate` | 0.8461538461538461 | 0.6 | 0 | True |
| `minimum_confirmatory_labeled_work_count` | 121 | 100 | 0 | True |
| `minimum_confirmatory_negative_work_count` | 82 | 20 | 0 | True |
| `minimum_confirmatory_positive_work_count` | 39 | 50 | 11 | False |
| `minimum_distinct_negative_work_count` | 82 | 20 | 0 | True |

## Rows And Ordering

- Unlabeled confirmatory-eligible universe: 22
- Rows generated: 22
- Selection excludes old-surface overlaps and works already explicitly labeled in `ml-label-dataset-v9`.
- Ordering is label-blind: `final_score` descending, then `heuristic_rank` ascending, then canonical work id ascending.
- Every row uses `sample_reason = fresh_hybrid_positive_topup`.

## Labeling Instructions

To reach the policy floor, the future ingest must add at least 11 new `good` or `acceptable` works among these rows. Filling all rows is useful, but readiness depends on work-level positives, not merely the number of completed labels.

Allowed label values:

- relevance_label: good, acceptable, miss, irrelevant
- novelty_label: surprising, useful, obvious, not_useful, neither
- bridge_like_label: yes, partial, no, not_applicable

## Worksheet Only / Not Validation / No Shadow-Prod

- Manual labeling worksheet only; no labels are ingested by this command.
- This is not validation and does not complete confirmatory hybrid validation.
- Rows are selected only from unlabeled confirmatory-eligible fresh-surface works.
- Ordering is label-blind: final_score descending, heuristic_rank ascending, canonical work id ascending.
- No scoring, ranking, training, embeddings, shadow, or production authorization.

## Next Step

Save a dated labeled CSV, then use a future explicit v10 or dedicated top-up label ingest. Rematerialize the fresh surface after ingest; run hybrid validation only if `ready_for_hybrid_validation_scoring` becomes true.
