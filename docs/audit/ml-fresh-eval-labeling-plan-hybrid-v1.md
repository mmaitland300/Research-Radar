# Fresh Eval Labeling Plan Hybrid (ml-fresh-eval-labeling-plan-hybrid-v1)

## Executive Summary

This plan explains why the DB-backed fresh hybrid surface is not ready for confirmatory hybrid validation scoring. It is plan-only: no DB query, scoring, training, labeling, worksheet generation, shadow, or production change is performed.

- **Surface status:** `materialized_needs_labels`
- **Ranking run:** `rank-3904fec89d`
- **Snapshot:** `source-snapshot-20260425-044015`
- **Confirmatory eligible works:** 44
- **Ready for hybrid validation scoring:** False
- **Current surface can be made ready by labeling alone:** False
- **Reason:** confirmatory_eligible_work_count 44 < policy minimum 100; labeling all 24 current unlabeled eligible works cannot satisfy candidate_count or absolute labeled_work_count 100 thresholds

## Surface Status

- Candidate works: 59
- Candidate SHA: `1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926`
- Disallowed old eval SHA: `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a`
- Old-surface overlaps excluded: 15
- Labeled works: 20
- Label coverage: 0.4545
- Positive works: 20
- Negative works: 0

## Threshold Gap Table

| Threshold | Observed | Required | Deficit | Status | Notes |
| --- | ---: | ---: | ---: | --- | --- |
| `minimum_confirmatory_candidate_work_count` | 44 | 100 | 56 | fail | Candidate-source blocker |
| `minimum_confirmatory_labeled_work_count` | 20 | 100 | 80 | fail | Hard labeled-work blocker (policy absolute minimum; requires expansion before achievable) |
| `minimum_confirmatory_label_coverage_rate` | 0.4545 | 0.6000 | 7 at current 44 | fail | ceil(0.60 * 44) = 27; 27 - 20 = 7; coverage-only on current surface; subordinate to 100 labeled-work rule |
| `minimum_confirmatory_positive_work_count` | 20 | 50 | 30 | fail | Positive-work blocker |
| `minimum_confirmatory_negative_work_count` | 0 | 20 | 20 | fail | Negative-work blocker |
| `minimum_distinct_negative_work_count` | 0 | 20 | 20 | fail | Distinct negative-work blocker |

## Two Label Deficits

- **Absolute labeled-work deficit:** 80 additional labeled works are needed to meet the policy absolute minimum. Formula: `policy_minimum_confirmatory_labeled_work_count - observed_labeled_works = 100 - 20`.
- **Coverage-only deficit on current 44-work surface:** 7 additional labeled works would reach 60% coverage on this slice. Formula: `ceil(0.60 * confirmatory_eligible_work_count) - observed_labeled_works = ceil(0.60 * 44) - 20 = 27 - 20`.
- The coverage-only gap is informational. It does not override the absolute 100 labeled-work minimum or the 100 eligible-work candidate-source minimum.

## Why Labeling Alone Cannot Make This Surface Ready

confirmatory_eligible_work_count 44 < policy minimum 100; labeling all 24 current unlabeled eligible works cannot satisfy candidate_count or absolute labeled_work_count 100 thresholds

## Current-Surface Labeling Opportunity

- Current confirmatory eligible works: 44
- Already labeled works: 20
- Unlabeled confirmatory eligible works: 24
- Note: Labeling all 24 is useful for partial audit practice but insufficient for readiness (candidate count < 100; absolute labeled minimum 100; 0 negatives).

## Required Larger Fresh Candidate Source

- Minimum additional confirmatory-eligible works needed: 56
- Minimum additional labeled works needed for policy absolute minimum: 80
- Minimum negative works needed: 20
- Minimum distinct negative works needed: 20

## Recommended Next Stages

- `create_fresh_product_candidate_ranking_source_v1`
- `create_fresh_eval_labeling_worksheet_hybrid_v1`

## Not Shadow / Not Production

- Shadow scoring allowed: False
- Production default allowed: False
- Confirmatory validation complete: False

## Caveats

- Plan only; no labels collected and no worksheet written.
- Not confirmatory validation.
- Current 44-work confirmatory surface cannot be made ready by labeling alone.
- A larger fresh product-candidate source is required before hybrid validation scoring.
- Negative labels are currently absent from the confirmatory-eligible surface.
- No shadow or production authorization.
