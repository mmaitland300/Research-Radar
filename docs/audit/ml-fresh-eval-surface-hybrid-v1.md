# Fresh Eval Surface Hybrid Materialization (ml-fresh-eval-surface-hybrid-v1)

## Executive Summary

This artifact inventories a concrete existing product-candidate pool for fresh hybrid validation. It does not run hybrid scoring, train, label, create rankings, or authorize shadow/production.

- **Status:** `materialized_needs_labels`
- **Ready for hybrid validation scoring:** False
- **Recommended next stage:** `create_fresh_eval_labeling_plan_hybrid_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Candidate Source

- **Mode:** `explicit`
- **Ranking run:** `rank-9f4b2a2084`
- **Family:** `emerging`
- **Corpus snapshot:** `source-snapshot-fresh-hybrid-v1-20260518`
- **Rationale/block reason:** explicit ranking_run_id is fresh relative to policy disallowed surface

## Candidate Count And SHA

- Candidate works: 358
- Candidate work-set SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- Differs from old eval SHA: True

## Overlap With Old 217

- Overlap work count: 215
- Excluded previous eval overlap count: 215
- Confirmatory denominator excludes overlap: True

## Confirmatory Eligible Counts

- Confirmatory metric eligible works: 143
- Missing canonical work exclusions: 0

## Label Coverage And Thresholds

- Labeled works: 121
- Unlabeled works: 22
- Positive works: 39
- Negative works: 82
- Conflicting target work groups: 0
- Label coverage rate: 0.846

| Threshold | Observed | Required | Passed |
| --- | ---: | ---: | --- |
| `minimum_candidate_work_count` | 143 | 100 | True |
| `minimum_confirmatory_labeled_work_count` | 121 | 100 | True |
| `minimum_confirmatory_positive_work_count` | 39 | 50 | False |
| `minimum_confirmatory_negative_work_count` | 82 | 20 | True |
| `minimum_confirmatory_label_coverage_rate` | 0.846 | 0.600 | True |
| `minimum_distinct_negative_work_count` | 82 | 20 | True |

## Blocked Actions

- shadow_scoring
- production_default_change
- bridge/default ranking changes
- model/scorer deployment
- public production-readiness claims

## Not Shadow / Not Production Caveats

- Surface materialization only.
- Not confirmatory validation.
- No hybrid scoring executed.
- Existing labels may be insufficient; labeling plan may be required.
- Overlap with old 217 is smoke/regression only, not confirmatory evidence.
- No shadow/production authorization.
