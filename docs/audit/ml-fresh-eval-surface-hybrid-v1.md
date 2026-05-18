# Fresh Eval Surface Hybrid Materialization (ml-fresh-eval-surface-hybrid-v1)

## Executive Summary

This artifact inventories a concrete existing product-candidate pool for fresh hybrid validation. It does not run hybrid scoring, train, label, create rankings, or authorize shadow/production.

- **Status:** `materialized_needs_labels`
- **Ready for hybrid validation scoring:** False
- **Recommended next stage:** `create_fresh_eval_labeling_plan_hybrid_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Candidate Source

- **Mode:** `discovered`
- **Ranking run:** `rank-3904fec89d`
- **Family:** `emerging`
- **Corpus snapshot:** `source-snapshot-20260425-044015`
- **Rationale/block reason:** first successful deterministic discovery candidate with fresh work-set SHA

## Candidate Count And SHA

- Candidate works: 59
- Candidate work-set SHA: `1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926`
- Differs from old eval SHA: True

## Overlap With Old 217

- Overlap work count: 15
- Excluded previous eval overlap count: 15
- Confirmatory denominator excludes overlap: True

## Confirmatory Eligible Counts

- Confirmatory metric eligible works: 44
- Missing canonical work exclusions: 0

## Label Coverage And Thresholds

- Labeled works: 20
- Unlabeled works: 24
- Positive works: 20
- Negative works: 0
- Conflicting target work groups: 0
- Label coverage rate: 0.455

| Threshold | Observed | Required | Passed |
| --- | ---: | ---: | --- |
| `minimum_candidate_work_count` | 44 | 100 | False |
| `minimum_confirmatory_labeled_work_count` | 20 | 100 | False |
| `minimum_confirmatory_positive_work_count` | 20 | 50 | False |
| `minimum_confirmatory_negative_work_count` | 0 | 20 | False |
| `minimum_confirmatory_label_coverage_rate` | 0.455 | 0.600 | False |
| `minimum_distinct_negative_work_count` | 0 | 20 | False |

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
