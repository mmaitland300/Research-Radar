# Fresh Eval Surface Hybrid Materialization (ml-fresh-eval-surface-hybrid-v1)

## Executive Summary

This artifact inventories a concrete existing product-candidate pool for fresh hybrid validation. It does not run hybrid scoring, train, label, create rankings, or authorize shadow/production.

- **Status:** `blocked_no_fresh_candidate_source`
- **Ready for hybrid validation scoring:** False
- **Recommended next stage:** `create_new_product_candidate_ranking_run_or_snapshot`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Candidate Source

- **Mode:** `blocked`
- **Ranking run:** `None`
- **Family:** `emerging`
- **Corpus snapshot:** `None`
- **Rationale/block reason:** database connection failed before candidate discovery: ConnectionTimeout

## Candidate Count And SHA

- Candidate works: 0
- Candidate work-set SHA: `None`
- Differs from old eval SHA: False

## Overlap With Old 217

- Overlap work count: 0
- Excluded previous eval overlap count: 0
- Confirmatory denominator excludes overlap: True

## Confirmatory Eligible Counts

- Confirmatory metric eligible works: 0
- Missing canonical work exclusions: 0

## Label Coverage And Thresholds

- Labeled works: 0
- Unlabeled works: 0
- Positive works: 0
- Negative works: 0
- Conflicting target work groups: 0
- Label coverage rate: 0.000

| Threshold | Observed | Required | Passed |
| --- | ---: | ---: | --- |
| `minimum_candidate_work_count` | 0 | 100 | False |
| `minimum_confirmatory_labeled_work_count` | 0 | 100 | False |
| `minimum_confirmatory_positive_work_count` | 0 | 50 | False |
| `minimum_confirmatory_negative_work_count` | 0 | 20 | False |
| `minimum_confirmatory_label_coverage_rate` | 0.000 | 0.600 | False |
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
