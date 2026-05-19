# Fresh Hybrid Eval Labeling Worksheet (ml-fresh-eval-labeling-worksheet-hybrid-v1)

## Summary

This worksheet is for manual labels on fresh confirmatory-eligible hybrid eval works. The CSV label fields are blank; this command does not ingest labels or run validation.

- Ranking run: `rank-9f4b2a2084`
- Family: `emerging`
- Snapshot: `source-snapshot-fresh-hybrid-v1-20260518`
- Confirmatory eligible works: 143
- Existing labeled works: 1
- Requested / achieved worksheet rows: 120 / 120
- Shortfall: 0

## Threshold Gaps Before Labeling

| Threshold | Observed | Required | Deficit | Passed |
| --- | ---: | ---: | ---: | --- |
| `minimum_candidate_work_count` | 143 | 100 | 0 | True |
| `minimum_confirmatory_label_coverage_rate` | 0.006993006993006993 | 0.6 | 85 | False |
| `minimum_confirmatory_labeled_work_count` | 1 | 100 | 99 | False |
| `minimum_confirmatory_negative_work_count` | 0 | 20 | 20 | False |
| `minimum_confirmatory_positive_work_count` | 1 | 50 | 49 | False |
| `minimum_distinct_negative_work_count` | 0 | 20 | 20 | False |

## Sampling Strategy

- fresh_hybrid_negative_candidate: 42
- fresh_hybrid_score_boundary: 30
- fresh_hybrid_score_spread: 15
- fresh_hybrid_seeded_fill: 33

## Rubric

- relevance_label: good, acceptable, miss, irrelevant
- novelty_label: surprising, useful, obvious, not_useful, neither
- bridge_like_label: yes, partial, no, not_applicable
- reviewer_notes: free text

## Worksheet Only / Not Validation / No Shadow-Prod

- Manual labeling worksheet only; no labels are ingested by this command.
- Rows are future confirmatory eval candidates, not validation results.
- Old 217-work overlaps and existing v8-labeled canonical works are excluded by default.
- Reviewer label columns are intentionally blank.
- No scoring, training, ranking, embeddings, hybrid validation, shadow, or production authorization.

## Next Step

Human reviewer fills the CSV, then a future explicit label ingest creates a v9 or fresh-hybrid label ingest artifact. Hybrid validation remains blocked until materialization shows policy thresholds pass.
