# Shadow Generalization Second Surface Labeling Worksheet (ml-shadow-scorer-second-surface-labeling-worksheet-v1)

## Executive Summary

This worksheet contains reviewer-blank rows for all confirmatory-eligible works on the selected second shadow-generalization surface. It is for manual labels only; it does not ingest labels, score, rank, embed, or authorize shadow/production.

- Ranking run: `rank-83787b91ef`
- Family: `emerging`
- Snapshot: `source-snapshot-shadow-generalization-v1-20260521`
- Candidate pool: 528
- Candidate SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- Prior-overlap union excluded: 360
- Selected worksheet rows: 168
- Recommended next stage: `manual_label_shadow_generalization_second_surface_worksheet_v1`

## Why This Worksheet Exists

The second surface has enough confirmatory-eligible works after exclusions, but label coverage is 0/168. Manual labels are needed before discovery can advance toward learned-probability coverage and a later generalization audit.

## Selection Policy

- Universe: `paper_scores` joined to `works` for `rank-83787b91ef` / `emerging`.
- Excluded old-217 eval works and first validated surface works.
- Excluded existing explicit v10 labels.
- Ordered label-blind by final_score descending, heuristic rank ascending, canonical OpenAlex ID ascending.
- Review columns are intentionally blank.

## Sample Reason Breakdown

- `second_surface_high_score_candidate`: 58
- `second_surface_low_score_negative_candidate`: 59
- `second_surface_score_boundary`: 42
- `second_surface_score_spread`: 9

## Thresholds Needed

| Threshold | Observed | Required | Deficit | Passed |
| --- | ---: | ---: | ---: | --- |
| `final_score_coverage` | 528 | 528 | 0 | True |
| `learned_probability_coverage` | 0 | 528 | 528 | False |
| `minimum_confirmatory_candidate_work_count` | 168 | 100 | 0 | True |
| `minimum_confirmatory_label_coverage_rate` | 0.0 | 0.6 | 0.6 | False |
| `minimum_confirmatory_labeled_work_count` | 0 | 100 | 100 | False |
| `minimum_confirmatory_negative_work_count` | 0 | 20 | 20 | False |
| `minimum_confirmatory_positive_work_count` | 0 | 50 | 50 | False |
| `minimum_distinct_negative_work_count` | 0 | 20 | 20 | False |
| `unresolved_label_conflicts` | 0 | 0 | 0 | True |

## Rubric Reminder

- relevance_label: good, acceptable, miss, irrelevant
- novelty_label: surprising, useful, obvious, not_useful, neither
- bridge_like_label: yes, partial, no, not_applicable
- reviewer_notes: required free text in the later labeled CSV

## Not Ingest / Not Ranking / Not Shadow / Not Production

- Manual labeling worksheet only; no labels are ingested by this command.
- Rows are future second-surface confirmatory eval candidates, not validation results.
- Old 217-work and first validated surface overlaps are excluded.
- Existing v10-labeled canonical works are excluded.
- Reviewer label columns are intentionally blank.
- No database writes, ranking, embeddings, learned probability generation, scorer execution, shadow, or production authorization.

## Manual Follow-Up

- Fill relevance_label, novelty_label, bridge_like_label, and reviewer_notes in the CSV.
- Future task ingests the dated labeled CSV into ml-label-dataset-v11.
- Rerun ml-shadow-scorer-generalization-second-surface pinned to rank-83787b91ef.
- If labels pass and learned probability remains missing, create the learned probability coverage plan.
