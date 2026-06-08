# Bridge Live Canary Top-20 Review Worksheet v1

Generated: 2026-06-08T13:58:08Z

## Purpose

This worksheet prepares human review of the LIVE scorer-served Bridge top 20 from
`docs/audit/bridge-scorer-live-canary-proof-v2.json`. It is not an offline replay
worksheet and it is not a label ingest artifact.

Use the rubric in [recommendation-review-rubric.md](../../recommendation-review-rubric.md)
when filling `relevance_label`, `novelty_label`, `bridge_like_label`, and
`reviewer_notes`.

## Files

- Blank CSV: `docs/audit/manual-review/bridge_live_canary_top20_rank-5a7efa5ca3_v1_blank.csv`
- Context sidecar: `docs/audit/manual-review/bridge_live_canary_top20_rank-5a7efa5ca3_v1_context.json`
- Source proof: `docs/audit/bridge-scorer-live-canary-proof-v2.json`

## Review Notes

- Review exactly 20 rows in the live canary order (`rank` 1 through 20).
- The live response used `ranking_mode=bounded_bridge_ml_scorer`.
- The live order is the bounded Bridge rank-percentile hybrid scorer order.
- `final_score` is materialized ranking metadata, not the live ordering score.
- `bridge_eligible` is copied from the live API response.
- Reviewer columns are intentionally blank.

## Priority Rows

Review the 7 previously unlabeled proposed top-20 rows first:

- `https://openalex.org/W7155391647`
- `https://openalex.org/W7150806375`
- `https://openalex.org/W7131744631`
- `https://openalex.org/W4414112292`
- `https://openalex.org/W7116661261`
- `https://openalex.org/W4412072230`
- `https://openalex.org/W4414581097`

These rows are marked with
`was_previously_unlabeled_proposed_top20=true` in the CSV.

## What This Does Not Prove

This worksheet does not prove Bridge ranking quality, does not authorize rollout,
does not validate the scorer, and does not change production behavior. It only
creates a structured place for human labels on the live scorer-served top 20.

## After Labeling

Save the filled worksheet as:

`bridge_live_canary_top20_rank-5a7efa5ca3_v1_labeled_<date>.csv`

Label ingestion is a separate future commit. Do not ingest these rows into
`ml-label-dataset` as part of this worksheet-prep step.
