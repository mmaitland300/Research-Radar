# Blind v2 review handoff decision

## Decision

Use `docs/audit/manual-review/ml_blind_snapshot_review_v2.csv` as the reviewer-facing worksheet for the next blind manual labeling pass.

Do not distribute `docs/audit/manual-review/ml_blind_snapshot_review_v2_context.json` to reviewers. The sidecar is for internal merge, provenance, and later offline audits only.

## Evidence context

The source-split tiny baseline and blind error analysis showed weak blind-source transfer, especially for `surprising_or_useful`. The next step is cleaner human labeling, not model tuning.

Relevant artifacts:

- `docs/audit/ml-source-split-tiny-baseline-emerging-rank-ee2ba6c816-v4.json`
- `docs/audit/ml-source-split-error-analysis-emerging-rank-ee2ba6c816-v4.json`
- `docs/audit/manual-review/ml_blind_snapshot_review_v2.csv`
- `docs/audit/manual-review/ml_blind_snapshot_review_v2_context.json`
- `docs/audit/manual-review/ml_blind_snapshot_review_v2.md`

## Reviewer handoff

Send only the CSV worksheet, or a copy of it, to reviewers.

The CSV intentionally omits ranking cues and model outputs:

- no `ranking_run_id`
- no internal Postgres `works.id`
- no `final_score`
- no score-component columns
- no family score or rank JSON
- no learned logits or model predictions

The blank manual label columns are:

- `relevance_label`
- `novelty_label`
- `bridge_like_label`
- `reviewer_notes`

## Returned labels

Do not overwrite the blank v2 template. Commit returned reviewer files as new artifacts, for example:

- `docs/audit/manual-review/ml_blind_snapshot_review_v2_labeled_2026-05-XX.csv`

Keep `row_id`, `worksheet_version`, `paper_id`, `openalex_work_id`, and `work_id` unchanged in returned files.

## Future ingest

After review, create the next label dataset version, for example `ml-label-dataset-v5`, by ingesting the filled v2 worksheet and merging context from the sidecar by `row_id`.

The ingest should preserve:

- `worksheet_version = ml-blind-snapshot-review-v2`
- `review_pool_variant = ml_blind_snapshot_audit`
- `family = null`
- `work_id` as the OpenAlex `W...` token
- `internal_work_id` only as sidecar/provenance context

## Guardrails

This is not validation and does not support production ranking or model changes.

The v2 worksheet exists to improve label quality by reducing reviewer-facing ranking contamination. Any later ML diagnostic should report source slice, label provenance, and caveats separately from production readiness.
