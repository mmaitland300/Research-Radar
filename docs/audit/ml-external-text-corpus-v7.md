# External Near-Miss Text Corpus

Read-only OpenAlex text hydration artifact for `ml_external_near_miss_audit` rows. This prepares text for offline featureization only: no Postgres writes, no ranking run, and no model training.

## Provenance

- **corpus_version:** `ml-external-text-corpus-v7`
- **label_dataset:** `docs/audit/ml-label-dataset-v7.json`
- **label_dataset_sha256:** `094af1a6083561803c26611e1d6f0afebba6eedec0d2e9ac21008f415117dc85`
- **dataset_version_reference:** `ml-label-dataset-v7`
- **review_pool_variant:** `ml_external_near_miss_audit`
- **OpenAlex endpoint:** `https://api.openalex.org/works`
- **context_sidecar:** `docs/audit/manual-review/ml_external_near_miss_review_v1_context.json`
- **context_sidecar_sha256:** `e0cfa2368b24c85eb309106863a99527879182f6d9708ca7b4e69beb6aaf9261`
- **sidecar row_id parity:** `true`

## Summary

- **rows:** `60`
- **fetch OK:** `60`
- **fetch failed:** `0`
- **empty OpenAlex abstracts:** `0`
- **preview fallback after successful fetch:** `0`
- **sufficient text heuristic true:** `56`

## Offline Embedding Note

For a future offline embedding pass, read `rows[].text_for_embedding` and keep `rows[].review_pool_variant` for stratified reporting rather than treating the pool as a label. Rows with `abstract_source=preview_fallback` or `fetch_failed` should be interpreted as preview-derived text, not confirmed full abstracts.

## Caveats

- Not validation.
- Text hydration only; hydrated OpenAlex text may differ from worksheet previews.
- No DB writes.
- No ranking or model training.
- OpenAlex metadata can drift vs frozen sidecar/context snapshots.
