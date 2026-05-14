# Labeled Text Corpus v1

Observation-level text corpus for explicitly labeled audit rows. This is data preparation only: no embeddings, no model training, no ranking, and no Postgres.

## Inputs

- **label_dataset:** `docs/audit/ml-label-dataset-v7.json`
- **label_dataset_sha256:** `094af1a6083561803c26611e1d6f0afebba6eedec0d2e9ac21008f415117dc85`
- **external_text_corpus:** `docs/audit/ml-external-text-corpus-v7.json`
- **external_text_corpus_sha256:** `4ef89db3a71f3c8313c78995873f025ce89fd910093738dd011ca7c111f00c54`
- **row_count:** `382`
- **sufficient_text_heuristic:** `375`

## Text Source Distribution

- `external_text_corpus_reuse`: `60`
- `openalex_fetch`: `322`

## Review Pool Distribution

- `(null)`: `65`
- `bridge_eligible_only`: `20`
- `full_family_top_k`: `40`
- `ml_blind_snapshot_audit`: `120`
- `ml_contrastive_offline_audit`: `45`
- `ml_emerging_target_gap_audit:good_or_acceptable`: `25`
- `ml_external_near_miss_audit`: `60`
- `ml_hard_negative_audit`: `7`

## Intended Next Step

Generate `ml-labeled-text-embeddings-v1` from this frozen corpus, then run source-transfer or cross-pool offline diagnostics stratified by `review_pool_variant` and `family`.

## Layering

Layering: ml-label-dataset-* supplies observation-level labels; ml-external-text-corpus-* supplies frozen external text reuse; ml-labeled-text-corpus-v1 freezes a labeled text substrate for future ml-labeled-text-embeddings-v1 and cross-pool offline diagnostics.

## Caveats

- Not validation.
- Text corpus only; no embeddings or training.
- Observation-level rows preserved; duplicate paper_id and raw label conflicts are intentional evidence, not resolved here.
- OpenAlex metadata can drift for newly fetched rows; external reused rows remain frozen relative to ml-external-text-corpus-v7.
- Mixed text formats may exist across sources; future embedding artifacts must document format_version per row or globally.

Full abstracts are intentionally omitted from this Markdown summary; see the JSON artifact for row-level text.
