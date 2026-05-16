# Labeled Text Corpus (ml-labeled-text-corpus-v3)

Observation-level text corpus for explicitly labeled audit rows. This is data preparation only: no embeddings, no model training, no ranking, and no Postgres.

## Inputs

- **label_dataset:** `docs/audit/ml-label-dataset-v8.json`
- **label_dataset_sha256:** `ea65b114cfef6e22e3299bf4e54d320b6ba13a66185ac0908700528a1846948c`
- **label_dataset_version:** `ml-label-dataset-v8`
- **corpus_version:** `ml-labeled-text-corpus-v3`
- **external_text_corpus:** `docs/audit/ml-external-text-corpus-v7.json`
- **external_text_corpus_sha256:** `4ef89db3a71f3c8313c78995873f025ce89fd910093738dd011ca7c111f00c54`
- **row_count:** `427`
- **sufficient_text_heuristic:** `420`

## Text Source Distribution

- `external_text_corpus_reuse`: `60`
- `openalex_fetch`: `367`

## Review Pool Distribution

- `(null)`: `65`
- `bridge_eligible_only`: `20`
- `full_family_top_k`: `40`
- `ml_blind_snapshot_audit`: `120`
- `ml_contrastive_offline_audit`: `45`
- `ml_emerging_target_gap_audit:good_or_acceptable`: `25`
- `ml_external_near_miss_audit`: `60`
- `ml_hard_negative_audit`: `7`
- `ml_transfer_gap_audit`: `45`

## Intended Next Step

Generate a matching `ml-labeled-text-embeddings-*` artifact from this frozen corpus, then run source-transfer or cross-pool offline diagnostics stratified by `review_pool_variant` and `family`.

## Layering

Layering: ml-label-dataset-* supplies observation-level labels; ml-external-text-corpus-* supplies frozen external text reuse; ml-labeled-text-corpus-v3 freezes a labeled text substrate for future ml-labeled-text-embeddings-* and cross-pool offline diagnostics.

## Caveats

- Not validation.
- Text corpus only; no embeddings or training.
- Observation-level rows preserved; duplicate paper_id and raw label conflicts are intentional evidence, not resolved here.
- OpenAlex metadata can drift for newly fetched rows; external reused rows remain frozen relative to ml-external-text-corpus-v7.
- Mixed text formats may exist across sources; future embedding artifacts must document format_version per row or globally.

Full abstracts are intentionally omitted from this Markdown summary; see the JSON artifact for row-level text.
