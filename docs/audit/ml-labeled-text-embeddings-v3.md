# Labeled Text Embeddings (ml-labeled-text-embeddings-v3)

Frozen vectorization artifact for `ml-labeled-text-corpus-v3-normalized`.

## Summary

- **source_text_corpus_path:** `docs/audit/ml-labeled-text-corpus-v3-normalized.json`
- **source_text_corpus_sha256:** `42531e6768fa446f50c78c018799d683e8ca119b4adcb2c09b1da42d8e7e0c80`
- **embedding_artifact_version:** `ml-labeled-text-embeddings-v3`
- **embedding_model:** `text-embedding-3-small`
- **embedding_dimensions:** `1536`
- **row_count:** `427`
- **n_embedded_ok:** `427`
- **n_mock:** `0`
- **aggregate_input_text_sha256:** `abcb17ce875c6c4ce5339edb0dec381d813f451dfdb3033e8bf43ad971e33b47`

## Review Pool Counts

- `(null)`: `65`
- `bridge_eligible_only`: `20`
- `full_family_top_k`: `40`
- `ml_blind_snapshot_audit`: `120`
- `ml_contrastive_offline_audit`: `45`
- `ml_emerging_target_gap_audit:good_or_acceptable`: `25`
- `ml_external_near_miss_audit`: `60`
- `ml_hard_negative_audit`: `7`
- `ml_transfer_gap_audit`: `45`

## Text Format Counts

- `labeled_text_corpus_v2_canonical_title_abstract`: `427`

## Layering

Layering: ml-labeled-text-corpus-v3-normalized freezes observation-level text; ml-labeled-text-embeddings-v3 freezes vectors for that exact text; later source-transfer or cross-pool baselines consume this artifact plus labels offline only.

## Caveats

- Not validation.
- Frozen vectorization of frozen labeled text corpus only.
- Vectors are not production ranking signals.
- No Postgres reads or writes.
- No ranking, no train/dev/test split generation, no production behavior change.
- Mixed text formats may exist across rows; downstream diagnostics must account for embedding_text_format_version.
- Future cross-pool baselines must document whether they pool or stratify by review_pool_variant, family, and text format.

Embedding vectors are intentionally omitted from this Markdown file; see the JSON artifact for vectors.
