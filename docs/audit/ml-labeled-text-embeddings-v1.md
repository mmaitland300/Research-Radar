# Labeled Text Embeddings v1

Frozen vectorization artifact for `ml-labeled-text-corpus-v1`.

## Summary

- **source_text_corpus_path:** `docs/audit/ml-labeled-text-corpus-v1.json`
- **source_text_corpus_sha256:** `d0417fbb85734bb92a81be97ecf8dd5f3a4ba6cdd59d4fc88b589f26b84a8464`
- **embedding_artifact_version:** `ml-labeled-text-embeddings-v1`
- **embedding_model:** `text-embedding-3-small`
- **embedding_dimensions:** `1536`
- **row_count:** `382`
- **n_embedded_ok:** `382`
- **n_mock:** `0`
- **aggregate_input_text_sha256:** `c30afb6868b2169ad033dc6529157d5995ef636ab19ff19d47ab36705778a0fb`

## Review Pool Counts

- `(null)`: `65`
- `bridge_eligible_only`: `20`
- `full_family_top_k`: `40`
- `ml_blind_snapshot_audit`: `120`
- `ml_contrastive_offline_audit`: `45`
- `ml_emerging_target_gap_audit:good_or_acceptable`: `25`
- `ml_external_near_miss_audit`: `60`
- `ml_hard_negative_audit`: `7`

## Text Format Counts

- `external_text_corpus_v7_verbatim`: `60`
- `labeled_text_corpus_v1_openalex_title_abstract`: `322`

## Layering

Layering: ml-labeled-text-corpus-v1 freezes observation-level text; ml-labeled-text-embeddings-v1 freezes vectors for that exact text; later source-transfer or cross-pool baselines consume this artifact plus labels offline only.

## Caveats

- Not validation.
- Frozen vectorization of frozen labeled text corpus only.
- Vectors are not production ranking signals.
- No Postgres reads or writes.
- No ranking, no train/dev/test split generation, no production behavior change.
- Mixed text formats may exist across rows; downstream diagnostics must account for embedding_text_format_version.
- Future cross-pool baselines must document whether they pool or stratify by review_pool_variant, family, and text format.

Embedding vectors are intentionally omitted from this Markdown file; see the JSON artifact for vectors.
