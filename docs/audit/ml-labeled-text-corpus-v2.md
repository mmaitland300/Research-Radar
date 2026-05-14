# Labeled Text Corpus v2

Canonical text-format normalization for `ml-labeled-text-corpus-v1`. This is data preparation only: no OpenAlex calls, no Postgres, no embeddings, no ranking, and no label edits.

## Summary

- **source_corpus_path:** `docs/audit/ml-labeled-text-corpus-v1.json`
- **source_corpus_sha256:** `d0417fbb85734bb92a81be97ecf8dd5f3a4ba6cdd59d4fc88b589f26b84a8464`
- **corpus_version:** `ml-labeled-text-corpus-v2`
- **row_count:** `382`
- **n_text_changed_from_v1:** `0`
- **n_sufficient_text_for_embedding_heuristic:** `375`

## Canonicalization Status

- `canonical_title_abstract`: `382`

## Previous Text Format Counts

- `external_text_corpus_v7_verbatim`: `60`
- `labeled_text_corpus_v1_openalex_title_abstract`: `322`

## Previous Text Source Counts

- `external_text_corpus_reuse`: `60`
- `openalex_fetch`: `322`

## Layering

Layering: ml-labeled-text-corpus-v1 freezes observation-level hydrated text; ml-labeled-text-corpus-v2 normalizes text_for_embedding into a canonical title+abstract string where available; future ml-labeled-text-embeddings-v2 and cross-pool diagnostics can use this as a text-format sensitivity control.

## Caveats

- Not validation.
- Normalization only; labels unchanged; duplicates/conflicts preserved.
- Rows in original_text_fallback still carry v1 format limits; missing_text rows cannot be de-confounded by this pass.
- Not product ranking quality evidence.

Full abstracts are intentionally omitted from this Markdown summary; see the JSON artifact for row-level text.
