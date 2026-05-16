# Labeled Text Corpus (ml-labeled-text-corpus-v3-normalized)

Canonical text-format normalization for `ml-labeled-text-corpus-v3`. This is data preparation only: no OpenAlex calls, no Postgres, no embeddings, no ranking, and no label edits.

## Summary

- **source_corpus_path:** `docs/audit/ml-labeled-text-corpus-v3.json`
- **source_corpus_sha256:** `cee8cf329f936c37b7d0faa3b6f19700f724bafc9d06f459b92c1bae98f3113c`
- **corpus_version:** `ml-labeled-text-corpus-v3-normalized`
- **row_count:** `427`
- **n_text_changed_from_v1:** `0`
- **n_sufficient_text_for_embedding_heuristic:** `420`

## Canonicalization Status

- `canonical_title_abstract`: `427`

## Previous Text Format Counts

- `external_text_corpus_v7_verbatim`: `60`
- `labeled_text_corpus_v1_openalex_title_abstract`: `367`

## Previous Text Source Counts

- `external_text_corpus_reuse`: `60`
- `openalex_fetch`: `367`

## Layering

Layering: ml-labeled-text-corpus-v3 freezes observation-level hydrated text; ml-labeled-text-corpus-v3-normalized normalizes text_for_embedding into a canonical title+abstract string where available; future ml-labeled-text-embeddings-* and cross-pool diagnostics can use this as a text-format sensitivity control.

## Caveats

- Not validation.
- Normalization only; labels unchanged; duplicates/conflicts preserved.
- Rows in original_text_fallback still carry v1 format limits; missing_text rows cannot be de-confounded by this pass.
- Not product ranking quality evidence.

Full abstracts are intentionally omitted from this Markdown summary; see the JSON artifact for row-level text.
