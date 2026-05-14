# External Text Embeddings

Frozen vectorization artifact for the external near-miss text corpus.

## Disciplined Layering

Layering: ml-external-text-corpus-* = frozen text hydration; ml-external-text-embeddings-* = frozen vectorization of that corpus only; future ml-text-only-baseline consumes embedding artifacts plus labels for offline diagnostics only.

## Summary

- **source_text_corpus_path:** `docs/audit/ml-external-text-corpus-v7.json`
- **source_text_corpus_sha256:** `4ef89db3a71f3c8313c78995873f025ce89fd910093738dd011ca7c111f00c54`
- **embedding_artifact_version:** `ml-external-text-embeddings-v7`
- **embedding_model:** `text-embedding-3-small`
- **embedding_dimensions:** `1536`
- **row_count:** `60`
- **n_embedded_ok:** `60`
- **n_mock:** `0`
- **aggregate_input_text_sha256:** `c6dab0a3dffe734b410bf1c90f300a300fcbf8c2db20c5a2fb8c68ff725017f2`

## Caveats

- Not validation.
- Frozen vectorization of frozen text corpus only; vectors are not production ranking signals.
- No Postgres reads or writes; not joinable to paper_scores unless a future explicit step adds ranked rows.
- Verbatim text_for_embedding was embedded; cross-pool comparison to DB corpus-v2 embedding string format (e.g. "Title: ... Abstract: ...") requires a documented transform in a later experiment, not silent relabeling here.

Embedding vectors are intentionally omitted from this Markdown file; see the JSON artifact for vectors.
