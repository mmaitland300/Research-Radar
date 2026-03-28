# Roadmap

## Phase 1 - API bootstrap corpus and schema

- Finalize the coded `CorpusPolicy` as the single inclusion source of truth
- Resolve named venues to OpenAlex source IDs and persist source provenance
- Stand up PostgreSQL + pgvector schema with snapshot, ingest-run, watermark, and raw-payload tables
- Bootstrap the curated corpus through the OpenAlex API for the defined slice
- Retain raw OpenAlex payloads and normalized rows side by side
- Persist `source_snapshot_version` and `ingest_run` metadata
- Emit counts for included works, excluded works by reason, unique authors, unique sources, unique topics, and citation edges

## Phase 2 - Search and embeddings

- Generate title+abstract embeddings
- Persist `embedding_version`
- Add vector retrieval support
- Ship search and paper detail API contracts
- Replace mock API paper data with Postgres-backed queries
- Ship frontend shells for search and paper detail

## Phase 3 - Ranking

- Assign local clusters
- Compute semantic relevance, citation velocity, topic growth, bridge score, and diversity penalty
- Persist `ranking_version`
- Materialize ranked lists for:
  - emerging
  - bridge
  - under-cited but relevant
- Add score explanations

## Phase 4 - Trends and evaluation

- Add rising topics and fast-growing cluster views
- Build benchmark set
- Compare against citation-only baselines
- Run freeze-at-T temporal backtest
- Publish evaluation page and failure modes

## Guardrails

- no full OpenAlex ingest
- no graph UI in V1
- no LLM summary feature as a core dependency
- no custom deep-model training
- no chat-with-papers detour
- no broad expansion beyond the defined corpus slice
