# Search Retrieval v1

**Branch:** `codex/search-retrieval-v1`  
**Baseline commit from `main`:** `5052b2c` (`fix(api): honest Emerging list copy and stable ranking tie-breaks`)

## Milestone goal

Ship a real lexical search surface for Research Radar before adding semantic assist. This milestone is intentionally narrower than "final search":

- dedicated `GET /api/v1/search`
- lexical retrieval over `title + abstract`
- deterministic ordering
- practical filters
- clean handoff into dossier, Recommended, and Evaluation

## API contract

### Request

- `q`
- `limit`
- `offset`
- `year_from`
- `year_to`
- `included_scope` = `core | all_included`
- `source_slug`
- `topic`
- `family_hint`
- `ranking_run_id` (optional; only meaningful when `family_hint` is set)
- `ranking_version` (optional; only meaningful when `family_hint` is set)

### Response

- `paper_id`
- `title`
- `year`
- `citation_count`
- `source_slug`
- `source_label`
- `is_core_corpus`
- `topics`
- `preview`
- `match` metadata:
  - `matched_fields`
  - `highlight_fragments`
  - `lexical_rank`
- `total`
- `ordering`
- `resolved_filters`
- `resolved_ranking_run_id` (only when family-filtered search depended on ranking state)
- `resolved_ranking_version` (only when family-filtered search depended on ranking state)
- `resolved_corpus_snapshot_version` (only when family-filtered search depended on ranking state)

`family_hint` in v1 is implemented as a real ranking-family filter, not just a UI hint.

When `family_hint` is set, search returns lexical matches intersected with works present in one
resolved succeeded ranking run for that family:

- if `ranking_run_id` is provided, that exact run is used
- else if `ranking_version` is provided, search resolves one succeeded run on the default snapshot
- else search resolves one default succeeded run explicitly

When family-filtered search depends on ranking state, the response emits the resolved run id,
version, and corpus snapshot version used for filtering. When `family_hint` is unset, search stays
lexical-only and does not emit ranking context.

## Scope guardrails

### In for v1

- title + abstract lexical retrieval
- filterable narrowing
- deterministic ordering
- honest copy in the Search page
- UI and API both allow up to `100` results per page in this branch

### Out for v1

- query-embedding-only retrieval
- hybrid ranking formulas with opaque blending
- cross-encoder reranking
- graph-first browsing
- scholar-clone breadth

## Sequencing

1. define API contract
2. implement lexical retrieval
3. wire the Search page to the new endpoint
4. verify retrieval quality on known queries
5. only then consider semantic assist behind explicit `embedding_version`
