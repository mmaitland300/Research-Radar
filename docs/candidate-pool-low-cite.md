# Low-cite candidate pool (frozen definition)

Operational definition for papers that qualify as **low-citation** candidates in Research Radar before ranking or heuristic changes move ahead. This doc freezes the pool semantics so evaluations and product copy stay comparable across runs.

## Intent

The undercited family and related surfaces should highlight **recent, relevant work that is not yet citation-dominant**, not a degenerate slice of **only zero-citation** papers (which skews toward brand-new or invisible work and breaks comparability with field norms).

## Definition (v0 — frozen)

A work is in the **low-cite candidate pool** when all of the following hold:

1. **Inclusion:** `works.inclusion_status = 'included'` for the corpus snapshot under review.
2. **Core corpus:** `works.is_core_corpus = TRUE` (same gate as the heuristic undercited API baseline).
3. **Recency:** `year >= min_year` with default `min_year = 2019` (aligns with `GET /api/v1/recommendations/undercited` defaults unless overridden).
4. **Low citations, not zero-only:** `citation_count <= max_citations` with default `max_citations = 30`. **Papers with zero citations are included** but the pool is **not restricted** to zero; the ceiling is the operational “low cite” band.
5. **Metadata gate:** Non-empty trimmed `title` and `abstract` (same as heuristic v0).

**Ordering reference (for API parity):** `year DESC`, `citation_count ASC`, `openalex_id ASC`.

## Scope boundaries

- This is separate from the full ranked **undercited family** from `paper_scores`, which uses additional signals and per-run config.
- This is not a promise of “undiscovered gems only”; it is a **transparent filter** for a comparable candidate set.

## API alignment

The heuristic endpoint `GET /api/v1/recommendations/undercited` implements the same filters with query params `min_year`, `max_citations`, and `limit`. Treat those defaults as the canonical expression of this frozen pool unless this doc is revised.

## Revision policy

Bump a short **revision label** in this section when the definition changes (e.g. `v0` → `v1`).

**Current revision:** `v0` (frozen for ML/product iteration until explicitly superseded).
