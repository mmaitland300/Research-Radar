-- Semantic v1 coverage baseline audit (Postgres)
--
-- Purpose: reproducible, tabular snapshot for one reference ranking run:
--   - included works in the run's corpus snapshot
--   - paper_scores row counts and non-null rates (semantic, bridge, bridge_eligible)
--   - embedding row coverage for the run's embedding_version
--   - splits: semantic null vs missing embedding row (emerging family)
--
-- Pin the run: edit ref_ranking_run_id in the params CTE once (NULL = latest succeeded).
--
-- Run:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/audit/semantic_coverage_baseline.sql
--
-- Output capture:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/audit/semantic_coverage_baseline.sql -o docs/audit/out/semantic_baseline_YYYYMMDD.txt

BEGIN;

DROP TABLE IF EXISTS _semantic_audit_ref;

CREATE TEMP TABLE _semantic_audit_ref AS
WITH params AS (
    SELECT 'rank-d7f3d82d05'::text AS ref_ranking_run_id
    -- ^ Frozen baseline reference run for the semantic v1 milestone.
),
picked AS (
    SELECT rr.ranking_run_id,
           rr.ranking_version,
           rr.corpus_snapshot_version,
           rr.embedding_version,
           rr.status,
           rr.finished_at,
           rr.started_at
    FROM ranking_runs rr
    CROSS JOIN params p
    WHERE rr.status = 'succeeded'
      AND (
          p.ref_ranking_run_id IS NOT NULL
          AND rr.ranking_run_id = p.ref_ranking_run_id
          OR (
              p.ref_ranking_run_id IS NULL
              AND rr.ranking_run_id = (
                  SELECT ranking_run_id
                  FROM ranking_runs
                  WHERE status = 'succeeded'
                  ORDER BY finished_at DESC NULLS LAST, started_at DESC
                  LIMIT 1
              )
          )
      )
    LIMIT 1
)
SELECT * FROM picked;

SELECT '1_reference_run' AS section,
       ranking_run_id,
       ranking_version,
       corpus_snapshot_version,
       embedding_version,
       finished_at
FROM _semantic_audit_ref;

WITH included AS (
    SELECT w.id
    FROM works w
    INNER JOIN _semantic_audit_ref r ON w.corpus_snapshot_version = r.corpus_snapshot_version
    WHERE w.inclusion_status = 'included'
)
SELECT '2_included_works_in_snapshot' AS section,
       count(*) AS included_works
FROM included;

SELECT '3_paper_scores_by_family' AS section,
       ps.recommendation_family AS family,
       count(*) AS score_rows,
       count(*) FILTER (WHERE ps.semantic_score IS NOT NULL) AS semantic_nonnull,
       count(*) FILTER (WHERE ps.bridge_score IS NOT NULL) AS bridge_score_nonnull,
       count(*) FILTER (WHERE ps.bridge_eligible IS NOT NULL) AS bridge_eligible_nonnull,
       count(*) FILTER (WHERE ps.semantic_score IS NULL) AS semantic_null_rows,
       count(*) FILTER (
           WHERE ps.semantic_score IS NULL
             AND ps.final_score IS NOT NULL
       ) AS row_exists_semantic_null,
       round(
           100.0 * count(*) FILTER (WHERE ps.semantic_score IS NOT NULL) / nullif(count(*), 0),
           2
       ) AS semantic_nonnull_pct
FROM paper_scores ps
INNER JOIN _semantic_audit_ref r ON ps.ranking_run_id = r.ranking_run_id
GROUP BY ps.recommendation_family
ORDER BY ps.recommendation_family;

WITH included AS (
    SELECT w.id
    FROM works w
    INNER JOIN _semantic_audit_ref r ON w.corpus_snapshot_version = r.corpus_snapshot_version
    WHERE w.inclusion_status = 'included'
)
SELECT '4_embedding_coverage_run_version' AS section,
       (SELECT embedding_version FROM _semantic_audit_ref) AS embedding_version,
       count(*) AS included_works,
       count(e.work_id) AS works_with_embedding_row,
       round(100.0 * count(e.work_id) / nullif(count(*), 0), 2) AS pct_with_embedding_row
FROM included i
LEFT JOIN embeddings e
       ON e.work_id = i.id
      AND e.embedding_version = (SELECT embedding_version FROM _semantic_audit_ref);

SELECT '5_emerging_semantic_null_vs_missing_embedding' AS section,
       count(*) FILTER (
           WHERE ps.recommendation_family = 'emerging'
             AND ps.semantic_score IS NULL
       ) AS emerging_rows_semantic_null,
       count(*) FILTER (
           WHERE ps.recommendation_family = 'emerging'
             AND ps.semantic_score IS NULL
             AND e.work_id IS NULL
       ) AS emerging_semantic_null_and_no_embedding_row,
       count(*) FILTER (
           WHERE ps.recommendation_family = 'emerging'
             AND ps.semantic_score IS NULL
             AND e.work_id IS NOT NULL
       ) AS emerging_semantic_null_but_embedding_row_exists
FROM paper_scores ps
INNER JOIN _semantic_audit_ref r ON ps.ranking_run_id = r.ranking_run_id
LEFT JOIN embeddings e
       ON e.work_id = ps.work_id
      AND e.embedding_version = (SELECT embedding_version FROM _semantic_audit_ref)
WHERE ps.recommendation_family = 'emerging';

ROLLBACK;

-- ROLLBACK ends the transaction (read-only audit). Temp table is dropped with the session/txn.
