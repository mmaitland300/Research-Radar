-- Distribution, correlation, top-N overlap, and bridge/undercited invariance between two materialized runs.
-- Ranks and top-k use stable ordering: final_score DESC, work_id ASC.
-- Keep baseline_run_id / new_run_id identical in every `params` CTE below (search/replace both ids).
-- Edit baseline_run_id / new_run_id in params, then from repo root:
--   psql --dbname="$env:DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/audit/semantic_run_distribution_compare.sql
--   psql ... -o docs/audit/out/semantic_compare_<baseline>_vs_<new>.txt

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
)
SELECT
    '1_run_pair_sanity' AS section,
    bl.ranking_run_id AS baseline_run_id,
    nw.ranking_run_id AS new_run_id,
    bl.ranking_version AS baseline_ranking_version,
    nw.ranking_version AS new_ranking_version,
    bl.status AS baseline_status,
    nw.status AS new_status,
    (bl.corpus_snapshot_version = nw.corpus_snapshot_version) AS same_corpus_snapshot,
    (bl.embedding_version = nw.embedding_version) AS same_embedding_version,
    bl.corpus_snapshot_version,
    bl.embedding_version
FROM params p
JOIN ranking_runs bl ON bl.ranking_run_id = p.baseline_run_id
JOIN ranking_runs nw ON nw.ranking_run_id = p.new_run_id;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
),
baseline AS (
    SELECT work_id, recommendation_family, final_score
    FROM paper_scores ps
    JOIN params p ON ps.ranking_run_id = p.baseline_run_id
    WHERE ps.recommendation_family IN ('bridge', 'undercited')
),
newer AS (
    SELECT work_id, recommendation_family, final_score
    FROM paper_scores ps
    JOIN params p ON ps.ranking_run_id = p.new_run_id
    WHERE ps.recommendation_family IN ('bridge', 'undercited')
)
SELECT
    '2_invariance_bridge_undercited_final_score' AS section,
    COALESCE(b.recommendation_family, n.recommendation_family) AS recommendation_family,
    b.work_id,
    b.final_score AS baseline_final_score,
    n.final_score AS new_final_score
FROM baseline b
FULL OUTER JOIN newer n
    ON b.work_id = n.work_id
   AND b.recommendation_family = n.recommendation_family
WHERE b.work_id IS NULL
   OR n.work_id IS NULL
   OR b.final_score IS DISTINCT FROM n.final_score;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
)
SELECT
    '3_emerging_component_identity_baseline_vs_new' AS section,
    count(*) FILTER (WHERE
        b.citation_velocity_score IS DISTINCT FROM n.citation_velocity_score
    ) AS rows_citation_velocity_mismatch,
    count(*) FILTER (WHERE
        b.topic_growth_score IS DISTINCT FROM n.topic_growth_score
    ) AS rows_topic_growth_mismatch,
    count(*) AS emerging_rows_joined
FROM params p
JOIN paper_scores b
    ON b.ranking_run_id = p.baseline_run_id
   AND b.recommendation_family = 'emerging'
JOIN paper_scores n
    ON n.ranking_run_id = p.new_run_id
   AND n.recommendation_family = 'emerging'
   AND n.work_id = b.work_id;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
)
SELECT
    '4a_emerging_distribution_baseline' AS section,
    count(*) AS n,
    min(final_score) AS final_min,
    avg(final_score) AS final_avg,
    stddev_pop(final_score) AS final_stddev_pop,
    max(final_score) AS final_max,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY final_score) AS final_p25,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY final_score) AS final_median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY final_score) AS final_p75
FROM paper_scores ps
JOIN params p ON ps.ranking_run_id = p.baseline_run_id
WHERE ps.recommendation_family = 'emerging';

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
)
SELECT
    '4b_emerging_distribution_new' AS section,
    count(*) AS n,
    min(final_score) AS final_min,
    avg(final_score) AS final_avg,
    stddev_pop(final_score) AS final_stddev_pop,
    max(final_score) AS final_max,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY final_score) AS final_p25,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY final_score) AS final_median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY final_score) AS final_p75,
    min(semantic_score) AS semantic_min,
    avg(semantic_score) AS semantic_avg,
    stddev_pop(semantic_score) AS semantic_stddev_pop,
    max(semantic_score) AS semantic_max
FROM paper_scores ps
JOIN params p ON ps.ranking_run_id = p.new_run_id
WHERE ps.recommendation_family = 'emerging';

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
)
SELECT
    '5_pearson_correlation_final_score_by_family' AS section,
    b.recommendation_family,
    count(*) AS n_pairs,
    corr(b.final_score, n.final_score) AS pearson_r_final_score
FROM params p
JOIN paper_scores b ON b.ranking_run_id = p.baseline_run_id
JOIN paper_scores n
    ON n.ranking_run_id = p.new_run_id
   AND n.work_id = b.work_id
   AND n.recommendation_family = b.recommendation_family
GROUP BY b.recommendation_family
ORDER BY b.recommendation_family;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
),
ranked AS (
    SELECT
        ps.ranking_run_id,
        ps.recommendation_family,
        ps.work_id,
        row_number() OVER (
            PARTITION BY ps.ranking_run_id, ps.recommendation_family
            ORDER BY ps.final_score DESC, ps.work_id ASC
        ) AS rk
    FROM paper_scores ps
    CROSS JOIN params p
    WHERE ps.ranking_run_id IN (p.baseline_run_id, p.new_run_id)
),
baseline_top10 AS (
    SELECT recommendation_family, work_id
    FROM ranked
    WHERE ranking_run_id = (SELECT baseline_run_id FROM params)
      AND rk <= 10
),
new_top10 AS (
    SELECT recommendation_family, work_id
    FROM ranked
    WHERE ranking_run_id = (SELECT new_run_id FROM params)
      AND rk <= 10
),
fam10 AS (
    SELECT recommendation_family FROM baseline_top10
    UNION
    SELECT recommendation_family FROM new_top10
)
SELECT
    '6_top10_overlap_by_family' AS section,
    f.recommendation_family,
    (SELECT count(*) FROM baseline_top10 b WHERE b.recommendation_family = f.recommendation_family) AS baseline_top10_rows,
    (SELECT count(*) FROM new_top10 n WHERE n.recommendation_family = f.recommendation_family) AS new_top10_rows,
    (
        SELECT count(*)
        FROM baseline_top10 b
        JOIN new_top10 n
            ON n.work_id = b.work_id
           AND n.recommendation_family = b.recommendation_family
        WHERE b.recommendation_family = f.recommendation_family
    ) AS overlap_work_ids_in_both_top10
FROM fam10 f
ORDER BY f.recommendation_family;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
),
ranked AS (
    SELECT
        ps.ranking_run_id,
        ps.recommendation_family,
        ps.work_id,
        row_number() OVER (
            PARTITION BY ps.ranking_run_id, ps.recommendation_family
            ORDER BY ps.final_score DESC, ps.work_id ASC
        ) AS rk
    FROM paper_scores ps
    CROSS JOIN params p
    WHERE ps.ranking_run_id IN (p.baseline_run_id, p.new_run_id)
),
baseline_top20 AS (
    SELECT recommendation_family, work_id
    FROM ranked
    WHERE ranking_run_id = (SELECT baseline_run_id FROM params)
      AND rk <= 20
),
new_top20 AS (
    SELECT recommendation_family, work_id
    FROM ranked
    WHERE ranking_run_id = (SELECT new_run_id FROM params)
      AND rk <= 20
),
fam20 AS (
    SELECT recommendation_family FROM baseline_top20
    UNION
    SELECT recommendation_family FROM new_top20
)
SELECT
    '7_top20_overlap_by_family' AS section,
    f.recommendation_family,
    (SELECT count(*) FROM baseline_top20 b WHERE b.recommendation_family = f.recommendation_family) AS baseline_top20_rows,
    (SELECT count(*) FROM new_top20 n WHERE n.recommendation_family = f.recommendation_family) AS new_top20_rows,
    (
        SELECT count(*)
        FROM baseline_top20 b
        JOIN new_top20 n
            ON n.work_id = b.work_id
           AND n.recommendation_family = b.recommendation_family
        WHERE b.recommendation_family = f.recommendation_family
    ) AS overlap_work_ids_in_both_top20
FROM fam20 f
ORDER BY f.recommendation_family;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
),
r_old AS (
    SELECT
        work_id,
        row_number() OVER (ORDER BY final_score DESC, work_id ASC) AS rank_desc
    FROM paper_scores ps
    JOIN params p ON ps.ranking_run_id = p.baseline_run_id
    WHERE ps.recommendation_family = 'emerging'
),
r_new AS (
    SELECT
        work_id,
        row_number() OVER (ORDER BY final_score DESC, work_id ASC) AS rank_desc
    FROM paper_scores ps
    JOIN params p ON ps.ranking_run_id = p.new_run_id
    WHERE ps.recommendation_family = 'emerging'
)
SELECT
    '8_emerging_mean_abs_rank_delta' AS section,
    count(*) AS n,
    avg(abs(o.rank_desc - n.rank_desc)::double precision) AS mean_abs_rank_change,
    max(abs(o.rank_desc - n.rank_desc)) AS max_abs_rank_change
FROM r_old o
JOIN r_new n ON n.work_id = o.work_id;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
)
SELECT
    '9_emerging_final_score_delta_summary' AS section,
    count(*) AS n,
    min(n.final_score - b.final_score) AS min_delta_final,
    avg(n.final_score - b.final_score) AS avg_delta_final,
    max(n.final_score - b.final_score) AS max_delta_final,
    avg(abs(n.final_score - b.final_score)) AS mean_abs_delta_final
FROM params p
JOIN paper_scores b
    ON b.ranking_run_id = p.baseline_run_id
   AND b.recommendation_family = 'emerging'
JOIN paper_scores n
    ON n.ranking_run_id = p.new_run_id
   AND n.recommendation_family = 'emerging'
   AND n.work_id = b.work_id;

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
)
SELECT
    '10_emerging_new_run_semantic_correlations' AS section,
    count(*) AS n,
    corr(semantic_score, final_score) AS r_semantic_vs_final,
    corr(semantic_score, citation_velocity_score) AS r_semantic_vs_citation_velocity,
    corr(semantic_score, topic_growth_score) AS r_semantic_vs_topic_growth
FROM paper_scores ps
JOIN params p ON ps.ranking_run_id = p.new_run_id
WHERE ps.recommendation_family = 'emerging';
