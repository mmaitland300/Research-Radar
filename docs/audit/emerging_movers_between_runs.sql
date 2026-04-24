-- Emerging family: papers with largest |Δ final_score| and rank moves between two runs.
-- Keep baseline_run_id / new_run_id identical in both params CTEs (search/replace).
-- Stable rank: final_score DESC, work_id ASC.

WITH params AS (
    SELECT
        'rank-d7f3d82d05'::text AS baseline_run_id,
        'rank-7c9ad919de'::text AS new_run_id
),
rb AS (
    SELECT
        ps.work_id,
        ps.final_score AS baseline_final,
        ps.semantic_score AS baseline_semantic,
        row_number() OVER (ORDER BY ps.final_score DESC, ps.work_id ASC) AS rank_b
    FROM paper_scores ps
    JOIN params p ON ps.ranking_run_id = p.baseline_run_id
    WHERE ps.recommendation_family = 'emerging'
),
rn AS (
    SELECT
        ps.work_id,
        ps.final_score AS new_final,
        ps.semantic_score AS new_semantic,
        row_number() OVER (ORDER BY ps.final_score DESC, ps.work_id ASC) AS rank_n
    FROM paper_scores ps
    JOIN params p ON ps.ranking_run_id = p.new_run_id
    WHERE ps.recommendation_family = 'emerging'
)
SELECT
    w.openalex_id,
    left(w.title, 120) AS title_short,
    rb.baseline_final,
    rn.new_final,
    (rn.new_final - rb.baseline_final) AS delta_final,
    rb.rank_b,
    rn.rank_n,
    (rn.rank_n - rb.rank_b) AS delta_rank,
    rb.baseline_semantic,
    rn.new_semantic
FROM rb
JOIN rn ON rn.work_id = rb.work_id
JOIN works w ON w.id = rb.work_id
ORDER BY abs(rn.new_final - rb.baseline_final) DESC, abs(rn.rank_n - rb.rank_b) DESC, w.openalex_id ASC
LIMIT 25;
