# Bridge experiment readiness

This artifact joins a completed recommendation review rollup with `paper_scores` top-k overlap for one explicit `ranking_run_id`. It is **not** validation of bridge ranking quality and does **not** prove that ML ranking is better.

## Provenance

- **ranking_run_id:** `rank-3904fec89d`
- **ranking_version:** `bridge-v2-nm1-zero-r3-k6-20260424`
- **corpus_snapshot_version:** `source-snapshot-20260425-044015`
- **embedding_version:** `v1-title-abstract-1536-cleantext-r3`
- **cluster_version:** `kmeans-l2-v0-cleantext-r3-k6`
- **k:** `20`

## Operational threshold (smoke evaluation)

- **emerging_overlap_delta:** `0.0` (full_bridge_vs_emerging_jaccard − eligible_only_bridge_vs_emerging_jaccard)
- **materially_lower_emerging_overlap:** `False` (true when delta ≥ `0.1`)

The 0.10 Jaccard delta is an **operational threshold for this smoke evaluation**, not a universal statistical cutoff.

## Label metrics (from rollup)

- **bridge_good_only_precision:** `0.9`
- **bridge_good_or_acceptable_precision:** `1.0`
- **bridge_like_yes_or_partial_share:** `1.0`
- **emerging_good_only_precision:** `1.0`
- **undercited_good_only_precision:** `0.7`

## Top-k overlap (Jaccard on work_id sets)

- **full_bridge vs emerging:** overlap=16, union=24, jaccard=0.666667
- **full_bridge vs undercited:** overlap=10, union=30, jaccard=0.333333
- **emerging vs undercited:** overlap=6, union=34, jaccard=0.176471
- **full_bridge vs eligible_only_bridge:** overlap=20, union=20, jaccard=1.0
- **eligible_only_bridge vs emerging:** overlap=16, union=24, jaccard=0.666667

## Readiness (conservative go / no-go)

- **label_quality_ready:** `True`
- **distinctness_ready:** `False`
- **ready_for_small_bridge_weight_experiment:** `False`

### Suggested next step

- Bridge labels are promising, but distinctness is not yet strong enough for a weight experiment.

## Remaining gap before validation-grade evidence

- Human rollup here is single-reviewer and small-n; it is not a reproducible benchmark.
- Even strong overlap separation is not causal evidence that a weight change will improve user outcomes.
- Validation would require multi-reviewer agreement, held-out runs, and product-facing evaluation — not this artifact alone.
