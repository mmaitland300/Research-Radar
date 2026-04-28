# Recommendation review rollup

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **ranking_version:** `bridge-v2-nm1-zero-corpusv2-r2-k12-elig-top50-cross040-20260428`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`

## Family metrics

| Family | P@k good-only | P@k good/acceptable | Bridge-like yes/partial | Surprising/useful |
| --- | --- | --- | --- | --- |
| bridge | 0.850 | 0.950 | 0.950 | 0.900 |
| emerging | 0.950 | 1.000 | null | 0.900 |
| undercited | 0.650 | 0.950 | null | 0.900 |

## Interpretation

- Best good-only family: **emerging**
- Weakest good-only family: **undercited**
- Ready for distinctness analysis: **True**
- Ready for weight experiment: **True**

## Readiness gates

- label_quality_ready: **True**
- bridge_like_ready: **True**
- distinctness_ready: **True**
- family_quality_context_ready: **True**
- ready_for_small_bridge_weight_experiment: **True**

## Evidence caveat

- Single-reviewer, top-20, offline evidence.
- This rollup does not prove bridge ranking superiority.
- This rollup is not validation; it is a conservative gating artifact.

## Limitations

- Single-reviewer labels can be noisy; treat as directional evidence.
- Small curated corpus can saturate relevance and novelty metrics.
- This rollup is not run-to-run validation and does not prove weight effectiveness.

## Suggested next step

- Candidate for a small gated bridge-weight experiment; not validation.

## Bridge distinctness

- full_bridge_vs_emerging_jaccard: `0.73913`
- eligible_bridge_vs_emerging_jaccard: `0.212121`
- emerging_overlap_delta_from_full_to_eligible: `0.527009`
- eligible_head_differs_from_full: `True`
- eligible_head_less_emerging_like_than_full: `True`
- eligible_distinctness_improves_by_threshold: `True`
