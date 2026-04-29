# Bridge weight experiment comparison

This is a ranking movement experiment, not validation.
Do not make positive bridge weight the default until the moved/new rows are reviewed.

## Provenance

- baseline ranking_run_id: `rank-bc1123e00c`
- baseline ranking_version: `bridge-v2-nm1-w005-corpusv2-r3-k12-elig-top50-cross040-20260428`
- baseline bridge_weight_for_family_bridge: `0.05`
- experiment ranking_run_id: `rank-9a02c81d40`
- experiment ranking_version: `bridge-v2-nm1-w010-corpusv2-r4-k12-elig-top50-cross040-20260428`
- experiment bridge_weight_for_family_bridge: `0.1`

## Same-stack check

- same_corpus_snapshot_version: `True`
- same_embedding_version: `True`
- same_cluster_version: `True`
- same_bridge_eligibility_mode: `True`
- only_bridge_weight_differs: `True`

## Bridge top-k movement

- full_bridge_overlap_jaccard: `0.904762`
- eligible_bridge_overlap_jaccard: `1.0`
- new_full_bridge_work_ids: `[124]`
- dropped_full_bridge_work_ids: `[137]`

## Family stability checks

- emerging_changed: `False` (jaccard `1.0`)
- undercited_changed: `False` (jaccard `1.0`)

## Distinctness

- baseline eligible bridge vs emerging jaccard: `0.212121`
- experiment eligible bridge vs emerging jaccard: `0.212121`
- delta (experiment - baseline): `0.0`

## Labeling risk and decision

- unlabeled_experiment_eligible_top_k_count: `0`
- candidate_for_labeling: `False`
- candidate_for_weight_increase: `False`
- ready_for_default: `False`
