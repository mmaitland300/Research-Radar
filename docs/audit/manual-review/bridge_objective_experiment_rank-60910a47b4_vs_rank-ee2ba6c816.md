# Bridge objective experiment comparison

Same bridge weight; different `bridge_eligibility_mode`. Diagnostic only, not validation.

## Provenance

- baseline ranking_run_id: `rank-ee2ba6c816`
- baseline bridge_eligibility_mode: `top50_cross_cluster_gte_0_40`
- experiment ranking_run_id: `rank-60910a47b4`
- experiment bridge_eligibility_mode: `top50_cross040_exclude_persistent_shared_v1`

## Same-stack check

- same_corpus_snapshot_version: `True`
- same_embedding_version: `True`
- same_cluster_version: `True`
- same_bridge_weight_for_family_bridge: `True`
- bridge_eligibility_modes_differ: `True`

## Distinctness

- baseline eligible bridge vs emerging jaccard: `0.212121`
- experiment eligible bridge vs emerging jaccard: `0.081081`
- delta (experiment - baseline): `-0.13104`

## Labeling

- unlabeled_experiment_eligible_top_k_count: `5`
- candidate_for_labeling: `True`
- ready_for_default: `False`
