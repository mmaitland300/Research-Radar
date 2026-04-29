# Bridge objective labeled outcome

Diagnostic only; this does not validate bridge ranking quality and does not justify default changes.

## Provenance

- baseline_ranking_run_id: `rank-ee2ba6c816`
- experiment_ranking_run_id: `rank-60910a47b4`
- baseline bridge_eligibility_mode: `top50_cross_cluster_gte_0_40`
- experiment bridge_eligibility_mode: `top50_cross040_exclude_persistent_shared_v1`

## Shares

- baseline good_or_acceptable_share: `0.95`
- experiment good_or_acceptable_share: `0.95`
- baseline bridge_like_yes_or_partial_share: `0.95`
- experiment bridge_like_yes_or_partial_share: `0.95`

## Distinctness

- baseline eligible_bridge_vs_emerging_jaccard: `0.212121`
- experiment eligible_bridge_vs_emerging_jaccard: `0.081081`

## Gates

- quality_preserved_under_new_mode: `True`
- bridge_like_preserved_under_new_mode: `True`
- distinctness_improves: `True`
- recommend_persistent_overlap_exclusion_as_experimental_arm: `True`
- ready_for_default: `False`

## Caveats

- This is not validation of bridge ranking quality.
- Single-reviewer, top-20, offline audit material only.
- Persistent-overlap exclusion is corpus-snapshot-specific (source-snapshot-v2-candidate-plan-20260428); the rule must not become default without rederivation on the active snapshot.
