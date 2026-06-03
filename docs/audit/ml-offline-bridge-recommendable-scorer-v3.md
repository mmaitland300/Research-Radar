# Offline bridge recommendable scorer v3 — three-pool shadow audit diagnostic

Offline diagnostic model for `bridge_recommendable` on the v14 bridge audit slice (negative-mining, top-ranked validation, shadow-pilot). Primary CV uses **130 deduped** unique work_ids; **160 row-level** readouts are audit-only.

## Slices

- Row-level audit rows: 160 (87 pos / 73 neg)
- Deduped primary rows: 130 (75 pos / 55 neg)
- Overlap with v13 bridge slice: 32
- Derived-target conflicts: 1

## Learned OOF CV (deduped 130)

- ROC AUC: 0.7175757575757575
- Average precision: 0.7590608599981735
- Pairwise accuracy: 0.7175757575757575
- Precision@5 / @10 / @20: 0.6 / 0.8 / 0.85

## Stratified deduped OOF metrics

| stratum | n | pos | neg | ROC AUC | AP | P@10 |
|---|---:|---:|---:|---:|---:|---:|
| all_deduped_130_rows | 130 | 75 | 55 | 0.7175757575757575 | 0.7590608599981735 | 0.8 |
| negative_mining_selected_62_rows | 62 | 35 | 27 | 0.6539682539682541 | 0.6561301111547349 | 0.7 |
| top_ranked_selected_8_rows | 8 | 6 | 2 | 0.6666666666666666 | 0.9107142857142857 | None |
| shadow_pilot_60_rows | 60 | 34 | 26 | 0.8054298642533936 | 0.8756564976344566 | 1.0 |
| rank-83787b91ef_deduped_70_rows | 70 | 41 | 29 | 0.6610597140454164 | 0.6834022439268876 | 0.7 |
| rank-5a7efa5ca3_deduped_60_rows | 60 | 34 | 26 | 0.8054298642533936 | 0.8756564976344566 | 1.0 |
| shadow_by_disagreement_bucket_demoted_by_hybrid | 20 | 8 | 12 | 0.5625 | 0.5729385198135198 | 0.3 |
| shadow_by_disagreement_bucket_high_bridge_score_low_ml | 10 | 2 | 8 | 0.1875 | 0.18253968253968253 | 0.2 |
| shadow_by_disagreement_bucket_high_ml_low_bridge_score | 10 | 10 | 0 | None | None | None |
| shadow_by_disagreement_bucket_promoted_by_hybrid | 20 | 14 | 6 | 0.8333333333333334 | 0.9195267869140049 | 0.9 |

## Targeted shadow disagreement readouts

- high_ml_low_bridge_score verdict: partial
- high_bridge_score_low_ml verdict: fails
- promoted_by_hybrid above-median vs shadow negatives: 18/20

## v2 baseline delta (100 work_ids)

- v3 ROC AUC on v2 work-id set: 0.6670673076923076
- Major regression vs v2: False
- Label policy drift count: 1

## Overfit sanity

- In-sample minus OOF ROC AUC gap: 0.28242424242424247

## Caveats

- This is not validation.
- Primary training/evaluation uses deduped 130 unique work_ids; row-level 160-row readouts are audit-only.
- OOF CV metrics on the deduped slice are not in-sample metrics.
- Stratified deduped metrics reuse OOF probabilities from the deduped 130-row CV.
- Row-level stratified readouts map deduped OOF probabilities and are duplicate/conflict sensitive.
- Derived-target conflict on W4415316343 is reported; shadow-pilot row wins dedupe priority.
- No ranking, API, DB-write, shadow rollout, or production serving changes are made or authorized.

Recommended next stage: `caution_overfit_on_deduped_slice_collect_more_labels`.
