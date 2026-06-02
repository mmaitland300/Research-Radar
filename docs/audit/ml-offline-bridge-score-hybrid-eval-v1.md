# Offline bridge_score hybrid eval v1

Offline diagnostic: ML v2 OOF probabilities vs `bridge_score` from a clustering-enabled ranking run. Not validation; no serving change.

- Ranking run (bridge_score source): `rank-5a7efa5ca3`
- Label dataset: `ml-label-dataset-v13`
- V2 scorer: `ml-offline-bridge-recommendable-scorer-v2`
- Total labeled rows: 100 (53 positive / 47 negative)
- bridge_score coverage: 100/100 (100.0%)
- Primary confirmatory arm: `hybrid_bridge_score_50_50`

## Arms

| arm | rows | ROC AUC | AP | Pairwise | P@5 | P@10 | P@20 | top20+ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `learned_v2_oof` | 100 | 0.6495 | 0.6399 | 0.6495 | 0.6000 | 0.7000 | 0.6500 | 13 |
| `bridge_score_heuristic` | 100 | 0.5909 | 0.5611 | 0.5909 | 0.4000 | 0.3000 | 0.5000 | 10 |
| `hybrid_bridge_score_50_50` | 100 | 0.6640 | 0.6961 | 0.6640 | 0.8000 | 0.8000 | 0.8000 | 16 |
| `hybrid_bridge_score_70_30_ml` | 100 | 0.6391 | 0.6522 | 0.6391 | 0.8000 | 0.8000 | 0.7000 | 14 |
| `hybrid_bridge_score_30_70_ml` | 100 | 0.6734 | 0.6813 | 0.6734 | 0.8000 | 0.8000 | 0.7500 | 15 |

## Readout

- Best arm by ROC AUC: `hybrid_bridge_score_30_70_ml` = `0.6734243275792854` (exploratory only)
- Best arm by average precision: `hybrid_bridge_score_50_50` = `0.6960884930444955` (exploratory only)
- Recommended next stage: `bridge_shadow_offline_pilot_plan_v1`

## Caveats

- This is not validation.
- This is a worksheet-selected two-slice offline diagnostic (100 rows).
- Rank percentiles are labeled_slice_only for covered rows; not full-pool production scores.
- bridge_score arm and hybrids use only rows where bridge_score is non-null in the new run.
- learned_v2_oof uses all 100 rows.
- Primary confirmatory arm is hybrid_bridge_score_50_50.
- No DB writes, ranking writes, serving changes, or production authorization.
