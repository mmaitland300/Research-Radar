# Source-split tiny baseline diagnostic (emerging)

Offline-only diagnostic: train a tiny logistic baseline on rank-shaped emerging audit labels and test it on blind-source rows. No ranking, API, web, or production model behavior changes.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **label_dataset_path:** `docs/audit/ml-label-dataset-v4.json`
- **label_dataset_sha256:** `88a3067b48f52b6a99295c51e75da54dd03b2b84bd43b9edc674755a28f92288`
- **conflict_policy_path:** `docs/audit/ml-label-conflict-policy.md`
- **conflict_policy_sha256:** `d0591de1a2bd9ab75c64f2318e9a5ea0b7acd94902e083931049893416d47841`
- **family_context / score family:** `emerging`

## Conflict Policy Summary

- observation-level rows
- no silent merge
- no automatic conflict resolution
- blind rows test-only

## Caveats

- This is not validation.
- Blind labels reduce but do not eliminate selection bias.
- This is a source-split offline diagnostic, not a production train/test policy.
- Results must not change production ranking defaults.
- No production model artifact is produced.

## Split Counts

- **train rows:** `60`
- **test rows:** `60`
- **train variants:** `{'full_family_top_k': 20, 'ml_contrastive_offline_audit': 15, 'ml_emerging_target_gap_audit:good_or_acceptable': 25}`
- **test variants:** `{'ml_blind_snapshot_audit': 60}`

## Feature Coverage

- **train joined / selected:** `60` / `60`
- **test joined / selected:** `60` / `60`
- **train missing features:** `0`
- **test missing features:** `0`

## Blind Test Metrics

| target | train pos/neg/null | test pos/neg/null | heuristic AUC | learned AUC | heuristic P@5/10/20 | learned P@5/10/20 |
|---|---:|---:|---:|---:|---:|---:|
| `good_or_acceptable` | 45/15/0 | 51/9/0 | 0.6776 | 0.6667 | 1.0000 / 1.0000 / 1.0000 | 1.0000 / 1.0000 / 1.0000 |
| `surprising_or_useful` | 43/17/0 | 46/14/0 | 0.6009 | 0.4798 | 1.0000 / 1.0000 / 0.8500 | 0.8000 / 0.7000 / 0.8000 |

## Notes

This fixed source split asks whether a tiny logistic model trained only on rank-shaped emerging labels transfers to blind-source labels. It is an offline diagnostic only and must not be described as validation or used to alter production ranking defaults.

P@k values are `n/a` when fewer than k labeled, scored blind rows are available for that target/channel. AUC is `n/a` unless the scored blind rows contain both classes.
