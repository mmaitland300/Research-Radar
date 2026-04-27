# Week 2 G3 - Manual Review Comparison Snapshot

Status: `in_progress`  
Scope: baseline stack `rank-3904fec89d` manual-review summaries for `bridge`, `emerging`, `undercited`

## Stack and artifacts

- `ranking_run_id`: `rank-3904fec89d`
- `ranking_version`: `bridge-v2-nm1-zero-r3-k6-20260424`
- `corpus_snapshot_version`: `source-snapshot-20260425-044015`
- `embedding_version`: `v1-title-abstract-1536-cleantext-r3`
- `cluster_version`: `kmeans-l2-v0-cleantext-r3-k6`

Summaries:
- `docs/audit/manual-review/bridge_rank-3904fec89d_top20_summary.json`
- `docs/audit/manual-review/emerging_rank-3904fec89d_top20_summary.json`
- `docs/audit/manual-review/undercited_rank-3904fec89d_top20_summary.json`

## Comparison snapshot (top-20 each family)

| Family | Good-only precision | Good/acceptable precision | Bridge-like yes/partial | Surprising/useful |
| --- | --- | --- | --- | --- |
| bridge | `0.90` | `1.00` | `1.00` | `1.00` |
| emerging | `1.00` | `1.00` | `null` (`not_applicable` on all rows) | `1.00` |
| undercited | `0.70` | `1.00` | `null` (`not_applicable` on all rows) | `1.00` |

## Interpretation (decision tracking)

- **Bridge sanity check passes:** all bridge rows were at least `partial` (`yes=10`, `partial=10`, `no=0`).
- **Undercited is meaningfully harder:** good-only drops to `0.70` with `acceptable=6`, which is healthier than a flat perfect sheet.
- **Novelty still saturates:** all three families have `obvious=0` and `not_useful=0`.
- Current label pass is therefore useful for directional checks, but still likely **novelty-generous**.

## Caveats

- This is a single-pass human judgment on a small curated corpus (`59` included works), not a benchmark.
- `bridge_like_label` was intentionally `not_applicable` for `emerging` and `undercited`; do not compare bridge-likeness across families from this pass.
- Avoid stronger product/ML claims until a stricter novelty calibration pass is recorded.

## Next actions

1. **Freeze this pass as baseline evidence** (no relabel churn unless clear errors).
2. **Run a calibration mini-pass** (5-10 rows/family) with explicit examples for `obvious` vs `useful` vs `surprising`.
3. **Re-score only novelty labels** with that calibration guidance; keep relevance labels unless disagreement is substantive.
4. **Re-run summaries** and compare deltas vs this snapshot.
5. If deltas remain stable, proceed to the planned experiment matrix and controlled ranking-variant comparisons.

