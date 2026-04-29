# Bridge weight experiment delta review summary

This artifact does **not** validate bridge ranking and does **not** justify making `0.05` the default.

## Provenance

- **Generated (UTC):** `2026-04-29T04:46:04.178444+00:00`
- **Input:** `C:\dev\Cursor Projects\Research-Radar\docs\audit\manual-review\bridge_weight_experiment_rank-bc1123e00c_delta_review.csv`
- **Review kind:** `bridge_weight_experiment_delta_only`
- **row_count:** `4`
- **baseline_ranking_run_id:** `rank-ee2ba6c816`
- **experiment_ranking_run_id:** `rank-bc1123e00c`

## Metrics

- **good_count:** `2`
- **acceptable_count:** `2`
- **good_or_acceptable_count:** `4`
- **useful_or_surprising_count:** `4`
- **bridge_like_yes_or_partial_count:** `4`
- **miss_or_irrelevant_count:** `0`
- **bridge_like_no_count:** `0`
- **good_or_acceptable_share:** `1.0`
- **useful_or_surprising_share:** `1.0`
- **bridge_like_yes_or_partial_share:** `1.0`

## Gates

- **delta_quality_pass:** `True` (good_or_acceptable_share ≥ 0.75)
- **delta_bridge_like_pass:** `True` (bridge_like_yes_or_partial_share ≥ 0.75)
- **experiment_quality_gate_pass:** `True`

## Decision

The 0.05 bridge-weight experiment preserved quality on moved-in rows; candidate for a second gated experiment, not default.

- **ready_for_default:** `False` (must remain false)

## Caveats

- This is not validation of bridge ranking or of any default bridge weight.
- This summary reflects only a 4-row delta review of moved-in eligible bridge top-20 rows.
- ready_for_default remains false.
- No further weight increase until this artifact is reviewed in context with the full pipeline evidence.
