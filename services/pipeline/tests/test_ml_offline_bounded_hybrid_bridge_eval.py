from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from pipeline.ml_offline_bounded_hybrid_bridge_eval import (
    ARTIFACT_TYPE,
    EVAL_VERSION,
    MLOfflineBoundedHybridBridgeEvalError,
    _rank_percentile_scores,
    _recommended_next_stage,
    build_ml_offline_bounded_hybrid_bridge_eval_payload,
    markdown_from_ml_offline_bounded_hybrid_bridge_eval,
    write_ml_offline_bounded_hybrid_bridge_eval,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_rows(count: int = 70, *, review_pool_variant: str = "ml_bridge_negative_mining_audit") -> list[dict]:
    rows: list[dict] = []
    for i in range(count):
        label = i < 38
        if label:
            bridge_like = "yes" if i < 13 else "partial"
            relevance = "good" if i < 33 else "acceptable"
        else:
            neg_i = i - 38
            bridge_like = "no"
            relevance = "acceptable" if neg_i < 22 else ("miss" if neg_i < 28 else "irrelevant")
        final_score = ((i * 29) % 70) / 69.0
        rows.append(
            {
                "row_id": f"row-{i:02d}",
                "paper_id": f"https://openalex.org/W{1000 + i}",
                "work_id": f"W{1000 + i}",
                "title": f"Synthetic bridge row {i}",
                "split": "audit_only",
                "ranking_run_id": "rank-83787b91ef",
                "family": "bridge",
                "review_pool_variant": review_pool_variant,
                "bridge_recommendable": label,
                "bridge_like_label": bridge_like,
                "relevance_label": relevance,
                "final_score": final_score,
                "bridge_negative_mining_context": {
                    "internal_work_id": i + 1,
                    "family": "bridge",
                    "family_rank": i + 1,
                    "ranking_run_id": "rank-83787b91ef",
                    "final_score": final_score,
                    "reason_short": f"reason {i}",
                    "sample_reason": "synthetic_fixture",
                },
            }
        )
    return rows


def _readiness_payload(label_sha: str, **overrides: object) -> dict:
    group = {
        "ranking_run_id": "rank-83787b91ef",
        "family": "bridge",
        "target": "bridge_recommendable",
        "total_labeled_rows": 70,
        "positive_count": 38,
        "negative_count": 32,
        "paper_scores_joinable_count": 70,
        "missing_score_count": 0,
        "derived_target_conflict_count": 0,
        "readiness": {
            "has_both_classes": True,
            "enough_for_diagnostic_auc": True,
            "enough_for_tiny_baseline": True,
        },
    }
    group.update(overrides)
    return {
        "artifact_type": "ml_label_readiness_matrix",
        "provenance": {
            "label_dataset_version": "ml-label-dataset-v12",
            "label_dataset_sha256": label_sha,
        },
        "groups": [group],
    }


def _embeddings_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_embeddings",
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "embedding_version": "shadow-generalization-text-embedding-v1",
            "snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
        },
        "embedding_result": {
            "status": "succeeded",
            "full_snapshot_embedding_coverage": True,
            "embedding_dimensions": 3,
        },
        "coverage": {
            "embedded_work_count": 528,
            "missing_embedding_count": 0,
        },
    }


def _oof_probability(row: dict, index: int) -> float:
    if row["bridge_recommendable"]:
        return 0.95 - index * 0.001
    return 0.05 + index * 0.001


def _scorer_payload(rows: list[dict], *, include_oof: bool = True) -> dict:
    learned_cv = {"aggregate_oof": {"status": "ok", "roc_auc": 1.0}}
    if include_oof:
        learned_cv["oof_predictions"] = [
            {
                "row_id": row["row_id"],
                "work_id": row["work_id"],
                "label": row["bridge_recommendable"],
                "probability": _oof_probability(row, i),
            }
            for i, row in enumerate(rows)
        ]
    return {
        "artifact_type": "ml_offline_bridge_recommendable_scorer",
        "scorer_version": "ml-offline-bridge-recommendable-scorer-v1",
        "metadata": {"target": "bridge_recommendable"},
        "evaluation": {"learned_cv": learned_cv},
    }


def _fixture(
    tmp_path: Path,
    *,
    rows: list[dict] | None = None,
    scorer_payload: dict | None = None,
    readiness_overrides: dict | None = None,
) -> dict[str, Path]:
    rows = rows if rows is not None else _make_rows()
    label_path = tmp_path / "docs/audit/ml-label-dataset-v12.json"
    scorer_path = tmp_path / "docs/audit/ml-offline-bridge-recommendable-scorer-v1.json"
    readiness_path = tmp_path / "docs/audit/ml-label-readiness-matrix-v9.json"
    embeddings_path = tmp_path / "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json"
    _write_json(label_path, {"dataset_version": "ml-label-dataset-v12", "rows": rows})
    label_sha = _sha256(label_path)
    _write_json(readiness_path, _readiness_payload(label_sha, **(readiness_overrides or {})))
    _write_json(scorer_path, scorer_payload if scorer_payload is not None else _scorer_payload(rows))
    _write_json(embeddings_path, _embeddings_payload())
    return {
        "label": label_path,
        "scorer": scorer_path,
        "readiness": readiness_path,
        "embeddings": embeddings_path,
    }


def _build(paths: dict[str, Path]) -> dict:
    return build_ml_offline_bounded_hybrid_bridge_eval_payload(
        label_dataset_path=paths["label"],
        bridge_scorer_path=paths["scorer"],
        readiness_matrix_path=paths["readiness"],
        embeddings_provenance_path=paths["embeddings"],
    )


def test_happy_path_with_synthetic_70_row_fixture_and_no_db(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    output = tmp_path / "out/eval.json"
    markdown = tmp_path / "out/eval.md"

    payload = write_ml_offline_bounded_hybrid_bridge_eval(
        label_dataset_path=paths["label"],
        bridge_scorer_path=paths["scorer"],
        readiness_matrix_path=paths["readiness"],
        embeddings_provenance_path=paths["embeddings"],
        json_path=output,
        markdown_path=markdown,
    )

    assert output.exists()
    assert markdown.exists()
    assert payload["artifact_type"] == ARTIFACT_TYPE
    assert payload["eval_version"] == EVAL_VERSION
    assert payload["no_db_access_required"] is True
    assert payload["rank_percentile_scope"] == "labeled_slice_only"
    assert payload["slice_counts"]["row_count"] == 70
    assert payload["slice_counts"]["positive_count"] == 38
    assert payload["slice_counts"]["negative_count"] == 32
    assert payload["slice_counts"]["hard_negative_count"] == 22
    assert payload["arm_metrics"]["learned_bridge_probability_oof"]["roc_auc"] == pytest.approx(1.0)
    assert payload["primary_confirmatory_arm"] == "hybrid_rank_mean_50_50"
    assert payload["best_arm_by_roc_auc"]["exploratory_only"] is True
    assert "hybrid_rank_mean_50_50" in payload["deltas_vs_heuristic_final_score"]
    assert "hybrid_rank_mean_50_50" in payload["deltas_vs_learned_bridge_probability_oof"]
    assert payload["disagreement_analysis"]["high_ml_low_heuristic"]
    assert payload["disagreement_analysis"]["high_heuristic_low_ml"]
    assert payload["disagreement_analysis"]["uncertain_ml"]


def test_rejects_scorer_artifact_without_oof_probabilities(tmp_path: Path) -> None:
    rows = _make_rows()
    paths = _fixture(tmp_path, rows=rows, scorer_payload=_scorer_payload(rows, include_oof=False))

    with pytest.raises(MLOfflineBoundedHybridBridgeEvalError, match="oof_predictions"):
        _build(paths)


def test_rejects_wrong_label_slice_count(tmp_path: Path) -> None:
    paths = _fixture(tmp_path, rows=_make_rows(count=69))

    with pytest.raises(MLOfflineBoundedHybridBridgeEvalError, match="expected 70"):
        _build(paths)


def test_rejects_oof_row_id_mismatch(tmp_path: Path) -> None:
    rows = _make_rows()
    scorer = _scorer_payload(rows)
    scorer["evaluation"]["learned_cv"]["oof_predictions"][0]["row_id"] = "row-missing"
    paths = _fixture(tmp_path, rows=rows, scorer_payload=scorer)

    with pytest.raises(MLOfflineBoundedHybridBridgeEvalError, match="OOF row_id set"):
        _build(paths)


def test_validates_readiness_matrix_counts(tmp_path: Path) -> None:
    paths = _fixture(tmp_path, readiness_overrides={"positive_count": 37})

    with pytest.raises(MLOfflineBoundedHybridBridgeEvalError, match="positive_count"):
        _build(paths)


def test_rank_percentile_calculation_is_deterministic_and_slice_scoped() -> None:
    rows = [
        {"work_id": "W1", "score": 10.0},
        {"work_id": "W2", "score": 10.0},
        {"work_id": "W3", "score": 5.0},
    ]

    ranks = _rank_percentile_scores(rows, "score")

    assert ranks == {"W1": pytest.approx(0.75), "W2": pytest.approx(0.75), "W3": pytest.approx(0.0)}


def test_primary_best_arm_and_delta_readouts_are_present(tmp_path: Path) -> None:
    payload = _build(_fixture(tmp_path))

    assert payload["primary_confirmatory_arm"] == "hybrid_rank_mean_50_50"
    assert payload["best_arm_by_average_precision"]["exploratory_only"] is True
    assert payload["arm_metrics"]["hybrid_rank_mean_50_50"]["delta_vs_heuristic_final_score"]
    assert payload["arm_metrics"]["hybrid_rank_mean_50_50"]["delta_vs_learned_bridge_probability_oof"]


def test_recommended_next_stage_conditional_branches() -> None:
    base = {
        "heuristic_final_score": {"roc_auc": 0.60, "average_precision": 0.60},
        "learned_bridge_probability_oof": {"roc_auc": 0.70, "average_precision": 0.70},
        "hybrid_rank_mean_70_30_heuristic": {"roc_auc": 0.0, "average_precision": 0.0},
        "hybrid_rank_mean_30_70_heuristic": {"roc_auc": 0.0, "average_precision": 0.0},
    }
    wins = copy.deepcopy(base)
    wins["hybrid_rank_mean_50_50"] = {"roc_auc": 0.80, "average_precision": 0.80}
    mixed = copy.deepcopy(base)
    mixed["hybrid_rank_mean_50_50"] = {"roc_auc": 0.71, "average_precision": 0.69}
    worse = copy.deepcopy(base)
    worse["hybrid_rank_mean_50_50"] = {"roc_auc": 0.69, "average_precision": 0.69}

    assert _recommended_next_stage(wins) == "bridge_shadow_offline_pilot_plan_v1"
    assert _recommended_next_stage(mixed) == "create_bridge_active_learning_worksheet_v1"
    assert _recommended_next_stage(worse) == "do_not_combine_signals_collect_labels_or_fix_features"


def test_markdown_says_offline_diagnostic_labeled_slice_only_and_not_validation(tmp_path: Path) -> None:
    payload = _build(_fixture(tmp_path))
    markdown = markdown_from_ml_offline_bounded_hybrid_bridge_eval(payload)

    assert "Offline diagnostic only" in markdown
    assert "labeled_slice_only" in markdown
    assert "not validation" in markdown
    assert "ml_bridge_negative_mining_audit" in markdown
