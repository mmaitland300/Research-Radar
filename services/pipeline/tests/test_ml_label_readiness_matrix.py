"""Tests for ml-label-readiness-matrix (read-only diagnostics, no DB writes)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline import ml_label_readiness_matrix as mlrm
from pipeline.ml_label_readiness_matrix import (
    CAVEATS,
    MLLabelReadinessMatrixError,
    build_ml_label_readiness_matrix_payload,
    filter_audit_only_rows,
    markdown_from_ml_label_readiness_matrix,
    write_ml_label_readiness_matrix,
    _derived_target_conflict_count,
    _duplicate_paper_id_count,
    _readiness_flags,
    _target_value_counts,
)


def _minimal_payload(*rows: dict) -> dict:
    return {"dataset_version": "ml-label-dataset-v1", "rows": list(rows)}


def test_filter_audit_only_rows_split_and_row_id_dedupe() -> None:
    rows = [
        {"split": "audit_only", "ranking_run_id": "r1", "row_id": "a", "family": "bridge"},
        {"split": "train", "ranking_run_id": "r1", "row_id": "b", "family": "bridge"},
        {"split": "audit_only", "ranking_run_id": "r1", "row_id": "a", "family": "bridge"},
    ]
    out, dup = filter_audit_only_rows(_minimal_payload(*rows))
    assert len(out) == 1 and dup == 1


def test_target_value_counts() -> None:
    rs = [
        {"good_or_acceptable": True},
        {"good_or_acceptable": False},
        {"good_or_acceptable": None},
    ]
    assert _target_value_counts(rs, "good_or_acceptable") == (1, 1, 1)


def test_duplicate_paper_id_count_counts_ids_with_multiple_rows() -> None:
    rs = [
        {"paper_id": "https://openalex.org/W1"},
        {"paper_id": "https://openalex.org/W1"},
        {"paper_id": "https://openalex.org/W2"},
    ]
    assert _duplicate_paper_id_count(rs) == 1


def test_derived_target_conflict_same_paper_opposite_labels() -> None:
    rs = [
        {"paper_id": "W1", "good_or_acceptable": True},
        {"paper_id": "W1", "good_or_acceptable": False},
        {"paper_id": "W2", "good_or_acceptable": True},
    ]
    assert _derived_target_conflict_count(rs, "good_or_acceptable") == 1


def test_readiness_flags_thresholds() -> None:
    assert _readiness_flags(1, 1)["has_both_classes"] is True
    assert _readiness_flags(1, 0)["has_both_classes"] is False
    assert _readiness_flags(3, 3)["enough_for_diagnostic_auc"] is True
    assert _readiness_flags(2, 3)["enough_for_diagnostic_auc"] is False
    assert _readiness_flags(10, 10)["enough_for_tiny_baseline"] is True
    assert _readiness_flags(9, 10)["enough_for_tiny_baseline"] is False


def _snap(*, exists: bool, succeeded: bool, n_scores: int) -> dict:
    return {
        "ranking_run_exists": exists,
        "ranking_run_succeeded": succeeded,
        "ranking_run_status": "succeeded" if succeeded else ("missing" if not exists else "failed"),
        "paper_scores_row_count": n_scores,
    }


def test_build_groups_by_run_family_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(
        json.dumps(
            _minimal_payload(
                {
                    "split": "audit_only",
                    "row_id": "1",
                    "ranking_run_id": "run-a",
                    "family": "bridge",
                    "review_pool_variant": "rank_top_k",
                    "work_id": "10",
                    "paper_id": "https://openalex.org/W10",
                    "good_or_acceptable": True,
                    "surprising_or_useful": False,
                    "bridge_like_yes_or_partial": None,
                },
                {
                    "split": "audit_only",
                    "row_id": "2",
                    "ranking_run_id": "run-a",
                    "family": "emerging",
                    "review_pool_variant": "ml_blind_snapshot_audit",
                    "work_id": "11",
                    "paper_id": "https://openalex.org/W11",
                    "good_or_acceptable": False,
                    "surprising_or_useful": True,
                    "bridge_like_yes_or_partial": True,
                },
            ),
            indent=2,
        ),
        encoding="utf-8",
    )

    def fake_snap(_conn: object, *, ranking_run_id: str) -> dict:
        assert ranking_run_id == "run-a"
        return _snap(exists=True, succeeded=True, n_scores=2)

    score_row = {
        "work_id": 10,
        "recommendation_family": "bridge",
        "semantic_score": 0.1,
        "citation_velocity_score": 0.2,
        "topic_growth_score": 0.3,
        "bridge_score": 0.4,
        "diversity_penalty": 0.0,
        "final_score": 0.5,
        "openalex_id": "https://openalex.org/W10",
        "_rank": 1,
    }
    score_row_e = {
        "work_id": 11,
        "recommendation_family": "emerging",
        "semantic_score": 0.1,
        "citation_velocity_score": 0.2,
        "topic_growth_score": 0.3,
        "bridge_score": 0.0,
        "diversity_penalty": 0.0,
        "final_score": 0.4,
        "openalex_id": "https://openalex.org/W11",
        "_rank": 2,
    }

    def fake_scores(_conn: object, *, ranking_run_id: str) -> list[dict]:
        assert ranking_run_id == "run-a"
        return [score_row, score_row_e]

    monkeypatch.setattr(mlrm, "fetch_run_db_snapshot", fake_snap)
    monkeypatch.setattr(mlrm, "fetch_paper_scores_with_openalex", fake_scores)

    conn = MagicMock()
    payload = build_ml_label_readiness_matrix_payload(conn, label_dataset_path=p)
    groups = { (g["ranking_run_id"], g["family"], g["target"]): g for g in payload["groups"] }
    assert len(groups) == 6
    b_go = groups[("run-a", "bridge", "good_or_acceptable")]
    assert b_go["positive_count"] == 1 and b_go["negative_count"] == 0
    assert b_go["review_pool_variant_counts"] == {"rank_top_k": 1}
    assert b_go["paper_scores_joinable_count"] == 1 and b_go["missing_score_count"] == 0
    e_go = groups[("run-a", "emerging", "good_or_acceptable")]
    assert e_go["positive_count"] == 0 and e_go["negative_count"] == 1
    assert e_go["review_pool_variant_counts"] == {"ml_blind_snapshot_audit": 1}
    assert e_go["paper_scores_joinable_count"] == 1
    assert payload["run_snapshots"]["run-a"]["ranking_run_succeeded"] is True
    assert payload["source_slice_summary"]


def test_run_missing_and_not_succeeded_joinable_zero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(
        json.dumps(
            _minimal_payload(
                {
                    "split": "audit_only",
                    "row_id": "1",
                    "ranking_run_id": "gone",
                    "family": "bridge",
                    "work_id": "1",
                    "paper_id": "https://openalex.org/W1",
                    "good_or_acceptable": True,
                    "surprising_or_useful": False,
                    "bridge_like_yes_or_partial": False,
                },
            ),
            indent=2,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        mlrm,
        "fetch_run_db_snapshot",
        lambda _c, *, ranking_run_id: _snap(exists=False, succeeded=False, n_scores=0),
    )
    monkeypatch.setattr(mlrm, "fetch_paper_scores_with_openalex", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no scores fetch when run failed")))

    payload = build_ml_label_readiness_matrix_payload(MagicMock(), label_dataset_path=p)
    g = next(x for x in payload["groups"] if x["target"] == "good_or_acceptable" and x["family"] == "bridge")
    assert g["paper_scores_joinable_count"] == 0
    assert g["missing_score_count"] == 1
    assert g["ranking_run_exists"] is False


def test_markdown_includes_caveats(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "x.json"
    p.write_text(json.dumps(_minimal_payload(), indent=2), encoding="utf-8")
    monkeypatch.setattr(
        mlrm,
        "fetch_run_db_snapshot",
        lambda _c, *, ranking_run_id: _snap(exists=True, succeeded=True, n_scores=0),
    )
    monkeypatch.setattr(mlrm, "fetch_paper_scores_with_openalex", lambda *_a, **_k: [])
    pl = build_ml_label_readiness_matrix_payload(MagicMock(), label_dataset_path=p)
    md = markdown_from_ml_label_readiness_matrix(pl)
    for c in CAVEATS:
        assert c in md


def test_write_no_mutating_sql(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "in.json"
    p.write_text(
        json.dumps(
            _minimal_payload(
                {
                    "split": "audit_only",
                    "row_id": "z",
                    "ranking_run_id": "r",
                    "family": "bridge",
                    "work_id": "9",
                    "paper_id": "https://openalex.org/W9",
                    "good_or_acceptable": True,
                    "surprising_or_useful": False,
                    "bridge_like_yes_or_partial": None,
                },
            ),
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        mlrm,
        "fetch_run_db_snapshot",
        lambda _c, *, ranking_run_id: _snap(exists=True, succeeded=True, n_scores=1),
    )
    monkeypatch.setattr(mlrm, "fetch_paper_scores_with_openalex", lambda *_a, **_k: [])

    class GuardConn:
        def cursor(self, row_factory: object | None = None) -> object:
            raise AssertionError("GuardConn should not execute SQL when DB helpers are patched")

    j = tmp_path / "out.json"
    m = tmp_path / "out.md"
    write_ml_label_readiness_matrix(GuardConn(), label_dataset_path=p, json_path=j, markdown_path=m)
    assert j.is_file() and m.is_file()


def test_missing_label_file_raises() -> None:
    with pytest.raises(MLLabelReadinessMatrixError):
        build_ml_label_readiness_matrix_payload(MagicMock(), label_dataset_path=Path("/nonexistent/nope.json"))
