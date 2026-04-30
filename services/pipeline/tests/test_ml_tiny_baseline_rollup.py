"""Tests for ml-tiny-baseline-rollup (ablations, fold stats, conservative fields)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_tiny_baseline import (
    prepare_stratified_cv_fold_tests,
    stratified_round_robin_fold_test_indices,
)
from pipeline.ml_tiny_baseline_rollup import (
    ROLLUP_CAVEATS,
    ABLATION_SPECS,
    MLTinyBaselineRollupError,
    _compare_learned_to_heuristic,
    build_ml_tiny_baseline_rollup_payload,
    evaluate_spec_on_folds,
    fold_tests_fingerprint,
    markdown_from_ml_tiny_baseline_rollup,
)


class _FakeCur:
    def __init__(self, parent: "_FakeConn") -> None:
        self._p = parent

    def execute(self, query: str, params: tuple | None = None) -> "_FakeCur":
        self._sql = query
        self._params = params
        return self

    def fetchone(self) -> dict | None:
        if "FROM ranking_runs" in self._sql:
            return self._p.run_row
        return None

    def fetchall(self) -> list[dict]:
        if "FROM paper_scores" in self._sql and "JOIN works" in self._sql:
            return list(self._p.score_rows)
        return []


class _FakeCurCtx:
    def __init__(self, parent: "_FakeConn") -> None:
        self._cur = _FakeCur(parent)

    def __enter__(self) -> _FakeCur:
        return self._cur

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, *, run_row: dict, score_rows: list[dict]) -> None:
        self.run_row = run_row
        self.score_rows = score_rows

    def cursor(self, row_factory: object | None = None) -> _FakeCurCtx:
        return _FakeCurCtx(self)


def _run_row() -> dict:
    return {
        "ranking_run_id": "rank-x",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {"clustering_artifact": {"cluster_version": "cv1"}},
        "status": "succeeded",
    }


def _score(wid: int, *, pos: bool) -> dict:
    return {
        "work_id": wid,
        "recommendation_family": "emerging",
        "semantic_score": 0.5 + 0.01 * wid + (0.15 if pos else -0.05),
        "citation_velocity_score": 0.04 * (wid % 4),
        "topic_growth_score": 0.4 if pos else 0.15,
        "bridge_score": 0.99,
        "diversity_penalty": 0.0,
        "final_score": 0.2 + 0.02 * wid + (0.1 if pos else 0.0),
        "openalex_id": f"https://openalex.org/W{wid}",
    }


def _row(rid: str, row_id: str, wid: int, *, goa: bool, sou: bool) -> dict:
    return {
        "split": "audit_only",
        "ranking_run_id": rid,
        "row_id": row_id,
        "family": "emerging",
        "work_id": str(wid),
        "paper_id": f"https://openalex.org/W{wid}",
        "good_or_acceptable": goa,
        "surprising_or_useful": sou,
        "bridge_like_yes_or_partial": None,
    }


def test_ablation_specs_feature_sets() -> None:
    ids_modes = [(s[0], s[1], s[2]) for s in ABLATION_SPECS]
    assert ids_modes[0] == ("heuristic_final_score", "heuristic", None)
    assert ids_modes[1][2] == ("final_score",)
    assert ids_modes[2][2] == ("semantic_score",)
    assert ids_modes[3][2] == ("citation_velocity_score", "topic_growth_score", "diversity_penalty")
    assert ids_modes[4][2] == (
        "semantic_score",
        "citation_velocity_score",
        "topic_growth_score",
        "diversity_penalty",
    )
    full = (
        "final_score",
        "semantic_score",
        "citation_velocity_score",
        "topic_growth_score",
        "diversity_penalty",
    )
    assert ids_modes[5][2] == full


def test_fold_fingerprint_stable_across_specs_for_same_target(tmp_path: Path) -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=i % 2 == 0) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(
        json.dumps({"dataset_version": "t", "rows": rows}),
        encoding="utf-8",
    )
    payload = build_ml_tiny_baseline_rollup_payload(
        fc, label_dataset_path=p, ranking_run_id="rank-x", family="emerging"
    )
    g = payload["targets"]["good_or_acceptable"]
    s = payload["targets"]["surprising_or_useful"]
    assert g["fold_tests_fingerprint_sha256"] != s["fold_tests_fingerprint_sha256"]
    y_g, ft_g, _, row_ids_g = prepare_stratified_cv_fold_tests(rows, target="good_or_acceptable")
    fp_g_heur = fold_tests_fingerprint(row_ids_g, y_g, ft_g)
    assert fp_g_heur == g["fold_tests_fingerprint_sha256"]
    for sid, _m, _f in ABLATION_SPECS:
        assert sid in g["specs"]


def test_precision_at_k_null_when_fold_test_small() -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=True) for i in range(1, 27)]
    y, fold_tests, _n, _r = prepare_stratified_cv_fold_tests(rows, target="good_or_acceptable")
    h = evaluate_spec_on_folds(
        rows, y, fold_tests, target="good_or_acceptable", spec_id="heuristic_final_score", mode="heuristic", feature_names=None
    )
    for f in h["per_fold"]:
        if f["n_test"] < 5:
            assert f["precision_at_5"] is None


def test_compare_win_tie_loss_and_worst_gap() -> None:
    learned = {
        "aggregate_out_of_fold": {"roc_auc_mann_whitney": 0.75, "pairwise_accuracy": 0.7},
        "per_fold": [
            {"roc_auc_mann_whitney": 0.9},
            {"roc_auc_mann_whitney": 0.5},
        ],
    }
    heuristic = {
        "aggregate_out_of_fold": {"roc_auc_mann_whitney": 0.7, "pairwise_accuracy": 0.65},
        "per_fold": [
            {"roc_auc_mann_whitney": 0.8},
            {"roc_auc_mann_whitney": 0.8},
        ],
    }
    c = _compare_learned_to_heuristic(learned, heuristic)
    assert c["learned_beat_heuristic_fold_count"] == 1
    assert c["learned_tied_heuristic_fold_count"] == 0
    assert c["learned_lost_to_heuristic_fold_count"] == 1
    assert c["worst_fold_auc_gap"] == pytest.approx(-0.3)
    assert c["aggregate_auc_delta"] == pytest.approx(0.05)


def test_compare_p_at_k_improved_and_worsened() -> None:
    learned = {
        "aggregate_out_of_fold": {"roc_auc_mann_whitney": 0.5, "pairwise_accuracy": 0.5},
        "per_fold": [
            {"precision_at_5": 0.8, "precision_at_10": None, "precision_at_20": None},
            {"precision_at_5": 0.4, "precision_at_10": None, "precision_at_20": None},
        ],
    }
    heuristic = {
        "aggregate_out_of_fold": {"roc_auc_mann_whitney": 0.5, "pairwise_accuracy": 0.5},
        "per_fold": [
            {"precision_at_5": 0.6, "precision_at_10": None, "precision_at_20": None},
            {"precision_at_5": 0.5, "precision_at_10": None, "precision_at_20": None},
        ],
    }
    c = _compare_learned_to_heuristic(learned, heuristic)
    assert c["p_at_k_improved_any_fold"] is True
    assert c["p_at_k_worsened_any_fold"] is True


def test_conservative_fields_product_and_validation_false() -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=i % 2 == 0) for i in range(1, 27)]
    p = Path("x")
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "lab.json"
        p.write_text(json.dumps({"dataset_version": "t", "rows": rows}), encoding="utf-8")
        payload = build_ml_tiny_baseline_rollup_payload(
            fc, label_dataset_path=p, ranking_run_id="rank-x", family="emerging"
        )
    for tgt in ("good_or_acceptable", "surprising_or_useful"):
        cdf = payload["targets"][tgt]["conservative_decision_fields"]
        assert cdf["supports_product_ranking_change"] is False
        assert cdf["supports_validation_claim"] is False


def test_refuses_non_emerging_family(tmp_path: Path) -> None:
    fc = _FakeConn(run_row=_run_row(), score_rows=[])
    p = tmp_path / "lab.json"
    p.write_text(json.dumps({"dataset_version": "t", "rows": []}), encoding="utf-8")
    with pytest.raises(MLTinyBaselineRollupError, match="only family"):
        build_ml_tiny_baseline_rollup_payload(fc, label_dataset_path=p, ranking_run_id="rank-x", family="bridge")


def test_module_sql_is_read_only() -> None:
    import pipeline.ml_tiny_baseline_rollup as m

    src = Path(m.__file__).read_text(encoding="utf-8").upper()
    for bad in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP "):
        assert bad not in src


def test_markdown_contains_caveats(tmp_path: Path) -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=i % 2 == 0) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps({"dataset_version": "t", "rows": rows}), encoding="utf-8")
    payload = build_ml_tiny_baseline_rollup_payload(
        fc, label_dataset_path=p, ranking_run_id="rank-x", family="emerging"
    )
    md = markdown_from_ml_tiny_baseline_rollup(payload)
    for c in ROLLUP_CAVEATS:
        assert c in md


def test_stratified_indices_match_tiny_baseline_helper() -> None:
    row_ids = [f"r{i:02d}" for i in range(20)]
    y = [1] * 10 + [0] * 10
    rows = [
        {"row_id": row_ids[i], "good_or_acceptable": bool(y[i]), "surprising_or_useful": True}
        for i in range(20)
    ]
    y2, f2, n_folds, rid2 = prepare_stratified_cv_fold_tests(rows, target="good_or_acceptable")
    f1 = stratified_round_robin_fold_test_indices(rid2, y2, n_folds)
    assert y == y2
    assert f1 == f2


def test_cli_requires_ranking_run_id(tmp_path: Path) -> None:
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-tiny-baseline-rollup",
        "--label-dataset",
        str(tmp_path / "nope.json"),
        "--ranking-run-id",
        "  ",
        "--family",
        "emerging",
        "--output",
        str(tmp_path / "out.json"),
    ]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            cli_main.main()
