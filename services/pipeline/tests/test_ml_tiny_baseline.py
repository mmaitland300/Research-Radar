"""Tests for ml-tiny-baseline (offline emerging-only CV, read-only DB)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_tiny_baseline import (
    TINY_BASELINE_CAVEATS,
    MLTinyBaselineError,
    build_ml_tiny_baseline_payload,
    collect_joined_emerging_rows,
    markdown_from_ml_tiny_baseline,
    run_stratified_cv_tiny_baseline,
    stratified_round_robin_fold_test_indices,
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


def _emerging_score(wid: int, final_score: float, *, pos: bool) -> dict:
    # bridge_score present but must not be consumed by tiny baseline features
    return {
        "work_id": wid,
        "recommendation_family": "emerging",
        "semantic_score": 0.5 + (0.01 * wid) + (0.2 if pos else -0.1),
        "citation_velocity_score": 0.05 * (wid % 5),
        "topic_growth_score": 0.3 if pos else 0.1,
        "bridge_score": 0.999,
        "diversity_penalty": 0.0,
        "final_score": final_score,
        "openalex_id": f"https://openalex.org/W{wid}",
    }


def _label_row(
    *,
    rid: str,
    row_id: str,
    wid: int,
    goa: bool,
    sou: bool,
) -> dict:
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


def _dataset_version(rows: list[dict]) -> dict:
    return {"dataset_version": "ml-label-dataset-test", "rows": rows}


def test_stratified_round_robin_deterministic() -> None:
    row_ids = ["z", "a", "m", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    y = [1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0]
    f1 = stratified_round_robin_fold_test_indices(row_ids, y, 4)
    f2 = stratified_round_robin_fold_test_indices(row_ids, y, 4)
    assert f1 == f2
    flat = [i for fold in f1 for i in fold]
    assert sorted(flat) == list(range(12))


def test_refuses_insufficient_class_counts() -> None:
    rows = [
        {"row_id": f"r{i}", "good_or_acceptable": i < 9, "final_score": 0.5, "semantic_score": 0.5}
        for i in range(18)
    ]
    for r in rows:
        r["surprising_or_useful"] = r["good_or_acceptable"]
        r["citation_velocity_score"] = 0.0
        r["topic_growth_score"] = 0.0
        r["diversity_penalty"] = 0.0
    with pytest.raises(MLTinyBaselineError, match="insufficient class balance"):
        run_stratified_cv_tiny_baseline(rows, target="good_or_acceptable")


def test_refuses_bridge_like_target_in_build() -> None:
    fc = _FakeConn(run_row=_run_row(), score_rows=[])
    p = Path("unused.json")
    with pytest.raises(MLTinyBaselineError, match="target must be one of"):
        build_ml_tiny_baseline_payload(
            fc,
            label_dataset_path=p,
            ranking_run_id="rank-x",
            family="emerging",
            target="bridge_like_yes_or_partial",
        )


def test_refuses_non_emerging_family_in_payload() -> None:
    fc = _FakeConn(run_row=_run_row(), score_rows=[])
    p = Path("nope-not-used")
    with pytest.raises(MLTinyBaselineError, match="currently supports only family"):
        build_ml_tiny_baseline_payload(
            fc,
            label_dataset_path=p,
            ranking_run_id="rank-x",
            family="bridge",
            target="good_or_acceptable",
        )


def test_collect_respects_explicit_ranking_run_only(tmp_path: Path) -> None:
    scores = [_emerging_score(i, 0.4 + i * 0.01, pos=i <= 15) for i in range(1, 25)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = []
    for i in range(1, 13):
        rows.append(
            _label_row(rid="rank-other", row_id=f"o{i}", wid=i, goa=True, sou=True),
        )
    for i in range(13, 25):
        rows.append(
            _label_row(rid="rank-x", row_id=f"x{i}", wid=i, goa=i % 2 == 0, sou=i % 3 == 0),
        )
    p = tmp_path / "lab.json"
    p.write_text(json.dumps(_dataset_version(rows)), encoding="utf-8")
    joined, meta = collect_joined_emerging_rows(fc, label_dataset_path=p, ranking_run_id="rank-x", target="good_or_acceptable")
    assert len(joined) == 12
    assert meta["missing_score_for_emerging_labeled_rows"] == 0


def test_heuristic_and_learned_same_test_rows_per_fold(tmp_path: Path) -> None:
    scores = [_emerging_score(i, 0.2 + (i % 7) * 0.05, pos=(i <= 15)) for i in range(1, 25)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_label_row(rid="rank-x", row_id=f"r{i:02d}", wid=i, goa=i <= 12, sou=True) for i in range(1, 25)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps(_dataset_version(rows)), encoding="utf-8")
    joined, _ = collect_joined_emerging_rows(fc, label_dataset_path=p, ranking_run_id="rank-x", target="good_or_acceptable")
    cv = run_stratified_cv_tiny_baseline(joined, target="good_or_acceptable")
    for fold in cv["per_fold"]:
        assert fold["learned"]["roc_auc_mann_whitney"] is not None
        assert fold["heuristic_final_score"]["roc_auc_mann_whitney"] is not None
        assert fold["n_test"] == fold["test_positive_count"] + fold["test_negative_count"]


def test_cv_deterministic_twice(tmp_path: Path) -> None:
    scores = [_emerging_score(i, 0.15 + i * 0.02, pos=(i <= 14)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_label_row(rid="rank-x", row_id=f"id{i:03d}", wid=i, goa=i <= 13, sou=i <= 20) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps(_dataset_version(rows)), encoding="utf-8")
    joined1, _ = collect_joined_emerging_rows(fc, label_dataset_path=p, ranking_run_id="rank-x", target="good_or_acceptable")
    a = json.dumps(run_stratified_cv_tiny_baseline(joined1, target="good_or_acceptable"), sort_keys=True)
    joined2, _ = collect_joined_emerging_rows(fc, label_dataset_path=p, ranking_run_id="rank-x", target="good_or_acceptable")
    b = json.dumps(run_stratified_cv_tiny_baseline(joined2, target="good_or_acceptable"), sort_keys=True)
    assert a == b


def test_caveats_in_payload_and_markdown(tmp_path: Path) -> None:
    scores = [_emerging_score(i, 0.1 + i * 0.03, pos=(i <= 14)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_label_row(rid="rank-x", row_id=f"c{i}", wid=i, goa=i <= 13, sou=True) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps(_dataset_version(rows)), encoding="utf-8")
    payload = build_ml_tiny_baseline_payload(
        fc,
        label_dataset_path=p,
        ranking_run_id="rank-x",
        family="emerging",
        target="good_or_acceptable",
    )
    for c in TINY_BASELINE_CAVEATS:
        assert c in payload["caveats"]
    md = markdown_from_ml_tiny_baseline(payload)
    for c in TINY_BASELINE_CAVEATS:
        assert c in md


def test_cli_requires_non_empty_ranking_run_id(tmp_path: Path) -> None:
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-tiny-baseline",
        "--label-dataset",
        str(tmp_path / "nope.json"),
        "--ranking-run-id",
        "   ",
        "--family",
        "emerging",
        "--target",
        "good_or_acceptable",
        "--output",
        str(tmp_path / "out.json"),
    ]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            cli_main.main()


def test_module_sql_is_read_only() -> None:
    import pipeline.ml_tiny_baseline as m

    src = Path(m.__file__).read_text(encoding="utf-8").upper()
    for bad in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP "):
        assert bad not in src


def test_end_to_end_payload(tmp_path: Path) -> None:
    scores = [_emerging_score(i, 0.25 + (i % 10) * 0.02, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_label_row(rid="rank-x", row_id=f"e{i}", wid=i, goa=i <= 15, sou=i % 2 == 0) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps(_dataset_version(rows)), encoding="utf-8")
    payload = build_ml_tiny_baseline_payload(
        fc,
        label_dataset_path=p,
        ranking_run_id="rank-x",
        family="emerging",
        target="surprising_or_useful",
    )
    assert payload["artifact_type"] == "ml_tiny_baseline"
    assert payload["provenance"]["target"] == "surprising_or_useful"
    agg = payload["cv_results"]["aggregate_out_of_fold"]
    assert agg["learned"]["roc_auc_mann_whitney"] is not None
    w = payload["cv_results"]["mean_coefficients_across_folds_standardized_space"]["weights"]
    assert set(w.keys()) == {
        "final_score",
        "semantic_score",
        "citation_velocity_score",
        "topic_growth_score",
        "diversity_penalty",
    }
