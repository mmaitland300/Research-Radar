"""Tests for ml-tiny-baseline-disagreement audit."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_tiny_baseline_disagreement import (
    DISAGREEMENT_CAVEATS,
    MLTinyBaselineDisagreementError,
    _build_target_audit,
    build_ml_tiny_baseline_disagreement_payload,
    markdown_from_ml_tiny_baseline_disagreement,
    ordinal_rank_descending,
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
        "title": f"Title {wid}",
        "good_or_acceptable": goa,
        "surprising_or_useful": sou,
        "bridge_like_yes_or_partial": None,
    }


def test_ordinal_rank_descending_ties() -> None:
    scores = [0.9, 0.9, 0.5]
    keys = ["b", "a", "c"]
    r = ordinal_rank_descending(scores, keys)
    assert r[1] == 1
    assert r[0] == 2
    assert r[2] == 3


def test_build_target_audit_promotion_and_demotion() -> None:
    rows = []
    for i in range(26):
        rows.append(
            {
                "row_id": f"r{i}",
                "work_id": f"w{i:04d}",
                "paper_id": "",
                "title": f"T{i}",
                "final_score": 1.0 - i * 0.01,
            }
        )
    for i, r in enumerate(rows):
        r["good_or_acceptable"] = i < 13
        r["surprising_or_useful"] = True
    learn = [float(rows[i]["final_score"]) for i in range(26)]
    learn[25] = 100.0
    learn[0] = -100.0
    audit = _build_target_audit(rows, target="good_or_acceptable", oof_logits=learn, top_n=5)
    assert audit["promoted_count"] >= 1
    assert audit["demoted_count"] >= 1
    assert any(r["work_id"] == "w0025" for r in audit["top_promotions_by_abs_rank_delta"])
    assert any(r["work_id"] == "w0000" for r in audit["top_demotions_by_abs_rank_delta"])


def test_oof_logits_cover_all_rows(tmp_path: Path) -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=i % 2 == 0) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps({"dataset_version": "t", "rows": rows}), encoding="utf-8")
    payload = build_ml_tiny_baseline_disagreement_payload(
        fc,
        label_dataset_path=p,
        ranking_run_id="rank-x",
        family="emerging",
        targets=("good_or_acceptable",),
        top_n=5,
    )
    g = payload["targets"]["good_or_acceptable"]
    assert g["n_rows"] == 26
    assert len(g["all_rows"]) == 26


def test_dual_targets_in_one_payload(tmp_path: Path) -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=i % 2 == 0) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps({"dataset_version": "t", "rows": rows}), encoding="utf-8")
    payload = build_ml_tiny_baseline_disagreement_payload(
        fc,
        label_dataset_path=p,
        ranking_run_id="rank-x",
        family="emerging",
        targets=("good_or_acceptable", "surprising_or_useful"),
        top_n=3,
    )
    assert set(payload["targets"].keys()) == {"good_or_acceptable", "surprising_or_useful"}


def test_refuses_non_emerging(tmp_path: Path) -> None:
    fc = _FakeConn(run_row=_run_row(), score_rows=[])
    p = tmp_path / "lab.json"
    p.write_text(json.dumps({"dataset_version": "t", "rows": []}), encoding="utf-8")
    with pytest.raises(MLTinyBaselineDisagreementError, match="only family"):
        build_ml_tiny_baseline_disagreement_payload(
            fc,
            label_dataset_path=p,
            ranking_run_id="rank-x",
            family="bridge",
            targets=("good_or_acceptable",),
            top_n=5,
        )


def test_markdown_has_caveats(tmp_path: Path) -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=i % 2 == 0) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps({"dataset_version": "t", "rows": rows}), encoding="utf-8")
    payload = build_ml_tiny_baseline_disagreement_payload(
        fc,
        label_dataset_path=p,
        ranking_run_id="rank-x",
        family="emerging",
        targets=("good_or_acceptable", "surprising_or_useful"),
        top_n=3,
    )
    md = markdown_from_ml_tiny_baseline_disagreement(payload)
    for c in DISAGREEMENT_CAVEATS:
        assert c in md


def test_module_sql_is_read_only() -> None:
    import pipeline.ml_tiny_baseline_disagreement as m

    src = Path(m.__file__).read_text(encoding="utf-8").upper()
    for bad in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP "):
        assert bad not in src


def test_cli_requires_target_or_all(tmp_path: Path) -> None:
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-tiny-baseline-disagreement",
        "--label-dataset",
        str(tmp_path / "nope.json"),
        "--ranking-run-id",
        "rank-x",
        "--family",
        "emerging",
        "--output",
        str(tmp_path / "out.json"),
    ]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            cli_main.main()
