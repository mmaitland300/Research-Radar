"""Tests for ml-offline-baseline-eval (read-only join + metrics, no DB writes)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_offline_baseline_eval import (
    CAVEATS,
    MLOfflineBaselineEvalError,
    build_ml_offline_baseline_eval_payload,
    filter_audit_rows_for_run,
    join_label_row_to_score,
    load_label_dataset,
    markdown_from_ml_offline_baseline_eval,
    pairwise_accuracy,
    roc_auc_mann_whitney,
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


def _minimal_label_payload(*rows: dict) -> dict:
    return {"dataset_version": "ml-label-dataset-v1", "rows": list(rows)}


def test_load_label_dataset(tmp_path: Path) -> None:
    p = tmp_path / "labels.json"
    p.write_text(json.dumps({"rows": []}), encoding="utf-8")
    d = load_label_dataset(p)
    assert d["rows"] == []


def test_filter_audit_rows_for_run_and_row_id_dedupe() -> None:
    rows = [
        {"split": "audit_only", "ranking_run_id": "rank-a", "row_id": "r1", "family": "bridge"},
        {"split": "audit_only", "ranking_run_id": "rank-b", "row_id": "r2", "family": "bridge"},
        {"split": "other", "ranking_run_id": "rank-a", "row_id": "r3", "family": "bridge"},
        {"split": "audit_only", "ranking_run_id": "rank-a", "row_id": "r1", "family": "bridge"},
    ]
    out, dup = filter_audit_rows_for_run(_minimal_label_payload(*rows), ranking_run_id="rank-a")
    assert len(out) == 1 and dup == 1


def test_join_by_work_id_and_openalex() -> None:
    scores = [
        {
            "work_id": 10,
            "recommendation_family": "bridge",
            "semantic_score": 0.1,
            "citation_velocity_score": 0.2,
            "topic_growth_score": 0.3,
            "bridge_score": 0.4,
            "diversity_penalty": 0.05,
            "final_score": 0.9,
            "openalex_id": "https://openalex.org/W99",
            "_rank": 1,
        }
    ]
    by_work: dict[tuple[str, int], dict] = {}
    by_wt: dict[tuple[str, str], dict] = {}
    for s in scores:
        by_work[(str(s["recommendation_family"]), int(s["work_id"]))] = s
        by_wt[("bridge", "W99")] = s
    lab1 = {"family": "bridge", "work_id": "10", "paper_id": "https://openalex.org/Wx"}
    assert join_label_row_to_score(lab1, by_work, by_wt)["final_score"] == 0.9
    lab2 = {"family": "bridge", "work_id": "W99", "paper_id": "https://openalex.org/W99"}
    assert join_label_row_to_score(lab2, by_work, by_wt)["work_id"] == 10


def test_null_targets_excluded_from_denominators_in_metrics() -> None:
    """Rows with target null do not inflate labeled_row_count for that target."""
    joined = [
        {
            "family": "bridge",
            "good_or_acceptable": True,
            "surprising_or_useful": None,
            "_joined_score": True,
            "final_score": 0.5,
            "_rank": 1,
        }
    ]
    from pipeline.ml_offline_baseline_eval import compute_family_target_metrics

    m = compute_family_target_metrics(joined)
    assert m["bridge"]["surprising_or_useful"]["labeled_row_count"] == 0
    assert m["bridge"]["good_or_acceptable"]["labeled_row_count"] == 1


def test_missing_ranking_reported() -> None:
    joined = [
        {
            "family": "bridge",
            "good_or_acceptable": True,
            "_joined_score": False,
            "final_score": None,
            "_rank": None,
        }
    ]
    from pipeline.ml_offline_baseline_eval import compute_family_target_metrics

    m = compute_family_target_metrics(joined)
    assert m["bridge"]["good_or_acceptable"]["missing_from_ranking_count"] == 1
    assert m["bridge"]["good_or_acceptable"]["matched_to_ranking_count"] == 0


def test_duplicate_observations_preserved_different_row_id(tmp_path: Path) -> None:
    run_row = {
        "ranking_run_id": "rank-x",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {"clustering_artifact": {"cluster_version": "cv1"}},
        "status": "succeeded",
    }
    score_rows = [
        {
            "work_id": 1,
            "recommendation_family": "bridge",
            "semantic_score": 0.1,
            "citation_velocity_score": 0.2,
            "topic_growth_score": 0.3,
            "bridge_score": 0.4,
            "diversity_penalty": 0.0,
            "final_score": 0.8,
            "openalex_id": "https://openalex.org/W1",
        },
    ]
    labels = _minimal_label_payload(
        {
            "split": "audit_only",
            "ranking_run_id": "rank-x",
            "row_id": "a",
            "family": "bridge",
            "work_id": "1",
            "paper_id": "https://openalex.org/W1",
            "good_or_acceptable": True,
            "surprising_or_useful": True,
            "bridge_like_yes_or_partial": True,
        },
        {
            "split": "audit_only",
            "ranking_run_id": "rank-x",
            "row_id": "b",
            "family": "bridge",
            "work_id": "1",
            "paper_id": "https://openalex.org/W1",
            "good_or_acceptable": False,
            "surprising_or_useful": False,
            "bridge_like_yes_or_partial": False,
        },
    )
    fc = _FakeConn(run_row=run_row, score_rows=score_rows)
    p = tmp_path / "labels.json"
    p.write_text(json.dumps(labels), encoding="utf-8")
    payload = build_ml_offline_baseline_eval_payload(fc, label_dataset_path=p, ranking_run_id="rank-x")
    assert len(payload["joined_rows"]) == 2


def test_auc_and_pairwise_deterministic() -> None:
    # negatives low score, positives high score -> perfect separation
    pts = [(1.0, False), (2.0, False), (3.0, True), (4.0, True)]
    assert roc_auc_mann_whitney(pts) == pytest.approx(1.0)
    assert pairwise_accuracy(pts) == pytest.approx(1.0)
    # surprising vs useful both True -> pairwise among positives only for neg set empty
    assert roc_auc_mann_whitney([(1.0, True), (2.0, True)]) is None


def test_markdown_caveats_present() -> None:
    payload = {
        "provenance": {
            "ranking_run_id": "r",
            "ranking_version": "v",
            "corpus_snapshot_version": "c",
            "embedding_version": "e",
            "cluster_version": "cl",
            "label_dataset_path": "/x.json",
            "label_dataset_version": "v1",
            "label_dataset_sha256": "h",
            "generated_at": "t",
        },
        "join_summary": {
            "label_rows_included": 1,
            "duplicate_row_id_skipped": 0,
            "joined_count": 1,
            "missing_score_join_count": 0,
        },
        "metrics": {"by_family": {"bridge": {}}},
    }
    md = markdown_from_ml_offline_baseline_eval(payload)
    for c in CAVEATS:
        assert c in md
    assert "separate" in md.lower()


def test_build_requires_succeeded_run(tmp_path: Path) -> None:
    bad_run = {
        "ranking_run_id": "rank-x",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {},
        "status": "running",
    }
    fc = _FakeConn(run_row=bad_run, score_rows=[])
    p = tmp_path / "labels.json"
    p.write_text(json.dumps(_minimal_label_payload()), encoding="utf-8")
    with pytest.raises(MLOfflineBaselineEvalError, match="not succeeded"):
        build_ml_offline_baseline_eval_payload(fc, label_dataset_path=p, ranking_run_id="rank-x")


def test_cli_requires_non_empty_ranking_run_id(tmp_path: Path) -> None:
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-baseline-eval",
        "--label-dataset",
        str(tmp_path / "nope.json"),
        "--ranking-run-id",
        "   ",
        "--output",
        str(tmp_path / "out.json"),
    ]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            cli_main.main()


def test_module_sql_is_read_only() -> None:
    """Implementation uses SELECT only (no mutating SQL literals)."""
    import pipeline.ml_offline_baseline_eval as m

    src = Path(m.__file__).read_text(encoding="utf-8").upper()
    for bad in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP "):
        assert bad not in src
