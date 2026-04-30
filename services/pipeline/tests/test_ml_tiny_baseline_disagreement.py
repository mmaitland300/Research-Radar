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
        self._p.executed_sql.append(query)
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
        self.executed_sql: list[str] = []

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
        "rank": str(wid),
        "review_pool_variant": "emerging_gap",
        "source_worksheet_path": "docs/audit/manual-review/test_emerging.csv",
        "source_row_number": wid + 1,
        "relevance_label": "good" if goa else "miss",
        "novelty_label": "surprising" if sou else "not_useful",
        "bridge_like_label": "not_applicable",
        "reviewer_notes": f"Reviewer note {wid}",
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
                "work_id": i,
                "paper_id": f"https://openalex.org/W100{i:04d}",
                "title": f"T{i}",
                "final_score": 1.0 - i * 0.01,
                "rank": str(i + 1),
                "_rank": i + 1,
                "review_pool_variant": "emerging_test",
                "source_worksheet_path": "docs/audit/manual-review/test.csv",
                "source_row_number": i + 2,
                "relevance_label": "good" if i < 13 else "miss",
                "novelty_label": "useful",
                "bridge_like_label": "not_applicable",
                "reviewer_notes": f"note {i}",
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
    assert any(r["openalex_work_id"] == "W1000025" for r in audit["top_promotions_by_abs_rank_delta"])
    assert any(r["openalex_work_id"] == "W1000000" for r in audit["top_demotions_by_abs_rank_delta"])
    assert any(r["disagreement_bucket"] == "promoted_negative" for r in audit["top_promotions_by_abs_rank_delta"])
    assert any(r["disagreement_bucket"] == "demoted_positive" for r in audit["top_demotions_by_abs_rank_delta"])


def test_judgment_bucket_assignment() -> None:
    rows = [
        {"row_id": "pos_promoted", "work_id": 1, "paper_id": "https://openalex.org/W1", "final_score": 0.2, "good_or_acceptable": True},
        {"row_id": "neg_promoted", "work_id": 2, "paper_id": "https://openalex.org/W2", "final_score": 0.1, "good_or_acceptable": False},
        {"row_id": "pos_demoted", "work_id": 3, "paper_id": "https://openalex.org/W3", "final_score": 0.9, "good_or_acceptable": True},
        {"row_id": "neg_demoted", "work_id": 4, "paper_id": "https://openalex.org/W4", "final_score": 0.8, "good_or_acceptable": False},
        {"row_id": "pos_stable", "work_id": 5, "paper_id": "https://openalex.org/W5", "final_score": 0.5, "good_or_acceptable": True},
        {"row_id": "neg_stable", "work_id": 6, "paper_id": "https://openalex.org/W6", "final_score": 0.4, "good_or_acceptable": False},
    ]
    audit = _build_target_audit(
        rows,
        target="good_or_acceptable",
        oof_logits=[1.0, 0.95, 0.3, 0.25, 0.5, 0.4],
        top_n=10,
    )
    buckets = {r["row_id"]: r["disagreement_bucket"] for r in audit["all_rows"]}
    assert buckets == {
        "pos_promoted": "promoted_positive",
        "neg_promoted": "promoted_negative",
        "pos_demoted": "demoted_positive",
        "neg_demoted": "demoted_negative",
        "pos_stable": "stable_positive",
        "neg_stable": "stable_negative",
    }
    assert audit["judgment_bucket_counts"] == {
        "promoted_positive": 1,
        "promoted_negative": 1,
        "demoted_positive": 1,
        "demoted_negative": 1,
        "stable_positive": 1,
        "stable_negative": 1,
    }


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


def test_rows_preserve_openalex_id_and_label_context(tmp_path: Path) -> None:
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
    row = payload["targets"]["good_or_acceptable"]["all_rows"][0]
    assert row["openalex_work_id"].startswith("W")
    assert row["openalex_work_url"].startswith("https://openalex.org/W")
    assert "work_id" not in row
    assert isinstance(row["internal_work_id"], int)
    assert row["relevance_label"] in {"good", "miss"}
    assert row["novelty_label"] in {"surprising", "not_useful"}
    assert row["bridge_like_label"] == "not_applicable"
    assert row["reviewer_notes"].startswith("Reviewer note")
    assert row["source_worksheet_path"] == "docs/audit/manual-review/test_emerging.csv"
    assert row["review_pool_variant"] == "emerging_gap"
    assert row["family_rank"] is not None


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


def test_payload_builder_executes_only_read_sql(tmp_path: Path) -> None:
    scores = [_score(i, pos=(i <= 15)) for i in range(1, 27)]
    fc = _FakeConn(run_row=_run_row(), score_rows=scores)
    rows = [_row("rank-x", f"id{i:03d}", i, goa=i <= 13, sou=i % 2 == 0) for i in range(1, 27)]
    p = tmp_path / "lab.json"
    p.write_text(json.dumps({"dataset_version": "t", "rows": rows}), encoding="utf-8")
    build_ml_tiny_baseline_disagreement_payload(
        fc,
        label_dataset_path=p,
        ranking_run_id="rank-x",
        family="emerging",
        targets=("good_or_acceptable",),
        top_n=5,
    )
    assert fc.executed_sql
    for sql in fc.executed_sql:
        normalized = " ".join(sql.split()).upper()
        assert normalized.startswith("SELECT ")
        assert not any(word in normalized for word in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER "))


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
    assert md.isascii()
    assert "rank_delta=" in md
    assert "\u0394rank" not in md
    assert "\u00ce\u201drank" not in md
    assert "\u2014" not in md
    assert "relevance=" in md
    assert "novelty=" in md
    assert "bridge_like=" in md
    assert "Reviewer note" in md
    assert "`1` rank_delta=" not in md
    assert "- `W" in md


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
