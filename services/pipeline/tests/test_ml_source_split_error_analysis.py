"""Tests for ml-source-split-error-analysis (frozen source-split blind audit)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ml_offline_baseline_eval import roc_auc_mann_whitney
from pipeline.ml_source_split_error_analysis import (
    ERROR_ANALYSIS_CAVEATS,
    MLSourceSplitErrorAnalysisError,
    build_ml_source_split_error_analysis_payload,
    markdown_from_ml_source_split_error_analysis,
    ordinal_rank_descending,
    write_ml_source_split_error_analysis,
)
from pipeline.ml_tiny_baseline import FEATURE_NAMES


class _FakeCur:
    def __init__(self, parent: "_FakeConn") -> None:
        self._p = parent
        self._sql = ""

    def execute(self, query: str, params: tuple | None = None) -> "_FakeCur":
        self._sql = query
        self._p.executed_sql.append(query)
        self._p.executed_params.append(params)
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
    def __init__(self, *, score_rows: list[dict]) -> None:
        self.run_row = {
            "ranking_run_id": "rank-x",
            "ranking_version": "rv",
            "corpus_snapshot_version": "snap",
            "embedding_version": "emb",
            "config_json": {},
            "status": "succeeded",
        }
        self.score_rows = score_rows
        self.executed_sql: list[str] = []
        self.executed_params: list[tuple | None] = []

    def cursor(self, row_factory: object | None = None) -> _FakeCurCtx:
        return _FakeCurCtx(self)


def _score(wid: int, *, final_score: float, semantic_score: float) -> dict:
    return {
        "work_id": wid,
        "recommendation_family": "emerging",
        "semantic_score": semantic_score,
        "citation_velocity_score": 0.0,
        "topic_growth_score": 0.0,
        "bridge_score": 99.0,
        "diversity_penalty": 0.0,
        "final_score": final_score,
        "openalex_id": f"https://openalex.org/W{wid}",
    }


def _blind_row(row_id: str, wid: int, *, goa: bool, sou: bool, note: str) -> dict:
    return {
        "split": "audit_only",
        "ranking_run_id": "rank-x",
        "row_id": row_id,
        "family": None,
        "review_pool_variant": "ml_blind_snapshot_audit",
        "work_id": f"W{wid}",
        "internal_work_id": str(wid),
        "paper_id": f"https://openalex.org/W{wid}",
        "openalex_work_id": f"W{wid}",
        "title": f"Title {wid}",
        "relevance_label": "good" if goa else "miss",
        "novelty_label": "useful" if sou else "obvious",
        "bridge_like_label": "partial",
        "good_or_acceptable": goa,
        "surprising_or_useful": sou,
        "reviewer_notes": note,
        "source_worksheet_path": "docs/audit/manual-review/blind.csv",
        "sample_reason": "seeded",
        "cluster_id": "c001",
        "topics": "topic-a",
    }


def _rows() -> list[dict]:
    return [
        _blind_row("a", 1, goa=True, sou=False, note="Strong baseline but demoted by learned semantic signal"),
        _blind_row("b", 2, goa=False, sou=True, note="Weak fit despite shiny semantic score"),
        _blind_row("c", 3, goa=True, sou=True, note="Useful and promoted"),
        _blind_row("d", 4, goa=True, sou=False, note="Plain but acceptable"),
        _blind_row("train-shaped", 5, goa=True, sou=True, note="must not appear") | {
            "family": "emerging",
            "review_pool_variant": "full_family_top_k",
        },
        _blind_row("other-run", 6, goa=True, sou=True, note="wrong run") | {
            "ranking_run_id": "rank-other",
        },
    ]


def _scores() -> list[dict]:
    return [
        _score(1, final_score=0.9, semantic_score=0.1),
        _score(2, final_score=0.8, semantic_score=0.9),
        _score(3, final_score=0.7, semantic_score=0.8),
        _score(4, final_score=0.6, semantic_score=0.0),
        _score(5, final_score=0.5, semantic_score=1.0),
        _score(6, final_score=0.4, semantic_score=1.0),
    ]


def _source_artifact(label_sha: str) -> dict:
    preprocessing = {
        "fit_on": "train rows only",
        "feature_names": list(FEATURE_NAMES),
        "medians": [0.0] * len(FEATURE_NAMES),
        "means": [0.0] * len(FEATURE_NAMES),
        "stds": [1.0] * len(FEATURE_NAMES),
    }
    weights = {name: 0.0 for name in FEATURE_NAMES}
    weights["semantic_score"] = 1.0

    feature_by_wid = {r["work_id"]: r for r in _scores()}
    logits: list[float] = []
    finals: list[float] = []
    goa: list[bool] = []
    sou: list[bool] = []
    for row in _rows()[:4]:
        wid = int(str(row["internal_work_id"]))
        sc = feature_by_wid[wid]
        logits.append(float(sc["semantic_score"]))
        finals.append(float(sc["final_score"]))
        goa.append(bool(row["good_or_acceptable"]))
        sou.append(bool(row["surprising_or_useful"]))

    def _target(labels: list[bool]) -> dict:
        learned_auc = roc_auc_mann_whitney(list(zip(logits, labels, strict=True)))
        heuristic_auc = roc_auc_mann_whitney(list(zip(finals, labels, strict=True)))
        return {
            "preprocessing": preprocessing,
            "learned_model": {
                "trained": True,
                "coefficients_standardized_space": {
                    "intercept": 0.0,
                    "weights": weights,
                },
            },
            "blind_test_metrics": {
                "learned_model": {"roc_auc_mann_whitney": learned_auc},
                "heuristic_final_score": {"roc_auc_mann_whitney": heuristic_auc},
            },
        }

    return {
        "artifact_type": "ml_source_split_tiny_baseline",
        "provenance": {
            "ranking_run_id": "rank-x",
            "family_context": "emerging",
            "score_family_for_blind_rows": "emerging",
            "label_dataset_sha256": label_sha,
            "conflict_policy_path": "docs/audit/ml-label-conflict-policy.md",
            "conflict_policy_sha256": "abc",
        },
        "targets": {
            "good_or_acceptable": _target(goa),
            "surprising_or_useful": _target(sou),
        },
    }


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    labels = tmp_path / "labels.json"
    labels.write_text(json.dumps({"dataset_version": "test", "rows": _rows()}), encoding="utf-8")
    import hashlib

    sha = hashlib.sha256(labels.read_bytes()).hexdigest()
    source = tmp_path / "source.json"
    source.write_text(json.dumps(_source_artifact(sha)), encoding="utf-8")
    return labels, source


def _payload(tmp_path: Path) -> tuple[dict, _FakeConn, Path, Path]:
    labels, source = _write_inputs(tmp_path)
    conn = _FakeConn(score_rows=_scores())
    payload = build_ml_source_split_error_analysis_payload(
        conn,
        label_dataset_path=labels,
        source_split_artifact_path=source,
        ranking_run_id="rank-x",
        family="emerging",
        top_n=10,
    )
    return payload, conn, labels, source


def test_blind_only_selection(tmp_path: Path) -> None:
    payload, _conn, _labels, _source = _payload(tmp_path)
    assert payload["feature_join_summary"]["blind_selected_row_count"] == 4
    assert payload["targets"]["good_or_acceptable"]["blind_boolean_row_count"] == 4
    ids = {r["row_id"] for r in payload["targets"]["good_or_acceptable"]["all_detail_rows"]}
    assert ids == {"a", "b", "c", "d"}


def test_rank_delta_sign_and_tie_break() -> None:
    rows = [
        {"_analysis_key": "z", "score": 1.0, "row_id": "z", "paper_id": "p2"},
        {"_analysis_key": "a", "score": 1.0, "row_id": "a", "paper_id": "p1"},
        {"_analysis_key": "b", "score": 0.5, "row_id": "b", "paper_id": "p0"},
    ]
    ranks = ordinal_rank_descending(rows, "score")
    assert ranks == {"a": 1, "z": 2, "b": 3}


def test_promoted_negative_and_demoted_positive_buckets(tmp_path: Path) -> None:
    payload, _conn, _labels, _source = _payload(tmp_path)
    goa = payload["targets"]["good_or_acceptable"]
    assert goa["promoted_negatives"][0]["row_id"] == "b"
    assert goa["promoted_negatives"][0]["rank_delta"] > 0
    assert goa["demoted_positives"][0]["row_id"] == "a"
    assert goa["demoted_positives"][0]["rank_delta"] < 0


def test_detail_rows_include_reviewer_notes_and_source_path(tmp_path: Path) -> None:
    payload, _conn, _labels, _source = _payload(tmp_path)
    row = payload["targets"]["good_or_acceptable"]["all_detail_rows"][0]
    assert row["reviewer_notes"]
    assert row["source_worksheet_path"] == "docs/audit/manual-review/blind.csv"


def test_openalex_identity_fields_not_replaced_by_internal_db_id(tmp_path: Path) -> None:
    payload, _conn, _labels, _source = _payload(tmp_path)
    row = next(r for r in payload["targets"]["good_or_acceptable"]["all_detail_rows"] if r["row_id"] == "a")
    assert row["work_id"] == "W1"
    assert row["internal_work_id"] == "1"
    assert row["openalex_work_id"] == "W1"


def test_provenance_mismatch_fails(tmp_path: Path) -> None:
    labels, source = _write_inputs(tmp_path)
    conn = _FakeConn(score_rows=_scores())
    with pytest.raises(MLSourceSplitErrorAnalysisError, match="ranking_run_id"):
        build_ml_source_split_error_analysis_payload(
            conn,
            label_dataset_path=labels,
            source_split_artifact_path=source,
            ranking_run_id="rank-other",
            family="emerging",
        )
    with pytest.raises(MLSourceSplitErrorAnalysisError, match="family"):
        build_ml_source_split_error_analysis_payload(
            conn,
            label_dataset_path=labels,
            source_split_artifact_path=source,
            ranking_run_id="rank-x",
            family="bridge",
        )


def test_markdown_includes_required_caveats(tmp_path: Path) -> None:
    payload, _conn, _labels, _source = _payload(tmp_path)
    md = markdown_from_ml_source_split_error_analysis(payload)
    for caveat in ERROR_ANALYSIS_CAVEATS:
        assert caveat in md


def test_sql_executed_has_no_writes(tmp_path: Path) -> None:
    payload, conn, labels, source = _payload(tmp_path)
    out_json = tmp_path / "out.json"
    out_md = tmp_path / "out.md"
    write_ml_source_split_error_analysis(
        conn,
        label_dataset_path=labels,
        source_split_artifact_path=source,
        ranking_run_id="rank-x",
        family="emerging",
        json_path=out_json,
        markdown_path=out_md,
    )
    assert json.loads(out_json.read_text(encoding="utf-8"))["artifact_type"] == payload["artifact_type"]
    sql = "\n".join(conn.executed_sql).upper()
    for bad in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE "):
        assert bad not in sql
