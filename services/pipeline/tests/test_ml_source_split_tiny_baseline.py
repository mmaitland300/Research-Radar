"""Tests for ml-source-split-tiny-baseline (source split, read-only DB)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pipeline.ml_offline_baseline_eval import _build_score_lookups
from pipeline.ml_source_split_tiny_baseline import (
    SOURCE_SPLIT_CAVEATS,
    _join_source_split_rows,
    build_ml_source_split_tiny_baseline_payload,
    markdown_from_ml_source_split_tiny_baseline,
    write_ml_source_split_tiny_baseline,
)


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
            "config_json": {"clustering_artifact": {"cluster_version": "cv1"}},
            "status": "succeeded",
        }
        self.score_rows = score_rows
        self.executed_sql: list[str] = []
        self.executed_params: list[tuple | None] = []

    def cursor(self, row_factory: object | None = None) -> _FakeCurCtx:
        return _FakeCurCtx(self)


def _score(wid: int, final_score: float) -> dict:
    return {
        "work_id": wid,
        "recommendation_family": "emerging",
        "semantic_score": final_score + 0.1,
        "citation_velocity_score": final_score + 0.2,
        "topic_growth_score": final_score + 0.3,
        "bridge_score": 999.0,
        "diversity_penalty": 0.0,
        "final_score": final_score,
        "openalex_id": f"https://openalex.org/W{wid}",
    }


def _row(
    *,
    rid: str = "rank-x",
    row_id: str,
    wid: int,
    variant: str,
    family: str | None,
    goa: bool | None,
    sou: bool | None,
) -> dict:
    return {
        "split": "audit_only",
        "ranking_run_id": rid,
        "row_id": row_id,
        "family": family,
        "review_pool_variant": variant,
        "work_id": str(wid),
        "paper_id": f"https://openalex.org/W{wid}",
        "title": f"Title {wid}",
        "good_or_acceptable": goa,
        "surprising_or_useful": sou,
        "bridge_like_yes_or_partial": None,
    }


def _dataset_rows() -> list[dict]:
    return [
        _row(
            row_id="tr-a",
            wid=1,
            variant="full_family_top_k",
            family="emerging",
            goa=True,
            sou=True,
        ),
        _row(
            row_id="tr-b",
            wid=2,
            variant="ml_contrastive_offline_audit",
            family="emerging",
            goa=False,
            sou=False,
        ),
        _row(
            row_id="tr-c",
            wid=3,
            variant="ml_emerging_target_gap_audit:good_or_acceptable",
            family="emerging",
            goa=True,
            sou=True,
        ),
        _row(
            row_id="blind-a",
            wid=101,
            variant="ml_blind_snapshot_audit",
            family=None,
            goa=True,
            sou=False,
        ),
        _row(
            row_id="blind-b",
            wid=102,
            variant="ml_blind_snapshot_audit",
            family=None,
            goa=False,
            sou=True,
        ),
        _row(
            row_id="blind-missing-feature",
            wid=999,
            variant="ml_blind_snapshot_audit",
            family=None,
            goa=True,
            sou=True,
        ),
        _row(
            row_id="wrong-run",
            rid="rank-other",
            wid=4,
            variant="full_family_top_k",
            family="emerging",
            goa=True,
            sou=True,
        ),
        _row(
            row_id="blind-family-not-null",
            wid=103,
            variant="ml_blind_snapshot_audit",
            family="emerging",
            goa=True,
            sou=True,
        ),
    ]


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    labels = tmp_path / "labels.json"
    labels.write_text(json.dumps({"dataset_version": "test-v4", "rows": _dataset_rows()}), encoding="utf-8")
    policy = tmp_path / "policy.md"
    policy.write_text("# Policy\nObservation-level rows only.\n", encoding="utf-8")
    return labels, policy


def _payload(tmp_path: Path) -> tuple[dict, _FakeConn, Path, Path]:
    labels, policy = _write_inputs(tmp_path)
    scores = [_score(1, 0.0), _score(2, 2.0), _score(3, 4.0), _score(101, 1000.0), _score(102, 1001.0)]
    conn = _FakeConn(score_rows=scores)
    payload = build_ml_source_split_tiny_baseline_payload(
        conn,
        label_dataset_path=labels,
        conflict_policy_path=policy,
        ranking_run_id="rank-x",
        family="emerging",
    )
    return payload, conn, labels, policy


def test_train_test_slice_selection_and_ranking_run_filtering(tmp_path: Path) -> None:
    payload, _conn, _labels, _policy = _payload(tmp_path)
    assert payload["row_counts"]["train"]["total"] == 3
    assert payload["row_counts"]["test"]["total"] == 3
    assert payload["row_counts"]["train"]["by_review_pool_variant"] == {
        "full_family_top_k": 1,
        "ml_contrastive_offline_audit": 1,
        "ml_emerging_target_gap_audit:good_or_acceptable": 1,
    }
    assert payload["row_counts"]["test"]["by_review_pool_variant"] == {"ml_blind_snapshot_audit": 3}
    assert payload["row_counts"]["audit_rows_for_ranking_run"] == 7


def test_blind_rows_remain_family_null_and_never_train() -> None:
    rows = _dataset_rows()
    by_work, by_wtoken = _build_score_lookups([_score(1, 0.0), _score(2, 1.0), _score(3, 2.0), _score(101, 3.0)])
    train_all, _train_joined, _train_missing, test_all, test_joined, _test_missing = _join_source_split_rows(
        [r for r in rows if r["ranking_run_id"] == "rank-x"],
        by_work=by_work,
        by_wtoken=by_wtoken,
        family="emerging",
    )
    assert all(r.get("review_pool_variant") != "ml_blind_snapshot_audit" for r in train_all)
    assert all(r.get("family") is None for r in test_all)
    assert all(r.get("family") is None for r in test_joined)
    assert all(r.get("score_family") == "emerging" for r in test_joined)


def test_train_only_preprocessing_stats_ignore_blind_values(tmp_path: Path) -> None:
    payload, _conn, _labels, _policy = _payload(tmp_path)
    stats = payload["targets"]["good_or_acceptable"]["preprocessing"]["by_feature"]["final_score"]
    assert stats["impute_median"] == 2.0
    assert stats["mean_after_imputation"] == 2.0
    assert stats["std_after_imputation"] < 2.0


def test_missing_feature_and_low_coverage_reporting(tmp_path: Path) -> None:
    payload, _conn, _labels, _policy = _payload(tmp_path)
    test_cov = payload["feature_coverage"]["test"]
    assert test_cov["selected_row_count"] == 3
    assert test_cov["joined_feature_row_count"] == 2
    assert test_cov["missing_feature_row_count"] == 1
    assert test_cov["coverage_rate"] == 2 / 3
    assert payload["targets"]["good_or_acceptable"]["excluded_rows"]["test_missing_feature_count"] == 1


def test_conflict_policy_path_and_sha_written_to_json(tmp_path: Path) -> None:
    payload, _conn, _labels, policy = _payload(tmp_path)
    expected_sha = hashlib.sha256(policy.read_bytes()).hexdigest()
    assert payload["provenance"]["conflict_policy_path"].endswith("policy.md")
    assert payload["provenance"]["conflict_policy_sha256"] == expected_sha
    assert payload["conflict_policy_summary"]["observation_level_rows"] is True
    assert payload["conflict_policy_summary"]["blind_rows_test_only"] is True


def test_markdown_includes_required_caveats(tmp_path: Path) -> None:
    payload, _conn, _labels, _policy = _payload(tmp_path)
    md = markdown_from_ml_source_split_tiny_baseline(payload)
    for caveat in SOURCE_SPLIT_CAVEATS:
        assert caveat in md


def test_payload_builder_executes_only_read_sql_and_write_outputs(tmp_path: Path) -> None:
    payload, conn, labels, policy = _payload(tmp_path)
    out_json = tmp_path / "out.json"
    out_md = tmp_path / "out.md"
    write_ml_source_split_tiny_baseline(
        conn,
        label_dataset_path=labels,
        conflict_policy_path=policy,
        ranking_run_id="rank-x",
        family="emerging",
        json_path=out_json,
        markdown_path=out_md,
    )
    assert out_json.is_file()
    assert out_md.is_file()
    assert json.loads(out_json.read_text(encoding="utf-8"))["artifact_type"] == payload["artifact_type"]
    sql = "\n".join(conn.executed_sql).upper()
    for bad in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE "):
        assert bad not in sql
