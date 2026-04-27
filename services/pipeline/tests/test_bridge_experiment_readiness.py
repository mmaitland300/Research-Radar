"""Tests for bridge experiment readiness (rollup + paper_scores top-k overlap)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.cli as cli_main
from pipeline.bridge_experiment_readiness import (
    BridgeExperimentReadinessError,
    build_bridge_experiment_readiness_payload,
    compute_readiness_flags,
    extract_label_metrics_from_rollup,
    markdown_from_readiness,
    top_k_work_ids_sql_fragment,
)


def _minimal_rollup(*, run_id: str = "rank-3904fec89d") -> dict:
    return {
        "provenance": {
            "ranking_run_id": run_id,
            "ranking_version": "rv-1",
            "corpus_snapshot_version": "snap-1",
            "embedding_version": "emb-1",
            "cluster_version": "cl-1",
        },
        "per_family": {
            "bridge": {
                "metrics": {
                    "precision_at_k_good_only": 0.9,
                    "precision_at_k_good_or_acceptable": 1.0,
                    "bridge_like_yes_or_partial_share": 1.0,
                }
            },
            "emerging": {"metrics": {"precision_at_k_good_only": 1.0}},
            "undercited": {"metrics": {"precision_at_k_good_only": 0.7}},
        },
    }


class _FakeResult:
    def __init__(self, conn: "_FakeConn", query: str, params: tuple | None) -> None:
        self._conn = conn
        self._query = query
        self._params = params

    def fetchone(self) -> dict | None:
        if self._params is None:
            return None
        if "FROM ranking_runs" in self._query:
            return self._conn.run_row
        return None

    def fetchall(self) -> list[dict]:
        if "FROM paper_scores" not in self._query or self._params is None:
            return []
        _rid, family, k = self._params
        elig = "bridge_eligible IS TRUE" in self._query
        key = (family, elig)
        ids = self._conn.topk_map.get(key, [])
        return [{"work_id": wid} for wid in ids[: int(k)]]


class _FakeConn:
    def __init__(
        self,
        *,
        run_row: dict,
        topk_map: dict[tuple[str, bool], list[int]],
    ) -> None:
        self.run_row = run_row
        self.topk_map = topk_map

    def execute(self, query: str, params: tuple | None = None) -> _FakeResult:
        return _FakeResult(self, query, params)


def _default_run_row() -> dict:
    return {
        "ranking_run_id": "rank-3904fec89d",
        "ranking_version": "rv-1",
        "corpus_snapshot_version": "snap-1",
        "embedding_version": "emb-1",
        "config_json": {"clustering_artifact": {"cluster_version": "cl-1"}},
        "status": "succeeded",
    }


def test_top_k_sql_uses_paper_scores_ranking_run_id_and_ordering() -> None:
    body = top_k_work_ids_sql_fragment(bridge_eligible_true_only=False)
    assert "FROM paper_scores ps" in body
    assert "ps.ranking_run_id = %s" in body
    assert "ORDER BY ps.final_score DESC, ps.work_id ASC" in body


def test_top_k_sql_eligible_only_includes_bridge_eligible_true() -> None:
    body = top_k_work_ids_sql_fragment(bridge_eligible_true_only=True)
    assert "ps.bridge_eligible IS TRUE" in body


def test_provenance_mismatch_rollup_vs_db_fails() -> None:
    rollup = _minimal_rollup()
    run_row = _default_run_row()
    run_row["ranking_version"] = "other-version"
    conn = _FakeConn(run_row=run_row, topk_map={})
    with pytest.raises(BridgeExperimentReadinessError, match="Provenance mismatch"):
        build_bridge_experiment_readiness_payload(conn, rollup=rollup, ranking_run_id="rank-3904fec89d", k=3)


def test_build_payload_happy_path_overlap_and_readiness() -> None:
    rollup = _minimal_rollup()
    # full bridge {1,2,3}, eligible {3,4,5} -> differs from full; emerging {3,10,11}
    # full vs emerging: {3} -> jaccard 1/5 = 0.2
    # eligible vs emerging: {3} -> jaccard 1/5 = 0.2 -> delta 0 -> not materially lower
    topk = {
        ("bridge", False): [1, 2, 3],
        ("bridge", True): [3, 4, 5],
        ("emerging", False): [3, 10, 11],
        ("undercited", False): [99, 100, 101],
    }
    conn = _FakeConn(run_row=_default_run_row(), topk_map=topk)
    p = build_bridge_experiment_readiness_payload(conn, rollup=rollup, ranking_run_id="rank-3904fec89d", k=3)
    assert p["overlaps"]["full_bridge_vs_emerging"]["jaccard"] == round(1 / 5, 6)
    assert p["overlap_thresholds"]["emerging_overlap_delta"] == 0.0
    assert p["overlap_thresholds"]["materially_lower_emerging_overlap"] is False
    assert p["readiness"]["label_quality_ready"] is True
    assert p["readiness"]["distinctness_ready"] is False


def test_materially_lower_emerging_overlap_when_delta_ge_point_one() -> None:
    rollup = _minimal_rollup()
    # full bridge vs emerging jaccard = 4/4 = 1.0 (same set {1,2,3,4})
    # eligible vs emerging: intersection {1,2}, union 6 -> 2/6
    topk = {
        ("bridge", False): [1, 2, 3, 4],
        ("bridge", True): [1, 2, 5, 6],
        ("emerging", False): [1, 2, 3, 4],
        ("undercited", False): [8, 9, 10, 11],
    }
    conn = _FakeConn(run_row=_default_run_row(), topk_map=topk)
    p = build_bridge_experiment_readiness_payload(conn, rollup=rollup, ranking_run_id="rank-3904fec89d", k=4)
    assert p["overlaps"]["full_bridge_vs_emerging"]["jaccard"] == 1.0
    assert p["overlaps"]["eligible_only_bridge_vs_emerging"]["jaccard"] == round(2 / 6, 6)
    delta = p["overlap_thresholds"]["emerging_overlap_delta"]
    assert delta >= 0.10
    assert p["overlap_thresholds"]["materially_lower_emerging_overlap"] is True
    assert p["overlap_thresholds"]["eligible_only_bridge_differs_from_full_bridge"] is True
    assert p["readiness"]["distinctness_ready"] is True
    assert p["readiness"]["ready_for_small_bridge_weight_experiment"] is True
    assert p["readiness"]["suggested_next_step"] == (
        "Candidate for a small gated bridge-weight experiment; not validation."
    )


def test_materially_lower_false_when_delta_below_point_one() -> None:
    flags = compute_readiness_flags(
        label_metrics={
            "bridge_good_or_acceptable_precision": 1.0,
            "bridge_like_yes_or_partial_share": 1.0,
        },
        full_bridge_top_k=[1, 2],
        eligible_only_bridge_top_k=[3, 4],
        full_vs_emerging_jaccard=0.5,
        eligible_vs_emerging_jaccard=0.41,
    )
    assert flags["emerging_overlap_delta"] == pytest.approx(0.09)
    assert flags["materially_lower_emerging_overlap"] is False


def test_label_quality_ready_false_when_bridge_good_or_acceptable_low() -> None:
    flags = compute_readiness_flags(
        label_metrics={
            "bridge_good_or_acceptable_precision": 0.79,
            "bridge_like_yes_or_partial_share": 1.0,
        },
        full_bridge_top_k=[1],
        eligible_only_bridge_top_k=[2],
        full_vs_emerging_jaccard=1.0,
        eligible_vs_emerging_jaccard=0.0,
    )
    assert flags["label_quality_ready"] is False


def test_label_quality_ready_false_when_bridge_like_share_low() -> None:
    flags = compute_readiness_flags(
        label_metrics={
            "bridge_good_or_acceptable_precision": 1.0,
            "bridge_like_yes_or_partial_share": 0.49,
        },
        full_bridge_top_k=[1],
        eligible_only_bridge_top_k=[2],
        full_vs_emerging_jaccard=1.0,
        eligible_vs_emerging_jaccard=0.0,
    )
    assert flags["label_quality_ready"] is False


def test_distinctness_ready_false_when_eligible_same_as_full() -> None:
    flags = compute_readiness_flags(
        label_metrics={
            "bridge_good_or_acceptable_precision": 1.0,
            "bridge_like_yes_or_partial_share": 1.0,
        },
        full_bridge_top_k=[1, 2, 3],
        eligible_only_bridge_top_k=[1, 2, 3],
        full_vs_emerging_jaccard=0.9,
        eligible_vs_emerging_jaccard=0.2,
    )
    assert flags["eligible_only_bridge_differs_from_full_bridge"] is False
    assert flags["distinctness_ready"] is False


def test_high_label_quality_high_emerging_overlap_blocks_experiment() -> None:
    """Strong rollup metrics but no emerging-overlap improvement -> no weight experiment."""
    rollup = _minimal_rollup()
    topk = {
        ("bridge", False): [1, 2, 3],
        ("bridge", True): [3, 4, 5],
        ("emerging", False): [3, 10, 11],
        ("undercited", False): [20, 21, 22],
    }
    conn = _FakeConn(run_row=_default_run_row(), topk_map=topk)
    p = build_bridge_experiment_readiness_payload(conn, rollup=rollup, ranking_run_id="rank-3904fec89d", k=3)
    assert p["readiness"]["label_quality_ready"] is True
    assert p["readiness"]["ready_for_small_bridge_weight_experiment"] is False
    assert (
        p["readiness"]["suggested_next_step"]
        == "Bridge labels are promising, but distinctness is not yet strong enough for a weight experiment."
    )


def test_markdown_does_not_claim_validation() -> None:
    rollup = _minimal_rollup()
    topk = {
        ("bridge", False): [1],
        ("bridge", True): [2],
        ("emerging", False): [3],
        ("undercited", False): [4],
    }
    conn = _FakeConn(run_row=_default_run_row(), topk_map=topk)
    p = build_bridge_experiment_readiness_payload(conn, rollup=rollup, ranking_run_id="rank-3904fec89d", k=1)
    md = markdown_from_readiness(p)
    lowered = md.lower()
    assert "operational threshold" in lowered
    assert "ml ranking is better" in lowered  # appears in the negated disclaimer sentence
    assert "bridge is validated" not in lowered


def test_extract_label_metrics_from_rollup() -> None:
    m = extract_label_metrics_from_rollup(_minimal_rollup())
    assert m["bridge_good_only_precision"] == 0.9
    assert m["emerging_good_only_precision"] == 1.0


def test_cli_bridge_experiment_readiness_writes_outputs(tmp_path: Path) -> None:
    rollup_path = tmp_path / "rollup.json"
    rollup_path.write_text(json.dumps(_minimal_rollup()), encoding="utf-8")
    out = tmp_path / "ready.json"
    md = tmp_path / "ready.md"

    topk = {
        ("bridge", False): [1, 2, 3],
        ("bridge", True): [3, 4, 5],
        ("emerging", False): [3, 10, 11],
        ("undercited", False): [20, 21, 22],
    }
    fake_conn = _FakeConn(run_row=_default_run_row(), topk_map=topk)
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = fake_conn
    mock_cm.__exit__.return_value = None

    with patch("pipeline.bridge_experiment_readiness.psycopg.connect", return_value=mock_cm):
        with patch.object(
            sys,
            "argv",
            [
                "pipeline.cli",
                "bridge-experiment-readiness",
                "--rollup",
                str(rollup_path),
                "--ranking-run-id",
                "rank-3904fec89d",
                "--k",
                "3",
                "--output",
                str(out),
                "--markdown-output",
                str(md),
            ],
        ):
            cli_main.main()

    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["provenance"]["ranking_run_id"] == "rank-3904fec89d"
    assert "paper_scores" not in json.dumps(data)  # artifact is IDs + metrics, not SQL
    assert md.is_file()
    md_text = md.read_text(encoding="utf-8")
    assert "operational threshold" in md_text.lower()
