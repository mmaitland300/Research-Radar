from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app import main
from app.bridge_distinctness_repo import (
    BridgeDistinctnessPayload,
    _top_family_ids,
    cluster_version_from_config,
    compute_decision_support,
    load_bridge_distinctness_report,
    overlap_count_and_jaccard,
)


client = TestClient(main.app)


def test_overlap_count_and_jaccard_deterministic() -> None:
    a = ["W1", "W2", "W3"]
    b = ["W2", "W4"]
    oc, jac = overlap_count_and_jaccard(a, b)
    assert oc == 1
    assert jac == round(1 / 4, 6)
    oc2, jac2 = overlap_count_and_jaccard([], [])
    assert oc2 == 0
    assert jac2 == 1.0


def test_cluster_version_from_config_null_when_missing() -> None:
    assert cluster_version_from_config({}) is None
    assert cluster_version_from_config({"clustering_artifact": {}}) is None
    assert cluster_version_from_config({"clustering_artifact": {"cluster_version": ""}}) is None


def test_cluster_version_from_config_present() -> None:
    assert (
        cluster_version_from_config(
            {"clustering_artifact": {"cluster_version": "kmeans-v1"}}
        )
        == "kmeans-v1"
    )


def test_compute_decision_support_insufficient_signal() -> None:
    ed, el, step = compute_decision_support(
        full_bridge_top_k_ids=["A", "B"],
        eligible_bridge_top_k_ids=["B"],
        emerging_top_k_ids=["C"],
        cluster_version="cv",
        bridge_family_row_count=0,
        bridge_signal_json_present_count=0,
        full_vs_eligible_jaccard=0.5,
    )
    assert step == "insufficient_bridge_signal_coverage"
    assert ed is True


def test_compute_decision_support_eligible_not_distinct() -> None:
    ids = ["x", "y"]
    ed, el, step = compute_decision_support(
        full_bridge_top_k_ids=ids,
        eligible_bridge_top_k_ids=list(ids),
        emerging_top_k_ids=["z"],
        cluster_version="cv",
        bridge_family_row_count=5,
        bridge_signal_json_present_count=5,
        full_vs_eligible_jaccard=1.0,
    )
    assert step == "eligible_filter_not_distinct_enough"
    assert ed is False


def test_compute_decision_support_inspect_cluster_quality_first_cluster_missing() -> None:
    """Distinct eligible head but no cluster_version pin -> inspect before experiments."""
    ed, el, step = compute_decision_support(
        full_bridge_top_k_ids=["a", "b"],
        eligible_bridge_top_k_ids=["b"],
        emerging_top_k_ids=["c"],
        cluster_version=None,
        bridge_family_row_count=5,
        bridge_signal_json_present_count=5,
        full_vs_eligible_jaccard=0.5,
    )
    assert step == "inspect_cluster_quality_first"
    assert ed is True


def test_compute_decision_support_inspect_cluster_quality_first_not_less_emerging_like() -> None:
    """Eligible head differs but Jaccard vs emerging is not strictly below full vs emerging."""
    ed, el, step = compute_decision_support(
        full_bridge_top_k_ids=["F1", "F2"],
        eligible_bridge_top_k_ids=["F1"],
        emerging_top_k_ids=["E1"],
        cluster_version="cv",
        bridge_family_row_count=5,
        bridge_signal_json_present_count=5,
        full_vs_eligible_jaccard=0.5,
    )
    assert el is False
    assert step == "inspect_cluster_quality_first"


def test_compute_decision_support_candidate_for_small_weight_experiment() -> None:
    ed, el, step = compute_decision_support(
        full_bridge_top_k_ids=["A", "B", "C"],
        eligible_bridge_top_k_ids=["D"],
        emerging_top_k_ids=["A", "B"],
        cluster_version="kmeans-v1",
        bridge_family_row_count=5,
        bridge_signal_json_present_count=5,
        full_vs_eligible_jaccard=0.25,
    )
    assert el is True
    assert step == "candidate_for_small_weight_experiment"


def test_openapi_bridge_distinctness_ranking_run_id_required() -> None:
    schema = client.app.openapi()
    path_item = schema["paths"]["/api/v1/evaluation/bridge-distinctness"]["get"]
    params = {p["name"]: p for p in path_item.get("parameters", []) if p.get("in") == "query"}
    assert params["ranking_run_id"].get("required") is True


def test_top_family_ids_rejects_eligible_filter_on_non_bridge_family() -> None:
    conn = MagicMock()
    with pytest.raises(ValueError, match="bridge_eligible_true_only"):
        _top_family_ids(
            conn,
            ranking_run_id="run-1",
            family="emerging",
            k=5,
            bridge_eligible_true_only=True,
        )
    conn.execute.assert_not_called()


def test_bridge_distinctness_ranking_run_id_required() -> None:
    r = client.get("/api/v1/evaluation/bridge-distinctness")
    assert r.status_code == 422
    r2 = client.get("/api/v1/evaluation/bridge-distinctness?ranking_run_id=")
    assert r2.status_code == 422
    r3 = client.get("/api/v1/evaluation/bridge-distinctness?ranking_run_id=%20%20")
    assert r3.status_code == 422


def test_bridge_distinctness_k_bounded_like_evaluation_compare() -> None:
    r = client.get("/api/v1/evaluation/bridge-distinctness?ranking_run_id=run-1&k=0")
    assert r.status_code == 422
    r2 = client.get("/api/v1/evaluation/bridge-distinctness?ranking_run_id=run-1&k=51")
    assert r2.status_code == 422


def test_bridge_distinctness_404_when_no_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        main,
        "load_bridge_distinctness_report",
        MagicMock(return_value=None),
    )
    r = client.get("/api/v1/evaluation/bridge-distinctness?ranking_run_id=missing")
    assert r.status_code == 404


def test_bridge_distinctness_smoke_no_raw_signal_json(monkeypatch) -> None:
    payload = BridgeDistinctnessPayload(
        ranking_run_id="run-pin",
        ranking_version="rv",
        corpus_snapshot_version="snap",
        embedding_version="emb",
        cluster_version=None,
        k=3,
        full_bridge_top_k_ids=["W1", "W2", "W3"],
        eligible_bridge_top_k_ids=["W2", "W3"],
        emerging_top_k_ids=["W9"],
        full_bridge_vs_eligible_bridge_overlap_count=2,
        full_bridge_vs_eligible_bridge_jaccard=2 / 3,
        full_bridge_vs_emerging_overlap_count=0,
        full_bridge_vs_emerging_jaccard=0.0,
        eligible_bridge_vs_emerging_overlap_count=0,
        eligible_bridge_vs_emerging_jaccard=0.0,
        bridge_family_row_count=20,
        bridge_score_nonnull_count=18,
        bridge_score_null_count=2,
        bridge_eligible_true_count=10,
        bridge_eligible_false_count=5,
        bridge_eligible_null_count=5,
        bridge_signal_json_present_count=15,
        bridge_signal_json_missing_count=5,
        eligible_head_differs_from_full=True,
        eligible_head_less_emerging_like_than_full=False,
        suggested_next_step="inspect_cluster_quality_first",
    )
    monkeypatch.setattr(main, "load_bridge_distinctness_report", MagicMock(return_value=payload))
    r = client.get("/api/v1/evaluation/bridge-distinctness?ranking_run_id=run-pin&k=3")
    assert r.status_code == 200
    body = r.json()
    assert body["ranking_run_id"] == "run-pin"
    assert body["k"] == 3
    assert body["full_bridge_top_k_ids"] == ["W1", "W2", "W3"]
    assert body["eligible_bridge_top_k_ids"] == ["W2", "W3"]
    assert body["emerging_top_k_ids"] == ["W9"]
    assert body["bridge_eligible_false_count"] == 5
    assert body["bridge_eligible_null_count"] == 5
    assert body["bridge_signal_json_present_count"] == 15
    assert body["bridge_signal_json_missing_count"] == 5
    assert "bridge_signal_json" not in body
    assert body["decision_support"]["suggested_next_step"] == "inspect_cluster_quality_first"
    main.load_bridge_distinctness_report.assert_called_once()
    call_kw = main.load_bridge_distinctness_report.call_args.kwargs
    assert call_kw["ranking_run_id"] == "run-pin"
    assert call_kw["k"] == 3


def test_bridge_distinctness_strips_ranking_run_id(monkeypatch) -> None:
    payload = BridgeDistinctnessPayload(
        ranking_run_id="r1",
        ranking_version="v",
        corpus_snapshot_version="s",
        embedding_version="e",
        cluster_version=None,
        k=10,
        full_bridge_top_k_ids=[],
        eligible_bridge_top_k_ids=[],
        emerging_top_k_ids=[],
        full_bridge_vs_eligible_bridge_overlap_count=0,
        full_bridge_vs_eligible_bridge_jaccard=1.0,
        full_bridge_vs_emerging_overlap_count=0,
        full_bridge_vs_emerging_jaccard=1.0,
        eligible_bridge_vs_emerging_overlap_count=0,
        eligible_bridge_vs_emerging_jaccard=1.0,
        bridge_family_row_count=1,
        bridge_score_nonnull_count=1,
        bridge_score_null_count=0,
        bridge_eligible_true_count=1,
        bridge_eligible_false_count=0,
        bridge_eligible_null_count=0,
        bridge_signal_json_present_count=1,
        bridge_signal_json_missing_count=0,
        eligible_head_differs_from_full=False,
        eligible_head_less_emerging_like_than_full=False,
        suggested_next_step="eligible_filter_not_distinct_enough",
    )
    mock_load = MagicMock(return_value=payload)
    monkeypatch.setattr(main, "load_bridge_distinctness_report", mock_load)
    client.get("/api/v1/evaluation/bridge-distinctness?ranking_run_id=%20r1%20")
    assert mock_load.call_args.kwargs["ranking_run_id"] == "r1"


def test_load_bridge_distinctness_same_ranking_run_id_all_queries() -> None:
    """No latest-run fallback; every list and coverage query pins the same ranking_run_id."""

    run_row = {
        "ranking_run_id": "run-xyz",
        "ranking_version": "ver",
        "corpus_snapshot_version": "css",
        "embedding_version": "ev",
        "config_json": {},
        "status": "succeeded",
    }
    cov_row = {
        "bridge_family_row_count": 3,
        "bridge_score_nonnull_count": 3,
        "bridge_score_null_count": 0,
        "bridge_eligible_true_count": 2,
        "bridge_eligible_false_count": 1,
        "bridge_eligible_null_count": 0,
        "bridge_signal_json_present_count": 2,
        "bridge_signal_json_missing_count": 1,
    }

    class _Result:
        def __init__(self, one=None, all_rows=None):
            self._one = one
            self._all = all_rows or []

        def fetchone(self):
            return self._one

        def fetchall(self):
            return list(self._all)

    class _FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple]] = []

        def execute(self, sql: str, params=None):
            self.calls.append((sql, params))
            if "FROM ranking_runs" in sql and "WHERE ranking_run_id" in sql:
                return _Result(one=run_row)
            if "COUNT(*)" in sql and "recommendation_family = 'bridge'" in sql:
                return _Result(one=cov_row)
            if "FROM paper_scores ps" in sql and "ORDER BY ps.final_score" in sql:
                assert params is not None
                rid, fam, lim = params
                assert rid == "run-xyz"
                assert lim == 2
                if fam == "bridge" and "bridge_eligible IS TRUE" not in sql:
                    return _Result(all_rows=[{"openalex_id": "Wb1"}, {"openalex_id": "Wb2"}])
                if fam == "bridge" and "bridge_eligible IS TRUE" in sql:
                    return _Result(all_rows=[{"openalex_id": "Wb2"}])
                if fam == "emerging":
                    return _Result(all_rows=[{"openalex_id": "We1"}, {"openalex_id": "We2"}])
            pytest.fail(f"unexpected SQL: {sql[:120]!r}")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    fake = _FakeConn()

    def fake_connect(*args, **kwargs):
        return fake

    with patch("app.bridge_distinctness_repo.psycopg.connect", fake_connect):
        out = load_bridge_distinctness_report(
            database_url="postgresql://x",
            ranking_run_id="run-xyz",
            k=2,
        )

    assert out is not None
    assert out.ranking_run_id == "run-xyz"
    assert out.full_bridge_top_k_ids == ["Wb1", "Wb2"]
    assert out.eligible_bridge_top_k_ids == ["Wb2"]
    assert out.emerging_top_k_ids == ["We1", "We2"]
    for sql, params in fake.calls:
        if params and "ranking_run_id" in sql:
            assert params[0] == "run-xyz"


def test_load_bridge_distinctness_no_fallback_uses_explicit_id_only(monkeypatch) -> None:
    """If the run row is missing, return None without consulting latest snapshot helpers."""

    class _R:
        def fetchone(self):
            return None

        def fetchall(self):
            return []

    class _C:
        def execute(self, sql, params=None):
            return _R()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    with patch("app.bridge_distinctness_repo.psycopg.connect", lambda *a, **k: _C()):
        assert (
            load_bridge_distinctness_report(
                database_url="postgresql://x",
                ranking_run_id="nope",
                k=5,
            )
            is None
        )


def test_load_bridge_distinctness_non_succeeded_returns_none() -> None:
    row_failed = {
        "ranking_run_id": "run-f",
        "ranking_version": "v",
        "corpus_snapshot_version": "s",
        "embedding_version": "e",
        "config_json": {},
        "status": "failed",
    }

    class _Result:
        def __init__(self, one):
            self._one = one

        def fetchone(self):
            return self._one

        def fetchall(self):
            return []

    class _C:
        def execute(self, sql, params=None):
            return _Result(row_failed)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    with patch("app.bridge_distinctness_repo.psycopg.connect", lambda *a, **k: _C()):
        assert (
            load_bridge_distinctness_report(
                database_url="postgresql://x",
                ranking_run_id="run-f",
                k=5,
            )
            is None
        )
