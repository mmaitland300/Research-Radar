from unittest.mock import MagicMock
from dataclasses import replace

from fastapi.testclient import TestClient

import app.evaluation_repo as evaluation_repo
from app import main
from app.evaluation_repo import (
    EvalCitationProxy,
    EvalComparePayload,
    EvalListArm,
    EvalPaperRow,
    EvalRecencyProxy,
    EvalTopicMixProxy,
    EvalTopicOverlap,
    _arm_stats,
    _pool_cte_sql,
    _select_from_pool,
    load_evaluation_compare,
)
from app.routers import evaluation as evaluation_router
from app.routers import health as health_router
from app.scores_repo import RankedRunContext


client = TestClient(main.app)


def _fake_payload() -> EvalComparePayload:
    paper = EvalPaperRow(
        paper_id="https://openalex.org/W1",
        title="Example",
        year=2022,
        citation_count=3,
        source_slug="tismir",
        topics=["mir"],
        final_score=0.9,
    )
    arm_ranked = EvalListArm(
        arm_label="ranked_family",
        arm_description="ranked",
        ordering_description="final_score DESC",
        items=[paper],
        recency=EvalRecencyProxy(2022.0, 2022, 2022, 1.0),
        citations=EvalCitationProxy(3.0, 3.0, 3, 3),
        topics=EvalTopicMixProxy(1, ["mir"]),
    )
    arm_cit = EvalListArm(
        arm_label="citation_baseline",
        arm_description="cit",
        ordering_description="cites",
        items=[paper],
        recency=EvalRecencyProxy(2022.0, 2022, 2022, 1.0),
        citations=EvalCitationProxy(3.0, 3.0, 3, 3),
        topics=EvalTopicMixProxy(1, ["mir"]),
    )
    arm_date = EvalListArm(
        arm_label="date_baseline",
        arm_description="date",
        ordering_description="date",
        items=[paper],
        recency=EvalRecencyProxy(2022.0, 2022, 2022, 1.0),
        citations=EvalCitationProxy(3.0, 3.0, 3, 3),
        topics=EvalTopicMixProxy(1, ["mir"]),
    )
    return EvalComparePayload(
        ranking_run_id="run-1",
        ranking_version="v0-test",
        corpus_snapshot_version="snap-a",
        embedding_version="none-v0",
        family="emerging",
        pool_definition="test pool",
        pool_size=10,
        low_cite_min_year=None,
        low_cite_max_citations=None,
        candidate_pool_doc_revision=None,
        ranked=arm_ranked,
        citation_baseline=arm_cit,
        date_baseline=arm_date,
        topic_overlap=EvalTopicOverlap(1.0, 1.0, 1.0),
    )


def test_arm_stats_builds_ordering_description() -> None:
    paper = EvalPaperRow(
        paper_id="https://openalex.org/W1",
        title="Example",
        year=2022,
        citation_count=3,
        source_slug="tismir",
        topics=["mir"],
        final_score=0.9,
    )
    arm = _arm_stats(
        [paper],
        arm_label="ranked_family",
        arm_desc="ranked",
        ordering_desc="final_score DESC",
    )
    assert arm.ordering_description == "final_score DESC"
    assert arm.recency.mean_year == 2022.0


def test_pool_sql_undercited_uses_low_cite_contract_filters() -> None:
    sql, params = _pool_cte_sql(
        family="undercited",
        corpus_snapshot_version="snap",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
    )
    assert "work_source_snapshot_memberships wssm" in sql
    assert "wssm.work_id = w.id" in sql
    assert "wssm.inclusion_status = 'included'" in sql
    assert "wssm.source_snapshot_version = %s" in sql
    assert "w.corpus_snapshot_version = %s" not in sql
    assert "w.is_core_corpus = TRUE" in sql
    assert "w.year >= %s" in sql
    assert "w.citation_count <= %s" in sql
    assert "length(trim(COALESCE(w.title, ''))) > 0" in sql
    assert "length(trim(COALESCE(w.abstract, ''))) > 0" in sql
    assert params == ("snap", 2019, 30)


def test_pool_sql_non_undercited_uses_all_included_snapshot_works_only() -> None:
    sql, params = _pool_cte_sql(
        family="emerging",
        corpus_snapshot_version="snap",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
    )
    assert "work_source_snapshot_memberships wssm" in sql
    assert "wssm.work_id = w.id" in sql
    assert "wssm.inclusion_status = 'included'" in sql
    assert "wssm.source_snapshot_version = %s" in sql
    assert "w.corpus_snapshot_version = %s" not in sql
    assert "w.is_core_corpus = TRUE" not in sql
    assert "w.year >= %s" not in sql
    assert "w.citation_count <= %s" not in sql
    assert params == ("snap",)


def test_select_from_pool_uses_parameterized_static_ordering() -> None:
    sql, params = _select_from_pool(
        ordering="citation",
        limit=5,
        corpus_snapshot_version="snap",
        family="emerging",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
    )

    assert "ORDER BY\n        CASE WHEN %s = 'citation' THEN pool.citation_count END DESC" in sql
    assert "ORDER BY {" not in sql
    assert params == ("snap", "citation", 5)


def test_select_from_pool_undercited_keeps_low_cite_params_before_ordering() -> None:
    sql, params = _select_from_pool(
        ordering="date",
        limit=10,
        corpus_snapshot_version="snap",
        family="undercited",
        low_cite_min_year=2020,
        low_cite_max_citations=12,
    )

    assert "w.is_core_corpus = TRUE" in sql
    assert "CASE WHEN %s = 'citation' THEN pool.citation_count END DESC" in sql
    assert params == ("snap", 2020, 12, "date", 10)


def test_ranked_evaluation_uses_membership_for_resolved_run_snapshot(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    pool_result = MagicMock()
    pool_result.fetchone.return_value = {"pool_n": 0}
    ranked_result = MagicMock()
    ranked_result.fetchall.return_value = []
    citation_result = MagicMock()
    citation_result.fetchall.return_value = []
    date_result = MagicMock()
    date_result.fetchall.return_value = []
    conn.execute.side_effect = [pool_result, ranked_result, citation_result, date_result]

    monkeypatch.setattr(evaluation_repo.psycopg, "connect", lambda *a, **k: conn)
    monkeypatch.setattr(
        evaluation_repo,
        "latest_corpus_snapshot_version_with_works",
        lambda _conn: "snapshot-default",
    )
    monkeypatch.setattr(
        evaluation_repo,
        "resolve_ranked_run_context",
        lambda *_a, **_k: RankedRunContext(
            ranking_run_id="rank-1",
            ranking_version="rank-v1",
            corpus_snapshot_version="snapshot-composed",
        ),
    )
    monkeypatch.setattr(
        evaluation_repo,
        "_fetch_ranking_run_row",
        lambda *_a, **_k: {
            "ranking_run_id": "rank-1",
            "ranking_version": "rank-v1",
            "corpus_snapshot_version": "snapshot-composed",
            "embedding_version": "embed-v1",
            "config_json": {},
            "status": "succeeded",
        },
    )

    payload = load_evaluation_compare(
        database_url="postgresql://test",
        family="emerging",
        limit=5,
    )

    assert payload is not None
    ranked_sql, ranked_params = conn.execute.call_args_list[1][0]
    assert "work_source_snapshot_memberships wssm" in ranked_sql
    assert "wssm.work_id = w.id" in ranked_sql
    assert "wssm.source_snapshot_version = %s" in ranked_sql
    assert "wssm.inclusion_status = 'included'" in ranked_sql
    assert "w.corpus_snapshot_version = %s" not in ranked_sql
    assert ranked_params == ("snapshot-composed", "rank-1", "emerging", 5)


def test_evaluation_compare_smoke(monkeypatch) -> None:
    monkeypatch.setattr(evaluation_router, "load_evaluation_compare", MagicMock(return_value=_fake_payload()))
    response = client.get("/api/v1/evaluation/compare?family=emerging&limit=5")

    assert response.status_code == 200
    body = response.json()
    assert body["ranking_run_id"] == "run-1"
    assert body["family"] == "emerging"
    assert body["pool_size"] == 10
    assert body["disclaimer"]["headline"]
    assert len(body["disclaimer"]["bullets"]) >= 2
    assert body["ranked"]["arm_label"] == "ranked_family"
    assert body["citation_baseline"]["items"][0]["paper_id"] == "https://openalex.org/W1"
    assert body["topic_overlap"]["jaccard_ranked_vs_citation_baseline"] == 1.0
    assert "topic_overlap_note" in body
    assert "not expert-reviewed evidence" in body["disclaimer"]["headline"].lower()
    assert "same candidate pool" in " ".join(body["disclaimer"]["bullets"]).lower()


def test_evaluation_compare_undercited_pool_contract_fields(monkeypatch) -> None:
    payload = replace(
        _fake_payload(),
        family="undercited",
        pool_definition="Low-cite candidate pool (revision v0): included core works.",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
        candidate_pool_doc_revision="v0",
    )
    monkeypatch.setattr(evaluation_router, "load_evaluation_compare", MagicMock(return_value=payload))

    response = client.get("/api/v1/evaluation/compare?family=undercited&limit=5")
    assert response.status_code == 200
    body = response.json()
    assert body["family"] == "undercited"
    assert body["low_cite_min_year"] == 2019
    assert body["low_cite_max_citations"] == 30
    assert body["candidate_pool_doc_revision"] == "v0"
    assert "low-cite candidate pool" in body["pool_definition"].lower()


def test_evaluation_compare_emerging_does_not_emit_low_cite_gate_fields(monkeypatch) -> None:
    payload = replace(
        _fake_payload(),
        family="emerging",
        pool_definition="All included works in this corpus snapshot.",
        low_cite_min_year=None,
        low_cite_max_citations=None,
        candidate_pool_doc_revision=None,
    )
    monkeypatch.setattr(evaluation_router, "load_evaluation_compare", MagicMock(return_value=payload))

    response = client.get("/api/v1/evaluation/compare?family=emerging&limit=5")
    assert response.status_code == 200
    body = response.json()
    assert body["family"] == "emerging"
    assert body["low_cite_min_year"] is None
    assert body["low_cite_max_citations"] is None
    assert body["candidate_pool_doc_revision"] is None


def test_evaluation_compare_not_found(monkeypatch) -> None:
    monkeypatch.setattr(evaluation_router, "load_evaluation_compare", MagicMock(return_value=None))
    response = client.get("/api/v1/evaluation/compare?family=bridge")

    assert response.status_code == 404


def test_evaluation_compare_invalid_family() -> None:
    response = client.get("/api/v1/evaluation/compare?family=notafamily")

    assert response.status_code == 422


def test_readiness_ok(monkeypatch) -> None:
    class FakeConn:
        def execute(self, *_a, **_k):
            return self

        def fetchone(self):
            return (1,)

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(health_router.psycopg, "connect", lambda *_a, **_k: FakeConn())
    monkeypatch.setattr(health_router, "fetch_latest_public_release_promotion", lambda _conn: None)
    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["database"] == "connected"
    assert response.json()["active_release"] is None
    assert response.json()["release_diagnostics"] is None


def test_readiness_db_down(monkeypatch) -> None:
    def boom(*_a, **_k):
        raise RuntimeError("no db")

    monkeypatch.setattr(health_router.psycopg, "connect", boom)
    response = client.get("/readyz")

    assert response.status_code == 503
