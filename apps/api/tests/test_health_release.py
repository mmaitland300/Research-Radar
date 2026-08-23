from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.main import app
from app.public_release_repo import (
    MaterializedRunContext,
    PublicReleaseDiagnostics,
    PublicReleasePromotion,
)
from app.routers import health as health_router


class _Conn:
    def execute(self, *_args, **_kwargs):
        return self

    def fetchone(self):
        return (1,)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _promotion() -> PublicReleasePromotion:
    return PublicReleasePromotion(
        promotion_id=11,
        promoted_at=datetime(2026, 8, 23, tzinfo=UTC),
        promoted_by="test",
        note=None,
        run=MaterializedRunContext(
            ranking_run_id="rank-public",
            ranking_version="ranking-public-v1",
            corpus_snapshot_version="snapshot-composed",
            embedding_version="embedding-v1",
            status="succeeded",
            finished_at=datetime(2026, 8, 23, tzinfo=UTC),
            error_message=None,
            config_json={"families_written": ["emerging", "bridge", "undercited"]},
            counts_json={},
        ),
    )


def _diagnostics(*, failures: tuple[str, ...] = ()) -> PublicReleaseDiagnostics:
    return PublicReleaseDiagnostics(
        serveable=not failures,
        membership_count=2,
        embedding_count=2 if not failures else 1,
        missing_embedding_count=0 if not failures else 1,
        family_score_counts={"emerging": 2, "bridge": 2, "undercited": 1},
        expected_family_score_counts={"emerging": 2, "bridge": 2, "undercited": 1},
        out_of_membership_score_count=0,
        cluster_version=None,
        cluster_assignment_count=None,
        missing_cluster_assignment_count=None,
        out_of_membership_cluster_count=None,
        failures=failures,
    )


def test_readiness_reports_serveable_active_release(monkeypatch) -> None:
    promotion = _promotion()
    monkeypatch.setattr(health_router.psycopg, "connect", lambda *_a, **_k: _Conn())
    monkeypatch.setattr(health_router, "fetch_latest_public_release_promotion", lambda _conn: promotion)
    monkeypatch.setattr(
        health_router,
        "inspect_public_release_serveability",
        lambda _conn, _promotion: _diagnostics(),
    )

    response = TestClient(app).get("/readyz")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["active_release"]["ranking_run_id"] == "rank-public"
    assert payload["active_release"]["scorer"]["kind"] == "materialized_paper_scores"
    assert payload["active_release"]["scorer"]["version"] == "rank-public"
    assert payload["release_diagnostics"]["serveable"] is True
    assert payload["release_diagnostics"]["missing_embedding_count"] == 0


def test_readiness_rejects_invalid_promoted_release(monkeypatch) -> None:
    promotion = _promotion()
    monkeypatch.setattr(health_router.psycopg, "connect", lambda *_a, **_k: _Conn())
    monkeypatch.setattr(health_router, "fetch_latest_public_release_promotion", lambda _conn: promotion)
    monkeypatch.setattr(
        health_router,
        "inspect_public_release_serveability",
        lambda _conn, _promotion: _diagnostics(failures=("embedding_coverage_incomplete",)),
    )

    response = TestClient(app).get("/readyz")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "not_ready"
    assert payload["database"] == "connected"
    assert payload["active_release"]["promotion_id"] == 11
    assert payload["release_diagnostics"]["serveable"] is False
    assert payload["release_diagnostics"]["failures"] == ["embedding_coverage_incomplete"]
