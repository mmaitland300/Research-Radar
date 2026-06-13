"""Tests for second shadow-generalization product-candidate ranking v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import pipeline.ml_shadow_scorer_second_product_candidate_ranking as ranking_mod
from pipeline.ml_shadow_scorer_second_product_candidate_ranking import (
    ARTIFACT_VERSION,
    DEFAULT_EMBEDDING_VERSION,
    DEFAULT_RANKING_VERSION,
    DEFAULT_SNAPSHOT_VERSION,
    DISALLOWED_RANKING_RUN_ID,
    MLShadowScorerSecondProductCandidateRankingError,
    build_ml_shadow_scorer_second_product_candidate_ranking_payload,
)


class _Result:
    def __init__(self, *, one: tuple | None = None, all_rows: list[tuple] | None = None) -> None:
        self._one = one
        self._all = all_rows or []

    def fetchone(self) -> tuple | None:
        return self._one

    def fetchall(self) -> list[tuple]:
        return self._all


class _FakeConn:
    def __init__(self) -> None:
        self.snapshots = {DEFAULT_SNAPSHOT_VERSION}
        self.snapshot_work_count = 528
        self.missing_embedding_count = 0
        self.paper_scores_by_family = {"bridge": 528, "emerging": 528, "undercited": 528}
        self.sql: list[str] = []

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple | None = None) -> _Result:
        params = params or ()
        compact = " ".join(sql.split())
        self.sql.append(compact)
        if compact.startswith("SELECT 1 FROM source_snapshot_versions"):
            return _Result(one=(1,) if str(params[0]) in self.snapshots else None)
        if compact.startswith("SELECT COUNT(*) FROM work_source_snapshot_memberships"):
            return _Result(one=(self.snapshot_work_count,))
        if compact.startswith("SELECT COUNT(*) FROM works w JOIN work_source_snapshot_memberships wssm"):
            return _Result(one=(self.missing_embedding_count,))
        if compact.startswith("SELECT COUNT(*) FROM works w WHERE w.inclusion_status = 'included'"):
            return _Result(one=(self.snapshot_work_count,))
        if compact.startswith("SELECT COUNT(*) FROM works w LEFT JOIN embeddings e"):
            return _Result(one=(self.missing_embedding_count,))
        if compact.startswith("SELECT recommendation_family, COUNT(*) FROM paper_scores"):
            return _Result(all_rows=sorted(self.paper_scores_by_family.items()))
        raise AssertionError(f"Unhandled SQL: {compact}")


def _embeddings_payload(
    *,
    full_coverage: bool = True,
    next_stage: str = "run_second_shadow_generalization_product_candidate_ranking_v1",
    snapshot_version: str = DEFAULT_SNAPSHOT_VERSION,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_embeddings",
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "snapshot_version": snapshot_version,
            "embedding_version": embedding_version,
        },
        "embedding_result": {
            "status": "succeeded",
            "full_snapshot_embedding_coverage": full_coverage,
            "cluster_required_before_ranking": False,
            "recommended_next_stage": next_stage,
        },
        "coverage": {
            "snapshot_work_count": 528,
            "embedded_work_count": 528 if full_coverage else 527,
            "missing_embedding_count": 0 if full_coverage else 1,
        },
        "sql_write_report": {
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "production_tables_modified": False,
        },
        "recommended_next_stage": next_stage,
    }


def _hydration_payload(snapshot_version: str = DEFAULT_SNAPSHOT_VERSION) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_hydration",
            "hydration_version": "ml-shadow-scorer-v1-second-snapshot-hydration-v1",
            "snapshot_version": snapshot_version,
        },
        "hydration_result": {
            "status": "succeeded",
            "snapshot_embedding_ready": True,
        },
    }


def _ingest_payload(snapshot_version: str = DEFAULT_SNAPSHOT_VERSION) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_candidate_plan_ingest",
            "ingest_version": "ml-shadow-scorer-v1-second-candidate-plan-ingest-v1",
        },
        "snapshot": {
            "source_snapshot_version": snapshot_version,
            "shadow_generalization_candidate_source": True,
        },
        "ingest_result": {
            "status": "succeeded",
            "snapshot_work_count": 528,
        },
    }


def _generalization_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "runtime_implementation_authorized": False,
    }


def _policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, *, embeddings: dict | None = None, hydration: dict | None = None) -> dict[str, Path]:
    return {
        "second_snapshot_embeddings_path": _write_json(
            tmp_path,
            "embeddings.json",
            embeddings or _embeddings_payload(),
        ),
        "second_snapshot_hydration_path": _write_json(tmp_path, "hydration.json", hydration or _hydration_payload()),
        "second_candidate_plan_ingest_path": _write_json(tmp_path, "ingest.json", _ingest_payload()),
        "generalization_audit_plan_path": _write_json(tmp_path, "plan.json", _generalization_plan_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", _policy_payload()),
    }


def _fake_run(ranking_run_id: str = "rank-shadowgen2") -> SimpleNamespace:
    return SimpleNamespace(
        ranking_run_id=ranking_run_id,
        status="succeeded",
        counts=SimpleNamespace(
            total_candidate_works=528,
            total_rows_written=1584,
            rows_by_family={"bridge": 528, "emerging": 528, "undercited": 528},
        ),
    )


def _build(tmp_path: Path, conn: _FakeConn, **kwargs: object) -> dict:
    with (
        patch.object(ranking_mod.psycopg, "connect", return_value=conn),
        patch.object(ranking_mod, "execute_ranking_run", return_value=_fake_run()) as execute_mock,
    ):
        payload = build_ml_shadow_scorer_second_product_candidate_ranking_payload(
            **_paths(tmp_path),
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            repo_root=tmp_path,
            **kwargs,
        )
    payload["_execute_mock"] = execute_mock
    return payload


def test_happy_path_with_mocked_execute_ranking_run(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn)
    execute_mock = payload.pop("_execute_mock")

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_product_candidate_ranking"
    assert payload["metadata"]["artifact_version"] == ARTIFACT_VERSION
    assert payload["metadata"]["eval_only"] is True
    assert payload["metadata"]["shadow_generalization_eval_only"] is True
    assert payload["ranking_result"]["status"] == "succeeded"
    assert payload["ranking_result"]["ranking_run_id"] == "rank-shadowgen2"
    assert payload["ranking_result"]["ranking_run_id"] != DISALLOWED_RANKING_RUN_ID
    assert payload["ranking_result"]["corpus_snapshot_version"] == DEFAULT_SNAPSHOT_VERSION
    assert payload["ranking_result"]["embedding_version"] == DEFAULT_EMBEDDING_VERSION
    assert payload["ranking_result"]["ranking_version"] == DEFAULT_RANKING_VERSION
    assert payload["ranking_result"]["paper_scores_written_count"] == 1584
    assert payload["ranking_result"]["emerging_family_work_count"] == 528
    assert payload["recommended_next_stage"] == "rerun_second_shadow_generalization_surface_discovery_v1"
    assert payload["sql_write_report"]["ranking_runs_written"] is True
    assert payload["sql_write_report"]["paper_scores_written"] is True
    assert payload["sql_write_report"]["source_snapshot_versions_written"] is False
    assert payload["sql_write_report"]["ingest_runs_written"] is False
    assert payload["sql_write_report"]["raw_openalex_works_written"] is False
    assert payload["sql_write_report"]["embeddings_modified"] is False
    assert payload["sql_write_report"]["works_modified"] is False
    assert payload["sql_write_report"]["production_tables_modified"] is False
    assert payload["shadow_and_production_blockers"]["missing_second_surface_ranking_run"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False

    kwargs = execute_mock.call_args.kwargs
    assert kwargs["ranking_version"] == DEFAULT_RANKING_VERSION
    assert kwargs["corpus_snapshot_version"] == DEFAULT_SNAPSHOT_VERSION
    assert kwargs["embedding_version"] == DEFAULT_EMBEDDING_VERSION
    assert kwargs["cluster_version"] is None
    assert kwargs["bridge_weight_for_bridge_family"] == 0.0


def test_dry_run_performs_no_ranking_writes(tmp_path: Path) -> None:
    conn = _FakeConn()
    with (
        patch.object(ranking_mod.psycopg, "connect", return_value=conn),
        patch.object(ranking_mod, "execute_ranking_run") as execute_mock,
    ):
        payload = build_ml_shadow_scorer_second_product_candidate_ranking_payload(
            **_paths(tmp_path),
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            dry_run=True,
            repo_root=tmp_path,
        )

    execute_mock.assert_not_called()
    assert payload["metadata"]["dry_run"] is True
    assert payload["ranking_result"]["status"] == "dry_run_validated"
    assert payload["ranking_result"]["ranking_run_id"] is None
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["ranking_runs_written"] is False
    assert payload["sql_write_report"]["paper_scores_written"] is False
    assert payload["recommended_next_stage"] != "rerun_second_shadow_generalization_surface_discovery_v1"


def test_rejects_embeddings_without_full_coverage(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondProductCandidateRankingError, match="full_snapshot_embedding_coverage"):
        build_ml_shadow_scorer_second_product_candidate_ranking_payload(
            **_paths(tmp_path, embeddings=_embeddings_payload(full_coverage=False)),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_wrong_embeddings_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondProductCandidateRankingError, match="recommended_next_stage"):
        build_ml_shadow_scorer_second_product_candidate_ranking_payload(
            **_paths(tmp_path, embeddings=_embeddings_payload(next_stage="audit_generalization_surface_v1")),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_snapshot_mismatch(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondProductCandidateRankingError, match="snapshot-version"):
        build_ml_shadow_scorer_second_product_candidate_ranking_payload(
            **_paths(tmp_path, embeddings=_embeddings_payload(snapshot_version="other-snapshot")),
            snapshot_version=DEFAULT_SNAPSHOT_VERSION,
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_missing_embeddings_in_db(tmp_path: Path) -> None:
    conn = _FakeConn()
    conn.missing_embedding_count = 1
    with patch.object(ranking_mod.psycopg, "connect", return_value=conn):
        with pytest.raises(MLShadowScorerSecondProductCandidateRankingError, match="missing 1 embedding"):
            build_ml_shadow_scorer_second_product_candidate_ranking_payload(
                **_paths(tmp_path),
                database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
                dry_run=True,
                repo_root=tmp_path,
            )


def test_rejects_hosted_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondProductCandidateRankingError, match="hosted production"):
        build_ml_shadow_scorer_second_product_candidate_ranking_payload(
            **_paths(tmp_path),
            database_url="postgresql://user:pass@project.neon.tech/db",
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_first_validated_ranking_run_id(tmp_path: Path) -> None:
    conn = _FakeConn()
    with (
        patch.object(ranking_mod.psycopg, "connect", return_value=conn),
        patch.object(ranking_mod, "execute_ranking_run", return_value=_fake_run(DISALLOWED_RANKING_RUN_ID)),
    ):
        with pytest.raises(MLShadowScorerSecondProductCandidateRankingError, match="must differ"):
            build_ml_shadow_scorer_second_product_candidate_ranking_payload(
                **_paths(tmp_path),
                database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
                repo_root=tmp_path,
            )


def test_cli_writes_json_and_markdown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr(ranking_mod.psycopg, "connect", lambda *args, **kwargs: conn)
    monkeypatch.setattr(ranking_mod, "execute_ranking_run", lambda **kwargs: _fake_run())
    paths = _paths(tmp_path)
    out_json = tmp_path / "ranking.json"
    out_md = tmp_path / "ranking.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-product-candidate-ranking",
        "--second-snapshot-embeddings",
        str(paths["second_snapshot_embeddings_path"]),
        "--second-snapshot-hydration",
        str(paths["second_snapshot_hydration_path"]),
        "--second-candidate-plan-ingest",
        str(paths["second_candidate_plan_ingest_path"]),
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["ranking_result"]["ranking_run_id"] == "rank-shadowgen2"
    assert "Not Learned Probability / Not Shadow / Not Production" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_shadow_scorer_second_product_candidate_ranking.py"
    ).read_text(encoding="utf-8")
    lower_source = module_source.lower()

    assert "from pipeline.openalex" not in lower_source
    assert "import pipeline.openalex" not in lower_source
    assert "openalex_client" not in lower_source
    assert "openai" not in lower_source
    assert "from sklearn" not in lower_source
    assert "import sklearn" not in lower_source
    assert "score_audit_embedding_probability" not in lower_source
    assert "from pipeline.ml_shadow_scorer_v1" not in lower_source
