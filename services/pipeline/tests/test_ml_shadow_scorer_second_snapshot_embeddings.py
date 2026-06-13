"""Tests for second shadow-generalization snapshot embeddings v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
import pipeline.corpus_v2_embed as corpus_embed
import pipeline.ml_shadow_scorer_second_snapshot_embeddings as embed_mod
from pipeline.corpus_v2_embed import OPENAI_API_KEY_ENV
from pipeline.ml_shadow_scorer_second_candidate_plan_ingest import (
    EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256,
)
from pipeline.ml_shadow_scorer_second_snapshot_embeddings import (
    ARTIFACT_VERSION,
    DEFAULT_EMBEDDING_VERSION,
    DEFAULT_SNAPSHOT_VERSION,
    MLShadowScorerSecondSnapshotEmbeddingsError,
    build_ml_shadow_scorer_second_snapshot_embeddings_payload,
)
from tests.snapshot_membership_fake_sql import (
    build_memberships_from_works,
    count_embeddings_for_snapshot,
    delete_embeddings_for_snapshot,
    embedding_coverage_counts,
    included_work_ids,
    is_membership_included_work_select,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
FRESH_EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"


class _Result:
    def __init__(self, *, one: tuple | None = None, all_rows: list[tuple] | None = None) -> None:
        self._one = one
        self._all = all_rows or []

    def fetchone(self) -> tuple | None:
        return self._one

    def fetchall(self) -> list[tuple]:
        return self._all


class _FakeConn:
    def __init__(self, *, work_count: int = 528) -> None:
        self.snapshots = {DEFAULT_SNAPSHOT_VERSION, "another-snapshot"}
        self.works: dict[int, dict] = {
            idx: {
                "title": f"Second shadow work {idx}",
                "abstract": f"Music information retrieval shadow generalization abstract {idx}",
                "type": "article",
                "language": "en",
                "inclusion_status": "included",
                "corpus_snapshot_version": DEFAULT_SNAPSHOT_VERSION,
            }
            for idx in range(1, work_count + 1)
        }
        self.works[9999] = {
            "title": "Other snapshot",
            "abstract": "Other abstract",
            "type": "article",
            "language": "en",
            "inclusion_status": "included",
            "corpus_snapshot_version": "another-snapshot",
        }
        self.embeddings: dict[tuple[int, str], str] = {
            (idx, FRESH_EMBEDDING_VERSION): "[0.1,0.2]" for idx in range(1, min(work_count, 5) + 1)
        }
        self.memberships = build_memberships_from_works(self.works)
        self.sql: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def execute(self, sql: str, params: tuple | None = None) -> _Result:
        params = params or ()
        compact = " ".join(sql.split())
        self.sql.append(compact)
        if compact.startswith("SELECT 1 FROM source_snapshot_versions"):
            return _Result(one=(1,) if str(params[0]) in self.snapshots else None)
        if is_membership_included_work_select(compact):
            snapshot = str(params[0])
            rows = []
            for work_id in included_work_ids(self.works, self.memberships, snapshot):
                work = self.works[work_id]
                rows.append((work_id, work["title"], work["abstract"], work["type"], work["language"]))
            return _Result(all_rows=rows)
        if compact.startswith("SELECT id, title, abstract, type, language FROM works"):
            snapshot = str(params[0])
            rows = []
            for work_id, work in self.works.items():
                if work["corpus_snapshot_version"] == snapshot and work["inclusion_status"] == "included":
                    rows.append((work_id, work["title"], work["abstract"], work["type"], work["language"]))
            rows.sort(key=lambda row: row[0])
            return _Result(all_rows=rows)
        if compact == "SELECT COUNT(*) FROM embeddings WHERE embedding_version = %s":
            version = str(params[0])
            return _Result(one=(sum(1 for (_work_id, ev) in self.embeddings if ev == version),))
        if compact.startswith("SELECT COUNT(*) FROM embeddings e JOIN work_source_snapshot_memberships wssm"):
            snapshot = str(params[0])
            version = str(params[1])
            return _Result(
                one=(
                    count_embeddings_for_snapshot(
                        self.embeddings,
                        self.memberships,
                        snapshot=snapshot,
                        version=version,
                    ),
                )
            )
        if compact.startswith("DELETE FROM embeddings e USING work_source_snapshot_memberships wssm"):
            snapshot = str(params[0])
            version = str(params[1])
            delete_embeddings_for_snapshot(
                self.embeddings,
                self.memberships,
                snapshot=snapshot,
                version=version,
            )
            return _Result()
        if compact.startswith("SELECT COUNT(*), COUNT(e.work_id) FROM works w JOIN work_source_snapshot_memberships wssm"):
            snapshot = str(params[0])
            version = str(params[1])
            total, embedded = embedding_coverage_counts(
                self.works,
                self.memberships,
                self.embeddings,
                snapshot=snapshot,
                version=version,
            )
            return _Result(one=(total, embedded))
        if compact.startswith("SELECT COUNT(*) FROM embeddings e JOIN works w ON w.id = e.work_id"):
            version = str(params[0])
            snapshot = str(params[1])
            count = sum(
                1
                for (work_id, ev) in self.embeddings
                if ev == version and self.works[work_id]["corpus_snapshot_version"] == snapshot
            )
            return _Result(one=(count,))
        if compact.startswith("SELECT COUNT(*), COUNT(e.work_id) FROM works w LEFT JOIN embeddings e"):
            version = str(params[0])
            snapshot = str(params[1])
            total = sum(
                1
                for work in self.works.values()
                if work["corpus_snapshot_version"] == snapshot and work["inclusion_status"] == "included"
            )
            embedded = sum(
                1
                for (work_id, ev) in self.embeddings
                if ev == version
                and self.works[work_id]["corpus_snapshot_version"] == snapshot
                and self.works[work_id]["inclusion_status"] == "included"
            )
            return _Result(one=(total, embedded))
        if compact.startswith("DELETE FROM embeddings e USING works w"):
            version = str(params[0])
            snapshot = str(params[1])
            for key in list(self.embeddings):
                work_id, ev = key
                if ev == version and self.works[work_id]["corpus_snapshot_version"] == snapshot:
                    del self.embeddings[key]
            return _Result()
        if compact.startswith("INSERT INTO embeddings"):
            work_id = int(params[0])
            version = str(params[1])
            self.embeddings[(work_id, version)] = str(params[2])
            return _Result()
        raise AssertionError(f"Unhandled SQL: {compact}")


def _plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_hybrid_candidate_plan",
            "plan_version": "ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1",
        },
        "candidate_selection": {"selected_total": 528},
        "readiness_estimate": {"planned_candidate_work_set_sha256": EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256},
    }


def _ingest_payload(*, plan_sha: str, snapshot_version: str = DEFAULT_SNAPSHOT_VERSION) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_candidate_plan_ingest",
            "ingest_version": "ml-shadow-scorer-v1-second-candidate-plan-ingest-v1",
            "inputs": [{"name": "second_hybrid_candidate_plan", "sha256": plan_sha}],
        },
        "snapshot": {
            "source_snapshot_version": snapshot_version,
            "shadow_generalization_candidate_source": True,
        },
        "candidate_plan_summary": {"planned_candidate_work_set_sha256": EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256},
        "ingest_result": {"snapshot_work_count": 528},
    }


def _hydration_payload(
    *,
    ready: bool = True,
    next_stage: str = "embed_second_shadow_generalization_snapshot_v1",
    snapshot_version: str = DEFAULT_SNAPSHOT_VERSION,
    works_considered: int = 528,
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_hydration",
            "hydration_version": "ml-shadow-scorer-v1-second-snapshot-hydration-v1",
            "snapshot_version": snapshot_version,
        },
        "hydration_result": {
            "status": "succeeded",
            "hydration_run_id": "hydrate-test",
            "works_considered_count": works_considered,
            "embedding_ready_count": 528 if ready else 527,
            "embedding_blocked_count": 0 if ready else 1,
            "snapshot_embedding_ready": ready,
            "recommended_next_stage": next_stage,
        },
        "sql_write_report": {
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "embeddings_written": False,
            "production_tables_modified": False,
        },
        "recommended_next_stage": next_stage,
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


def _paths(tmp_path: Path, *, hydration: dict | None = None, ingest: dict | None = None, plan: dict | None = None) -> dict[str, Path]:
    plan_path = _write_json(tmp_path, "plan.json", plan or _plan_payload())
    plan_sha = embed_mod.sha256_file(plan_path)
    return {
        "second_snapshot_hydration_path": _write_json(tmp_path, "hydration.json", hydration or _hydration_payload()),
        "second_candidate_plan_ingest_path": _write_json(
            tmp_path,
            "ingest.json",
            ingest or _ingest_payload(plan_sha=plan_sha),
        ),
        "second_hybrid_candidate_plan_path": plan_path,
        "generalization_audit_plan_path": _write_json(tmp_path, "generalization-plan.json", _generalization_plan_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", _policy_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn, **kwargs: object) -> dict:
    with (
        patch.object(embed_mod.psycopg, "connect", return_value=conn),
        patch.object(corpus_embed.psycopg, "connect", return_value=conn),
    ):
        return build_ml_shadow_scorer_second_snapshot_embeddings_payload(
            **_paths(tmp_path),
            embedding_version=DEFAULT_EMBEDDING_VERSION,
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            repo_root=tmp_path,
            **kwargs,
        )


def test_happy_path_with_mock_embeddings_writes_528_shadow_version_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENAI_API_KEY_ENV, raising=False)
    conn = _FakeConn()

    payload = _build(tmp_path, conn, mock_embeddings=True)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_snapshot_embeddings"
    assert payload["metadata"]["artifact_version"] == ARTIFACT_VERSION
    assert payload["metadata"]["embedding_version"] == DEFAULT_EMBEDDING_VERSION
    assert payload["embedding_result"]["status"] == "succeeded"
    assert payload["embedding_result"]["works_considered_count"] == 528
    assert payload["embedding_result"]["embeddings_written_count"] == 528
    assert payload["embedding_result"]["embedding_provider"] == "openai"
    assert payload["embedding_result"]["embedding_model"] == "text-embedding-3-small"
    assert payload["embedding_result"]["embedding_dimensions"] == 1536
    assert payload["embedding_result"]["cluster_required_before_ranking"] is False
    assert payload["coverage"] == {
        "snapshot_work_count": 528,
        "embedded_work_count": 528,
        "missing_embedding_count": 0,
    }
    assert payload["embedding_result"]["full_snapshot_embedding_coverage"] is True
    assert payload["recommended_next_stage"] == "run_second_shadow_generalization_product_candidate_ranking_v1"
    assert all((idx, DEFAULT_EMBEDDING_VERSION) in conn.embeddings for idx in range(1, 529))
    assert all((idx, FRESH_EMBEDDING_VERSION) in conn.embeddings for idx in range(1, 6))


def test_dry_run_performs_no_writes_or_openai(tmp_path: Path) -> None:
    conn = _FakeConn()
    before = dict(conn.embeddings)
    payload = _build(tmp_path, conn, dry_run=True)

    assert payload["metadata"]["dry_run"] is True
    assert payload["embedding_result"]["status"] == "dry_run_validated"
    assert payload["embedding_result"]["embeddings_written_count"] == 0
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["affected_row_counts"] == {}
    assert conn.embeddings == before


def test_rejects_hydration_when_snapshot_not_ready(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSnapshotEmbeddingsError, match="snapshot_embedding_ready"):
        build_ml_shadow_scorer_second_snapshot_embeddings_payload(
            **_paths(tmp_path, hydration=_hydration_payload(ready=False)),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_wrong_hydration_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSnapshotEmbeddingsError, match="recommended_next_stage"):
        build_ml_shadow_scorer_second_snapshot_embeddings_payload(
            **_paths(
                tmp_path,
                hydration=_hydration_payload(next_stage="run_second_shadow_generalization_product_candidate_ranking_v1"),
            ),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_snapshot_work_count_mismatch(tmp_path: Path) -> None:
    conn = _FakeConn(work_count=527)
    with pytest.raises(MLShadowScorerSecondSnapshotEmbeddingsError, match="snapshot work count mismatch"):
        _build(tmp_path, conn, dry_run=True)


def test_rejects_candidate_sha_mismatch_vs_ingest_inputs(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSnapshotEmbeddingsError, match="sha256 mismatch"):
        build_ml_shadow_scorer_second_snapshot_embeddings_payload(
            **_paths(tmp_path, ingest=_ingest_payload(plan_sha="wrong-sha")),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_fresh_hybrid_embedding_version(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSnapshotEmbeddingsError, match="distinct"):
        build_ml_shadow_scorer_second_snapshot_embeddings_payload(
            **_paths(tmp_path),
            embedding_version=FRESH_EMBEDDING_VERSION,
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_hosted_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSnapshotEmbeddingsError, match="hosted production"):
        build_ml_shadow_scorer_second_snapshot_embeddings_payload(
            **_paths(tmp_path),
            database_url="postgresql://user:pass@project.neon.tech/db",
            dry_run=True,
            repo_root=tmp_path,
        )


def test_sql_write_report_only_embeddings_written(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn, mock_embeddings=True)
    sql = "\n".join(conn.sql).lower()

    assert payload["sql_write_report"]["allowed_tables"] == ["embeddings"]
    assert payload["sql_write_report"]["affected_row_counts"] == {"embeddings": 528}
    assert payload["sql_write_report"]["ranking_runs_written"] is False
    assert payload["sql_write_report"]["paper_scores_written"] is False
    assert payload["sql_write_report"]["embeddings_written"] is True
    assert payload["sql_write_report"]["source_snapshot_versions_written"] is False
    assert payload["sql_write_report"]["ingest_runs_written"] is False
    assert payload["sql_write_report"]["works_written"] is False
    assert payload["sql_write_report"]["production_tables_modified"] is False
    assert "insert into source_snapshot_versions" not in sql
    assert "insert into ingest_runs" not in sql
    assert "update works" not in sql
    assert "ranking_runs" not in sql
    assert "paper_scores" not in sql


def test_shadow_prod_runtime_flags_false(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn, mock_embeddings=True)

    assert payload["metadata"]["runtime_implementation_authorized"] is False
    assert payload["metadata"]["online_shadow_execution_enabled"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False
    assert payload["shadow_and_production_blockers"]["runtime_implementation_authorized"] is False


def test_missing_openai_key_fails_without_mock_or_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENAI_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    with (
        patch.object(embed_mod.psycopg, "connect", return_value=conn),
        patch.object(corpus_embed.psycopg, "connect", return_value=conn),
    ):
        with pytest.raises(MLShadowScorerSecondSnapshotEmbeddingsError, match=OPENAI_API_KEY_ENV):
            build_ml_shadow_scorer_second_snapshot_embeddings_payload(
                **_paths(tmp_path),
                database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
                repo_root=tmp_path,
            )


def test_cli_writes_json_and_markdown_with_mock_embeddings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENAI_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    monkeypatch.setattr(embed_mod.psycopg, "connect", lambda *args, **kwargs: conn)
    monkeypatch.setattr(corpus_embed.psycopg, "connect", lambda *args, **kwargs: conn)
    paths = _paths(tmp_path)
    out_json = tmp_path / "embeddings.json"
    out_md = tmp_path / "embeddings.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-snapshot-embeddings",
        "--second-snapshot-hydration",
        str(paths["second_snapshot_hydration_path"]),
        "--second-candidate-plan-ingest",
        str(paths["second_candidate_plan_ingest_path"]),
        "--second-hybrid-candidate-plan",
        str(paths["second_hybrid_candidate_plan_path"]),
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--mock-embeddings",
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
    assert payload["embedding_result"]["embedding_dimensions"] == 1536
    assert payload["metadata"]["embedding_version"] == DEFAULT_EMBEDDING_VERSION
    assert "Not Ranking / Not Learned Probability / Not Shadow / Not Production" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_shape() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_snapshot_embeddings.py").read_text(
        encoding="utf-8"
    ).lower()
    import_lines = "\n".join(
        line.strip()
        for line in module_source.splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    cli_source = read_cli_parser_source(PACKAGE_ROOT)
    start = cli_source.index('"ml-shadow-scorer-second-snapshot-embeddings"')
    end = cli_source.index('"ml-fresh-hybrid-product-candidate-ranking"', start)
    cli_block = cli_source[start:end]

    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert "openalex" not in import_lines
    assert "ml_shadow_scorer_v1" not in import_lines
    assert "ml_hybrid_validation" not in import_lines
    assert "--database-url" in cli_block
    assert "--embedding-version" in cli_block
    assert "fresh-hybrid-text-embedding-v1" not in cli_block
