"""Tests for the production-scoped live read-only shadow pilot run."""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Mapping

import pytest

from pipeline import cli as cli_module
from pipeline import ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot as live_pilot_module
from pipeline.ml_shadow_scorer_online_shadow_runtime import FEATURE_FLAG
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle,
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    plan_ml_shadow_scorer_production_scoped_shadow_bundle,
    prove_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    write_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot import (
    MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError,
    run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot import (
    run_ml_shadow_scorer_production_scoped_shadow_pilot,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_harness import (
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_harness_review import (
    review_ml_shadow_scorer_production_scoped_shadow_pilot_harness,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_review import (
    review_ml_shadow_scorer_production_scoped_shadow_pilot,
)
from pipeline.shadow_write_path_guards import PROD_SCOPED_SHADOW_ROOT

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]
LOCAL_DATABASE_URL = "postgresql://research_radar:research_radar@localhost:5432/research_radar"

FIXTURE_RELS = {
    "production_readiness_bundle": "docs/audit/bundles/production-readiness-v1/bundle.json",
    "production_readiness_bundle_md": "docs/audit/bundles/production-readiness-v1/bundle.md",
    "production_readiness_criteria": "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json",
    "phase2_bundle": "docs/audit/bundles/phase2-v1/bundle.json",
    "phase2_bundle_md": "docs/audit/bundles/phase2-v1/bundle.md",
    "online_shadow_policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
    "execution_authorization_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "phase2_write_mode_plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "phase2_write_mode_proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "generalization_audit_gates": "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json",
    "production_readiness_plan": "docs/audit/ml-production-readiness-plan-v1.json",
    "production_readiness_plan_md": "docs/audit/ml-production-readiness-plan-v1.md",
    "phase2_write_authorization_request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "phase2_write_authorization_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "phase1_review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "learned_probability": "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
    "offline_audit_embedding_scorer": "docs/audit/ml-offline-audit-embedding-scorer-v2.json",
    "generalization_audit": "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
}


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self.query = ""

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...]) -> None:
        self.query = query
        self.conn.executed.append((query, params))

    def fetchone(self) -> dict[str, Any] | None:
        if "FROM ranking_runs" not in self.query:
            return None
        return {
            "ranking_run_id": "rank-83787b91ef",
            "status": "succeeded",
            "ranking_version": "shadow-generalization-product-candidate-ranking-v1",
            "corpus_snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
            "embedding_version": "shadow-generalization-text-embedding-v1",
        }

    def fetchall(self) -> list[dict[str, Any]]:
        if "FROM paper_scores" not in self.query:
            return []
        return list(self.conn.rows)


class FakeConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.closed = False

    def cursor(self, **_kwargs: Any) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return root


def _copy_template_repo(template_root: Path, tmp_path: Path) -> Path:
    root = tmp_path / "case-repo"
    shutil.copytree(template_root, root)
    return root


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _optional_kwargs(root: Path) -> dict[str, Path]:
    return {
        "execution_authorization_grant_path": _fixture(root, "execution_authorization_grant"),
        "phase2_write_mode_plan_path": _fixture(root, "phase2_write_mode_plan"),
        "phase2_write_mode_proof_path": _fixture(root, "phase2_write_mode_proof"),
        "generalization_audit_gates_path": _fixture(root, "generalization_audit_gates"),
    }


def _write_rev10_bundle(root: Path) -> Path:
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    markdown_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md"
    write_ml_shadow_scorer_production_scoped_shadow_bundle(
        production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
        output_path=bundle_path,
        markdown_output_path=markdown_path,
        repo_root=root,
        **_optional_kwargs(root),
    )
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id="live-pilot-proof",
        repo_root=root,
    )
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent review for bounded pilot",
        repo_root=root,
    )
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        pilot_run_id="live-pilot-harness",
        repo_root=root,
    )
    review_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        reviewer="Harness Reviewer",
        repo_root=root,
    )
    run_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        learned_probability_artifact_path=_fixture(root, "learned_probability"),
        second_surface_generalization_audit_path=_fixture(root, "generalization_audit"),
        pilot_run_id="live-pilot-audit-artifact",
        repo_root=root,
    )
    review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        reviewer="Pilot Reviewer",
        repo_root=root,
        reviewed_at="2026-05-29T21:30:00Z",
    )
    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Live Requester",
        repo_root=root,
    )
    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live read-only review",
        repo_root=root,
    )
    return bundle_path


@pytest.fixture(scope="module")
def rev10_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("live-read-only-pilot-template"))
    _write_rev10_bundle(root)
    return root


def _live_rows(root: Path, *, count: int = 528, include_label: bool = False) -> list[dict[str, Any]]:
    payload = _load(_fixture(root, "learned_probability"))
    rows = []
    for index, row in enumerate(payload["candidate_work_scores"][:count], start=1):
        out = {
            "ranking_run_id": row["ranking_run_id"],
            "internal_work_id": index,
            "recommendation_family": row["family"],
            "final_score": row["final_score"],
            "openalex_id": f"https://openalex.org/{row['canonical_openalex_work_id']}",
            "title": row["title"],
            "year": row["year"],
            "corpus_snapshot_version": row["corpus_snapshot_version"],
            "observed_embedding_version": row["embedding_version"],
            "vector": [0.0] * 1536,
        }
        if include_label:
            out["good_or_acceptable"] = True
        rows.append(out)
    return rows


def _patch_fake_db(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]) -> FakeConnection:
    conn = FakeConnection(rows)
    monkeypatch.setattr(
        live_pilot_module,
        "_assert_live_read_only_database_url",
        lambda _url: {
            "database_target_redacted": "postgresql://research_radar:***@localhost:5432/research_radar",
            "local_database_url_confirmed": True,
            "read_only_contract": "SELECT-only queries; no database mutations",
        },
    )
    monkeypatch.setattr(live_pilot_module, "_connect_readonly", lambda _url: conn)
    return conn


def _run_live_pilot(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    bundle_path: Path | None = None,
    rows: list[dict[str, Any]] | None = None,
    pilot_run_id: str = "live-read-happy",
    update_bundle: bool = True,
) -> dict[str, Any]:
    _patch_fake_db(monkeypatch, rows if rows is not None else _live_rows(root))
    return run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
        bundle_path=bundle_path or _write_rev10_bundle(root),
        database_url=LOCAL_DATABASE_URL,
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
        confirm_live_read_only_prod_source_reads=True,
        generated_at="2026-05-29T22:45:00Z",
    )


def test_requires_confirmation_before_database_runtime_artifacts_or_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    before = bundle_path.read_text(encoding="utf-8")

    def forbidden_connect(_url: str) -> Any:
        raise AssertionError("database connection should not be opened without confirmation")

    monkeypatch.setattr(live_pilot_module, "_connect_readonly", forbidden_connect)

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError, match="confirm-live-read-only"):
        run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="missing-confirm",
            repo_root=root,
        )

    assert bundle_path.read_text(encoding="utf-8") == before
    assert not (root / PROD_SCOPED_SHADOW_ROOT / "missing-confirm").exists()


def test_happy_path_mocked_live_reads_write_four_artifacts_and_file_revision_11(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    before = _load(bundle_path)

    result = _run_live_pilot(root, monkeypatch, bundle_path=bundle_path)
    updated = _load(bundle_path)
    run_dir = root / PROD_SCOPED_SHADOW_ROOT / "live-read-happy"

    assert result["prod_scoped_shadow_live_read_only_pilot_passed"] is True
    assert updated["metadata"]["bundle_revision"] == 11
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["execution"]["pilot_harness"] == before["execution"]["pilot_harness"]
    assert updated["execution"]["pilot_run"] == before["execution"]["pilot_run"]
    assert updated["review"] == before["review"]
    assert updated["authorization"]["grant_decision"] == before["authorization"]["grant_decision"]
    assert updated["authorization"]["granted_scope"] == before["authorization"]["granted_scope"]
    assert updated["authorization"]["request_decision"] == before["authorization"]["request_decision"]
    assert updated["authorization"]["requested_scope"] == before["authorization"]["requested_scope"]
    assert updated["authorization"]["live_read_only_grant_decision"] == before["authorization"]["live_read_only_grant_decision"]
    assert updated["authorization"]["live_read_only_granted_scope"] == before["authorization"]["live_read_only_granted_scope"]
    assert updated["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["live_prod_source_reads_performed"] is True
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["recommended_next_stage"] == "review_production_scoped_online_shadow_live_read_only_pilot_v1"

    live_run = updated["execution"]["live_read_only_pilot_run"]
    assert live_run["pilot_surface"] == "bounded_live_read_only_prod_scoped_pilot"
    assert live_run["live_prod_source_reads_performed"] is True
    assert live_run["input_join_summary"]["joined_candidate_count"] == 528
    assert live_run["input_join_summary"]["runtime_row_count"] == 528
    assert live_run["live_source_reads"]["approved_tables"] == [
        "ranking_runs",
        "paper_scores",
        "works",
        "embeddings",
    ]
    assert live_run["live_source_reads"]["labels_not_used_for_scoring"] is True
    assert live_run["live_source_reads"]["refit_training_performed"] is False
    assert live_run["live_source_reads"]["embedding_generation_performed"] is False
    assert live_run["live_source_reads"]["label_ingest_performed"] is False
    derivation = live_run["live_source_reads"]["audit_embedding_probability_derivation"]
    assert derivation["source"] == "computed_from_live_embedding_vectors_with_frozen_scorer"
    assert derivation["live_embedding_vectors_used"] is True
    assert derivation["frozen_candidate_score_artifact_used_as_primary_input"] is False
    assert live_run["runtime_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]
    assert set(path.name for path in run_dir.iterdir()) == {
        "manifest.json",
        "shadow_rows.jsonl",
        "observability.json",
        "write_counts.json",
    }
    assert [record["relative_path"] for record in live_run["files_written"]] == [
        "manifest.json",
        "shadow_rows.jsonl",
        "observability.json",
        "write_counts.json",
    ]
    assert live_run["write_count_verification"]["write_counts_by_isolated_target"]["isolated_prod_scoped_audit_artifacts"] == 4
    assert live_run["write_count_verification"]["write_counts_by_isolated_target"]["prod_scoped_shadow_tables"] == 0
    assert live_run["observability_summary"]["live_prod_source_reads_performed"] is True
    assert updated["execution"]["pilot_harness"]["live_prod_source_reads_performed"] is False
    assert updated["execution"]["pilot_run"]["live_prod_source_reads_performed"] is False

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_read_only_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    assert verified["verification_mode"] == "post_live_read_only_pilot_run"


def test_rejects_frozen_artifact_arguments_on_live_read_only_cli(
    tmp_path: Path,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"

    with pytest.raises(SystemExit):
        old_argv = sys.argv
        try:
            sys.argv = [
                "pipeline.cli",
                "ml-shadow-scorer-production-scoped-shadow-live-read-only-pilot-run",
                "--bundle",
                str(bundle_path),
                "--confirm-live-read-only-prod-source-reads",
                "--learned-probability-artifact",
                str(_fixture(root, "learned_probability")),
            ]
            cli_module.main()
        finally:
            sys.argv = old_argv


def test_rejects_database_unavailable_without_revision_11(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    before = bundle_path.read_text(encoding="utf-8")
    monkeypatch.setattr(live_pilot_module, "_assert_live_read_only_database_url", lambda _url: {})
    monkeypatch.setattr(live_pilot_module, "_connect_readonly", lambda _url: (_ for _ in ()).throw(RuntimeError("down")))

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError, match="database unavailable"):
        run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="db-down",
            repo_root=root,
            confirm_live_read_only_prod_source_reads=True,
        )

    assert bundle_path.read_text(encoding="utf-8") == before
    assert not (root / PROD_SCOPED_SHADOW_ROOT / "db-down").exists()


def test_rejects_incomplete_coverage_without_revision_11(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    before = bundle_path.read_text(encoding="utf-8")
    _patch_fake_db(monkeypatch, _live_rows(root, count=527))

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError, match="incomplete live read coverage"):
        run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="incomplete-coverage",
            repo_root=root,
            confirm_live_read_only_prod_source_reads=True,
        )

    assert bundle_path.read_text(encoding="utf-8") == before
    assert not (root / PROD_SCOPED_SHADOW_ROOT / "incomplete-coverage").exists()


def test_rejects_label_fields_bad_pilot_id_and_forbidden_write_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError, match="path traversal|letters"):
        run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="../bad",
            repo_root=root,
            confirm_live_read_only_prod_source_reads=True,
        )

    _patch_fake_db(monkeypatch, _live_rows(root, include_label=True))
    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError, match="labels or holdout"):
        run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="label-field",
            repo_root=root,
            confirm_live_read_only_prod_source_reads=True,
        )

    _patch_fake_db(monkeypatch, _live_rows(root))
    original_counts = live_pilot_module._write_counts_by_isolated_target

    def bad_counts(*, file_count: int = 4) -> dict[str, int]:
        counts = original_counts(file_count=file_count)
        counts["prod_scoped_shadow_tables"] = 1
        return counts

    monkeypatch.setattr(live_pilot_module, "_write_counts_by_isolated_target", bad_counts)
    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError, match="forbidden prod-scoped write targets"):
        run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="bad-write-counts",
            repo_root=root,
            confirm_live_read_only_prod_source_reads=True,
        )
    assert _load(bundle_path)["metadata"]["bundle_revision"] == 10


def test_env_restore_runtime_order_and_fail_closed_pass_fail_false(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    monkeypatch.setenv(FEATURE_FLAG, "original-off")
    original_runtime = live_pilot_module.run_ml_shadow_scorer_v1_online_shadow_runtime
    calls: list[tuple[str | None, int]] = []

    def wrapped_runtime(candidate_rows: list[Mapping[str, Any]], *, env: Mapping[str, str] | None = None) -> dict[str, Any]:
        calls.append((os.environ.get(FEATURE_FLAG), len(candidate_rows)))
        return original_runtime(candidate_rows, env=env)

    monkeypatch.setattr(live_pilot_module, "run_ml_shadow_scorer_v1_online_shadow_runtime", wrapped_runtime)
    result = _run_live_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="env-order",
        update_bundle=False,
    )

    assert os.environ[FEATURE_FLAG] == "original-off"
    assert calls == [(None, 0), ("true", 528), (None, 0)]
    assert result["execution"]["runtime_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]

    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    before = bundle_path.read_text(encoding="utf-8")
    _patch_fake_db(monkeypatch, _live_rows(root))

    def failed_pass_fail(**kwargs: Any) -> dict[str, Any]:
        passed = live_pilot_module._build_pass_fail_original(**kwargs)
        passed["overall_passed"] = False
        passed["checks"]["expected_files_recorded"] = False
        passed["failed_checks"] = ["expected_files_recorded"]
        return passed

    monkeypatch.setattr(live_pilot_module, "_build_pass_fail_original", live_pilot_module._build_pass_fail, raising=False)
    monkeypatch.setattr(live_pilot_module, "_build_pass_fail", failed_pass_fail)
    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError, match="failed checks"):
        run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="failed-pass-fail",
            repo_root=root,
            confirm_live_read_only_prod_source_reads=True,
        )
    assert bundle_path.read_text(encoding="utf-8") == before


def test_verifier_passes_without_local_shadow_run_files_and_rejects_bad_live_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    _run_live_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="optional-files")
    shutil.rmtree(root / PROD_SCOPED_SHADOW_ROOT / "optional-files")

    verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_read_only_pilot_run_filed=True,
        verify_local_pilot_files=True,
    )

    payload = _load(bundle_path)
    payload["execution"]["pilot_run"]["live_prod_source_reads_performed"] = True
    bundle_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_prod_source_reads_performed"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )

    payload = _load(bundle_path)
    payload["execution"]["pilot_run"]["live_prod_source_reads_performed"] = False
    payload["execution"]["live_read_only_pilot_run"]["live_source_reads"]["ranking_run"][
        "ranking_version"
    ] = "test-ranking-version"
    bundle_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="ranking_version"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )


def test_bundle_apply_verify_markdown_are_paperwork_only() -> None:
    source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_scoped_shadow_bundle.py").read_text(
        encoding="utf-8"
    )
    for forbidden in ("psycopg", "run_ml_shadow_scorer_v1_online_shadow_runtime", "_connect_readonly"):
        assert forbidden not in source
    assert "LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS" not in source


def test_cli_smoke_run_with_confirmation_then_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev10_template_root: Path,
) -> None:
    root = _copy_template_repo(rev10_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    _patch_fake_db(monkeypatch, _live_rows(root))
    old_argv = sys.argv
    try:
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-live-read-only-pilot-run",
            "--bundle",
            str(bundle_path),
            "--confirm-live-read-only-prod-source-reads",
            "--database-url",
            LOCAL_DATABASE_URL,
            "--pilot-run-id",
            "cli-live-read",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
            "--bundle",
            str(bundle_path),
            "--expect-live-read-only-pilot-run-filed",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
    finally:
        sys.argv = old_argv

    assert _load(bundle_path)["recommended_next_stage"] == (
        "review_production_scoped_online_shadow_live_read_only_pilot_v1"
    )
