"""Tests for the production-scoped live execution shadow pilot run."""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Mapping

import pytest

from pipeline import cli as cli_module
from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline import ml_shadow_scorer_production_scoped_shadow_live_execution_pilot as live_execution_module
from pipeline.ml_shadow_scorer_online_shadow_runtime import FEATURE_FLAG
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_EXECUTION_GRANT_NEXT_STAGE,
    POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE,
    apply_production_scoped_shadow_live_execution_pilot_run,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_live_execution_pilot import (
    MLShadowScorerProductionScopedShadowLiveExecutionPilotError,
    run_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot,
)
from pipeline.shadow_write_path_guards import PROD_SCOPED_SHADOW_ROOT

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]
LOCAL_DATABASE_URL = "postgresql://research_radar:research_radar@localhost:5432/research_radar"

FIXTURE_RELS = {
    "production_scoped_bundle": "docs/audit/bundles/production-scoped-shadow-v1/bundle.json",
    "production_scoped_bundle_md": "docs/audit/bundles/production-scoped-shadow-v1/bundle.md",
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
        if not src.exists():
            continue
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _set_path(payload: dict[str, Any], dotted_path: str, value: Any) -> None:
    current: dict[str, Any] = payload
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value


def _prepare_rev14_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 18:
        payload = bundle_module._without_flag_enablement_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 17:
        payload = bundle_module._without_flag_enablement_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 16:
        payload = bundle_module._without_live_execution_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] == 15:
        payload = bundle_module._without_live_execution_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] != 14:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 14, 15, 16, 17, or 18, got {payload['metadata']['bundle_revision']}"
        )
    _write_json(bundle_path, payload)


def _prepare_rev15_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 18:
        payload = bundle_module._without_flag_enablement_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 17:
        payload = bundle_module._without_flag_enablement_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 16:
        payload = bundle_module._without_live_execution_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] != 15:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 15, 16, 17, or 18, got {payload['metadata']['bundle_revision']}"
        )
    _write_json(bundle_path, payload)


def _prepare_rev16_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 18:
        payload = bundle_module._without_flag_enablement_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 17:
        payload = bundle_module._without_flag_enablement_request_payload(payload)
    if payload["metadata"]["bundle_revision"] != 16:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 16, 17, or 18, got {payload['metadata']['bundle_revision']}"
        )
    _write_json(bundle_path, payload)


def _prepare_rev17_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 18:
        payload = bundle_module._without_flag_enablement_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] != 17:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 17 or 18, got {payload['metadata']['bundle_revision']}"
        )
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev17_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("flag-enablement-grant-template"))
    _prepare_rev17_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


@pytest.fixture(scope="module")
def rev16_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("flag-enablement-request-template"))
    _prepare_rev16_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


@pytest.fixture(scope="module")
def rev15_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("live-execution-pilot-review-template"))
    _prepare_rev15_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


@pytest.fixture(scope="module")
def rev14_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("live-execution-pilot-template"))
    _prepare_rev14_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _live_rows(root: Path, *, count: int = 528) -> list[dict[str, Any]]:
    payload = _load(_fixture(root, "learned_probability"))
    rows = []
    for index, row in enumerate(payload["candidate_work_scores"][:count], start=1):
        rows.append(
            {
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
        )
    return rows


def _patch_fake_db(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]) -> FakeConnection:
    conn = FakeConnection(rows)
    monkeypatch.setattr(
        live_execution_module,
        "_assert_live_read_only_database_url",
        lambda _url: {
            "database_target_redacted": "postgresql://research_radar:***@localhost:5432/research_radar",
            "local_database_url_confirmed": True,
            "read_only_contract": "SELECT-only queries; no database mutations",
        },
    )
    monkeypatch.setattr(live_execution_module, "_connect_readonly", lambda _url: conn)
    return conn


def _run_live_execution_pilot(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    bundle_path: Path,
    rows: list[dict[str, Any]] | None = None,
    pilot_run_id: str = "prod-live-exec-happy",
    update_bundle: bool = True,
) -> dict[str, Any]:
    _patch_fake_db(monkeypatch, rows if rows is not None else _live_rows(root))
    return run_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        database_url=LOCAL_DATABASE_URL,
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
        confirm_live_execution_pilot=True,
        confirm_live_read_only_prod_source_reads=True,
        generated_at="2026-05-30T18:00:00Z",
    )


def _valid_slice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    template_root: Path,
    *,
    run_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    root = _copy_template_repo(template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    result = _run_live_execution_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id=run_id,
        update_bundle=False,
    )
    return before, result["execution"]


def test_default_pilot_run_id_uses_prod_live_exec_prefix() -> None:
    run_id = live_execution_module._default_pilot_run_id("2026-05-30T18:00:00Z")

    assert run_id == "prod-live-exec-rank-83787b91ef-20260530T180000Z"


def test_requires_both_confirmations_before_database_runtime_artifacts_or_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = bundle_path.read_text(encoding="utf-8")

    def forbidden_connect(_url: str) -> Any:
        raise AssertionError("database connection should not be opened without both confirmations")

    monkeypatch.setattr(live_execution_module, "_connect_readonly", forbidden_connect)

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotError, match="confirm-live-execution"):
        run_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="prod-live-exec-missing-confirm",
            repo_root=root,
            confirm_live_read_only_prod_source_reads=True,
        )
    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotError, match="confirm-live-read-only"):
        run_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="prod-live-exec-missing-confirm",
            repo_root=root,
            confirm_live_execution_pilot=True,
        )

    assert bundle_path.read_text(encoding="utf-8") == before
    assert not (root / PROD_SCOPED_SHADOW_ROOT / "prod-live-exec-missing-confirm").exists()


def test_happy_path_mocked_live_reads_write_four_artifacts_and_file_revision_15(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    result = _run_live_execution_pilot(root, monkeypatch, bundle_path=bundle_path)
    updated = _load(bundle_path)
    run_dir = root / PROD_SCOPED_SHADOW_ROOT / "prod-live-exec-happy"

    assert result["prod_scoped_shadow_live_execution_pilot_passed"] is True
    assert updated["metadata"]["bundle_revision"] == 15
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["review"] == before["review"]
    for key, value in before["execution"].items():
        assert updated["execution"][key] == value
    for key in (
        "grant_decision",
        "granted_scope",
        "request_decision",
        "requested_scope",
        "live_read_only_grant_decision",
        "live_read_only_granted_scope",
        "live_execution_request_decision",
        "live_execution_requested_scope",
        "live_execution_grant_decision",
        "live_execution_granted_scope",
    ):
        assert updated["authorization"][key] == before["authorization"][key]
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["prod_scoped_shadow_live_execution_pilot_executed"] is True
    assert updated["posture"]["prod_scoped_shadow_live_execution_pilot_passed"] is True
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["writes_performed"] is False
    assert updated["runtime_writes_performed"] is False
    assert updated["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE

    live_run = updated["execution"]["live_execution_pilot_run"]
    assert live_run["pilot_surface"] == "bounded_live_execution_prod_scoped_pilot"
    assert live_run["input_join_summary"]["joined_candidate_count"] == 528
    assert live_run["runtime_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]
    assert live_run["runtime_drill"]["pilot"]["shadow_row_count"] == 528
    assert live_run["incomplete_coverage_drill"]["status"] == "skipped_incomplete_coverage"
    assert live_run["incomplete_coverage_drill"]["shadow_row_count"] == 0
    assert live_run["incomplete_coverage_drill"]["writes_performed"] is False
    assert live_run["input_provenance"]["reread_approved_production_sources"] is True
    assert live_run["input_provenance"]["fixture_ranking_version_used"] is False
    assert live_run["live_execution_scope"]["prod_scoped_shadow_execution_authorized"] is False
    assert set(LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS) == set(live_run["pass_fail_evaluation"]["checks"])
    assert all(live_run["pass_fail_evaluation"]["checks"].values())
    assert [record["relative_path"] for record in live_run["files_written"]] == [
        "manifest.json",
        "shadow_rows.jsonl",
        "observability.json",
        "write_counts.json",
    ]
    assert set(path.name for path in run_dir.iterdir()) == {
        "manifest.json",
        "shadow_rows.jsonl",
        "observability.json",
        "write_counts.json",
    }
    assert live_run["write_count_verification"]["write_counts_by_isolated_target"]["isolated_prod_scoped_audit_artifacts"] == 4
    assert live_run["write_count_verification"]["write_counts_by_isolated_target"]["prod_scoped_shadow_tables"] == 0

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    assert verified["verification_mode"] == "post_live_execution_pilot_run"


def test_compositional_verify_strips_rev15_overlay_back_to_rev14(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_live_execution_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-live-exec-strip")
    payload = _load(bundle_path)

    stripped = bundle_module._without_live_execution_pilot_run_payload(payload)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_live_execution_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert result["verification_mode"] == "post_live_execution_grant"
    assert stripped["metadata"]["bundle_revision"] == 14
    assert stripped["recommended_next_stage"] == POST_LIVE_EXECUTION_GRANT_NEXT_STAGE


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("metadata.bundle_revision", 13, "bundle_revision"),
        ("recommended_next_stage", "wrong_stage", "recommended_next_stage"),
    ],
)
def test_rejects_wrong_revision_or_next_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
    path: str,
    value: Any,
    message: str,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotError, match=message):
        _run_live_execution_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-live-exec-bad-precondition",
        )


def test_rejects_missing_live_execution_grant_or_authorized_false(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev14_template_root,
        run_id="prod-live-exec-valid-slice-grant",
    )

    missing_grant = json.loads(json.dumps(before))
    missing_grant["authorization"].pop("live_execution_grant_decision")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_execution_grant_decision"):
        apply_production_scoped_shadow_live_execution_pilot_run(missing_grant, pilot_slice)

    not_authorized = json.loads(json.dumps(before))
    not_authorized["authorization"]["prod_scoped_shadow_live_execution_authorized"] = False
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_execution_authorized"):
        apply_production_scoped_shadow_live_execution_pilot_run(not_authorized, pilot_slice)


def test_rejects_existing_live_execution_pilot_run_or_double_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_live_execution_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-live-exec-double")

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotError, match="already been filed"):
        _run_live_execution_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-live-exec-double-again",
        )


def test_rejects_failed_pass_fail_and_preexisting_execution_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev14_template_root,
        run_id="prod-live-exec-valid-slice-passfail",
    )

    failed = json.loads(json.dumps(pilot_slice))
    failed["pass_fail_evaluation"]["overall_passed"] = False
    failed["pass_fail_evaluation"]["failed_checks"] = ["expected_files_recorded"]
    failed["pass_fail_evaluation"]["checks"]["expected_files_recorded"] = False
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="overall_passed"):
        apply_production_scoped_shadow_live_execution_pilot_run(before, failed)

    preflagged = json.loads(json.dumps(before))
    preflagged["execution"]["prod_scoped_shadow_live_execution_pilot_executed"] = True
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already true"):
        apply_production_scoped_shadow_live_execution_pilot_run(preflagged, pilot_slice)


@pytest.mark.parametrize(
    "path",
    [
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "posture.writes_performed",
        "posture.runtime_writes_performed",
    ],
)
def test_rejects_global_execution_default_api_user_visible_and_write_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
    path: str,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev14_template_root,
        run_id=f"prod-live-exec-valid-slice-{path.split('.')[-1].replace('_', '-')}",
    )
    payload = json.loads(json.dumps(before))
    _set_path(payload, path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=path.split(".")[-1]):
        apply_production_scoped_shadow_live_execution_pilot_run(payload, pilot_slice)


def test_rejects_incomplete_main_join_without_bundle_advance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = bundle_path.read_text(encoding="utf-8")

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotError, match="incomplete live read coverage"):
        _run_live_execution_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            rows=_live_rows(root, count=527),
            pilot_run_id="prod-live-exec-incomplete-main",
        )

    assert bundle_path.read_text(encoding="utf-8") == before
    assert not (root / PROD_SCOPED_SHADOW_ROOT / "prod-live-exec-incomplete-main").exists()


def test_env_restore_runtime_order_and_flag_only_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    monkeypatch.setenv(FEATURE_FLAG, "original-off")
    original_runtime = live_execution_module.run_ml_shadow_scorer_v1_online_shadow_runtime
    calls: list[tuple[str | None, int]] = []

    def wrapped_runtime(candidate_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
        calls.append((os.environ.get(FEATURE_FLAG), len(candidate_rows)))
        return original_runtime(candidate_rows)

    monkeypatch.setattr(live_execution_module, "run_ml_shadow_scorer_v1_online_shadow_runtime", wrapped_runtime)
    result = _run_live_execution_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="prod-live-exec-env-order",
        update_bundle=False,
    )

    assert os.environ[FEATURE_FLAG] == "original-off"
    assert calls == [(None, 0), ("true", 528), (None, 0), ("true", 1)]
    assert result["execution"]["runtime_drill"]["process_scoped_runtime_flag_only"] is True
    assert result["execution"]["runtime_drill"]["environment_restored"] is True


def test_rejects_forbidden_write_counts_and_fixture_ranking_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _patch_fake_db(monkeypatch, _live_rows(root))
    original_counts = live_execution_module._write_counts_by_isolated_target

    def bad_counts(*, file_count: int = 4) -> dict[str, int]:
        counts = original_counts(file_count=file_count)
        counts["prod_scoped_shadow_tables"] = 1
        return counts

    monkeypatch.setattr(live_execution_module, "_write_counts_by_isolated_target", bad_counts)
    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotError, match="forbidden prod-scoped write targets"):
        run_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="prod-live-exec-bad-writes",
            repo_root=root,
            confirm_live_execution_pilot=True,
            confirm_live_read_only_prod_source_reads=True,
        )
    monkeypatch.setattr(live_execution_module, "_write_counts_by_isolated_target", original_counts)

    before, pilot_slice = _valid_slice(
        tmp_path / "ranking-case",
        monkeypatch,
        rev14_template_root,
        run_id="prod-live-exec-valid-slice-ranking",
    )
    bad_ranking = json.loads(json.dumps(pilot_slice))
    bad_ranking["live_source_reads"]["ranking_run"]["ranking_version"] = "test-fixture-ranking-version"
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="ranking_version"):
        apply_production_scoped_shadow_live_execution_pilot_run(before, bad_ranking)


def test_verifier_passes_without_local_shadow_run_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_live_execution_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-live-exec-no-files")
    shutil.rmtree(root / PROD_SCOPED_SHADOW_ROOT / "prod-live-exec-no-files")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_pilot_run_filed=True,
        verify_local_pilot_files=True,
    )

    assert result["verification_mode"] == "post_live_execution_pilot_run"


def test_cli_smoke_run_with_confirmation_then_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev14_template_root: Path,
) -> None:
    root = _copy_template_repo(rev14_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _patch_fake_db(monkeypatch, _live_rows(root))
    old_argv = sys.argv
    try:
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-live-execution-pilot-run",
            "--bundle",
            str(bundle_path),
            "--confirm-live-execution-pilot",
            "--confirm-live-read-only-prod-source-reads",
            "--database-url",
            LOCAL_DATABASE_URL,
            "--pilot-run-id",
            "prod-live-exec-cli",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
            "--bundle",
            str(bundle_path),
            "--expect-live-execution-pilot-run-filed",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
    finally:
        sys.argv = old_argv

    assert _load(bundle_path)["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE


def test_committed_bundle_matches_post_live_execution_pilot_run_if_present() -> None:
    committed = REPO_ROOT / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] >= 16:
        pytest.skip("committed production-scoped-shadow bundle advanced to rev16 review")
    if payload["metadata"]["bundle_revision"] < 15:
        pytest.skip("committed production-scoped-shadow bundle not advanced to rev15 yet")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_live_execution_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 15
    assert result["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE
