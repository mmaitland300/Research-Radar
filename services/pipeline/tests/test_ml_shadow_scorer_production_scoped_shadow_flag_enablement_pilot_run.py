"""Tests for the production-scoped flag enablement shadow pilot run."""

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
from pipeline import ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot as flag_enablement_pilot_module
from pipeline.ml_shadow_scorer_online_shadow_runtime import FEATURE_FLAG
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FLAG_ENABLEMENT_PILOT_RUN_EXPECTED_FILES,
    FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE,
    POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE,
    apply_production_scoped_shadow_flag_enablement_authorization_grant,
    apply_production_scoped_shadow_flag_enablement_pilot_run,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot import (
    MLShadowScorerProductionScopedShadowFlagEnablementPilotError,
    run_ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot,
)
from pipeline.shadow_write_path_guards import PROD_SCOPED_SHADOW_ROOT
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    LOCAL_DATABASE_URL,
    REPO_ROOT,
    FakeConnection,
    _copy_fixture_repo,
    _copy_template_repo,
    _live_rows,
    _load,
    _set_path,
    _write_json,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _prepare_rev18_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 21:
        payload = bundle_module._without_production_default_api_user_visible_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 20:
        payload = bundle_module._without_flag_enablement_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] == 19:
        payload = bundle_module._without_flag_enablement_pilot_run_payload(payload)
    execution = payload.get("execution") or {}
    if execution.get("flag_enablement_pilot_run") or execution.get(
        "prod_scoped_shadow_flag_enablement_pilot_executed"
    ):
        payload = bundle_module._without_flag_enablement_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] == 18:
        if payload.get("recommended_next_stage") != POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE:
            payload = bundle_module._without_flag_enablement_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 17:
        payload = apply_production_scoped_shadow_flag_enablement_authorization_grant(
            payload,
            owner_documents_equivalent_review="owner equivalent flag enablement review",
        )
    if payload["metadata"]["bundle_revision"] != 18:
        raise AssertionError(
            "expected production-scoped bundle revision 18 grant state after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE:
        raise AssertionError(
            "expected recommended_next_stage post flag enablement grant after template preparation"
        )
    _write_json(bundle_path, payload)


def _patch_fake_db(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]) -> FakeConnection:
    conn = FakeConnection(rows)
    monkeypatch.setattr(
        flag_enablement_pilot_module,
        "_assert_live_read_only_database_url",
        lambda _url: {
            "database_target_redacted": "postgresql://research_radar:***@localhost:5432/research_radar",
            "local_database_url_confirmed": True,
            "read_only_contract": "SELECT-only queries; no database mutations",
        },
    )
    monkeypatch.setattr(flag_enablement_pilot_module, "_connect_readonly", lambda _url: conn)
    return conn


@pytest.fixture(scope="module")
def rev18_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("flag-enablement-pilot-run-template"))
    _prepare_rev18_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _run_flag_enablement_pilot(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    bundle_path: Path,
    rows: list[dict[str, Any]] | None = None,
    pilot_run_id: str = "prod-flag-enable-happy",
    update_bundle: bool = True,
) -> dict[str, Any]:
    _patch_fake_db(monkeypatch, rows if rows is not None else _live_rows(root))
    return run_ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot(
        bundle_path=bundle_path,
        database_url=LOCAL_DATABASE_URL,
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
        confirm_flag_enablement_pilot=True,
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
    result = _run_flag_enablement_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id=run_id,
        update_bundle=False,
    )
    return before, result["execution"]


def test_default_pilot_run_id_uses_prod_flag_enable_prefix() -> None:
    run_id = flag_enablement_pilot_module._default_pilot_run_id("2026-05-30T18:00:00Z")

    assert run_id == "prod-flag-enable-rank-83787b91ef-20260530T180000Z"


def test_happy_path_mocked_live_reads_write_four_artifacts_and_file_revision_19(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    result = _run_flag_enablement_pilot(root, monkeypatch, bundle_path=bundle_path)
    updated = _load(bundle_path)
    run_dir = root / PROD_SCOPED_SHADOW_ROOT / "prod-flag-enable-happy"

    assert result["prod_scoped_shadow_flag_enablement_pilot_passed"] is True
    assert updated["metadata"]["bundle_revision"] == 19
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
        "flag_enablement_request_decision",
        "flag_enablement_requested_scope",
        "flag_enablement_grant_decision",
        "flag_enablement_granted_scope",
    ):
        assert updated["authorization"][key] == before["authorization"][key]
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_flag_enablement_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["prod_scoped_shadow_flag_enablement_pilot_executed"] is True
    assert updated["posture"]["prod_scoped_shadow_flag_enablement_pilot_passed"] is True
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["writes_performed"] is False
    assert updated["runtime_writes_performed"] is False
    assert updated["recommended_next_stage"] == POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE

    flag_run = updated["execution"]["flag_enablement_pilot_run"]
    assert flag_run["pilot_surface"] == "bounded_flag_enablement_prod_scoped_pilot"
    assert flag_run["input_join_summary"]["joined_candidate_count"] == 528
    assert flag_run["runtime_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]
    assert flag_run["runtime_drill"]["pilot"]["shadow_row_count"] == 528
    assert flag_run["incomplete_coverage_drill"]["status"] == "skipped_incomplete_coverage"
    assert flag_run["flag_enablement_scope"]["prod_scoped_shadow_execution_authorized"] is False
    assert set(FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS) == set(flag_run["pass_fail_evaluation"]["checks"])
    assert all(flag_run["pass_fail_evaluation"]["checks"].values())
    assert [record["relative_path"] for record in flag_run["files_written"]] == list(
        FLAG_ENABLEMENT_PILOT_RUN_EXPECTED_FILES
    )
    assert set(path.name for path in run_dir.iterdir()) == set(FLAG_ENABLEMENT_PILOT_RUN_EXPECTED_FILES)

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_flag_enablement_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    assert verified["verification_mode"] == "post_flag_enablement_pilot_run"


def test_compositional_verify_strips_rev19_overlay_back_to_post_flag_enablement_grant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_flag_enablement_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="prod-flag-enable-strip",
    )
    payload = _load(bundle_path)

    stripped = bundle_module._without_flag_enablement_pilot_run_payload(payload)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_flag_enablement_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert result["verification_mode"] == "post_flag_enablement_grant"
    assert stripped["metadata"]["bundle_revision"] == 18
    assert stripped["recommended_next_stage"] == POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("metadata.bundle_revision", 17, "bundle_revision"),
        ("recommended_next_stage", "wrong_stage", "recommended_next_stage"),
    ],
)
def test_rejects_wrong_revision_or_next_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
    path: str,
    value: Any,
    message: str,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowFlagEnablementPilotError, match=message):
        _run_flag_enablement_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-flag-enable-bad-precondition",
        )


def test_rejects_missing_flag_enablement_grant_slices(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev18_template_root,
        run_id="prod-flag-enable-valid-slice-grant",
    )

    missing_grant = json.loads(json.dumps(before))
    missing_grant["authorization"].pop("flag_enablement_grant_decision")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="flag_enablement_grant_decision"):
        apply_production_scoped_shadow_flag_enablement_pilot_run(missing_grant, pilot_slice)

    missing_scope = json.loads(json.dumps(before))
    missing_scope["authorization"].pop("flag_enablement_granted_scope")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="flag_enablement_granted_scope"):
        apply_production_scoped_shadow_flag_enablement_pilot_run(missing_scope, pilot_slice)


def test_rejects_existing_flag_enablement_pilot_run_or_double_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_flag_enablement_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="prod-flag-enable-double",
    )

    with pytest.raises(MLShadowScorerProductionScopedShadowFlagEnablementPilotError, match="already been filed"):
        _run_flag_enablement_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-flag-enable-double-again",
        )


def test_rejects_failed_pass_fail_and_preexisting_execution_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev18_template_root,
        run_id="prod-flag-enable-valid-slice-passfail",
    )

    failed = json.loads(json.dumps(pilot_slice))
    failed["pass_fail_evaluation"]["overall_passed"] = False
    failed["pass_fail_evaluation"]["failed_checks"] = ["expected_files_recorded"]
    failed["pass_fail_evaluation"]["checks"]["expected_files_recorded"] = False
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="overall_passed"):
        apply_production_scoped_shadow_flag_enablement_pilot_run(before, failed)

    preflagged = json.loads(json.dumps(before))
    preflagged["execution"]["prod_scoped_shadow_flag_enablement_pilot_executed"] = True
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already true"):
        apply_production_scoped_shadow_flag_enablement_pilot_run(preflagged, pilot_slice)


@pytest.mark.parametrize(
    "path",
    [
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "posture.writes_performed",
        "posture.runtime_writes_performed",
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
    ],
)
def test_rejects_global_execution_default_api_user_visible_and_write_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
    path: str,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev18_template_root,
        run_id=f"prod-flag-enable-valid-slice-{path.split('.')[-1].replace('_', '-')}",
    )
    payload = json.loads(json.dumps(before))
    _set_path(payload, path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_flag_enablement_pilot_run(payload, pilot_slice)


def test_authorization_section_byte_identical_after_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before_auth = json.dumps(_load(bundle_path)["authorization"], sort_keys=True)

    _run_flag_enablement_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-flag-enable-auth")

    after_auth = json.dumps(_load(bundle_path)["authorization"], sort_keys=True)
    assert after_auth == before_auth


def test_env_restore_runtime_order_and_flag_only_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    monkeypatch.setenv(FEATURE_FLAG, "original-off")
    original_runtime = flag_enablement_pilot_module.run_ml_shadow_scorer_v1_online_shadow_runtime
    calls: list[tuple[str | None, int]] = []

    def wrapped_runtime(candidate_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
        calls.append((os.environ.get(FEATURE_FLAG), len(candidate_rows)))
        return original_runtime(candidate_rows)

    monkeypatch.setattr(
        flag_enablement_pilot_module,
        "run_ml_shadow_scorer_v1_online_shadow_runtime",
        wrapped_runtime,
    )
    result = _run_flag_enablement_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="prod-flag-enable-env-order",
        update_bundle=False,
    )

    assert os.environ[FEATURE_FLAG] == "original-off"
    assert calls == [(None, 0), ("true", 528), (None, 0), ("true", 1)]
    assert result["execution"]["runtime_drill"]["process_scoped_runtime_flag_only"] is True
    assert result["execution"]["runtime_drill"]["environment_restored"] is True


def test_path_isolation_rejects_harness_prefix_and_requires_prod_flag_enable_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _patch_fake_db(monkeypatch, _live_rows(root))

    with pytest.raises(MLShadowScorerProductionScopedShadowFlagEnablementPilotError, match="prod-flag-enable"):
        run_ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="prod-live-exec-wrong-prefix",
            repo_root=root,
            confirm_flag_enablement_pilot=True,
            confirm_live_read_only_prod_source_reads=True,
        )
    with pytest.raises(MLShadowScorerProductionScopedShadowFlagEnablementPilotError, match="harness"):
        run_ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot(
            bundle_path=bundle_path,
            database_url=LOCAL_DATABASE_URL,
            pilot_run_id="prod-flag-enable-harness-bad",
            repo_root=root,
            confirm_flag_enablement_pilot=True,
            confirm_live_read_only_prod_source_reads=True,
        )


def test_live_execution_chain_still_authorized_after_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    assert before["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert before["execution"]["live_execution_pilot_run"]["pilot_run_id"]

    _run_flag_enablement_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-flag-enable-chain")
    updated = _load(bundle_path)

    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert updated["execution"]["live_execution_pilot_run"] == before["execution"]["live_execution_pilot_run"]
    assert updated["execution"]["live_read_only_pilot_run"] == before["execution"]["live_read_only_pilot_run"]


def test_verifier_passes_without_local_shadow_run_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_flag_enablement_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="prod-flag-enable-no-files",
    )
    shutil.rmtree(root / PROD_SCOPED_SHADOW_ROOT / "prod-flag-enable-no-files")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_flag_enablement_pilot_run_filed=True,
        verify_local_pilot_files=True,
    )

    assert result["verification_mode"] == "post_flag_enablement_pilot_run"


def test_verifier_checks_local_hashes_when_files_exist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_flag_enablement_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="prod-flag-enable-with-files",
    )

    verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_flag_enablement_pilot_run_filed=True,
        verify_local_pilot_files=True,
    )

    manifest = root / PROD_SCOPED_SHADOW_ROOT / "prod-flag-enable-with-files" / "manifest.json"
    manifest.write_text(manifest.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="sha256"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_flag_enablement_pilot_run_filed=True,
            verify_local_pilot_files=True,
        )


def test_cli_smoke_run_with_confirmation_then_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev18_template_root: Path,
) -> None:
    root = _copy_template_repo(rev18_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _patch_fake_db(monkeypatch, _live_rows(root))
    old_argv = sys.argv
    try:
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-flag-enablement-pilot-run",
            "--bundle",
            str(bundle_path),
            "--confirm-flag-enablement-pilot",
            "--confirm-live-read-only-prod-source-reads",
            "--database-url",
            LOCAL_DATABASE_URL,
            "--pilot-run-id",
            "prod-flag-enable-cli",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
            "--bundle",
            str(bundle_path),
            "--expect-flag-enablement-pilot-run-filed",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
    finally:
        sys.argv = old_argv

    assert _load(bundle_path)["recommended_next_stage"] == POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE


def test_committed_bundle_matches_post_flag_enablement_pilot_run_if_present() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] not in (19, 20):
        pytest.skip("committed production-scoped-shadow bundle not at rev19/20 yet")
    if payload["metadata"]["bundle_revision"] == 20:
        pytest.skip("committed bundle already advanced to rev20")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_flag_enablement_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 19
    assert result["recommended_next_stage"] == POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE
