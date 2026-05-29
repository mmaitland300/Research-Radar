"""Tests for the bounded production-scoped online shadow pilot harness."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_pilot_harness as pilot_module
from pipeline.ml_shadow_scorer_online_shadow_runtime import FEATURE_FLAG
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    plan_ml_shadow_scorer_production_scoped_shadow_bundle,
    prove_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    write_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_harness import (
    MLShadowScorerProductionScopedShadowPilotHarnessError,
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness,
)
from pipeline.shadow_write_path_guards import PROD_SCOPED_SHADOW_ROOT

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

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
}

EXPECTED_HARNESS_FILES = {
    "manifest.json",
    "shadow_rows.jsonl",
    "observability.json",
    "write_counts.json",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return root


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _optional_kwargs(root: Path) -> dict[str, Path]:
    return {
        "execution_authorization_grant_path": _fixture(root, "execution_authorization_grant"),
        "phase2_write_mode_plan_path": _fixture(root, "phase2_write_mode_plan"),
        "phase2_write_mode_proof_path": _fixture(root, "phase2_write_mode_proof"),
        "generalization_audit_gates_path": _fixture(root, "generalization_audit_gates"),
    }


def _write_pre_plan_bundle(root: Path) -> Path:
    out_json = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    out_md = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md"
    write_ml_shadow_scorer_production_scoped_shadow_bundle(
        production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
        **_optional_kwargs(root),
    )
    return out_json


def _write_proof_bundle(root: Path, *, proof_run_id: str = "proof-pilot") -> Path:
    bundle_path = _write_pre_plan_bundle(root)
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id=proof_run_id,
        repo_root=root,
    )
    return bundle_path


def _write_pilot_request_bundle(root: Path, *, proof_run_id: str = "proof-pilot") -> Path:
    bundle_path = _write_proof_bundle(root, proof_run_id=proof_run_id)
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    return bundle_path


def _write_pilot_grant_bundle(root: Path, *, proof_run_id: str = "proof-pilot") -> Path:
    bundle_path = _write_pilot_request_bundle(root, proof_run_id=proof_run_id)
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review=(
            "Owner reviewed the production-scoped proof, pilot request, and bounded pilot contract as equivalent review."
        ),
        repo_root=root,
    )
    return bundle_path


def _run_harness(
    root: Path,
    *,
    bundle_path: Path | None = None,
    pilot_run_id: str = "pilot-harness-test",
    update_bundle: bool = False,
) -> dict[str, Any]:
    return run_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path or _write_pilot_grant_bundle(root),
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
    )


def test_happy_path_writes_four_gitignored_files_and_updates_bundle_revision_five(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_grant_bundle(root, proof_run_id="proof-before-harness")
    before = _load(bundle_path)

    result = _run_harness(root, bundle_path=bundle_path, pilot_run_id="happy-harness", update_bundle=True)
    updated = _load(bundle_path)
    pilot_dir = root / PROD_SCOPED_SHADOW_ROOT / "happy-harness"

    assert result["pilot_harness_passed"] is True
    assert updated["metadata"]["bundle_revision"] == 5
    assert updated["metadata"]["pinned_identity"] == before["metadata"]["pinned_identity"]
    assert updated["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["execution"]["prod_scoped_shadow_pilot_harness_executed"] is True
    assert updated["execution"]["prod_scoped_shadow_pilot_harness_passed"] is True
    assert updated["execution"]["prod_scoped_shadow_pilot_executed"] is False
    assert updated["posture"]["prod_scoped_shadow_pilot_harness_executed"] is True
    assert updated["posture"]["prod_scoped_shadow_pilot_executed"] is False
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["posture"]["writes_performed"] is False
    assert updated["posture"]["runtime_writes_performed"] is False
    assert updated["authorization"]["prod_scoped_shadow_pilot_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["authorization"]["prod_scoped_shadow_pilot_harness_allowed_by_grant"] is True
    assert updated["recommended_next_stage"] == "run_production_scoped_online_shadow_pilot_v1"
    assert "This is a bounded fixture pilot harness, not live production traffic." in updated["caveats"]
    assert all("production-scoped pilot has run" not in caveat.lower() for caveat in updated["caveats"])

    harness = updated["execution"]["pilot_harness"]
    assert harness["pilot_surface"] == "bounded_fixture_pilot_harness"
    assert harness["fixture_row_count"] == 3
    assert harness["live_prod_source_reads_performed"] is False
    assert harness["pilot_run_id"] != before["proof"]["pilot_run_id"]
    assert harness["pilot_run_directory"]["relative_path"] == (
        f"{PROD_SCOPED_SHADOW_ROOT}happy-harness/"
    )
    assert set(path.name for path in pilot_dir.iterdir()) == EXPECTED_HARNESS_FILES
    assert [record["relative_path"] for record in harness["files_written"]] == [
        "manifest.json",
        "shadow_rows.jsonl",
        "observability.json",
        "write_counts.json",
    ]
    for record in harness["files_written"]:
        artifact = pilot_dir / record["relative_path"]
        assert artifact.exists()
        assert record["sha256"] == _sha256(artifact)
        assert record["byte_count"] == artifact.stat().st_size
        assert record["write_target"] == "isolated_prod_scoped_audit_artifacts"
    assert harness["write_count_verification"]["local_artifact_tree_writes_performed"] is True
    assert harness["write_count_verification"]["production_writes_performed"] is False
    assert harness["write_count_verification"]["committed_artifact_writes_performed"] is False
    assert harness["write_count_verification"]["runtime_writes_performed"] is False
    assert harness["write_count_verification"]["write_counts_by_isolated_target"]["prod_scoped_shadow_tables"] == 0

    manifest = _load(pilot_dir / "manifest.json")
    observability = _load(pilot_dir / "observability.json")
    assert manifest["pilot_surface"] == "bounded_fixture_pilot_harness"
    assert manifest["live_prod_source_reads_performed"] is False
    assert observability["live_prod_source_reads_performed"] is False
    assert observability["row_counts"]["fixture_rows"] == 3
    assert len((pilot_dir / "shadow_rows.jsonl").read_text(encoding="utf-8").splitlines()) == 3

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_pilot_harness_filed=True,
    )
    assert verified["verification_mode"] == "post_pilot_harness"


def test_rejects_invalid_pilot_run_id(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotHarnessError, match="pilot_run_id"):
        _run_harness(root, pilot_run_id="../bad")


def test_rejects_path_outside_prod_scoped_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    monkeypatch.setattr(
        pilot_module,
        "resolve_prod_scoped_pilot_directory",
        lambda repo_root, pilot_run_id: root / "outside-shadow-runs",
    )

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotHarnessError, match="prod-scoped shadow root"):
        _run_harness(root, pilot_run_id="outside-path")


def test_rejects_forbidden_write_count_nonzero_before_bundle_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_grant_bundle(root, proof_run_id="proof-bad-counts")
    original_counts = pilot_module._write_counts_by_isolated_target

    def bad_counts(*, file_count: int = 4) -> dict[str, int]:
        counts = original_counts(file_count=file_count)
        counts["prod_scoped_shadow_tables"] = 1
        return counts

    monkeypatch.setattr(pilot_module, "_write_counts_by_isolated_target", bad_counts)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowPilotHarnessError,
        match="forbidden prod-scoped write targets",
    ):
        _run_harness(root, bundle_path=bundle_path, pilot_run_id="bad-counts", update_bundle=True)
    assert _load(bundle_path)["metadata"]["bundle_revision"] == 4


def test_restores_environment_variable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    monkeypatch.setenv(FEATURE_FLAG, "original-off")

    _run_harness(root, pilot_run_id="env-restore")

    assert os.environ[FEATURE_FLAG] == "original-off"


def test_preflight_pilot_postflight_runtime_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    monkeypatch.delenv(FEATURE_FLAG, raising=False)
    original_runtime_call = pilot_module._runtime_call
    calls: list[tuple[str | None, int]] = []

    def wrapped_runtime_call(candidate_rows: list[Mapping[str, Any]], *, flag_value: str | None) -> dict[str, Any]:
        calls.append((flag_value, len(candidate_rows)))
        return original_runtime_call(candidate_rows, flag_value=flag_value)

    monkeypatch.setattr(pilot_module, "_runtime_call", wrapped_runtime_call)

    result = _run_harness(root, pilot_run_id="drill-order")

    assert calls == [(None, 0), ("true", 3), (None, 0)]
    assert result["execution"]["runtime_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]
    assert result["execution"]["runtime_drill"]["environment_restored"] is True


def test_rejects_bundle_not_pilot_granted(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root, proof_run_id="proof-not-granted")

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotHarnessError, match="bundle_revision"):
        _run_harness(root, bundle_path=bundle_path, pilot_run_id="not-granted")


def test_rejects_double_harness_run_without_override(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_grant_bundle(root, proof_run_id="proof-double")
    _run_harness(root, bundle_path=bundle_path, pilot_run_id="first-harness", update_bundle=True)

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotHarnessError, match="bundle_revision|already"):
        _run_harness(root, bundle_path=bundle_path, pilot_run_id="second-harness")


def test_rejects_live_prod_source_read_claim_in_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_grant_bundle(root, proof_run_id="proof-live-claim")
    _run_harness(root, bundle_path=bundle_path, pilot_run_id="live-claim", update_bundle=True)
    payload = _load(bundle_path)
    payload["execution"]["pilot_harness"]["live_prod_source_reads_performed"] = True
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_prod_source_reads_performed"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_pilot_harness_filed=True,
        )


def test_runtime_import_call_is_limited_to_pilot_runner_module() -> None:
    production_scoped_modules = list((PACKAGE_ROOT / "pipeline").glob("ml_shadow_scorer_production_scoped*.py"))
    assert production_scoped_modules
    for module_path in production_scoped_modules:
        source = module_path.read_text(encoding="utf-8")
        if module_path.name in {
            "ml_shadow_scorer_production_scoped_shadow_pilot.py",
            "ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot.py",
        }:
            assert "run_ml_shadow_scorer_v1_online_shadow_runtime" in source
        else:
            assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in source


def test_no_tracked_shadow_run_artifacts() -> None:
    result = subprocess.run(
        ["git", "ls-files", "docs/audit/shadow-runs"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.strip() == ""


def test_cli_smoke_verify_rev4_run_harness_and_verify_rev5(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_grant_bundle(root, proof_run_id="cli-proof")
    verify_grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-pilot-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified_grant = subprocess.run(verify_grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified_grant.stdout.splitlines() == [
        "passed",
        "post_pilot_grant",
        "online-shadow-production-scoped-v1",
        "run_production_scoped_online_shadow_pilot_v1",
    ]

    harness_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-pilot-harness-run",
        "--bundle",
        str(bundle_path),
        "--pilot-run-id",
        "cli-harness",
        "--repo-root",
        str(root),
    ]
    harness = subprocess.run(harness_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert harness.stdout.splitlines() == [
        "cli-harness",
        "True",
        "run_production_scoped_online_shadow_pilot_v1",
    ]

    verify_harness_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-pilot-harness-filed",
        "--repo-root",
        str(root),
    ]
    verified_harness = subprocess.run(
        verify_harness_cmd,
        cwd=PACKAGE_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert verified_harness.stdout.splitlines() == [
        "passed",
        "post_pilot_harness",
        "online-shadow-production-scoped-v1",
        "run_production_scoped_online_shadow_pilot_v1",
    ]
