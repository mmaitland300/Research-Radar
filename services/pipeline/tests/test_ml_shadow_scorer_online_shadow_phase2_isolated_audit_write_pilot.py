"""Tests for the Phase 2 isolated audit write pilot runner."""

from __future__ import annotations

import copy
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_phase_bundle import (
    verify_ml_shadow_scorer_phase_bundle,
    write_ml_shadow_scorer_phase_bundle,
)
from pipeline import ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_pilot as pilot_module
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_pilot import (
    MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError,
    run_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_pilot,
)
from pipeline.shadow_write_path_guards import PHASE2_PROOF_ROOT

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "prior_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
    "learned": "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
    "audit": "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    _write_pre_pilot_bundle(root)
    return root


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _bundle(root: Path) -> Path:
    return root / "docs/audit/bundles/phase2-v1/bundle.json"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_pre_pilot_bundle(root: Path) -> None:
    write_ml_shadow_scorer_phase_bundle(
        phase2_write_mode_plan_path=_fixture(root, "plan"),
        phase2_write_mode_proof_path=_fixture(root, "proof"),
        phase2_write_authorization_request_path=_fixture(root, "request"),
        phase2_write_authorization_grant_path=_fixture(root, "grant"),
        phase1_no_write_pilot_review_path=_fixture(root, "review"),
        prior_execution_authorization_grant_path=_fixture(root, "prior_grant"),
        online_shadow_policy_path=_fixture(root, "policy"),
        output_path=_bundle(root),
        markdown_output_path=_bundle(root).with_name("bundle.md"),
        repo_root=root,
    )


def _run(root: Path, *, pilot_run_id: str = "pilot-test", update_bundle: bool = False) -> dict[str, Any]:
    return run_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_pilot(
        bundle_path=_bundle(root),
        learned_probability_artifact_path=_fixture(root, "learned"),
        second_surface_generalization_audit_path=_fixture(root, "audit"),
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
        generated_at="2026-05-28T23:00:00Z",
    )


def _rewrite_bundle_grant_ref(root: Path, grant_path: Path) -> None:
    bundle = _load(_bundle(root))
    rel = grant_path.relative_to(root).as_posix()
    sha = sha256_file(grant_path)
    for record in bundle["metadata"]["legacy_artifacts_index"]:
        if record["role"] == "phase2_write_authorization_grant":
            record["path"] = rel
            record["sha256"] = sha
    bundle["authorization"]["phase2_write_authorization_grant"]["path"] = rel
    bundle["authorization"]["phase2_write_authorization_grant"]["sha256"] = sha
    _write_json(_bundle(root), bundle)


def test_happy_path_writes_four_files_and_updates_bundle_execution(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)

    result = _run(root, pilot_run_id="happy-pilot", update_bundle=True)
    updated = _load(_bundle(root))
    run_dir = root / PHASE2_PROOF_ROOT / "happy-pilot"

    assert result["phase2_write_pilot_passed"] is True
    assert run_dir.exists()
    assert sorted(path.name for path in run_dir.iterdir()) == [
        "manifest.json",
        "observability.json",
        "shadow_rows.jsonl",
        "write_counts.json",
    ]
    assert updated["metadata"]["bundle_revision"] == 2
    assert updated["execution"]["phase2_write_pilot_executed"] is True
    assert updated["execution"]["phase2_write_pilot_passed"] is True
    assert updated["execution"]["isolated_file_writes"]["file_count"] == 4
    assert updated["execution"]["write_count_verification"]["forbidden_targets_zero"] is True
    assert updated["recommended_next_stage"] == "review_online_shadow_phase2_isolated_audit_write_pilot_v1"
    verify_ml_shadow_scorer_phase_bundle(
        bundle_path=_bundle(root),
        repo_root=root,
        expect_pilot_executed=True,
    )


def test_rejects_bundle_with_pilot_already_executed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    _run(root, pilot_run_id="already-ran", update_bundle=True)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError, match="phase2_write_pilot_executed"):
        _run(root, pilot_run_id="second-run", update_bundle=False)


def test_rejects_bundle_verify_failure_and_grant_not_authorized(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_bundle(root))
    bundle["authorization"]["phase2_writes_authorized"] = False
    _write_json(_bundle(root), bundle)
    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError, match="phase2_writes_authorized"):
        _run(root, pilot_run_id="bad-bundle")

    root = _copy_fixture_repo(tmp_path)
    grant = copy.deepcopy(_load(_fixture(root, "grant")))
    grant["phase2_write_pilot_authorized"] = False
    bad_grant_path = _write_json(root / "docs/audit/grant-not-authorized.json", grant)
    _rewrite_bundle_grant_ref(root, bad_grant_path)
    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError, match="phase2_write_pilot_authorized"):
        _run(root, pilot_run_id="bad-grant")


def test_rejects_invalid_pilot_run_id(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError, match="pilot_run_id"):
        _run(root, pilot_run_id="../bad")


def test_rejects_path_outside_phase2_proof_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)

    monkeypatch.setattr(pilot_module, "resolve_pilot_directory", lambda repo_root, pilot_run_id: root / "outside")
    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError, match="write path"):
        _run(root, pilot_run_id="outside-path")


def test_rejects_forbidden_write_counts_before_writing_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)

    def bad_counts(*, file_count: int = 0) -> dict[str, int]:
        counts = {target: 0 for target in pilot_module.proof_module.WRITE_COUNT_TARGETS}
        counts["isolated_audit_shadow_artifacts"] = file_count
        counts["paper_scores"] = 1
        return counts

    monkeypatch.setattr(pilot_module.proof_module, "_write_count_template", bad_counts)
    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError, match="forbidden write targets"):
        _run(root, pilot_run_id="bad-counts")
    assert not (root / PHASE2_PROOF_ROOT / "bad-counts").exists()


def test_environment_variable_restored_after_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    monkeypatch.setenv(pilot_module.FEATURE_FLAG, "already-set")

    _run(root, pilot_run_id="env-restore")

    assert os.environ[pilot_module.FEATURE_FLAG] == "already-set"


def test_drill_order_is_preflight_pilot_postflight(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    result = _run(root, pilot_run_id="drill-order")

    assert result["execution"]["disable_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]


def test_no_database_imports_in_pilot_module() -> None:
    module_source = (
        PACKAGE_ROOT
        / "pipeline"
        / "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_pilot.py"
    ).read_text(encoding="utf-8")
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
