"""Tests for reviewing the recorded production-scoped pilot run."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_pilot_review as review_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    PILOT_RUN_REVIEW_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    plan_ml_shadow_scorer_production_scoped_shadow_bundle,
    prove_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    write_ml_shadow_scorer_production_scoped_shadow_bundle,
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
    MLShadowScorerProductionScopedShadowPilotReviewError,
    review_ml_shadow_scorer_production_scoped_shadow_pilot,
)

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
    "learned_probability": "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
    "generalization_audit": "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
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


def _write_harness_review_bundle(root: Path) -> Path:
    bundle_path = _write_pre_plan_bundle(root)
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id="proof-before-review",
        repo_root=root,
    )
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="Owner reviewed the production-scoped proof and bounded pilot contract.",
        repo_root=root,
    )
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        pilot_run_id="harness-before-review",
        repo_root=root,
    )
    review_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        reviewer="Harness Reviewer",
        repo_root=root,
    )
    return bundle_path


def _write_pilot_run_bundle(root: Path, *, pilot_run_id: str = "pilot-before-review") -> Path:
    bundle_path = _write_harness_review_bundle(root)
    run_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        learned_probability_artifact_path=_fixture(root, "learned_probability"),
        second_surface_generalization_audit_path=_fixture(root, "generalization_audit"),
        pilot_run_id=pilot_run_id,
        repo_root=root,
    )
    return bundle_path


def test_happy_path_accepts_revision_seven_pilot_evidence(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_run_bundle(root, pilot_run_id="pilot-review-happy")
    before = _load(bundle_path)

    result = review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        reviewer="Review Person",
        review_notes="review notes",
        repo_root=root,
        reviewed_at="2026-05-29T21:30:00Z",
    )
    updated = _load(bundle_path)

    assert result["pilot_accepted"] is True
    assert updated["metadata"]["bundle_revision"] == 8
    assert updated["metadata"]["generated_at"] == "2026-05-29T21:30:00Z"
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["execution"] == before["execution"]
    assert updated["review"]["review_decision"] == before["review"]["review_decision"]
    assert updated["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert updated["review"]["prod_scoped_shadow_pilot_reviewed"] is True
    assert updated["review"]["prod_scoped_shadow_pilot_accepted"] is True
    assert updated["review"]["pilot_review_decision"]["decision"] == "accepted"
    assert updated["review"]["pilot_review_decision"]["reviewer"] == "Review Person"
    assert updated["review"]["pilot_review_decision"]["review_notes"] == "review notes"
    assert updated["review"]["pilot_review_decision"]["failed_review_checks"] == []
    assert set(updated["review"]["pilot_review_decision"]["checks"]) == set(PILOT_RUN_REVIEW_CHECKS)
    assert updated["posture"]["prod_scoped_shadow_pilot_reviewed"] is True
    assert updated["posture"]["prod_scoped_shadow_pilot_accepted"] is True
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["recommended_next_stage"] == (
        "request_production_scoped_online_shadow_live_read_only_authorization_v1"
    )
    assert verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_pilot_review_filed=True,
    )["verification_mode"] == "post_pilot_review"


def test_failing_review_check_files_not_accepted_review(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_run_bundle(root, pilot_run_id="pilot-review-fail")
    original_evaluate = review_module.evaluate_production_scoped_shadow_pilot_review_checks

    def failing_evaluate(bundle: dict[str, Any]) -> dict[str, bool]:
        checks = original_evaluate(bundle)
        checks["environment_restored"] = False
        return checks

    monkeypatch.setattr(review_module, "evaluate_production_scoped_shadow_pilot_review_checks", failing_evaluate)

    result = review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        reviewer="Review Person",
        repo_root=root,
    )
    updated = _load(bundle_path)

    assert result["pilot_accepted"] is False
    assert updated["review"]["pilot_review_decision"]["decision"] == "not_accepted"
    assert updated["review"]["pilot_review_decision"]["failed_review_checks"] == ["environment_restored"]
    assert updated["recommended_next_stage"] == "remediate_production_scoped_online_shadow_pilot_v1"
    assert verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_pilot_review_filed=True,
    )["verification_mode"] == "post_pilot_review"


def test_rejects_pre_pilot_or_double_review(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    pre_pilot = _write_harness_review_bundle(root)
    with pytest.raises(MLShadowScorerProductionScopedShadowPilotReviewError, match="bundle_revision"):
        review_ml_shadow_scorer_production_scoped_shadow_pilot(
            bundle_path=pre_pilot,
            reviewer="Review Person",
            repo_root=root,
        )

    double_root = _copy_fixture_repo(tmp_path / "double")
    pilot = _write_pilot_run_bundle(double_root, pilot_run_id="pilot-review-double")
    review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=pilot,
        reviewer="Review Person",
        repo_root=double_root,
    )
    with pytest.raises(MLShadowScorerProductionScopedShadowPilotReviewError, match="bundle_revision|already"):
        review_ml_shadow_scorer_production_scoped_shadow_pilot(
            bundle_path=pilot,
            reviewer="Review Person",
            repo_root=double_root,
        )


def test_review_does_not_call_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_run_bundle(root, pilot_run_id="pilot-review-no-runtime")

    import pipeline.ml_shadow_scorer_online_shadow_runtime as runtime_module

    def fail_runtime(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("review must not call runtime")

    monkeypatch.setattr(runtime_module, "run_ml_shadow_scorer_v1_online_shadow_runtime", fail_runtime)

    review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        reviewer="Review Person",
        repo_root=root,
    )


def test_review_does_not_read_or_write_shadow_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_run_bundle(root, pilot_run_id="pilot-review-no-shadow-io")
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"review must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        reviewer="Review Person",
        repo_root=root,
    )


def test_cli_smoke_review_then_verify_revision_eight(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_run_bundle(root, pilot_run_id="pilot-review-cli")
    review_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-pilot-review",
        "--bundle",
        str(bundle_path),
        "--reviewer",
        "CLI Reviewer",
        "--review-notes",
        "cli review notes",
        "--repo-root",
        str(root),
    ]
    reviewed = subprocess.run(review_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert reviewed.stdout.splitlines() == [
        "accepted",
        "True",
        "request_production_scoped_online_shadow_live_read_only_authorization_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-pilot-review-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_pilot_review",
        "online-shadow-production-scoped-v1",
        "request_production_scoped_online_shadow_live_read_only_authorization_v1",
    ]


def test_verifier_rejects_bad_pilot_review_decision(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_run_bundle(root, pilot_run_id="pilot-review-bad-decision")
    review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        reviewer="Review Person",
        repo_root=root,
    )
    payload = _load(bundle_path)
    payload["review"]["pilot_review_decision"]["decision"] = "not_accepted"
    bundle_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="decision"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )


def test_review_module_has_no_runtime_or_database_imports() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_scoped_shadow_pilot_review.py"
    ).read_text(encoding="utf-8")
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
