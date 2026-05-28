"""Tests for the ml-shadow-scorer-v1 Phase 1 no-write online shadow pilot run."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run import (
    MLShadowScorerOnlineShadowPhase1NoWritePilotRunError,
    _verify_recorded_records,
    build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run_payload,
    write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    FEATURE_FLAG,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

PLAN_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1.json"
GRANT_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json"
RUNTIME_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json"
LEARNED_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json"
AUDIT_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json"

POLICY_CONTRACT_KEYS = (
    "component_coverage",
    "missing_learned_probability",
    "score_distributions",
    "top_k_overlap_with_heuristic",
    "rank_displacement",
    "family_counts",
    "output_completeness",
    "runtime_errors",
    "latency",
    "skipped_candidates_and_reasons",
    "skipped_ranking_run_records",
    "write_counts_by_isolated_target",
)
RUN_LEVEL_FIELDS = (
    "status",
    "shadow_row_count",
    "writes_performed",
    "production_default_changed",
    "user_visible_ranking_changed",
    "api_web_changes_allowed",
    "runtime_feature_flag_value",
    "labels_used_for_scoring",
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build(
    *,
    plan_path: Path = PLAN_PATH,
    grant_path: Path = GRANT_PATH,
    runtime_path: Path = RUNTIME_PATH,
    learned_path: Path = LEARNED_PATH,
    audit_path: Path = AUDIT_PATH,
    generated_at: str = "2026-05-27T00:00:00Z",
) -> dict:
    return build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run_payload(
        phase1_no_write_pilot_plan_path=plan_path,
        authorization_grant_path=grant_path,
        online_shadow_runtime_path=runtime_path,
        learned_probability_artifact_path=learned_path,
        second_surface_generalization_audit_path=audit_path,
        repo_root=REPO_ROOT,
        generated_at=generated_at,
    )


def test_recorded_sha_accepts_line_ending_equivalent_text_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    lf_bytes = b'{\n  "ok": true\n}\n'
    crlf_sha = hashlib.sha256(lf_bytes.replace(b"\n", b"\r\n")).hexdigest()
    artifact.write_bytes(lf_bytes)

    verified = _verify_recorded_records(
        [{"name": "artifact", "path": "artifact.json", "sha256": crlf_sha}],
        repo_root=tmp_path,
        label="test metadata.inputs",
    )

    assert verified[0]["verification_status"] == "confirmed"


def test_happy_path_runs_flag_on_528_rows_and_writes_no_shadow_storage() -> None:
    payload = _build()

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run"
    assert payload["phase1_no_write_pilot_executed"] is True
    assert payload["phase1_no_write_pilot_passed"] is True
    assert payload["online_shadow_execution_authorized"] is True
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["missing_online_shadow_execution_authorization"] is False
    assert payload["missing_production_readiness_authorization"] is True
    assert payload["runtime_execution_authorized"] is True
    assert payload["shadow_scoring_allowed"] is True
    assert payload["writes_allowed"] is False
    assert payload["writes_performed"] is False
    assert payload["recommended_next_stage"] == "review_online_shadow_phase1_pilot_results_v1"

    join = payload["input_join_summary"]
    assert join["joined_candidate_count"] == 528
    assert join["recomputed_candidate_pool_work_set_sha256"] == CANDIDATE_POOL_WORK_SET_SHA256
    assert join["runtime_input_fields"] == [
        "canonical_openalex_work_id",
        "final_score",
        "audit_embedding_probability_work",
        "ranking_run_id",
        "family",
        "candidate_pool_work_set_sha256",
        "corpus_snapshot_version",
        "embedding_version",
    ]

    pilot = payload["pilot_runtime_result"]
    assert pilot["status"] == "succeeded_test_only"
    assert pilot["shadow_row_count"] == 528
    assert pilot["runtime_feature_flag_value"] == "true"
    assert pilot["writes_performed"] is False
    assert pilot["production_default_changed"] is False
    assert pilot["user_visible_ranking_changed"] is False
    assert pilot["labels_used_for_scoring"] is False
    assert pilot["shadow_rows_persisted"] is False
    assert "shadow_rows" not in pilot


def test_disable_drill_and_environment_restore(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(FEATURE_FLAG, "outer-value")
    payload = _build()

    assert os.environ[FEATURE_FLAG] == "outer-value"
    assert payload["disable_drill_passed"] is True
    assert payload["disable_drill"]["preflight"]["status"] == "skipped_runtime_disabled"
    assert payload["disable_drill"]["postflight"]["status"] == "skipped_runtime_disabled"
    assert payload["disable_drill"]["environment_restored"] is True
    assert payload["rollback_summary"]["feature_flag_restored_after_runner"] is True


def test_observability_is_complete_and_no_write_counts_are_zero() -> None:
    payload = _build()
    observability = payload["observability"]

    assert set(observability["plan_policy_contract_keys"]) == set(POLICY_CONTRACT_KEYS)
    assert set(RUN_LEVEL_FIELDS).issubset(set(observability["plan_run_level_fields"]))
    assert all(observability["policy_contract_satisfied"][key] is True for key in POLICY_CONTRACT_KEYS)
    assert all(observability["run_level_fields_satisfied"][key] is True for key in RUN_LEVEL_FIELDS)
    assert observability["component_coverage"]["complete"] is True
    assert observability["missing_learned_probability"]["missing_count"] == 0
    assert observability["output_completeness"]["complete"] is True
    assert observability["runtime_errors"] == []
    assert all(value == 0 for value in observability["write_counts_by_isolated_target"].values())
    assert payload["no_write_verification"]["all_write_counts_zero"] is True


def test_rejects_plan_already_executed(tmp_path: Path) -> None:
    plan = copy.deepcopy(_load(PLAN_PATH))
    plan["phase1_no_write_pilot_executed"] = True
    plan["shadow_and_production_blockers"]["phase1_no_write_pilot_executed"] = True
    plan_path = _write_json(tmp_path, "plan.json", plan)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="phase1_no_write_pilot_executed"):
        _build(plan_path=plan_path)


def test_rejects_invalid_grant(tmp_path: Path) -> None:
    grant = copy.deepcopy(_load(GRANT_PATH))
    grant["grant_decision"]["decision"] = "denied"
    grant_path = _write_json(tmp_path, "grant.json", grant)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="grant_decision.decision"):
        _build(grant_path=grant_path)


def test_rejects_tampered_plan_sha_chain(tmp_path: Path) -> None:
    plan = copy.deepcopy(_load(PLAN_PATH))
    plan["metadata"]["inputs"][0]["sha256"] = "0" * 64
    plan_path = _write_json(tmp_path, "plan.json", plan)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="sha256 mismatch"):
        _build(plan_path=plan_path)


def test_rejects_incomplete_learned_probability_coverage(tmp_path: Path) -> None:
    learned = copy.deepcopy(_load(LEARNED_PATH))
    learned["candidate_work_scores"] = learned["candidate_work_scores"][:-1]
    learned_path = _write_json(tmp_path, "learned.json", learned)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="candidate_work_scores length"):
        _build(learned_path=learned_path)


def test_rejects_join_count_not_528(tmp_path: Path) -> None:
    learned = copy.deepcopy(_load(LEARNED_PATH))
    learned["candidate_work_scores"][0]["canonical_openalex_work_id"] = "W999999999999"
    learned_path = _write_json(tmp_path, "learned.json", learned)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="joined_candidate_count"):
        _build(learned_path=learned_path)


def test_rejects_learned_final_score_mismatch(tmp_path: Path) -> None:
    learned = copy.deepcopy(_load(LEARNED_PATH))
    learned["candidate_work_scores"][0]["final_score"] = float(learned["candidate_work_scores"][0]["final_score"]) + 0.01
    learned_path = _write_json(tmp_path, "learned.json", learned)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="final_score mismatch"):
        _build(learned_path=learned_path)


def test_rejects_forbidden_label_fields_in_source_rows(tmp_path: Path) -> None:
    learned = copy.deepcopy(_load(LEARNED_PATH))
    learned["candidate_work_scores"][0]["good_or_acceptable"] = True
    learned_path = _write_json(tmp_path, "learned.json", learned)

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="forbidden label fields"):
        _build(learned_path=learned_path)


def test_writer_does_not_emit_artifacts_on_validation_error(tmp_path: Path) -> None:
    plan = copy.deepcopy(_load(PLAN_PATH))
    plan["online_shadow_execution_enabled"] = True
    plan_path = _write_json(tmp_path, "plan.json", plan)
    out_json = tmp_path / "out.json"
    out_md = tmp_path / "out.md"

    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotRunError, match="online_shadow_execution_enabled"):
        write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run(
            phase1_no_write_pilot_plan_path=plan_path,
            authorization_grant_path=GRANT_PATH,
            online_shadow_runtime_path=RUNTIME_PATH,
            learned_probability_artifact_path=LEARNED_PATH,
            second_surface_generalization_audit_path=AUDIT_PATH,
            output_path=out_json,
            markdown_output_path=out_md,
            repo_root=REPO_ROOT,
        )

    assert not out_json.exists()
    assert not out_md.exists()


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "pilot-run.json"
    out_md = tmp_path / "pilot-run.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase1-no-write-pilot-run",
        "--phase1-no-write-pilot-plan",
        str(PLAN_PATH),
        "--authorization-grant",
        str(GRANT_PATH),
        "--online-shadow-runtime",
        str(RUNTIME_PATH),
        "--learned-probability-artifact",
        str(LEARNED_PATH),
        "--second-surface-generalization-audit",
        str(AUDIT_PATH),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run"
    assert payload["phase1_no_write_pilot_executed"] is True
    assert payload["phase1_no_write_pilot_passed"] is True
    assert payload["recommended_next_stage"] == "review_online_shadow_phase1_pilot_results_v1"
    assert result.stdout.splitlines() == ["True", "True", "review_online_shadow_phase1_pilot_results_v1"]
    assert "Online Shadow Phase 1 No-Write Pilot Run" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-phase1-no-write-pilot-run"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
