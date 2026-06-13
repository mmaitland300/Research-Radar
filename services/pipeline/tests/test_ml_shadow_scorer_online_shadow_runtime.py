"""Tests for disabled-by-default ml-shadow-scorer-v1 online shadow runtime."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_online_shadow_runtime as runtime_module
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    FEATURE_FLAG,
    MLShadowScorerOnlineShadowRuntimeError,
    build_ml_shadow_scorer_online_shadow_runtime_disabled_payload,
    parse_ml_shadow_scorer_v1_runtime_flag,
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.ml_shadow_scorer_v1 import compute_shadow_score_rows


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _candidate_rows() -> list[dict]:
    return [
        {
            "canonical_openalex_work_id": "W000000001",
            "final_score": 0.9,
            "audit_embedding_probability_work": 0.2,
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": runtime_module.EMBEDDING_VERSION,
        },
        {
            "canonical_openalex_work_id": "W000000002",
            "final_score": 0.4,
            "audit_embedding_probability_work": 0.8,
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": runtime_module.EMBEDDING_VERSION,
        },
        {
            "canonical_openalex_work_id": "W000000003",
            "final_score": 0.1,
            "audit_embedding_probability_work": 0.1,
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": runtime_module.EMBEDDING_VERSION,
        },
    ]


def _gates_payload(*, passed: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_gates",
            "gates_version": "ml-shadow-scorer-v1-generalization-audit-gates-v1",
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": runtime_module.EMBEDDING_VERSION,
        },
        "generalization_audit_gates_passed": passed,
        "second_surface_generalization_passed": passed,
        "disabled_by_default_runtime_implementation_next_stage_allowed": passed,
        "recommended_next_stage": "implement_online_shadow_runtime_disabled_by_default",
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }


def _audit_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_surface_generalization_audit",
            "artifact_version": "ml-shadow-scorer-v1-second-surface-generalization-audit-v1",
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": runtime_module.EMBEDDING_VERSION,
        }
    }


def _policy_payload(*, default_off: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
        },
        "runtime_implementation_authorized": False,
        "runtime_isolation_policy": {
            "feature_flag": FEATURE_FLAG,
            "feature_flag_default": "off" if default_off else "on",
            "feature_flag_default_off": default_off,
        },
        "disable_and_rollback_policy": {"disable_switch_default": "off" if default_off else "on"},
    }


def _spec_payload() -> dict:
    return {
        "metadata": {"artifact_type": "ml_shadow_scorer_spec", "spec_version": "ml-shadow-scorer-v1-spec"},
        "scoring_formula": {"formula_id": "hybrid_rank_mean_50_50"},
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
            "overall_status": "research_only",
        },
        "targets": {"good_or_acceptable": {"production_eligible": False}},
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    gates: dict | None = None,
    audit: dict | None = None,
    policy: dict | None = None,
    spec: dict | None = None,
    production: dict | None = None,
) -> dict[str, Path]:
    return {
        "generalization_audit_gates_path": _write_json(tmp_path, "gates.json", gates or _gates_payload()),
        "second_surface_generalization_audit_path": _write_json(tmp_path, "audit.json", audit or _audit_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "shadow_scorer_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "production_readiness_plan_path": _write_json(tmp_path, "production.json", production or _production_plan_payload()),
    }


@pytest.mark.parametrize("flag_value", [None, "", "0", "false", "off", "no", "disabled", "unknown"])
def test_feature_flag_missing_or_off_skips_runtime(flag_value: str | None) -> None:
    env = {} if flag_value is None else {FEATURE_FLAG: flag_value}
    result = run_ml_shadow_scorer_v1_online_shadow_runtime(_candidate_rows(), env=env)

    assert result["status"] == "skipped_runtime_disabled"
    assert result["shadow_rows"] == []
    assert result["shadow_row_count"] == 0
    assert result["writes_performed"] is False


@pytest.mark.parametrize("flag_value", ["1", "true", "on", "yes", "enabled", "TRUE", "Enabled"])
def test_flag_parser_treats_on_values_case_insensitively(flag_value: str) -> None:
    assert parse_ml_shadow_scorer_v1_runtime_flag(flag_value) is True


@pytest.mark.parametrize("flag_value", [None, "", "0", "false", "off", "no", "disabled", "banana"])
def test_flag_parser_treats_off_unknown_and_unset_as_off(flag_value: str | None) -> None:
    assert parse_ml_shadow_scorer_v1_runtime_flag(flag_value) is False


def test_explicit_on_computes_same_scores_as_formula_helper() -> None:
    rows = _candidate_rows()
    result = run_ml_shadow_scorer_v1_online_shadow_runtime(rows, env={FEATURE_FLAG: "on"})
    expected = sorted(
        compute_shadow_score_rows(rows),
        key=lambda row: (-float(row["ml_shadow_scorer_v1_score"]), str(row["canonical_openalex_work_id"])),
    )

    assert result["status"] == "succeeded_test_only"
    assert result["writes_performed"] is False
    assert result["shadow_rows"] == expected


def test_missing_score_or_probability_skips_incomplete_coverage() -> None:
    rows = _candidate_rows()
    del rows[0]["audit_embedding_probability_work"]
    result = run_ml_shadow_scorer_v1_online_shadow_runtime(rows, env={FEATURE_FLAG: "true"})

    assert result["status"] == "skipped_incomplete_coverage"
    assert result["shadow_row_count"] == 0
    assert result["writes_performed"] is False
    assert result["missing_coverage"][0]["missing_or_invalid"] == ["audit_embedding_probability_work"]


def test_candidate_rows_with_label_fields_are_rejected() -> None:
    rows = _candidate_rows()
    rows[0]["good_or_acceptable"] = True
    rows[0]["reviewer_notes"] = "not allowed"
    result = run_ml_shadow_scorer_v1_online_shadow_runtime(rows, env={FEATURE_FLAG: "on"})

    assert result["status"] == "rejected_label_fields_present"
    assert result["shadow_row_count"] == 0
    assert result["writes_performed"] is False
    assert result["forbidden_label_fields"][0]["forbidden_fields"] == ["good_or_acceptable", "reviewer_notes"]


def test_identity_mismatch_rejected_when_flag_on() -> None:
    rows = _candidate_rows()
    rows[0]["ranking_run_id"] = "rank-other"
    result = run_ml_shadow_scorer_v1_online_shadow_runtime(rows, env={FEATURE_FLAG: "enabled"})

    assert result["status"] == "rejected_identity_mismatch"
    assert result["shadow_row_count"] == 0
    assert result["writes_performed"] is False


def test_artifact_writer_rejects_gates_not_passed(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerOnlineShadowRuntimeError, match="generalization_audit_gates_passed"):
        build_ml_shadow_scorer_online_shadow_runtime_disabled_payload(
            **_paths(tmp_path, gates=_gates_payload(passed=False)),
            repo_root=tmp_path,
        )


def test_artifact_writer_rejects_policy_without_default_off(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerOnlineShadowRuntimeError, match="policy disable default"):
        build_ml_shadow_scorer_online_shadow_runtime_disabled_payload(
            **_paths(tmp_path, policy=_policy_payload(default_off=False)),
            repo_root=tmp_path,
        )


def test_artifact_writer_records_disabled_run_and_blockers(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_online_shadow_runtime_disabled_payload(
        **_paths(tmp_path),
        repo_root=tmp_path,
        generated_at="2026-05-26T00:00:00Z",
    )

    assert payload["runtime_implementation_present"] is True
    assert payload["runtime_disabled_by_default"] is True
    assert payload["runtime_default_state"] == "off"
    assert payload["runtime_execution_authorized"] is False
    assert payload["last_disabled_run"] == {
        "status": "skipped_runtime_disabled",
        "shadow_row_count": 0,
        "writes_performed": False,
    }
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_online_shadow_implementation_disabled_by_default"] is False
    assert blockers["missing_shadow_runtime_isolation_verification"] is True
    assert blockers["online_shadow_execution_enabled"] is False
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False
    assert blockers["runtime_implementation_authorized"] is False


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "runtime.json"
    out_md = tmp_path / "runtime.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-runtime-disabled",
        "--generalization-audit-gates",
        str(paths["generalization_audit_gates_path"]),
        "--second-surface-generalization-audit",
        str(paths["second_surface_generalization_audit_path"]),
        "--online-shadow-policy",
        str(paths["online_shadow_policy_path"]),
        "--shadow-scorer-spec",
        str(paths["shadow_scorer_spec_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_runtime_disabled"
    assert payload["last_disabled_run"]["status"] == "skipped_runtime_disabled"
    assert payload["recommended_next_stage"] == "run_ml_shadow_scorer_v1_runtime_isolation_verification_v1"
    assert "skipped_runtime_disabled" in result.stdout
    assert "Online Shadow Runtime Disabled" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_runtime.py").read_text(
        encoding="utf-8"
    )
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-runtime-disabled"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
