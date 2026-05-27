"""Disabled-by-default ml-shadow-scorer-v1 online shadow runtime shell.

This module implements an inert runtime entry point for future online shadow
execution. The feature flag defaults off, disabled runs return no rows, and the
artifact writer records that runtime implementation exists without authorizing
runtime execution. It does not query databases, call APIs, write outputs,
generate embeddings, refit scorers, ingest labels, or change production
behavior.
"""

from __future__ import annotations

import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_v1 import compute_shadow_score_rows
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_runtime_disabled"
RUNTIME_VERSION = "ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1"

GATES_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_gates"
GATES_VERSION = "ml-shadow-scorer-v1-generalization-audit-gates-v1"
AUDIT_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_generalization_audit"
AUDIT_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-surface-generalization-audit-v1"
ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"
SPEC_ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SPEC_VERSION = "ml-shadow-scorer-v1-spec"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"

SCORER_ID = "ml-shadow-scorer-v1"
FORMULA_ID = "hybrid_rank_mean_50_50"
RANKING_RUN_ID = "rank-83787b91ef"
FAMILY = "emerging"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
CANDIDATE_POOL_WORK_SET_SHA256 = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
FEATURE_FLAG = "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED"
RECOMMENDED_NEXT_STAGE = "run_ml_shadow_scorer_v1_runtime_isolation_verification_v1"
FUTURE_VERIFICATION_ARTIFACT = "docs/audit/ml-shadow-scorer-v1-runtime-isolation-verification-v1.json"

FLAG_ON_VALUES = ("1", "true", "on", "yes", "enabled")
FLAG_OFF_VALUES = ("", "0", "false", "off", "no", "disabled")

FORBIDDEN_LABEL_FIELDS = {
    "label_any_positive",
    "good_or_acceptable",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
    "review_pool_variant",
    "sample_reason",
    "holdout_assignment",
    "holdout_assignment_version",
    "holdout_split",
    "holdout_set",
    "train_eval_split",
    "split",
}

IDENTITY_FIELDS = {
    "ranking_run_id": RANKING_RUN_ID,
    "family": FAMILY,
    "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
    "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
    "embedding_version": EMBEDDING_VERSION,
}

CAVEATS = (
    "Runtime implementation is present but disabled by default.",
    "This artifact does not authorize runtime execution, online shadowing, DB writes, API/web behavior, or production default changes.",
    "Runtime scoring can only use supplied read-only rows with final_score and audit_embedding_probability_work.",
    "Incomplete learned-probability coverage skips the entire run; no partial shadow scoring is produced.",
    "Labels and holdout assignment fields are rejected as scoring inputs.",
    "Runtime isolation verification remains required before any future online shadow execution.",
)


class MLShadowScorerOnlineShadowRuntimeError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowRuntimeError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowRuntimeError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowRuntimeError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowRuntimeError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def parse_ml_shadow_scorer_v1_runtime_flag(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in set(FLAG_ON_VALUES)


def _flag_value(env: Mapping[str, str] | None) -> Any:
    if env is None:
        return os.environ.get(FEATURE_FLAG)
    return env.get(FEATURE_FLAG)


def _base_runtime_result(*, status: str, flag_value: Any, reason: str) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_feature_flag_value": flag_value,
        "runtime_enabled": parse_ml_shadow_scorer_v1_runtime_flag(flag_value),
        "shadow_rows": [],
        "shadow_row_count": 0,
        "writes_performed": False,
        "write_count": 0,
        "labels_used_for_scoring": False,
        "production_default_changed": False,
        "user_visible_ranking_changed": False,
    }


def _forbidden_fields_present(candidate_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, row in enumerate(candidate_rows):
        present = sorted(field for field in FORBIDDEN_LABEL_FIELDS if field in row)
        if present:
            findings.append(
                {
                    "row_index": index,
                    "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                    "forbidden_fields": present,
                }
            )
    return findings


def _identity_mismatches(candidate_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    for index, row in enumerate(candidate_rows):
        for field, expected in IDENTITY_FIELDS.items():
            observed = row.get(field)
            if observed is not None and observed != expected:
                mismatches.append(
                    {
                        "row_index": index,
                        "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                        "field": field,
                        "expected": expected,
                        "observed": observed,
                    }
                )
    return mismatches


def _coverage_missing(candidate_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, row in enumerate(candidate_rows):
        work_id = str(row.get("canonical_openalex_work_id") or "").strip()
        fields = []
        if not work_id or work_id in seen_ids:
            fields.append("canonical_openalex_work_id")
        seen_ids.add(work_id)
        if _float_or_none(row.get("final_score")) is None:
            fields.append("final_score")
        if _float_or_none(row.get("audit_embedding_probability_work")) is None:
            fields.append("audit_embedding_probability_work")
        if fields:
            missing.append({"row_index": index, "canonical_openalex_work_id": work_id or None, "missing_or_invalid": fields})
    return missing


def run_ml_shadow_scorer_v1_online_shadow_runtime(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    rows = list(candidate_rows)
    flag_value = _flag_value(env)

    if not all(isinstance(row, Mapping) for row in rows):
        return _base_runtime_result(
            status="skipped_incomplete_coverage",
            flag_value=flag_value,
            reason="candidate rows must be objects",
        )

    forbidden = _forbidden_fields_present(rows)
    if forbidden:
        result = _base_runtime_result(
            status="rejected_label_fields_present",
            flag_value=flag_value,
            reason="candidate rows contain label or holdout fields forbidden for runtime scoring",
        )
        result["forbidden_label_fields"] = forbidden
        return result

    if not parse_ml_shadow_scorer_v1_runtime_flag(flag_value):
        return _base_runtime_result(
            status="skipped_runtime_disabled",
            flag_value=flag_value,
            reason=f"{FEATURE_FLAG} is off or unset",
        )

    identity_mismatches = _identity_mismatches(rows)
    if identity_mismatches:
        result = _base_runtime_result(
            status="rejected_identity_mismatch",
            flag_value=flag_value,
            reason="candidate row identity fields do not match approved second-surface evidence scope",
        )
        result["identity_mismatches"] = identity_mismatches
        return result

    missing = _coverage_missing(rows)
    if missing:
        result = _base_runtime_result(
            status="skipped_incomplete_coverage",
            flag_value=flag_value,
            reason="candidate rows must have complete final_score and audit_embedding_probability_work coverage",
        )
        result["missing_coverage"] = missing
        return result

    scored_rows = compute_shadow_score_rows(rows)
    scored_rows.sort(key=lambda row: (-float(row["ml_shadow_scorer_v1_score"]), str(row["canonical_openalex_work_id"])))
    return {
        "status": "succeeded_test_only",
        "reason": "feature flag enabled in explicit test-only context; no writes performed",
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_feature_flag_value": flag_value,
        "runtime_enabled": True,
        "shadow_rows": scored_rows,
        "shadow_row_count": len(scored_rows),
        "writes_performed": False,
        "write_count": 0,
        "labels_used_for_scoring": False,
        "production_default_changed": False,
        "user_visible_ranking_changed": False,
    }


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowRuntimeError(f"{name} must be {expected!r}, got {observed!r}")


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    _require_equal(f"{name} metadata.artifact_type", metadata.get("artifact_type"), artifact_type)
    _require_equal(f"{name} metadata.{version_field}", metadata.get(version_field), version)
    return metadata


def _production_plan_observed_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    good = _get(payload, "targets.good_or_acceptable")
    return {
        "overall_status": payload.get("overall_status") or _get(payload, "metadata.overall_status"),
        "production_default_authorized": payload.get("production_default_authorized"),
        "good_or_acceptable_production_eligible": good.get("production_eligible") if isinstance(good, Mapping) else None,
    }


def _production_plan_blocked(payload: Mapping[str, Any]) -> bool:
    observed = _production_plan_observed_fields(payload)
    return (
        observed["overall_status"] == "research_only"
        and observed["production_default_authorized"] is not True
        and observed["good_or_acceptable_production_eligible"] is False
    )


def _validate_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-audit-gates",
        artifact_type=GATES_ARTIFACT_TYPE,
        version_field="gates_version",
        version=GATES_VERSION,
    )
    required = {
        "generalization_audit_gates_passed": True,
        "second_surface_generalization_passed": True,
        "disabled_by_default_runtime_implementation_next_stage_allowed": True,
        "recommended_next_stage": "implement_online_shadow_runtime_disabled_by_default",
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }
    for path, expected in required.items():
        _require_equal(f"gates {path}", _get(payload, path), expected)
    for field, expected in IDENTITY_FIELDS.items():
        _require_equal(f"gates metadata.{field}", metadata.get(field), expected)
    return metadata


def _validate_audit(payload: Mapping[str, Any], gates_metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="second-surface-generalization-audit",
        artifact_type=AUDIT_ARTIFACT_TYPE,
        version_field="artifact_version",
        version=AUDIT_ARTIFACT_VERSION,
    )
    for field in ("ranking_run_id", "family", "candidate_pool_work_set_sha256", "corpus_snapshot_version", "embedding_version"):
        _require_equal(f"audit/gates identity {field}", metadata.get(field), gates_metadata.get(field))
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-policy",
        artifact_type=ONLINE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=ONLINE_POLICY_VERSION,
    )
    _require_equal("online policy runtime_implementation_authorized", payload.get("runtime_implementation_authorized"), False)
    _require_equal("policy disable default", _get(payload, "disable_and_rollback_policy.disable_switch_default"), "off")
    _require_equal("policy feature flag default off", _get(payload, "runtime_isolation_policy.feature_flag_default_off"), True)
    _require_equal("policy feature flag", _get(payload, "runtime_isolation_policy.feature_flag"), FEATURE_FLAG)
    return metadata


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-spec",
        artifact_type=SPEC_ARTIFACT_TYPE,
        version_field="spec_version",
        version=SPEC_VERSION,
    )
    _require_equal("spec formula id", _get(payload, "scoring_formula.formula_id"), FORMULA_ID)
    _require_equal("spec shadow_scoring_allowed", payload.get("shadow_scoring_allowed"), False)
    _require_equal("spec production_default_allowed", payload.get("production_default_allowed"), False)
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="production-readiness-plan",
        artifact_type=PRODUCTION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=PRODUCTION_PLAN_VERSION,
    )
    if not _production_plan_blocked(payload):
        raise MLShadowScorerOnlineShadowRuntimeError("production readiness plan must remain research-only and blocked")
    return metadata


def build_ml_shadow_scorer_online_shadow_runtime_disabled_payload(
    *,
    generalization_audit_gates_path: Path,
    second_surface_generalization_audit_path: Path,
    online_shadow_policy_path: Path,
    shadow_scorer_spec_path: Path,
    production_readiness_plan_path: Path,
    runtime_version: str = RUNTIME_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    gates_path = Path(generalization_audit_gates_path).resolve()
    audit_path = Path(second_surface_generalization_audit_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()
    spec_path = Path(shadow_scorer_spec_path).resolve()
    production_path = Path(production_readiness_plan_path).resolve()

    gates_payload = _load_json_object(gates_path)
    audit_payload = _load_json_object(audit_path)
    policy_payload = _load_json_object(policy_path)
    spec_payload = _load_json_object(spec_path)
    production_payload = _load_json_object(production_path)

    gates_metadata = _validate_gates(gates_payload)
    audit_metadata = _validate_audit(audit_payload, gates_metadata)
    policy_metadata = _validate_policy(policy_payload)
    spec_metadata = _validate_spec(spec_payload)
    production_metadata = _validate_production_plan(production_payload)

    inputs = [
        _input_record("generalization_audit_gates", gates_path, repo_root=root),
        _input_record("second_surface_generalization_audit", audit_path, repo_root=root),
        _input_record("online_shadow_policy", policy_path, repo_root=root),
        _input_record("shadow_scorer_spec", spec_path, repo_root=root),
        _input_record("production_readiness_plan", production_path, repo_root=root),
    ]
    last_disabled_run = run_ml_shadow_scorer_v1_online_shadow_runtime([], env={})
    if last_disabled_run["status"] != "skipped_runtime_disabled" or last_disabled_run["shadow_row_count"] != 0:
        raise MLShadowScorerOnlineShadowRuntimeError("runtime default disabled run did not skip with zero rows")

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "runtime_version": runtime_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "scorer_id": SCORER_ID,
        "formula_id": FORMULA_ID,
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
        "source_generalization_audit_gates_version": gates_metadata.get("gates_version"),
        "source_generalization_audit_version": audit_metadata.get("artifact_version"),
        "source_online_shadow_policy_version": policy_metadata.get("policy_version"),
        "source_shadow_scorer_spec_version": spec_metadata.get("spec_version"),
        "source_production_readiness_plan_version": production_metadata.get("plan_version"),
    }
    blockers = {
        "missing_generalization_audit_on_second_surface": False,
        "missing_generalization_audit_gates": False,
        "missing_online_shadow_implementation_disabled_by_default": False,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "runtime_implementation_authorized": False,
    }
    return {
        "metadata": metadata,
        "runtime_implementation_present": True,
        "runtime_disabled_by_default": True,
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_default_state": "off",
        "runtime_flag_on_values": list(FLAG_ON_VALUES),
        "runtime_flag_off_values": list(FLAG_OFF_VALUES),
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "production_default_changed": False,
        "user_visible_ranking_changed": False,
        "runtime_contract": {
            "entry_point": "run_ml_shadow_scorer_v1_online_shadow_runtime",
            "feature_flag": FEATURE_FLAG,
            "default_state": "off",
            "input_rows_required_fields": [
                "canonical_openalex_work_id",
                "final_score",
                "audit_embedding_probability_work",
                "ranking_run_id",
                "family",
            ],
            "identity_fields_consistent_when_present": sorted(IDENTITY_FIELDS),
            "forbidden_label_fields": sorted(FORBIDDEN_LABEL_FIELDS),
            "skip_on_incomplete_coverage": True,
            "partial_scoring_allowed": False,
            "writes_performed": False,
        },
        "last_disabled_run": {
            "status": last_disabled_run["status"],
            "shadow_row_count": last_disabled_run["shadow_row_count"],
            "writes_performed": last_disabled_run["writes_performed"],
        },
        "source_evidence": {
            "generalization_audit_gates_passed": gates_payload["generalization_audit_gates_passed"],
            "second_surface_generalization_passed": gates_payload["second_surface_generalization_passed"],
            "disabled_by_default_runtime_implementation_next_stage_allowed": gates_payload[
                "disabled_by_default_runtime_implementation_next_stage_allowed"
            ],
            "online_shadow_policy_is_cited_not_authorizing_runtime": True,
            "production_plan_blocked": _production_plan_blocked(production_payload),
            "production_plan_observed": _production_plan_observed_fields(production_payload),
        },
        "future_verification_artifact": FUTURE_VERIFICATION_ARTIFACT,
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_runtime_disabled(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    contract = payload["runtime_contract"]
    disabled = payload["last_disabled_run"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Runtime Disabled ({metadata['runtime_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records an inert, disabled-by-default runtime implementation for ml-shadow-scorer-v1. It does not enable online shadow execution, write shadow tables, integrate API/web paths, or change production defaults.",
        "",
        f"- Runtime implementation present: {payload['runtime_implementation_present']}",
        f"- Runtime default state: `{payload['runtime_default_state']}`",
        f"- Feature flag: `{payload['runtime_feature_flag']}`",
        f"- Runtime execution authorized: {payload['runtime_execution_authorized']}",
        f"- Last disabled run status: `{disabled['status']}`",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Feature Flag Behavior",
        "",
        f"- On values: {', '.join(f'`{value}`' for value in payload['runtime_flag_on_values'])}",
        f"- Off values: {', '.join(f'`{value}`' for value in payload['runtime_flag_off_values'])}",
        "- Unset, empty, unknown, and all non-on values are treated as off.",
        "",
        "## Runtime Contract",
        "",
        f"- Entry point: `{contract['entry_point']}`",
        f"- Required input fields: {', '.join(f'`{value}`' for value in contract['input_rows_required_fields'])}",
        f"- Identity fields checked when present: {', '.join(f'`{value}`' for value in contract['identity_fields_consistent_when_present'])}",
        f"- Partial scoring allowed: {contract['partial_scoring_allowed']}",
        f"- Skip on incomplete coverage: {contract['skip_on_incomplete_coverage']}",
        f"- Writes performed: {contract['writes_performed']}",
        "",
        "## Forbidden Label Fields",
        "",
    ]
    lines.extend(f"- `{field}`" for field in contract["forbidden_label_fields"])
    lines.extend(["", "## Remaining Blockers", ""])
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(
        [
            "",
            "## Future Verification",
            "",
            f"- Future artifact: `{payload['future_verification_artifact']}`",
            "- Runtime isolation verification is still required before any future online shadow execution.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_runtime_disabled(
    *,
    generalization_audit_gates_path: Path,
    second_surface_generalization_audit_path: Path,
    online_shadow_policy_path: Path,
    shadow_scorer_spec_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    runtime_version: str = RUNTIME_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_runtime_disabled_payload(
        generalization_audit_gates_path=generalization_audit_gates_path,
        second_surface_generalization_audit_path=second_surface_generalization_audit_path,
        online_shadow_policy_path=online_shadow_policy_path,
        shadow_scorer_spec_path=shadow_scorer_spec_path,
        production_readiness_plan_path=production_readiness_plan_path,
        runtime_version=runtime_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_online_shadow_runtime_disabled(payload), encoding="utf-8")
    return payload
