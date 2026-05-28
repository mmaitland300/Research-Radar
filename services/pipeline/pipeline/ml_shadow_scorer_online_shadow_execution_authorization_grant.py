"""Bounded pilot grant artifact for ml-shadow-scorer-v1 online shadow execution.

This module records owner authorization for a narrowly scoped non-production
pilot. It does not run the runtime, enable feature flags, query data stores,
write DB rows, change API/web behavior, or alter production defaults.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_execution_authorization_request import (
    ARTIFACT_TYPE as REQUEST_ARTIFACT_TYPE,
    E10_GATE_ID,
    PREREQUISITE_GATE_IDS,
    REQUEST_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    FORMULA_ID,
    RANKING_RUN_ID,
    SCORER_ID,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_execution_authorization_grant"
GRANT_VERSION = "ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1"
RECOMMENDED_NEXT_STAGE = "prepare_online_shadow_phase1_no_write_pilot_plan_v1"

OWNER = "Matt Maitland"
REVIEW_BY = "2026-08-27"

CAVEATS = (
    "Grant artifact only; it does not run the runtime or enable a feature flag.",
    "Authorization is bounded to a non-production pilot on the approved second-surface identity.",
    "Phase 1 allows no writes.",
    "Isolated audit-only writes require Phase 1 success and separate write-mode isolation proof for Phase 2.",
    "This grant does not authorize production default, production readiness, API/web behavior, or user-visible ranking changes.",
    "Flag default remains off outside the approved pilot environment.",
)


class MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _identity_fields() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
    }


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError("recorded input path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_recorded_records(
    records: Any,
    *,
    repo_root: Path,
    label: str,
) -> list[dict[str, Any]]:
    if not isinstance(records, list) or not records:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"{label} must be a non-empty list")
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"{label}[{index}] must be an object")
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"{label}[{index}].name missing")
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(f"{label}[{index}].sha256 missing")
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        actual_sha = sha256_file(resolved)
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(
                f"{label} input {name} sha256 mismatch: recorded {recorded_sha}, actual {actual_sha}"
            )
        verified.append(
            {
                "name": name,
                "path": portable_repo_path(resolved, repo_root=repo_root),
                "sha256": recorded_sha,
                "verification_status": "confirmed",
            }
        )
    return verified


def _load_enablement_gates_run_from_request(request_payload: Mapping[str, Any], *, repo_root: Path) -> dict[str, Any]:
    record = _enablement_gates_run_input_record(request_payload)
    run_path = _resolve_recorded_path(record.get("path"), repo_root=repo_root)
    return _load_json_object(run_path)


def _enablement_gates_run_input_record(request_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    inputs = _get(request_payload, "metadata.inputs")
    if not isinstance(inputs, list):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError("request metadata.inputs must be a list")
    matches = [record for record in inputs if isinstance(record, Mapping) and record.get("name") == "enablement_gates_run"]
    if len(matches) != 1:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(
            "request metadata.inputs must contain exactly one enablement_gates_run record"
        )
    return matches[0]


def _gate_results_by_id(run_payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    results = run_payload.get("enablement_gate_results")
    if not isinstance(results, list):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError("enablement_gate_results must be a list")
    by_id: dict[str, Mapping[str, Any]] = {}
    for index, gate in enumerate(results):
        if not isinstance(gate, Mapping):
            raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(
                f"enablement_gate_results[{index}] must be an object"
            )
        gate_id = gate.get("gate_id")
        if not isinstance(gate_id, str) or not gate_id:
            raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(
                f"enablement_gate_results[{index}].gate_id missing"
            )
        by_id[gate_id] = gate
    return by_id


def _validate_enablement_gates_run(run_payload: Mapping[str, Any]) -> None:
    _require_equal("enablement run online_shadow_enablement_gates_executed", run_payload.get("online_shadow_enablement_gates_executed"), True)
    _require_equal("enablement run all_prerequisite_gates_satisfied", run_payload.get("all_prerequisite_gates_satisfied"), True)
    by_id = _gate_results_by_id(run_payload)
    missing = [gate_id for gate_id in (*PREREQUISITE_GATE_IDS, E10_GATE_ID) if gate_id not in by_id]
    if missing:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationGrantError(
            f"enablement_gate_results missing required gates: {', '.join(missing)}"
        )
    for gate_id in PREREQUISITE_GATE_IDS:
        gate = by_id[gate_id]
        _require_equal(f"{gate_id} enablement_gate_executed", gate.get("enablement_gate_executed"), True)
        _require_equal(f"{gate_id} decision", gate.get("decision"), "passed")
    _require_equal("E10 enablement_gate_executed", by_id[E10_GATE_ID].get("enablement_gate_executed"), True)
    _require_equal("E10 decision", by_id[E10_GATE_ID].get("decision"), "enablement_evaluation_only_not_authorized")


def _validate_request(request_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(request_payload, name="authorization-request")
    _require_equal("request metadata.artifact_type", metadata.get("artifact_type"), REQUEST_ARTIFACT_TYPE)
    _require_equal("request metadata.request_version", metadata.get("request_version"), REQUEST_VERSION)
    for field, expected in _identity_fields().items():
        _require_equal(f"request metadata.{field}", metadata.get(field), expected)
    required = {
        "online_shadow_execution_authorization_requested": True,
        "online_shadow_execution_authorized": False,
        "authorization_granted": False,
        "all_prerequisite_gates_satisfied": True,
        "recommended_next_stage": "record_online_shadow_execution_authorization_grant_v1",
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "shadow_and_production_blockers.missing_online_shadow_execution_authorization": True,
        "shadow_and_production_blockers.missing_production_readiness_authorization": True,
    }
    for path, expected in required.items():
        _require_equal(f"request {path}", _get(request_payload, path), expected)
    return metadata


def build_ml_shadow_scorer_online_shadow_execution_authorization_grant_payload(
    *,
    authorization_request_path: Path,
    grant_version: str = GRANT_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    request_path = Path(authorization_request_path).resolve()
    request_payload = _load_json_object(request_path)
    request_metadata = _validate_request(request_payload)
    enablement_run_record = _enablement_gates_run_input_record(request_payload)
    verified_request_inputs = _verify_recorded_records(_get(request_payload, "metadata.inputs"), repo_root=root, label="request metadata.inputs")
    verified_chain = _verify_recorded_records(
        _get(request_payload, "metadata.verified_input_chain"),
        repo_root=root,
        label="request metadata.verified_input_chain",
    )
    gates_run_payload = _load_enablement_gates_run_from_request(request_payload, repo_root=root)
    _validate_enablement_gates_run(gates_run_payload)
    generated = generated_at or _now_iso_z()
    request_blockers = dict(request_payload["shadow_and_production_blockers"])
    blockers = {
        **request_blockers,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "runtime_execution_authorized": True,
        "shadow_scoring_allowed": True,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "authorization_scope": "bounded_non_prod_pilot_only",
        "blockers_changed_by_grant": ["missing_online_shadow_execution_authorization"],
    }
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "grant_version": grant_version,
            "generated_at": generated,
            "inputs": [_input_record("authorization_request", request_path, repo_root=root)],
            "source_request_version": request_metadata.get("request_version"),
            "verified_request_inputs": verified_request_inputs,
            "verified_input_chain": verified_chain,
            **_identity_fields(),
        },
        "grant_decision": {
            "decision": "granted",
            "owner": OWNER,
            "review_by": REVIEW_BY,
            "expiry_date": REVIEW_BY,
            "granted_at": generated,
        },
        "authorization_granted": True,
        "online_shadow_execution_authorized": True,
        "missing_online_shadow_execution_authorization": False,
        "online_shadow_execution_enabled": False,
        "feature_flag_default_off": True,
        "flag_may_be_enabled_only_in_pilot_env": True,
        "runtime_execution_authorized": True,
        "shadow_scoring_allowed": True,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_production_readiness_authorization": True,
        "grant_scope": {
            "scope": (
                "Pilot-only ml-shadow-scorer-v1 online shadow on second surface rank-83787b91ef / "
                "emerging / candidate SHA f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc / "
                "hybrid_rank_mean_50_50; non-prod only; read-only prod inputs; skip incomplete coverage; "
                "no production default/API/user-visible ranking changes."
            ),
            "basis": "authorization-request-v1 plus verified enablement run chain",
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "formula_id": FORMULA_ID,
            "scorer_id": SCORER_ID,
        },
        "pilot_authorization": {
            "runtime_execution_authorized": True,
            "shadow_scoring_allowed": True,
            "environments": "non-prod pilot only",
            "initial_ranking_run_ids": [RANKING_RUN_ID],
            "formula_id": FORMULA_ID,
            "scorer_id": SCORER_ID,
        },
        "pilot_bounds": {
            "non_prod_only": True,
            "one_approved_ranking_run_to_start": True,
            "flag_on_only_in_pilot_env": True,
            "manual_or_scheduled_jobs_only": True,
            "second_surface_identity_only": True,
            "no_fleet_wide_enable": True,
            "read_only_prod_inputs": True,
            "skip_incomplete_coverage": True,
        },
        "write_mode_policy": {
            "phase_1": "no_writes",
            "phase_1_writes_allowed": False,
            "phase_2": "isolated_audit_only_writes_after_phase1_and_write_mode_proof",
            "phase_2_requires_separate_authorization": True,
        },
        "required_observability": {
            "policy_contract": {
                "component_coverage": True,
                "missing_learned_probability": True,
                "score_distributions": True,
                "top_k_overlap_with_heuristic": True,
                "rank_displacement": True,
                "family_counts": True,
                "output_completeness": True,
                "runtime_errors": True,
                "latency": True,
                "skipped_candidates_and_reasons": True,
                "skipped_ranking_run_records": True,
                "write_counts_by_isolated_target": True,
            },
            "run_level_fields": [
                "status",
                "shadow_row_count",
                "writes_performed",
                "production_default_changed",
                "user_visible_ranking_changed",
                "api_web_changes_allowed",
                "runtime_feature_flag_value",
            ],
        },
        "rollback_disable_policy": {
            "disable_switch": f"{FEATURE_FLAG}=off",
            "disable_immediately_on_revoke_or_incident": True,
            "pre_post_disable_drill_required": True,
            "production_ranking_unchanged": True,
            "stop_pilot_jobs": True,
            "revoke_via_superseding_grant": True,
        },
        "production_separation_note": (
            "This grant does not grant production default, production readiness, API/web behavior, or user-visible ranking changes."
        ),
        "revocation_policy": {
            "revocation_mechanism": "superseding grant or denied grant",
            "flag_off_first": True,
            "renew_by_review_by_date": REVIEW_BY,
        },
        "basis_artifacts": {
            "authorization_request": portable_repo_path(request_path, repo_root=root),
            "enablement_gates_run": enablement_run_record.get("path"),
            "verified_chain": verified_chain,
        },
        "consumer_guidance": {
            "executed_truth_source": "Use enablement_gate_results as executed truth.",
            "grant_scope_limit": "Top-level runtime_execution_authorized and shadow_scoring_allowed apply solely within pilot_authorization scope.",
            "pilot_plan_authorization_source": "Use authorization_granted, online_shadow_execution_authorized, pilot_authorization, and aligned shadow_and_production_blockers.",
            "flag_global_default": "Grant does not enable the flag globally; default off outside pilot.",
            "online_shadow_execution_enabled_meaning": "False means the feature flag remains default-off globally; it does not negate the bounded pilot grant.",
            "no_production_default": "Grant does not authorize production default or production readiness.",
            "phase_1_no_writes": "Allowed write mode is no writes for Phase 1.",
            "inherit_required_observability": "Pilot plans must inherit required_observability verbatim.",
        },
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_execution_authorization_grant(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    decision = payload["grant_decision"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Execution Authorization Grant ({metadata['grant_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records a bounded non-production pilot grant for ml-shadow-scorer-v1 online shadow execution. It does not run the runtime, enable the feature flag, write DB rows, change API/web behavior, or affect production defaults.",
        "",
        f"- Decision: `{decision['decision']}`",
        f"- Owner: {decision['owner']}",
        f"- Review by: {decision['review_by']}",
        f"- Expiry date: {decision['expiry_date']}",
        f"- Online shadow execution authorized: {payload['online_shadow_execution_authorized']}",
        f"- Online shadow execution enabled: {payload['online_shadow_execution_enabled']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Pilot Scope",
        "",
        f"- {payload['grant_scope']['scope']}",
        f"- Environments: {payload['pilot_authorization']['environments']}",
        f"- Initial ranking run ids: {payload['pilot_authorization']['initial_ranking_run_ids']}",
        "",
        "## Write Mode",
        "",
        f"- Phase 1: {payload['write_mode_policy']['phase_1']}",
        f"- Phase 2: {payload['write_mode_policy']['phase_2']}",
        "",
        "## Observability",
        "",
        f"- Policy contract keys: {list(payload['required_observability']['policy_contract'])}",
        f"- Run-level fields: {payload['required_observability']['run_level_fields']}",
        "",
        "## Rollback",
        "",
        f"- Disable switch: `{payload['rollback_disable_policy']['disable_switch']}`",
        "- Stop pilot jobs on revoke or incident.",
        "- Production ranking remains unchanged.",
        "",
        "## Production Separation",
        "",
        f"- {payload['production_separation_note']}",
        "",
        "## Blockers",
        "",
    ]
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_execution_authorization_grant(
    *,
    authorization_request_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    grant_version: str = GRANT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_execution_authorization_grant_payload(
        authorization_request_path=authorization_request_path,
        grant_version=grant_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_execution_authorization_grant(payload),
        encoding="utf-8",
    )
    return payload
