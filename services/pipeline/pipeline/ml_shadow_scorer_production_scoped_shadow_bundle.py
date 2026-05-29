"""Production-scoped online shadow plan bundle for ml-shadow-scorer-v1.

The production-scoped-shadow bundle is the canonical ladder view after the
production-readiness grant. Revision 0 is an assemble-only pre-plan skeleton;
revision 1 records a paperwork-only plan contract. It does not run runtime
scoring, touch shadow-runs, access databases, enable feature flags, or authorize
production/default/API/user-visible behavior.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_shadow_scorer_phase_bundle import (
    PINNED_IDENTITY,
    verify_ml_shadow_scorer_phase_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_readiness_bundle import (
    GRANT_BUNDLE_REVISION as PRODUCTION_READINESS_GRANT_REVISION,
    POST_GRANT_NEXT_STAGE as PRODUCTION_READINESS_POST_GRANT_NEXT_STAGE,
    verify_ml_shadow_scorer_production_readiness_bundle_payload,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_production_scoped_shadow_bundle"
BUNDLE_VERSION = "online-shadow-production-scoped-v1"
PRE_PLAN_BUNDLE_REVISION = 0
POST_PLAN_BUNDLE_REVISION = 1
PRE_PLAN_NEXT_STAGE = "begin_production_scoped_online_shadow_plan_v1"
POST_PLAN_NEXT_STAGE = "implement_production_scoped_online_shadow_proof_v1"
FEATURE_FLAG = "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED"
FUTURE_ARTIFACT_ROOT = "docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/<pilot_run_id>/"

REQUIRED_ROLES = ("production_readiness_bundle", "phase2_bundle", "online_shadow_policy")
OPTIONAL_ROLES = (
    "execution_authorization_grant",
    "phase2_write_mode_plan",
    "phase2_write_mode_proof",
    "generalization_audit_gates",
)
LEGACY_ARTIFACT_ROLES = REQUIRED_ROLES + OPTIONAL_ROLES

PLAN_SUBSECTIONS = (
    "prod_scoped_identity_and_rollout_boundaries",
    "feature_flag_iam_config_requirements",
    "prod_read_only_input_contract",
    "production_default_api_user_visible_separation",
    "observability_and_slo_plan",
    "rollback_and_revocation_drill_plan",
    "proof_and_pilot_prerequisites",
    "ci_and_live_gate_requirements",
)

COMMON_PLAN_CAVEATS = (
    "Bundle plan surface only; does not run runtime or shadow scoring.",
    "Bundle does not enable online shadow execution or change the global feature flag default.",
    "Bundle does not authorize production default/API/user-visible ranking behavior.",
    "Bundle does not write shadow-runs files, databases, embeddings, labels, or scorer artifacts.",
    "Frozen upstream bundles and legacy artifacts remain referenced by path and SHA only.",
)

PLAN_CAVEATS = (
    "Plan milestone only; does not authorize production-scoped proof execution or pilot execution.",
    "Future proof must clear missing_prod_scoped_shadow_proof before any prod-scoped pilot can be considered.",
    "Production default/API/user-visible behavior remain separate authorization chains.",
)

EXPLICITLY_NOT_INCLUDED = (
    "global flag enablement",
    "prod default",
    "API/web",
    "user-visible ranking",
    "DB writes/DDL",
)


class MLShadowScorerProductionScopedShadowBundleError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowBundleError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowBundleError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, label: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} missing metadata object")
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
        raise MLShadowScorerProductionScopedShadowBundleError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _require_true(name: str, observed: Any) -> None:
    _require_equal(name, observed, True)


def _require_false(name: str, observed: Any) -> None:
    _require_equal(name, observed, False)


def _validate_identity(identity: Any, *, label: str) -> None:
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} must be an object")
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(f"{label}.{field}", identity.get(field), expected)


def _artifact_record(role: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerProductionScopedShadowBundleError(f"{role} artifact does not exist: {path}")
    return {
        "role": role,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": _sha256_file(resolved),
    }


def _ref_from_record(record: Mapping[str, Any]) -> dict[str, str]:
    return {"path": str(record["path"]), "sha256": str(record["sha256"])}


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerProductionScopedShadowBundleError("referenced path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_reference(ref: Any, *, repo_root: Path, label: str) -> Path:
    if not isinstance(ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} reference must be an object")
    recorded_sha = ref.get("sha256")
    if not isinstance(recorded_sha, str) or not recorded_sha.strip():
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label}.sha256 missing")
    resolved = _resolve_recorded_path(ref.get("path"), repo_root=repo_root)
    if not resolved.exists():
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} path missing on disk: {ref.get('path')}")
    if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
        raise MLShadowScorerProductionScopedShadowBundleError(
            f"{label} sha256 mismatch: recorded {recorded_sha}, actual {_sha256_file(resolved)}"
        )
    return resolved


def _records_by_role(records: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(records, list) or not records:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "metadata.legacy_artifacts_index must be a non-empty list"
        )
    by_role: dict[str, Mapping[str, Any]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"metadata.legacy_artifacts_index[{index}] must be an object"
            )
        role = record.get("role")
        if not isinstance(role, str) or not role:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"metadata.legacy_artifacts_index[{index}].role missing"
            )
        if role not in LEGACY_ARTIFACT_ROLES:
            raise MLShadowScorerProductionScopedShadowBundleError(f"unsupported legacy artifact role {role!r}")
        by_role[role] = record
    missing = [role for role in REQUIRED_ROLES if role not in by_role]
    if missing:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "metadata.legacy_artifacts_index missing roles: " + ", ".join(missing)
        )
    return by_role


def _verify_legacy_index(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Path]]:
    records = _records_by_role(_metadata(bundle, label="production-scoped-shadow bundle").get("legacy_artifacts_index"))
    resolved: dict[str, Path] = {}
    for role, record in records.items():
        resolved[role] = _verify_reference(
            record,
            repo_root=repo_root,
            label=f"metadata.legacy_artifacts_index.{role}",
        )
    return records, resolved


def _validate_production_readiness_bundle(bundle: Mapping[str, Any], *, repo_root: Path) -> None:
    try:
        verify_ml_shadow_scorer_production_readiness_bundle_payload(
            bundle,
            repo_root=repo_root,
            expect_grant_filed=True,
        )
    except Exception as exc:
        raise MLShadowScorerProductionScopedShadowBundleError(str(exc)) from exc
    _require_equal(
        "production-readiness bundle metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        PRODUCTION_READINESS_GRANT_REVISION,
    )
    _require_true(
        "production-readiness bundle authorization.production_readiness_authorization_granted",
        _get(bundle, "authorization.production_readiness_authorization_granted"),
    )
    _require_false(
        "production-readiness bundle posture.missing_production_readiness_authorization",
        _get(bundle, "posture.missing_production_readiness_authorization"),
    )
    _require_false(
        "production-readiness bundle posture.online_shadow_execution_enabled",
        _get(bundle, "posture.online_shadow_execution_enabled"),
    )
    _require_equal(
        "production-readiness bundle recommended_next_stage",
        bundle.get("recommended_next_stage"),
        PRODUCTION_READINESS_POST_GRANT_NEXT_STAGE,
    )
    _validate_identity(_get(bundle, "metadata.pinned_identity"), label="production-readiness metadata.pinned_identity")


def _validate_phase2_bundle(bundle: Mapping[str, Any], *, repo_root: Path) -> None:
    try:
        verify_ml_shadow_scorer_phase_bundle_payload(
            bundle,
            repo_root=repo_root,
            expect_pilot_reviewed=True,
        )
    except Exception as exc:
        raise MLShadowScorerProductionScopedShadowBundleError(str(exc)) from exc
    revision = _get(bundle, "metadata.bundle_revision")
    if not isinstance(revision, int) or revision < 3:
        raise MLShadowScorerProductionScopedShadowBundleError(
            f"phase2 bundle metadata.bundle_revision must be >= 3, got {revision!r}"
        )
    _require_true("phase2 bundle review.phase2_write_pilot_accepted", _get(bundle, "review.phase2_write_pilot_accepted"))
    _validate_identity(_get(bundle, "posture.pinned_identity"), label="phase2 posture.pinned_identity")


def _validate_online_shadow_policy(policy: Mapping[str, Any]) -> None:
    _require_equal(
        "online shadow policy metadata.artifact_type",
        _get(policy, "metadata.artifact_type"),
        "ml_shadow_scorer_online_shadow_policy",
    )
    _require_false("online shadow policy online_shadow_execution_enabled", policy.get("online_shadow_execution_enabled"))
    _require_false("online shadow policy production_default_allowed", policy.get("production_default_allowed"))
    _require_equal(
        "online shadow policy runtime_isolation_policy.feature_flag",
        _get(policy, "runtime_isolation_policy.feature_flag"),
        FEATURE_FLAG,
    )


def _caveats(*, plan_filed: bool) -> list[str]:
    caveats = list(COMMON_PLAN_CAVEATS)
    if plan_filed:
        caveats.extend(PLAN_CAVEATS)
    return caveats


def _authorization() -> dict[str, Any]:
    return {
        "prod_scoped_shadow_plan_authorization_scope": "production_scoped_shadow_plan_paperwork_only",
        "prod_scoped_shadow_execution_authorized": False,
        "prod_scoped_shadow_proof_authorized": False,
        "prod_scoped_shadow_pilot_authorized": False,
        "explicitly_not_included": list(EXPLICITLY_NOT_INCLUDED),
    }


def _execution() -> dict[str, bool]:
    return {
        "prod_scoped_shadow_plan_execution_performed": False,
        "prod_scoped_shadow_proof_executed": False,
        "prod_scoped_shadow_pilot_executed": False,
    }


def _posture(*, plan_defined: bool) -> dict[str, Any]:
    return {
        "prod_scoped_shadow_plan_defined": plan_defined,
        "prod_scoped_shadow_proof_passed": False,
        "prod_scoped_shadow_pilot_executed": False,
        "missing_prod_scoped_shadow_proof": True,
        "prod_scoped_shadow_proof_authorized": False,
        "production_readiness_authorization_granted": True,
        "missing_production_readiness_authorization": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }


def _blockers(production_readiness_bundle: Mapping[str, Any]) -> dict[str, Any]:
    upstream = _get(production_readiness_bundle, "shadow_and_production_blockers")
    blockers = deepcopy(dict(upstream)) if isinstance(upstream, Mapping) else {}
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": True,
            "prod_scoped_shadow_proof_authorized": False,
            "blockers_changed_by_plan": [],
            "blockers_unchanged_by_plan": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    return blockers


def _empty_plan() -> dict[str, Any]:
    return {
        "prod_scoped_shadow_plan_defined": False,
        "plan_decision": None,
    }


def _planned_sections(*, planner: str, planned_at: str, plan_notes: str | None) -> dict[str, Any]:
    return {
        "prod_scoped_shadow_plan_defined": True,
        "plan_decision": {
            "decision": "planned",
            "planner": planner,
            "planned_at": planned_at,
            "plan_notes": plan_notes,
        },
        "prod_scoped_identity_and_rollout_boundaries": {
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "family_scope": "emerging only",
            "ranking_run_id_scope": "rank-83787b91ef and explicitly bounded successors only",
            "rollout": {
                "manual_or_scheduled_jobs_only": True,
                "no_fleet_wide_enable": True,
                "no_cron_without_explicit_later_authorization": True,
            },
            "environment": "prod-scoped read-only evaluation surface; distinct from Phase 2 non-prod isolated file tree",
            "future_artifact_root_proposal": FUTURE_ARTIFACT_ROOT,
            "explicitly_not_in_scope": [
                "production default pins",
                "API-visible tables",
                "user-visible ranking paths",
            ],
        },
        "feature_flag_iam_config_requirements": {
            "runtime_feature_flag": FEATURE_FLAG,
            "feature_flag_default": "off",
            "global_default_unchanged_by_this_plan": True,
            "prod_scoped_flag_enablement_authorized_now": False,
            "iam_config": "read-only prod input access only; no write IAM expansion; no prod config/default bridge changes",
            "config_surfaces_that_may_be_read": [
                "ranking inputs",
                "candidate pool hashes",
                "scorer metadata",
            ],
            "config_surfaces_forbidden_to_change": [
                "production default",
                "API response shaping",
                "bridge weights",
                "fleet env toggles",
            ],
        },
        "prod_read_only_input_contract": {
            "inputs_are_read_only_from_approved_prod_sources": True,
            "labels_used_for_scoring": False,
            "must_include_fields_from_online_shadow_policy": [
                "ranking_run_id",
                "family",
                "candidate_pool_work_set_sha256",
                "final_score_rank_pct",
                "audit_embedding_probability_rank_pct",
                "component coverage",
                "generated_at",
                "input hashes",
            ],
            "input_hashes_traceability_required": True,
            "incomplete_coverage_behavior": "skip entire run per policy; no partial shadow scoring",
            "forbidden_writes": [
                "ranking_runs",
                "paper_scores production paths",
                "embeddings",
                "labels",
                "scorer artifacts",
            ],
        },
        "production_default_api_user_visible_separation": {
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "results_use": "audit/monitoring only",
            "must_not_affect": [
                "user-visible ranking",
                "API responses",
                "bridge defaults",
                "production defaults",
            ],
        },
        "observability_and_slo_plan": {
            "inherits_prod_readiness_grant_slo_targets": True,
            "extends_online_shadow_policy_observability_contract": True,
            "required_signals": [
                "run status",
                "row counts",
                "error counters",
                "latency",
                "component coverage",
                "score distributions",
                "skipped runs/reasons",
                "forbidden write target counts",
                "rank displacement audit-only",
            ],
            "slo_thresholds": "plan targets for proof/pilot verification; not enforced at this plan milestone",
            "forbidden_write_target_counts_must_remain_zero": True,
        },
        "rollback_and_revocation_drill_plan": {
            "first_response": f"flag-off ({FEATURE_FLAG}=off)",
            "stop_prod_scoped_jobs_before_cleanup": True,
            "cleanup_scope": "prod-scoped pilot subdirectory only when proof/pilot exist later",
            "revoke_path": "supersede bundle authorization or deny follow-up review",
            "reverify": "production ranking/API/default unchanged with flag on versus off",
            "derived_from": [
                "production-readiness grant incident_response_and_revocation_plan",
                "Phase 2 disable drill patterns",
            ],
        },
        "proof_and_pilot_prerequisites": {
            "prerequisites_before_proof": [
                "this plan filed",
                "production-readiness grant filed",
                "Phase 2 pilot accepted",
            ],
            "proof_must_demonstrate": [
                "read-only prod input contract honored",
                "zero forbidden writes",
                "observability complete",
                "rollback drill documented and executable",
                "CI gates pass",
            ],
            "pilot_prerequisites_deferred_to_post_proof_authorization_chain": True,
            "missing_prod_scoped_shadow_proof": True,
        },
        "ci_and_live_gate_requirements": {
            "ci_must_continue_to_verify": [
                "phase2 bundle post-review",
                "production-readiness bundle post-grant",
                "production-scoped-shadow bundle post-plan",
            ],
            "future_live_prod_execution_gates": [
                "manual job only",
                "explicit authorization artifact or bundle revision",
                "forbidden-write guards",
                "observability artifact emission",
                "rollback drill evidence",
            ],
            "this_plan_commit_adds_ci_bundle_verify_only": True,
        },
    }


def assemble_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
    *,
    production_readiness_bundle_path: Path,
    phase_bundle_path: Path,
    online_shadow_policy_path: Path,
    execution_authorization_grant_path: Path | None = None,
    phase2_write_mode_plan_path: Path | None = None,
    phase2_write_mode_proof_path: Path | None = None,
    generalization_audit_gates_path: Path | None = None,
    bundle_version: str = BUNDLE_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    production_readiness_path = Path(production_readiness_bundle_path).resolve()
    phase2_path = Path(phase_bundle_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()
    production_readiness_bundle = _load_json_object(production_readiness_path)
    phase2_bundle = _load_json_object(phase2_path)
    policy = _load_json_object(policy_path)
    _validate_production_readiness_bundle(production_readiness_bundle, repo_root=root)
    _validate_phase2_bundle(phase2_bundle, repo_root=root)
    _validate_online_shadow_policy(policy)
    _validate_identity(_get(production_readiness_bundle, "metadata.pinned_identity"), label="production-readiness pinned_identity")
    _validate_identity(_get(phase2_bundle, "posture.pinned_identity"), label="phase2 posture.pinned_identity")

    paths: dict[str, Path] = {
        "production_readiness_bundle": production_readiness_path,
        "phase2_bundle": phase2_path,
        "online_shadow_policy": policy_path,
    }
    optional_paths = {
        "execution_authorization_grant": execution_authorization_grant_path,
        "phase2_write_mode_plan": phase2_write_mode_plan_path,
        "phase2_write_mode_proof": phase2_write_mode_proof_path,
        "generalization_audit_gates": generalization_audit_gates_path,
    }
    for role, path in optional_paths.items():
        if path is not None:
            paths[role] = Path(path).resolve()
    records = {role: _artifact_record(role, paths[role], repo_root=root) for role in paths}
    ordered_records = [records[role] for role in LEGACY_ARTIFACT_ROLES if role in records]
    payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "bundle_version": bundle_version,
            "bundle_revision": PRE_PLAN_BUNDLE_REVISION,
            "generated_at": generated_at or _now_iso_z(),
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "legacy_artifacts_index": ordered_records,
        },
        "upstream_ref": {
            "production_readiness_bundle": {
                **_ref_from_record(records["production_readiness_bundle"]),
                "bundle_revision": _get(production_readiness_bundle, "metadata.bundle_revision"),
            },
            "phase2_bundle": {
                **_ref_from_record(records["phase2_bundle"]),
                "bundle_revision": _get(phase2_bundle, "metadata.bundle_revision"),
            },
            "production_readiness_authorization_granted": _get(
                production_readiness_bundle,
                "authorization.production_readiness_authorization_granted",
            ),
            "phase2_write_pilot_accepted": _get(phase2_bundle, "review.phase2_write_pilot_accepted"),
        },
        "plan": _empty_plan(),
        "authorization": _authorization(),
        "execution": _execution(),
        "posture": _posture(plan_defined=False),
        "shadow_and_production_blockers": _blockers(production_readiness_bundle),
        "writes_performed": False,
        "runtime_writes_performed": False,
        "recommended_next_stage": PRE_PLAN_NEXT_STAGE,
        "caveats": _caveats(plan_filed=False),
    }
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_plan_filed=False,
    )
    return payload


def apply_production_scoped_shadow_plan(
    bundle: Mapping[str, Any],
    *,
    planner: str = "Matt Maitland",
    plan_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _require_equal("metadata.bundle_revision", _get(bundle, "metadata.bundle_revision"), PRE_PLAN_BUNDLE_REVISION)
    _require_false("plan.prod_scoped_shadow_plan_defined", _get(bundle, "plan.prod_scoped_shadow_plan_defined"))
    planned_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PLAN_BUNDLE_REVISION
    metadata["generated_at"] = planned_at
    updated["metadata"] = metadata
    updated["plan"] = _planned_sections(planner=planner, planned_at=planned_at, plan_notes=plan_notes)
    updated["authorization"] = _authorization()
    updated["execution"] = _execution()
    updated["posture"] = _posture(plan_defined=True)
    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": True,
            "prod_scoped_shadow_proof_authorized": False,
            "blockers_changed_by_plan": [],
            "blockers_unchanged_by_plan": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PLAN_NEXT_STAGE
    updated["caveats"] = _caveats(plan_filed=True)
    return updated


def _infer_plan_mode(bundle: Mapping[str, Any], *, expect_plan_filed: bool | None) -> str:
    if expect_plan_filed is True:
        return "post_plan"
    if expect_plan_filed is False:
        return "pre_plan"
    revision = _get(bundle, "metadata.bundle_revision")
    plan_defined = _get(bundle, "plan.prod_scoped_shadow_plan_defined")
    if revision == POST_PLAN_BUNDLE_REVISION and plan_defined is True:
        return "post_plan"
    if revision == PRE_PLAN_BUNDLE_REVISION and plan_defined is False:
        return "pre_plan"
    raise MLShadowScorerProductionScopedShadowBundleError(
        "could not infer production-scoped-shadow bundle mode from revision and plan state"
    )


def _verify_plan_subsections(plan: Mapping[str, Any]) -> None:
    for subsection in PLAN_SUBSECTIONS:
        value = plan.get(subsection)
        if not isinstance(value, Mapping) or not value:
            raise MLShadowScorerProductionScopedShadowBundleError(f"plan.{subsection} must be populated")
    _require_equal("plan.plan_decision.decision", _get(plan, "plan_decision.decision"), "planned")
    if not isinstance(_get(plan, "plan_decision.planner"), str) or not _get(plan, "plan_decision.planner"):
        raise MLShadowScorerProductionScopedShadowBundleError("plan.plan_decision.planner must be populated")
    if not isinstance(_get(plan, "plan_decision.planned_at"), str) or not _get(plan, "plan_decision.planned_at"):
        raise MLShadowScorerProductionScopedShadowBundleError("plan.plan_decision.planned_at must be populated")
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(plan, "feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "plan.prod_read_only_input_contract.labels_used_for_scoring",
        _get(plan, "prod_read_only_input_contract.labels_used_for_scoring"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        _get(plan, "production_default_api_user_visible_separation.production_default_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        _get(plan, "production_default_api_user_visible_separation.api_web_changes_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
        _get(plan, "production_default_api_user_visible_separation.user_visible_ranking_changed"),
    )
    _require_true(
        "plan.proof_and_pilot_prerequisites.missing_prod_scoped_shadow_proof",
        _get(plan, "proof_and_pilot_prerequisites.missing_prod_scoped_shadow_proof"),
    )


def verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path | None = None,
    expect_plan_filed: bool | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal("metadata.artifact_type", metadata.get("artifact_type"), ARTIFACT_TYPE)
    _require_equal("metadata.bundle_version", metadata.get("bundle_version"), BUNDLE_VERSION)
    mode = _infer_plan_mode(bundle, expect_plan_filed=expect_plan_filed)
    expected_revision = POST_PLAN_BUNDLE_REVISION if mode == "post_plan" else PRE_PLAN_BUNDLE_REVISION
    _require_equal("metadata.bundle_revision", metadata.get("bundle_revision"), expected_revision)
    _validate_identity(metadata.get("pinned_identity"), label="metadata.pinned_identity")
    records, resolved_paths = _verify_legacy_index(bundle, repo_root=root)

    production_readiness_bundle = _load_json_object(resolved_paths["production_readiness_bundle"])
    phase2_bundle = _load_json_object(resolved_paths["phase2_bundle"])
    policy = _load_json_object(resolved_paths["online_shadow_policy"])
    _validate_production_readiness_bundle(production_readiness_bundle, repo_root=root)
    _validate_phase2_bundle(phase2_bundle, repo_root=root)
    _validate_online_shadow_policy(policy)

    upstream_ref = bundle.get("upstream_ref")
    if not isinstance(upstream_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("upstream_ref must be an object")
    production_readiness_ref_path = _verify_reference(
        upstream_ref.get("production_readiness_bundle"),
        repo_root=root,
        label="upstream_ref.production_readiness_bundle",
    )
    phase2_ref_path = _verify_reference(
        upstream_ref.get("phase2_bundle"),
        repo_root=root,
        label="upstream_ref.phase2_bundle",
    )
    if production_readiness_ref_path != resolved_paths["production_readiness_bundle"]:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "upstream_ref.production_readiness_bundle path must match legacy index"
        )
    if phase2_ref_path != resolved_paths["phase2_bundle"]:
        raise MLShadowScorerProductionScopedShadowBundleError("upstream_ref.phase2_bundle path must match legacy index")
    _require_equal(
        "upstream_ref.production_readiness_bundle.bundle_revision",
        _get(upstream_ref, "production_readiness_bundle.bundle_revision"),
        PRODUCTION_READINESS_GRANT_REVISION,
    )
    _require_equal(
        "upstream_ref.phase2_bundle.bundle_revision",
        _get(upstream_ref, "phase2_bundle.bundle_revision"),
        _get(phase2_bundle, "metadata.bundle_revision"),
    )
    _require_true(
        "upstream_ref.production_readiness_authorization_granted",
        upstream_ref.get("production_readiness_authorization_granted"),
    )
    _require_true("upstream_ref.phase2_write_pilot_accepted", upstream_ref.get("phase2_write_pilot_accepted"))

    plan = bundle.get("plan")
    if not isinstance(plan, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("plan must be an object")
    if mode == "pre_plan":
        _require_false("plan.prod_scoped_shadow_plan_defined", plan.get("prod_scoped_shadow_plan_defined"))
        _require_equal("plan.plan_decision", plan.get("plan_decision"), None)
        _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), PRE_PLAN_NEXT_STAGE)
    else:
        _require_true("plan.prod_scoped_shadow_plan_defined", plan.get("prod_scoped_shadow_plan_defined"))
        _verify_plan_subsections(plan)
        _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PLAN_NEXT_STAGE)

    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_equal(
        "authorization.prod_scoped_shadow_plan_authorization_scope",
        authorization.get("prod_scoped_shadow_plan_authorization_scope"),
        "production_scoped_shadow_plan_paperwork_only",
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_proof_authorized",
        authorization.get("prod_scoped_shadow_proof_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_pilot_authorized",
        authorization.get("prod_scoped_shadow_pilot_authorized"),
    )
    for item in EXPLICITLY_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )

    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    for field in (
        "prod_scoped_shadow_plan_execution_performed",
        "prod_scoped_shadow_proof_executed",
        "prod_scoped_shadow_pilot_executed",
    ):
        _require_false(f"execution.{field}", execution.get(field))

    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    posture_required = {
        "prod_scoped_shadow_plan_defined": mode == "post_plan",
        "prod_scoped_shadow_proof_passed": False,
        "prod_scoped_shadow_pilot_executed": False,
        "missing_prod_scoped_shadow_proof": True,
        "prod_scoped_shadow_proof_authorized": False,
        "production_readiness_authorization_granted": True,
        "missing_production_readiness_authorization": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }
    for field, expected in posture_required.items():
        _require_equal(f"posture.{field}", posture.get(field), expected)

    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true("shadow_and_production_blockers.missing_prod_scoped_shadow_proof", blockers.get("missing_prod_scoped_shadow_proof"))
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_proof_authorized",
        blockers.get("prod_scoped_shadow_proof_authorized"),
    )
    _require_equal("shadow_and_production_blockers.blockers_changed_by_plan", blockers.get("blockers_changed_by_plan"), [])
    _require_true("shadow_and_production_blockers.blockers_unchanged_by_plan", blockers.get("blockers_unchanged_by_plan"))
    _require_false(
        "shadow_and_production_blockers.online_shadow_execution_enabled",
        blockers.get("online_shadow_execution_enabled"),
    )
    _require_false(
        "shadow_and_production_blockers.production_default_allowed",
        blockers.get("production_default_allowed"),
    )
    _require_false("shadow_and_production_blockers.api_web_changes_allowed", blockers.get("api_web_changes_allowed"))
    _require_false(
        "shadow_and_production_blockers.user_visible_ranking_changed",
        blockers.get("user_visible_ranking_changed"),
    )

    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(plan_filed=(mode == "post_plan")):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(f"caveats missing {caveat!r}")
    if mode == "pre_plan":
        for caveat in PLAN_CAVEATS:
            if caveat in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"pre-plan caveats must not include post-plan {caveat!r}"
                )
    if mode == "post_plan":
        caveat_text = " ".join(str(caveat).lower() for caveat in caveats)
        forbidden_phrases = (
            "authorizes proof execution",
            "authorizes pilot",
            "enables online shadow",
            "production default allowed",
        )
        for phrase in forbidden_phrases:
            if phrase in caveat_text:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post-plan caveats imply forbidden enablement: {phrase}"
                )

    return {
        "verification_status": "passed",
        "verification_mode": mode,
        "bundle_version": metadata.get("bundle_version"),
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
        "legacy_artifact_count": len(records),
    }


def verify_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    repo_root: Path | None = None,
    expect_plan_filed: bool | None = None,
) -> dict[str, Any]:
    payload = _load_json_object(Path(bundle_path).resolve())
    return verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=repo_root,
        expect_plan_filed=expect_plan_filed,
    )


def markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    identity = metadata["pinned_identity"]
    upstream = payload["upstream_ref"]
    plan = payload["plan"]
    authorization = payload["authorization"]
    posture = payload["posture"]
    lines = [
        f"# ml-shadow-scorer-v1 Production-Scoped Shadow Bundle ({metadata['bundle_version']})",
        "",
        "## Executive Summary",
        "",
        "This bundle defines the production-scoped online shadow plan contract while keeping proof, pilot, runtime, production default, API/web, and user-visible behavior disabled.",
        "",
        f"- Bundle revision: {metadata['bundle_revision']}",
        f"- Production-scoped plan defined: {plan['prod_scoped_shadow_plan_defined']}",
        f"- Missing production-scoped shadow proof: {posture['missing_prod_scoped_shadow_proof']}",
        f"- Online shadow execution enabled: {posture['online_shadow_execution_enabled']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Pinned Identity",
        "",
    ]
    for key, value in identity.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Legacy Artifact Index",
            "",
            "| Role | Path | SHA-256 |",
            "| --- | --- | --- |",
        ]
    )
    for record in metadata["legacy_artifacts_index"]:
        lines.append(f"| {record['role']} | `{record['path']}` | `{record['sha256']}` |")
    lines.extend(
        [
            "",
            "## Upstream Evidence",
            "",
            f"- Production-readiness bundle: `{upstream['production_readiness_bundle']['path']}`",
            f"- Production-readiness revision: {upstream['production_readiness_bundle']['bundle_revision']}",
            f"- Production-readiness authorization granted: {upstream['production_readiness_authorization_granted']}",
            f"- Phase 2 bundle: `{upstream['phase2_bundle']['path']}`",
            f"- Phase 2 revision: {upstream['phase2_bundle']['bundle_revision']}",
            f"- Phase 2 write pilot accepted: {upstream['phase2_write_pilot_accepted']}",
            "",
            "## Plan Contract",
            "",
        ]
    )
    if plan["prod_scoped_shadow_plan_defined"]:
        decision = plan["plan_decision"]
        lines.extend(
            [
                f"- Decision: `{decision['decision']}`",
                f"- Planner: {decision['planner']}",
                f"- Planned at: {decision['planned_at']}",
                f"- Plan notes: {decision.get('plan_notes')}",
                f"- Future artifact root proposal: `{plan['prod_scoped_identity_and_rollout_boundaries']['future_artifact_root_proposal']}`",
                f"- Runtime feature flag: `{plan['feature_flag_iam_config_requirements']['runtime_feature_flag']}`",
                f"- Results use: {plan['production_default_api_user_visible_separation']['results_use']}",
                "",
                "## Plan Sections",
                "",
            ]
        )
        for subsection in PLAN_SUBSECTIONS:
            lines.append(f"- `{subsection}`")
    else:
        lines.append("- Plan not filed yet.")
    lines.extend(
        [
            "",
            "## Authorization Boundaries",
            "",
            f"- Plan authorization scope: `{authorization['prod_scoped_shadow_plan_authorization_scope']}`",
            f"- Execution authorized: {authorization['prod_scoped_shadow_execution_authorized']}",
            f"- Proof authorized: {authorization['prod_scoped_shadow_proof_authorized']}",
            f"- Pilot authorized: {authorization['prod_scoped_shadow_pilot_authorized']}",
            "",
            "## Explicitly Not Included",
            "",
        ]
    )
    for item in authorization["explicitly_not_included"]:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Production/API/Default Separation",
            "",
            f"- Production default allowed: {posture['production_default_allowed']}",
            f"- API/web changes allowed: {posture['api_web_changes_allowed']}",
            f"- User-visible ranking changed: {posture['user_visible_ranking_changed']}",
            f"- Writes performed: {payload['writes_performed']}",
            f"- Runtime writes performed: {payload['runtime_writes_performed']}",
            "",
            "## Recommended Next Stage",
            "",
            f"`{payload['recommended_next_stage']}`",
            "",
            "## Caveats",
            "",
        ]
    )
    for caveat in payload["caveats"]:
        lines.append(f"- {caveat}")
    lines.append("")
    return "\n".join(lines)


def write_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    production_readiness_bundle_path: Path,
    phase_bundle_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    execution_authorization_grant_path: Path | None = None,
    phase2_write_mode_plan_path: Path | None = None,
    phase2_write_mode_proof_path: Path | None = None,
    generalization_audit_gates_path: Path | None = None,
    bundle_version: str = BUNDLE_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = assemble_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        production_readiness_bundle_path=production_readiness_bundle_path,
        phase_bundle_path=phase_bundle_path,
        online_shadow_policy_path=online_shadow_policy_path,
        execution_authorization_grant_path=execution_authorization_grant_path,
        phase2_write_mode_plan_path=phase2_write_mode_plan_path,
        phase2_write_mode_proof_path=phase2_write_mode_proof_path,
        generalization_audit_gates_path=generalization_audit_gates_path,
        bundle_version=bundle_version,
        repo_root=repo_root,
    )
    output_path = Path(output_path)
    markdown_output_path = Path(markdown_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(payload),
        encoding="utf-8",
    )
    return payload


def plan_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    planner: str = "Matt Maitland",
    plan_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_plan_filed=False,
    )
    updated = apply_production_scoped_shadow_plan(
        payload,
        planner=planner,
        plan_notes=plan_notes,
    )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_plan_filed=True,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated
