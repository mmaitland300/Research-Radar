"""Machine-checkable label split policy for offline ML ranker research.

This module writes a deterministic spec artifact only. It does not assign
folds, train models, call OpenAlex, use Postgres, or change production ranking.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.openalex_ids import normalize_w_token
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_label_split_policy"
POLICY_VERSION = "ml-label-split-policy-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
TRANSFER_READINESS_ARTIFACT_TYPE = "ml_text_transfer_readiness"
TRANSFER_READINESS_VERSION_PREFIX = "ml-text-transfer-readiness-"

TARGET_GOOD = "good_or_acceptable"
TARGET_SURPRISING = "surprising_or_useful"
TARGETS = (TARGET_GOOD, TARGET_SURPRISING)
SOURCE_FIELDS_CHECKED_IN_ORDER = ("work_id", "openalex_work_id", "paper_id")

CAVEATS = (
    "Not validation.",
    "Not production.",
    "Single-reviewer audit labels.",
    "No fold assignment yet.",
    "No ranking/API/web changes.",
    "No production ranking change implied.",
)


class MLLabelSplitPolicyError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLLabelSplitPolicyError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLLabelSplitPolicyError(f"Expected JSON object in {path}")
    return payload


def _input_record(name: str, path: Path, *, repo_root: Path | None = None) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLLabelSplitPolicyError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLLabelSplitPolicyError(f"{name} JSON missing metadata object")
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLLabelSplitPolicyError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLabelSplitPolicyError("label dataset missing rows array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _validate_production_readiness_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    if metadata.get("artifact_type") != PRODUCTION_PLAN_ARTIFACT_TYPE:
        raise MLLabelSplitPolicyError(
            "expected production-readiness-plan metadata.artifact_type="
            f"{PRODUCTION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PRODUCTION_PLAN_VERSION:
        raise MLLabelSplitPolicyError(
            f"expected production-readiness-plan metadata.plan_version={PRODUCTION_PLAN_VERSION!r}, "
            f"got {metadata.get('plan_version')!r}"
        )
    return metadata


def _validate_transfer_readiness(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="transfer-readiness")
    if metadata.get("artifact_type") != TRANSFER_READINESS_ARTIFACT_TYPE:
        raise MLLabelSplitPolicyError(
            "expected transfer-readiness metadata.artifact_type="
            f"{TRANSFER_READINESS_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    version = metadata.get("readiness_version")
    if not isinstance(version, str) or not version.startswith(TRANSFER_READINESS_VERSION_PREFIX):
        raise MLLabelSplitPolicyError(
            "expected transfer-readiness metadata.readiness_version beginning with "
            f"{TRANSFER_READINESS_VERSION_PREFIX!r}, got {version!r}"
        )
    return metadata


def _bucket(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "(null)"


def _target_bucket(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "null"


def canonical_openalex_work_id(row: Mapping[str, Any]) -> str | None:
    for field in SOURCE_FIELDS_CHECKED_IN_ORDER:
        token = normalize_w_token(str(row.get(field) or ""))
        if token:
            return token
    return None


def _count_by(rows: list[Mapping[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(_bucket(row.get(field)) for row in rows).items()))


def _target_counts(rows: list[Mapping[str, Any]], target: str) -> dict[str, int]:
    counts = Counter(_target_bucket(row.get(target)) for row in rows)
    return {"true": counts["true"], "false": counts["false"], "null": counts["null"], "total": len(rows)}


def _target_counts_by_variant(rows: list[Mapping[str, Any]], target: str) -> dict[str, dict[str, int]]:
    by_variant: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_variant[_bucket(row.get("review_pool_variant"))].append(row)
    return {variant: _target_counts(items, target) for variant, items in sorted(by_variant.items())}


def _duplicate_conflict_rollups(label_payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = label_payload.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    duplicate = metadata.get("duplicate_paper_id_report") if isinstance(metadata.get("duplicate_paper_id_report"), Mapping) else {}
    raw_conflict = metadata.get("conflicting_label_report") if isinstance(metadata.get("conflicting_label_report"), Mapping) else {}
    derived_conflict = (
        metadata.get("derived_target_conflict_report")
        if isinstance(metadata.get("derived_target_conflict_report"), Mapping)
        else {}
    )
    return {
        "duplicate_paper_id_count": int(duplicate.get("duplicate_paper_id_count") or 0),
        "raw_conflicting_label_count": int(raw_conflict.get("conflicting_label_count") or 0),
        "derived_target_conflict_count": int(derived_conflict.get("derived_target_conflict_count") or 0),
    }


def build_dataset_inventory(label_payload: Mapping[str, Any]) -> dict[str, Any]:
    rows = _validate_label_dataset(label_payload)
    explicit_rows = [row for row in rows if row_has_explicit_label(row)]
    explicit_audit_rows = [row for row in explicit_rows if str(row.get("split") or "").strip() == "audit_only"]

    work_groups: dict[str, list[str]] = defaultdict(list)
    rows_missing_canonical_work_id = 0
    for row in explicit_audit_rows:
        canonical = canonical_openalex_work_id(row)
        if canonical is None:
            rows_missing_canonical_work_id += 1
            continue
        work_groups[canonical].append(str(row.get("row_id") or ""))

    duplicate_groups = {
        work_id: row_ids
        for work_id, row_ids in sorted(work_groups.items())
        if len(row_ids) > 1
    }

    target_counts = {}
    for target in TARGETS:
        target_counts[target] = {
            "overall": _target_counts(rows, target),
            "by_review_pool_variant": _target_counts_by_variant(rows, target),
        }

    good_eligible = [
        row
        for row in explicit_audit_rows
        if canonical_openalex_work_id(row) is not None and isinstance(row.get(TARGET_GOOD), bool)
    ]

    inventory = {
        "inventory_scope": {
            "total_observation_rows": "all rows in the label dataset",
            "work_group_counts": "explicit audit_only rows with at least one manual label",
            "target_counts": "all rows in the label dataset",
        },
        "total_observation_rows": len(rows),
        "explicit_labeled_rows": len(explicit_rows),
        "explicit_audit_labeled_rows": len(explicit_audit_rows),
        "v1_good_or_acceptable_split_eligible_observation_rows": len(good_eligible),
        "unique_canonical_work_count": len(work_groups),
        "duplicate_work_group_count": len(duplicate_groups),
        "duplicate_work_observation_pressure_count": sum(len(row_ids) - 1 for row_ids in duplicate_groups.values()),
        "rows_missing_canonical_work_id": rows_missing_canonical_work_id,
        "rows_by_review_pool_variant": _count_by(rows, "review_pool_variant"),
        "rows_by_family": _count_by(rows, "family"),
        "targets": target_counts,
        "duplicate_work_groups_preview": [
            {"canonical_openalex_work_id": work_id, "row_count": len(row_ids), "row_ids": row_ids}
            for work_id, row_ids in list(duplicate_groups.items())[:25]
        ],
        "duplicate_conflict_rollups": _duplicate_conflict_rollups(label_payload),
    }
    return inventory


def _target_policy() -> dict[str, Any]:
    return {
        TARGET_GOOD: {
            "status": "eligible_for_offline_ranker_research",
            "production_eligible": False,
            "allowed_next_stage": "offline_ranker_research_only",
            "reason": "Current evidence supports treating good_or_acceptable as the only v1 offline research target.",
        },
        TARGET_SURPRISING: {
            "status": "excluded_from_v1_split",
            "production_eligible": False,
            "reason": "Hard exclusion from v1 split eligibility because current evidence shows weak/inconsistent transfer and rubric instability.",
        },
    }


def _grouping_policy() -> dict[str, Any]:
    return {
        "grouping_key_name": "canonical_openalex_work_id",
        "source_fields_checked_in_order": list(SOURCE_FIELDS_CHECKED_IN_ORDER),
        "normalization_rules": [
            "accept W tokens and https://openalex.org/W... URLs",
            "strip whitespace",
            "normalize to uppercase W token",
            "reject missing/non-OpenAlex identifiers from split eligibility",
        ],
        "leakage_rule": "all rows with the same canonical_openalex_work_id must share the same future split assignment",
    }


def _eligibility_rules() -> dict[str, Any]:
    return {
        "required_split": "audit_only",
        "require_explicit_manual_labels": True,
        "require_good_or_acceptable_boolean": True,
        "exclude_rows_with_missing_canonical_work_id": True,
        "preserve_observation_rows": True,
        "do_not_dedupe_for_modeling_convenience": True,
        "review_pool_variant_handling": {
            "rank_shaped_family_pools": "may be eligible only under future experiment-specific candidate-pool rules",
            "external_blind_transfer_gap_pools": "remain audit evidence unless the offline ranker experiment explicitly opts into them",
            "silent_pooling_allowed": False,
            "policy_citation_required_for_pooling": True,
        },
    }


def _duplicate_paper_policy(conflict_policy_path: Path, *, repo_root: Path | None) -> dict[str, Any]:
    return {
        "duplicate_conflicting_observations_are_preserved": True,
        "duplicate_work_groups_assigned_as_unit": True,
        "silent_label_merge_allowed": False,
        "conflict_policy_path": portable_repo_path(conflict_policy_path.resolve(), repo_root=repo_root),
        "conflict_policy_citation_required": True,
    }


def _leakage_rules() -> list[str]:
    return [
        "no canonical work may appear in both train and eval",
        "no row_id-level splitting without work grouping",
        "no target may use labels from its eval group in feature construction or sampling",
        "audit-only labels do not authorize production behavior",
    ]


def _randomness_policy() -> dict[str, Any]:
    return {
        "assigns_folds": False,
        "recommended_default_seed": 20260515,
        "future_split_generation_must_declare": [
            "seed",
            "grouping key",
            "stratification fields",
            "leakage report",
        ],
        "no_row_receives_split_id_here": True,
    }


def _policy_assertions() -> dict[str, bool]:
    return {
        "surprising_or_useful_allowed_for_v1_split": False,
        "requires_grouped_split_by_work": True,
        "permits_row_level_random_split": False,
        "permits_silent_conflict_resolution": False,
        "production_default_change_allowed": False,
    }


def _transfer_note(transfer_metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    if transfer_metadata is None:
        return {
            "provided": False,
            "note": "No transfer-readiness artifact was provided; split policy still hard-codes conservative v1 target rules.",
        }
    return {
        "provided": True,
        "transfer_readiness_version": transfer_metadata.get("readiness_version"),
        "note": "Transfer-readiness is evidence-only in this policy; it is not used as a hard gate except where policy metadata explicitly states target eligibility.",
    }


def build_ml_label_split_policy_payload(
    *,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    production_readiness_plan_path: Path,
    transfer_readiness_path: Path | None = None,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()
    transfer_path = Path(transfer_readiness_path).resolve() if transfer_readiness_path is not None else None

    if not conflict_path.exists():
        raise MLLabelSplitPolicyError(f"conflict policy file does not exist: {conflict_path}")

    label_payload = _load_json_object(label_path)
    plan_payload = _load_json_object(plan_path)
    transfer_payload = _load_json_object(transfer_path) if transfer_path is not None else None

    _validate_label_dataset(label_payload)
    plan_metadata = _validate_production_readiness_plan(plan_payload)
    transfer_metadata = _validate_transfer_readiness(transfer_payload) if transfer_payload is not None else None

    inputs = [
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]
    if transfer_path is not None:
        inputs.append(_input_record("transfer_readiness", transfer_path, repo_root=root))

    inventory = build_dataset_inventory(label_payload)
    conflict_sha = sha256_file(conflict_path)

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "policy_version": policy_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "label_dataset_version": label_payload.get("dataset_version"),
        "conflict_policy_sha256": conflict_sha,
        "production_readiness_plan_version": plan_metadata.get("plan_version"),
        "transfer_readiness_version": transfer_metadata.get("readiness_version") if transfer_metadata else None,
        "caveats": list(CAVEATS),
    }

    return {
        "metadata": metadata,
        "allowed_targets_for_v1_split": [TARGET_GOOD],
        "forbidden_targets": [TARGET_SURPRISING],
        "target_policy": _target_policy(),
        "grouping_policy": _grouping_policy(),
        "eligibility_rules": _eligibility_rules(),
        "duplicate_paper_policy": _duplicate_paper_policy(conflict_path, repo_root=root),
        "leakage_rules": _leakage_rules(),
        "conflict_preservation": {
            "conflicts_remain_observable": True,
            "downstream_experiments_must_report_conflict_counts": True,
            "downstream_experiments_must_report_exclusions": True,
        },
        "randomness_policy": _randomness_policy(),
        "dataset_inventory": inventory,
        "policy_assertions": _policy_assertions(),
        "transfer_readiness_note": _transfer_note(transfer_metadata),
    }


def markdown_from_ml_label_split_policy(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    inventory = payload["dataset_inventory"]
    target_policy = payload["target_policy"]
    rollups = inventory["duplicate_conflict_rollups"]

    lines = [
        f"# ML Label Split Policy ({metadata['policy_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact defines the v1 split contract for future offline ranker research. It does not assign folds, train a model, or authorize production ranking changes.",
        "",
        f"- **Allowed v1 target:** `{TARGET_GOOD}`",
        f"- **Forbidden v1 target:** `{TARGET_SURPRISING}`",
        "- **Grouping unit:** canonical OpenAlex work identity, not row_id",
        "- **Production status:** blocked; offline research infrastructure only",
        "",
        "## Target Policy",
        "",
        "| Target | Status | Production Eligible | Reason |",
        "| --- | --- | --- | --- |",
    ]
    for target in TARGETS:
        policy = target_policy[target]
        lines.append(
            f"| `{target}` | {policy['status']} | {policy['production_eligible']} | {policy['reason']} |"
        )

    lines.extend(
        [
            "",
            "## Grouping And Leakage Policy",
            "",
            "Future split generation must normalize `work_id`, `openalex_work_id`, then `paper_id` to an uppercase OpenAlex W token. All observations for the same canonical work must share one split assignment.",
            "",
            "- No canonical work may appear in both train and eval.",
            "- No row_id-level random splitting without work grouping.",
            "- No target may use labels from its eval group in feature construction or sampling.",
            "",
            "## Dataset Inventory",
            "",
            "| Metric | Count |",
            "| --- | ---: |",
            f"| Total observation rows | {inventory['total_observation_rows']} |",
            f"| Explicit labeled rows | {inventory['explicit_labeled_rows']} |",
            f"| Explicit audit labeled rows | {inventory['explicit_audit_labeled_rows']} |",
            f"| Unique canonical work groups | {inventory['unique_canonical_work_count']} |",
            f"| Duplicate canonical work groups | {inventory['duplicate_work_group_count']} |",
            f"| Rows missing canonical work id | {inventory['rows_missing_canonical_work_id']} |",
            f"| v1 good_or_acceptable eligible observations | {inventory['v1_good_or_acceptable_split_eligible_observation_rows']} |",
            "",
            "### Rows By Review Pool Variant",
            "",
            "| Review Pool Variant | Rows |",
            "| --- | ---: |",
        ]
    )
    for variant, count in inventory["rows_by_review_pool_variant"].items():
        lines.append(f"| `{variant}` | {count} |")

    lines.extend(
        [
            "",
            "### Target Counts",
            "",
            "| Target | True | False | Null | Total |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for target in TARGETS:
        counts = inventory["targets"][target]["overall"]
        lines.append(
            f"| `{target}` | {counts['true']} | {counts['false']} | {counts['null']} | {counts['total']} |"
        )

    lines.extend(
        [
            "",
            "## Duplicate And Conflict Policy",
            "",
            "Duplicate and conflicting observations are preserved. Duplicate work groups must be assigned as a unit, and downstream experiments must cite `ml-label-conflict-policy.md` when reporting conflicts or exclusions.",
            "",
            "| Rollup | Count |",
            "| --- | ---: |",
            f"| Duplicate paper IDs in dataset metadata | {rollups['duplicate_paper_id_count']} |",
            f"| Raw label conflicts in dataset metadata | {rollups['raw_conflicting_label_count']} |",
            f"| Derived target conflicts in dataset metadata | {rollups['derived_target_conflict_count']} |",
            "",
            "## Transfer-Readiness Note",
            "",
            payload["transfer_readiness_note"]["note"],
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.extend(
        [
            "",
            "No production ranking, API, web, or default behavior change is implied by this policy.",
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_label_split_policy(
    *,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    transfer_readiness_path: Path | None = None,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_label_split_policy_payload(
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        transfer_readiness_path=transfer_readiness_path,
        policy_version=policy_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_label_split_policy(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "POLICY_VERSION",
    "MLLabelSplitPolicyError",
    "build_dataset_inventory",
    "build_ml_label_split_policy_payload",
    "canonical_openalex_work_id",
    "markdown_from_ml_label_split_policy",
    "write_ml_label_split_policy",
]
