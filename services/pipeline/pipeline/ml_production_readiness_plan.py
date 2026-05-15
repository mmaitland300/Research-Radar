"""Production-readiness gate contract for ML ranking experiments.

This module is a deterministic spec artifact writer. It reads existing JSON
audit artifacts and a conflict-policy Markdown file, then emits a gate plan.
It does not use Postgres, sklearn fitting, OpenAlex, embedding APIs, ranking,
split generation, learned weights, or product/runtime code.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_production_readiness_plan"
PLAN_VERSION = "ml-production-readiness-plan-v1"
PLAN_SCHEMA_VERSION = 1

TRANSFER_ARTIFACT_TYPE = "ml_text_transfer_readiness"
TRANSFER_VERSION = "ml-text-transfer-readiness-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v7"
CROSS_POOL_ARTIFACT_TYPE = "ml_text_baseline_cross_pool"
CROSS_POOL_VERSION = "ml-text-baseline-cross-pool-v1"
TEXT_CORPUS_ARTIFACT_TYPE = "ml_labeled_text_corpus"
TEXT_CORPUS_V2_VERSION = "ml-labeled-text-corpus-v2"
EMBEDDINGS_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDINGS_V1_VERSION = "ml-labeled-text-embeddings-v1"

TARGET_GOOD = "good_or_acceptable"
TARGET_SURPRISING = "surprising_or_useful"
TARGETS = (TARGET_GOOD, TARGET_SURPRISING)

DEFAULT_EVALUATION_RULES: dict[str, Any] = {
    "strong_in_pool_balanced_accuracy_threshold": 0.70,
    "strong_in_pool_roc_auc_threshold": 0.80,
    "weak_transfer_balanced_accuracy_threshold": 0.55,
    "weak_transfer_roc_auc_threshold": 0.60,
    "moderate_transfer_balanced_accuracy_threshold": 0.60,
    "moderate_transfer_roc_auc_threshold": 0.70,
    "sparse_class_min_count_threshold": 20,
    "primary_target_label_gap_min_class_threshold": 20,
}

REQUIRED_GATE_IDS = (
    "G1_target_selection",
    "G2_label_volume_and_balance",
    "G3_multi_source_transfer",
    "G4_rubric_adjudication",
    "G5_split_policy_artifact",
    "G6_candidate_pool_definition",
    "G7_offline_metric_gates",
    "G8_shadow_mode_contract",
    "G9_leakage_controls",
    "G10_production_rollout",
)

CAVEATS = (
    "Not validation.",
    "Single-reviewer audit labels.",
    "Gates are prerequisites not guarantees.",
    "No production ranking implication.",
    "No new training/embeddings/ranking/splits.",
    "Observation-level duplicates/conflicts preserved.",
    "good_or_acceptable research-only.",
    "surprising_or_useful deferred for production.",
)

STATIC_PRODUCTION_MISSING_GATES = (
    "multi-reviewer/adjudication",
    "deliberate split policy",
    "product-matched candidate pool",
    "offline top-k workflow metrics",
    "heuristic comparison",
    "leakage controls",
    "shadow/flagged experiment",
    "human approval gate",
)


class MLProductionReadinessPlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLProductionReadinessPlanError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLProductionReadinessPlanError(f"Expected JSON object in {path}")
    return payload


def _input_record(name: str, path: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLProductionReadinessPlanError(f"Input {name} does not exist: {path}")
    return {"name": name, "path": portable_repo_path(resolved), "sha256": sha256_file(resolved)}


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLProductionReadinessPlanError(f"{name} JSON missing metadata object")
    return metadata


def _validate_transfer_readiness(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="transfer-readiness")
    if metadata.get("artifact_type") != TRANSFER_ARTIFACT_TYPE:
        raise MLProductionReadinessPlanError(
            f"expected transfer-readiness metadata.artifact_type={TRANSFER_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("readiness_version") != TRANSFER_VERSION:
        raise MLProductionReadinessPlanError(
            f"expected transfer-readiness metadata.readiness_version={TRANSFER_VERSION!r}, got {metadata.get('readiness_version')!r}"
        )
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLProductionReadinessPlanError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    if not isinstance(payload.get("rows"), list):
        raise MLProductionReadinessPlanError("label dataset missing rows array")


def _validate_optional_artifact(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_key: str,
    version: str,
) -> None:
    metadata = _metadata(payload, name=name)
    if metadata.get("artifact_type") != artifact_type:
        raise MLProductionReadinessPlanError(
            f"expected {name} metadata.artifact_type={artifact_type!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get(version_key) != version:
        raise MLProductionReadinessPlanError(
            f"expected {name} metadata.{version_key}={version!r}, got {metadata.get(version_key)!r}"
        )


def _default_conflict_policy_path() -> Path:
    return default_repo_root() / "docs" / "audit" / "ml-label-conflict-policy.md"


def _resolve_conflict_policy(path: Path | None) -> Path:
    resolved = Path(path).resolve() if path is not None else _default_conflict_policy_path().resolve()
    if not resolved.exists():
        raise MLProductionReadinessPlanError(f"conflict policy file does not exist: {resolved}")
    return resolved


def _bucket(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "(null)"


def _target_bucket(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "null"


def _explicit_audit_rows(label_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = label_payload.get("rows")
    assert isinstance(rows, list)
    selected: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("split") or "").strip() != "audit_only":
            continue
        if row_has_explicit_label(raw):
            selected.append(dict(raw))
    return selected


def _fallback_class_balance(label_payload: Mapping[str, Any]) -> dict[str, Any]:
    selected = _explicit_audit_rows(label_payload)
    by_target: dict[str, dict[str, dict[str, int]]] = {}
    for target in TARGETS:
        by_variant: dict[str, dict[str, int]] = {}
        for row in selected:
            variant = _bucket(row.get("review_pool_variant"))
            counts = by_variant.setdefault(variant, {"true": 0, "false": 0, "null": 0, "total": 0})
            counts[_target_bucket(row.get(target))] += 1
            counts["total"] += 1
        by_target[target] = dict(sorted(by_variant.items()))
    return {"total_explicit_labeled_row_count": len(selected), "by_target": by_target}


def _class_balance(transfer_payload: Mapping[str, Any], label_payload: Mapping[str, Any]) -> dict[str, Any]:
    balance = transfer_payload.get("class_balance_by_review_pool_variant")
    if isinstance(balance, Mapping) and isinstance(balance.get("by_target"), Mapping):
        return dict(balance)
    return _fallback_class_balance(label_payload)


def _flag_value(transfer_payload: Mapping[str, Any], target: str, flag_name: str) -> bool:
    flags = transfer_payload.get("heuristic_readiness_flags")
    if not isinstance(flags, Mapping):
        return False
    target_flags = flags.get(target)
    if not isinstance(target_flags, Mapping):
        return False
    flag = target_flags.get(flag_name)
    if not isinstance(flag, Mapping):
        return False
    return bool(flag.get("value"))


def _flag_evidence(transfer_payload: Mapping[str, Any], target: str, flag_name: str) -> list[Any]:
    flags = transfer_payload.get("heuristic_readiness_flags")
    if not isinstance(flags, Mapping):
        return []
    target_flags = flags.get(target)
    if not isinstance(target_flags, Mapping):
        return []
    flag = target_flags.get(flag_name)
    if not isinstance(flag, Mapping):
        return []
    evidence = flag.get("evidence")
    return list(evidence) if isinstance(evidence, list) else []


def _embedding_metric(
    transfer_payload: Mapping[str, Any],
    target: str,
    comparison: str,
    metric: str,
) -> Any:
    synthesis = transfer_payload.get("cross_pool_synthesis")
    if not isinstance(synthesis, Mapping):
        return None
    block = synthesis.get(target)
    if not isinstance(block, Mapping):
        return None
    source_transfer = block.get("source_transfer")
    if not isinstance(source_transfer, Mapping):
        return None
    comp = source_transfer.get(comparison)
    if not isinstance(comp, Mapping):
        return None
    models = comp.get("models")
    if not isinstance(models, Mapping):
        return None
    model = models.get("embedding_logistic")
    if not isinstance(model, Mapping):
        return None
    return model.get(metric)


def _variant_counts(class_balance: Mapping[str, Any], target: str, variant: str) -> dict[str, int]:
    by_target = class_balance.get("by_target")
    if not isinstance(by_target, Mapping):
        return {"true": 0, "false": 0, "null": 0, "total": 0}
    target_counts = by_target.get(target)
    if not isinstance(target_counts, Mapping):
        return {"true": 0, "false": 0, "null": 0, "total": 0}
    counts = target_counts.get(variant)
    if not isinstance(counts, Mapping):
        return {"true": 0, "false": 0, "null": 0, "total": 0}
    return {
        "true": int(counts.get("true") or 0),
        "false": int(counts.get("false") or 0),
        "null": int(counts.get("null") or 0),
        "total": int(counts.get("total") or 0),
    }


def _aggregate_counts(class_balance: Mapping[str, Any], target: str, variants: tuple[str, ...]) -> dict[str, int]:
    total = {"true": 0, "false": 0, "null": 0, "total": 0}
    for variant in variants:
        counts = _variant_counts(class_balance, target, variant)
        for key in total:
            total[key] += counts[key]
    return total


def _sparse_pools(class_balance: Mapping[str, Any], *, threshold: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    by_target = class_balance.get("by_target")
    if not isinstance(by_target, Mapping):
        return out
    for target, variants in by_target.items():
        if not isinstance(variants, Mapping):
            continue
        for variant, counts in variants.items():
            if not isinstance(counts, Mapping):
                continue
            pos = int(counts.get("true") or 0)
            neg = int(counts.get("false") or 0)
            null = int(counts.get("null") or 0)
            if min(pos, neg) < threshold:
                out.append(
                    {
                        "target": str(target),
                        "pool": str(variant),
                        "current_pos": pos,
                        "current_neg": neg,
                        "current_null": null,
                        "min_class_count": min(pos, neg),
                    }
                )
    return out


def _merge_missing_gates(transfer_payload: Mapping[str, Any]) -> list[str]:
    def canonical_key(value: str) -> str:
        text = value.lower().strip()
        if "multi-reviewer" in text or "adjudication" in text:
            return "multi-reviewer/adjudication"
        if "split policy" in text:
            return "deliberate split policy"
        if "product-matched candidate pool" in text:
            return "product-matched candidate pool"
        if "top-k" in text or "workflow metrics" in text:
            return "offline top-k workflow metrics"
        if "heuristic" in text:
            return "heuristic comparison"
        if "leakage" in text:
            return "leakage controls"
        if "shadow" in text or "flagged" in text:
            return "shadow/flagged experiment"
        if "human approval" in text:
            return "human approval gate"
        return text

    seen: set[str] = set()
    merged: list[str] = []
    for gate in STATIC_PRODUCTION_MISSING_GATES:
        seen.add(canonical_key(gate))
        merged.append(gate)
    echoed = transfer_payload.get("production_recommender_missing_gates")
    if isinstance(echoed, list):
        for item in echoed:
            text = str(item).strip()
            key = canonical_key(text)
            if text and key not in seen:
                seen.add(key)
                merged.append(text)
    return merged


def _blocking(*, offline: bool, shadow: bool = False, production: bool = True) -> dict[str, bool]:
    return {
        "offline_ranker_experiment": offline,
        "shadow_scoring": shadow,
        "production_default": production,
    }


def _evidence(source: str, field_paths: list[str], summary_counts: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "source_artifact": source,
        "field_paths": field_paths,
        "summary_counts": dict(summary_counts or {}),
    }


def build_targets(transfer_payload: Mapping[str, Any], class_balance: Mapping[str, Any]) -> dict[str, Any]:
    good_counts = {
        "external_near_miss": _variant_counts(class_balance, TARGET_GOOD, "ml_external_near_miss_audit"),
        "blind_snapshot": _variant_counts(class_balance, TARGET_GOOD, "ml_blind_snapshot_audit"),
        "rank_gap": _variant_counts(class_balance, TARGET_GOOD, "ml_emerging_target_gap_audit:good_or_acceptable"),
        "external_to_blind_balanced_accuracy": _embedding_metric(
            transfer_payload, TARGET_GOOD, "external_near_miss_to_blind_snapshot", "balanced_accuracy"
        ),
        "external_to_blind_roc_auc": _embedding_metric(
            transfer_payload, TARGET_GOOD, "external_near_miss_to_blind_snapshot", "roc_auc"
        ),
        "blind_to_external_balanced_accuracy": _embedding_metric(
            transfer_payload, TARGET_GOOD, "blind_snapshot_to_external_near_miss", "balanced_accuracy"
        ),
        "blind_to_external_roc_auc": _embedding_metric(
            transfer_payload, TARGET_GOOD, "blind_snapshot_to_external_near_miss", "roc_auc"
        ),
    }
    surprising_counts = {
        "external_near_miss": _variant_counts(class_balance, TARGET_SURPRISING, "ml_external_near_miss_audit"),
        "blind_snapshot": _variant_counts(class_balance, TARGET_SURPRISING, "ml_blind_snapshot_audit"),
        "external_to_blind_balanced_accuracy": _embedding_metric(
            transfer_payload, TARGET_SURPRISING, "external_near_miss_to_blind_snapshot", "balanced_accuracy"
        ),
        "external_to_blind_roc_auc": _embedding_metric(
            transfer_payload, TARGET_SURPRISING, "external_near_miss_to_blind_snapshot", "roc_auc"
        ),
        "blind_to_external_balanced_accuracy": _embedding_metric(
            transfer_payload, TARGET_SURPRISING, "blind_snapshot_to_external_near_miss", "balanced_accuracy"
        ),
        "blind_to_external_roc_auc": _embedding_metric(
            transfer_payload, TARGET_SURPRISING, "blind_snapshot_to_external_near_miss", "roc_auc"
        ),
    }
    return {
        TARGET_GOOD: {
            "status": "primary_candidate",
            "allowed_next_stage": "offline_ranker_research_only",
            "production_eligible": False,
            "rationale": (
                "good_or_acceptable has an in-pool text signal and is not flagged as weak for external/blind transfer, "
                "so it is the only v1 target allowed to proceed to offline ranker research."
            ),
            "evidence": _evidence(
                "ml-text-transfer-readiness-v1",
                [
                    "heuristic_readiness_flags.good_or_acceptable.in_pool_signal_strong",
                    "heuristic_readiness_flags.good_or_acceptable.external_blind_transfer_weak",
                    "cross_pool_synthesis.good_or_acceptable.source_transfer.external_near_miss_to_blind_snapshot.models.embedding_logistic",
                    "cross_pool_synthesis.good_or_acceptable.source_transfer.blind_snapshot_to_external_near_miss.models.embedding_logistic",
                ],
                good_counts,
            ),
        },
        TARGET_SURPRISING: {
            "status": "deferred",
            "allowed_next_stage": "rubric_and_labeling_only",
            "production_eligible": False,
            "rationale": (
                "surprising_or_useful is flagged for weak external/blind transfer and transfer inconsistency, so v1 limits it "
                "to rubric clarification and additional labeling."
            ),
            "evidence": _evidence(
                "ml-text-transfer-readiness-v1",
                [
                    "heuristic_readiness_flags.surprising_or_useful.external_blind_transfer_weak",
                    "heuristic_readiness_flags.surprising_or_useful.transfer_inconsistent",
                    "cross_pool_synthesis.surprising_or_useful.source_transfer.external_near_miss_to_blind_snapshot.models.embedding_logistic",
                    "cross_pool_synthesis.surprising_or_useful.source_transfer.blind_snapshot_to_external_near_miss.models.embedding_logistic",
                ],
                surprising_counts,
            ),
        },
    }


def build_label_gaps(transfer_payload: Mapping[str, Any], class_balance: Mapping[str, Any]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    recommendations = transfer_payload.get("recommendations")
    rec_by_priority: dict[int, Mapping[str, Any]] = {}
    if isinstance(recommendations, list):
        for rec in recommendations:
            if isinstance(rec, Mapping) and isinstance(rec.get("priority"), int):
                rec_by_priority[int(rec["priority"])] = rec

    surprising_counts = _aggregate_counts(
        class_balance, TARGET_SURPRISING, ("ml_external_near_miss_audit", "ml_blind_snapshot_audit")
    )
    rec1 = rec_by_priority.get(1, {})
    gaps.append(
        {
            "priority": "P1",
            "target": TARGET_SURPRISING,
            "pool": "ml_external_near_miss_audit + ml_blind_snapshot_audit",
            "current_pos": surprising_counts["true"],
            "current_neg": surprising_counts["false"],
            "current_null": surprising_counts["null"],
            "recommended_action": rec1.get(
                "action",
                "Clarify the surprising_or_useful rubric and collect balanced cross-source labels.",
            ),
            "rationale": rec1.get(
                "rationale",
                "surprising_or_useful shows weak external/blind transfer and should not be a v1 production target.",
            ),
            "depends_on_artifact": rec1.get("depends_on_artifact", "ml-text-transfer-readiness-v1"),
        }
    )

    good_counts = _aggregate_counts(
        class_balance, TARGET_GOOD, ("ml_external_near_miss_audit", "ml_blind_snapshot_audit")
    )
    rec2 = rec_by_priority.get(2, {})
    gaps.append(
        {
            "priority": "P2",
            "target": TARGET_GOOD,
            "pool": "ml_external_near_miss_audit + ml_blind_snapshot_audit",
            "current_pos": good_counts["true"],
            "current_neg": good_counts["false"],
            "current_null": good_counts["null"],
            "recommended_action": rec2.get(
                "action",
                "Expand cross-source/product-like labels for offline ranker research.",
            ),
            "rationale": rec2.get(
                "rationale",
                "good_or_acceptable is the primary research target but still needs product-like label coverage.",
            ),
            "depends_on_artifact": rec2.get("depends_on_artifact", "ml-label-dataset-v7"),
        }
    )

    sparse = _sparse_pools(class_balance, threshold=int(DEFAULT_EVALUATION_RULES["sparse_class_min_count_threshold"]))
    for item in sparse:
        if item["pool"] in {"ml_hard_negative_audit", "(null)", "bridge_eligible_only", "full_family_top_k"}:
            gaps.append(
                {
                    "priority": "P3",
                    "target": item["target"],
                    "pool": item["pool"],
                    "current_pos": item["current_pos"],
                    "current_neg": item["current_neg"],
                    "current_null": item["current_null"],
                    "recommended_action": "Add negatives or mark this pool unsuitable for split/eval until balanced.",
                    "rationale": "The pool has sparse or one-class labels under the configured threshold.",
                    "depends_on_artifact": "ml-text-transfer-readiness-v1",
                }
            )
    return gaps


def _primary_label_gaps_below_threshold(label_gaps: list[Mapping[str, Any]]) -> bool:
    threshold = int(DEFAULT_EVALUATION_RULES["primary_target_label_gap_min_class_threshold"])
    for gap in label_gaps:
        if gap.get("target") != TARGET_GOOD:
            continue
        if min(int(gap.get("current_pos") or 0), int(gap.get("current_neg") or 0)) < threshold:
            return False
    return True


def _overall_status(transfer_payload: Mapping[str, Any], label_gaps: list[Mapping[str, Any]]) -> tuple[str, str]:
    if not _flag_value(transfer_payload, TARGET_GOOD, "in_pool_signal_strong"):
        return (
            "blocked",
            "blocked because good_or_acceptable does not have a strong in-pool signal in transfer-readiness.",
        )
    production_ready = _flag_value(transfer_payload, TARGET_GOOD, "production_ready")
    g5_satisfied = False
    g6_satisfied = False
    if g5_satisfied and g6_satisfied and _primary_label_gaps_below_threshold(label_gaps):
        return (
            "ready_for_offline_gate_experiment",
            "ready for an offline gate experiment because target selection, split policy, candidate pool, and label gaps meet configured rules.",
        )
    if not production_ready:
        return (
            "research_only",
            "research_only because inputs are valid and good_or_acceptable has in-pool signal, but production gates remain unsatisfied.",
        )
    return (
        "research_only",
        "research_only because gate artifacts required for production/default ranking are not yet satisfied.",
    )


def build_gates(
    *,
    transfer_payload: Mapping[str, Any],
    class_balance: Mapping[str, Any],
    label_gaps: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    good_strong = _flag_value(transfer_payload, TARGET_GOOD, "in_pool_signal_strong")
    good_weak_transfer = _flag_value(transfer_payload, TARGET_GOOD, "external_blind_transfer_weak")
    surprising_weak = _flag_value(transfer_payload, TARGET_SURPRISING, "external_blind_transfer_weak")
    surprising_inconsistent = _flag_value(transfer_payload, TARGET_SURPRISING, "transfer_inconsistent")
    sparse_counts = Counter(gap.get("priority") for gap in label_gaps)
    return [
        {
            "gate_id": "G1_target_selection",
            "title": "Target Selection",
            "category": "target",
            "description": "Choose a target eligible for the next offline research stage.",
            "required_for": ["offline_ranker_experiment", "production_default"],
            "status": "partial",
            "evidence": _evidence(
                "ml-text-transfer-readiness-v1",
                [
                    "heuristic_readiness_flags.good_or_acceptable.in_pool_signal_strong",
                    "heuristic_readiness_flags.surprising_or_useful.external_blind_transfer_weak",
                ],
                {"good_or_acceptable_primary_candidate": True, "surprising_or_useful_deferred": True},
            ),
            "blocking": _blocking(offline=False, production=True),
            "next_action": "proceed with offline ranker research for good_or_acceptable only",
        },
        {
            "gate_id": "G2_label_volume_and_balance",
            "title": "Label Volume And Balance",
            "category": "labels",
            "description": "Ensure each evaluated pool has enough positives, negatives, and null accounting.",
            "required_for": ["offline_ranker_research_only", "production_default"],
            "status": "partial",
            "evidence": _evidence(
                "ml-text-transfer-readiness-v1",
                ["class_balance_by_review_pool_variant", "label_gaps"],
                {"label_gap_count": len(label_gaps), "p3_sparse_or_one_class_gap_count": sparse_counts.get("P3", 0)},
            ),
            "blocking": _blocking(offline=False, production=True),
            "next_action": "collect labels in sparse/imbalanced pools per label_gaps",
        },
        {
            "gate_id": "G3_multi_source_transfer",
            "title": "Multi-Source Transfer",
            "category": "transfer",
            "description": "Separate source-transfer evidence from worksheet- or source-specific behavior.",
            "required_for": ["production_default"],
            "status": "partial" if good_strong and not good_weak_transfer else "not_started",
            "evidence": _evidence(
                "ml-text-transfer-readiness-v1",
                [
                    "heuristic_readiness_flags.good_or_acceptable.external_blind_transfer_weak",
                    "heuristic_readiness_flags.surprising_or_useful.external_blind_transfer_weak",
                    "heuristic_readiness_flags.surprising_or_useful.transfer_inconsistent",
                ],
                {
                    "good_or_acceptable_external_blind_transfer_weak": good_weak_transfer,
                    "surprising_or_useful_external_blind_transfer_weak": surprising_weak,
                    "surprising_or_useful_transfer_inconsistent": surprising_inconsistent,
                },
            ),
            "blocking": _blocking(offline=False, production=True),
            "next_action": "improve cross-source labels before treating transfer as production evidence",
        },
        {
            "gate_id": "G4_rubric_adjudication",
            "title": "Rubric Adjudication",
            "category": "labels",
            "description": "Define multi-reviewer or adjudication policy for label disagreements and rubric ambiguity.",
            "required_for": ["production_default"],
            "status": "not_started",
            "evidence": _evidence("ml-label-conflict-policy.md", ["conflict_policy_path"], {}),
            "blocking": _blocking(offline=False, production=True),
            "next_action": "write and apply a multi-reviewer/adjudication policy before production claims",
        },
        {
            "gate_id": "G5_split_policy_artifact",
            "title": "Split Policy Artifact",
            "category": "evaluation",
            "description": "Define seed, eligibility, paper_id leakage controls, and conflict-policy reference for split/eval.",
            "required_for": ["offline_ranker_experiment", "production_default"],
            "status": "not_started",
            "evidence": _evidence("static_gate_contract", ["next_artifacts.ml-label-split-policy-v1"], {}),
            "blocking": _blocking(offline=True, production=True),
            "next_action": "create ml-label-split-policy-v1 before offline ranker gate experiments",
        },
        {
            "gate_id": "G6_candidate_pool_definition",
            "title": "Candidate Pool Definition",
            "category": "evaluation",
            "description": "Define product-matched candidate pools and audit-only pool exclusions.",
            "required_for": ["offline_ranker_experiment", "production_default"],
            "status": "not_started",
            "evidence": _evidence("static_gate_contract", ["next_artifacts.ml-offline-ranker-experiment-v1"], {}),
            "blocking": _blocking(offline=True, production=True),
            "next_action": "define a product-matched candidate pool before offline ranker experiments",
        },
        {
            "gate_id": "G7_offline_metric_gates",
            "title": "Offline Metric Gates",
            "category": "evaluation",
            "description": "Specify top-k, PR@k, calibration, and heuristic comparison thresholds on frozen snapshots.",
            "required_for": ["shadow_scoring", "production_default"],
            "status": "not_started",
            "evidence": _evidence("static_gate_contract", ["production_recommender_missing_gates"], {}),
            "blocking": _blocking(offline=False, shadow=True, production=True),
            "next_action": "define and pass offline top-k workflow metrics before shadow scoring",
        },
        {
            "gate_id": "G8_shadow_mode_contract",
            "title": "Shadow Mode Contract",
            "category": "deployment",
            "description": "Define no-user-impact shadow scoring, logging, review, and rollback boundaries.",
            "required_for": ["shadow_scoring", "production_default"],
            "status": "not_started",
            "evidence": _evidence("static_gate_contract", ["next_artifacts.ml-shadow-scorer-v1"], {}),
            "blocking": _blocking(offline=False, shadow=True, production=True),
            "next_action": "write ml-shadow-scorer-v1 only after offline gates pass",
        },
        {
            "gate_id": "G9_leakage_controls",
            "title": "Leakage Controls",
            "category": "evaluation",
            "description": "Prevent label leakage into sampling/features for held-out evaluation.",
            "required_for": ["offline_ranker_experiment", "shadow_scoring", "production_default"],
            "status": "not_started",
            "evidence": _evidence("static_gate_contract", ["next_artifacts.ml-label-split-policy-v1"], {}),
            "blocking": _blocking(offline=True, shadow=True, production=True),
            "next_action": "document leakage controls in split and experiment artifacts",
        },
        {
            "gate_id": "G10_production_rollout",
            "title": "Production Rollout",
            "category": "deployment",
            "description": "Require human approval, shadow evidence, rollout flags, and rollback plan before defaults change.",
            "required_for": ["production_default"],
            "status": "not_started",
            "evidence": _evidence("static_gate_contract", ["next_artifacts.production flag change"], {}),
            "blocking": _blocking(offline=False, shadow=False, production=True),
            "next_action": "do not change production defaults until all prior gates and human approval are complete",
        },
    ]


def build_no_go_conditions(conflict_policy_path: Path) -> list[dict[str, Any]]:
    conflict_evidence = {
        "source_artifact": "ml-label-conflict-policy.md",
        "field_paths": [portable_repo_path(conflict_policy_path)],
        "summary_counts": {},
    }
    return [
        {"condition": "Training alone does not authorize production.", "evidence": {"source_artifact": "static_gate_contract", "field_paths": [], "summary_counts": {}}},
        {"condition": "Single-reviewer audit labels are insufficient for default ranking.", "evidence": {"source_artifact": "static_gate_contract", "field_paths": [], "summary_counts": {}}},
        {"condition": "surprising_or_useful is not eligible as a v1 production target.", "evidence": {"source_artifact": "ml-text-transfer-readiness-v1", "field_paths": ["heuristic_readiness_flags.surprising_or_useful"], "summary_counts": {}}},
        {"condition": "good_or_acceptable is offline ranker research only, not production-eligible until gates satisfied.", "evidence": {"source_artifact": "ml-text-transfer-readiness-v1", "field_paths": ["heuristic_readiness_flags.good_or_acceptable"], "summary_counts": {}}},
        {"condition": "Shadow scoring cannot start before offline gates pass.", "evidence": {"source_artifact": "static_gate_contract", "field_paths": ["G7_offline_metric_gates", "G8_shadow_mode_contract"], "summary_counts": {}}},
        {"condition": "Production default cannot change without shadow evidence and human approval.", "evidence": {"source_artifact": "static_gate_contract", "field_paths": ["G10_production_rollout"], "summary_counts": {}}},
        {"condition": "Silent deduplication or conflict resolution of paper_id labels is forbidden.", "evidence": conflict_evidence},
        {"condition": "Rank-shaped-only evidence cannot justify production ML.", "evidence": {"source_artifact": "static_gate_contract", "field_paths": ["G3_multi_source_transfer"], "summary_counts": {}}},
    ]


def build_next_artifacts() -> list[dict[str, Any]]:
    return [
        {
            "name": "ml-transfer-gap-review-worksheet",
            "purpose": "Collect targeted labels for weak transfer and sparse pools.",
            "prerequisites": ["ml-production-readiness-plan-v1"],
            "out_of_scope": "No model training or production ranking changes.",
            "expected_commit_focus": "review worksheet and sidecar only",
        },
        {
            "name": "ml-label-split-policy-v1",
            "purpose": "Define deterministic split eligibility, seed, leakage controls, and conflict-policy references.",
            "prerequisites": ["sufficient label coverage for selected target"],
            "out_of_scope": "No split generation in this plan artifact.",
            "expected_commit_focus": "spec artifact for split policy",
        },
        {
            "name": "ml-offline-ranker-experiment-v1",
            "purpose": "Evaluate a production-candidate offline ranker against heuristic baselines on frozen candidate pools.",
            "prerequisites": ["ml-label-split-policy-v1", "product-matched candidate pool definition"],
            "out_of_scope": "No production model artifact or default ranking change.",
            "expected_commit_focus": "offline experiment artifact and metrics",
        },
        {
            "name": "ml-shadow-scorer-v1",
            "purpose": "Define no-user-impact shadow scoring contract after offline gates pass.",
            "prerequisites": ["passed offline metric gates", "leakage controls", "human review"],
            "out_of_scope": "No production ranking influence.",
            "expected_commit_focus": "shadow contract/spec only",
        },
        {
            "name": "production flag change",
            "purpose": "Human-approved default or feature-flag change after all gates pass.",
            "prerequisites": ["shadow evidence", "rollback plan", "human approval"],
            "out_of_scope": "Out of scope for all current audit artifacts.",
            "expected_commit_focus": "human-approved production change only",
        },
    ]


def build_ml_production_readiness_plan_payload(
    *,
    transfer_readiness_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path | None = None,
    cross_pool_path: Path | None = None,
    text_corpus_v2_path: Path | None = None,
    embeddings_v1_path: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    conflict_path = _resolve_conflict_policy(conflict_policy_path)
    transfer_payload = _load_json_object(Path(transfer_readiness_path))
    label_payload = _load_json_object(Path(label_dataset_path))
    transfer_meta = _validate_transfer_readiness(transfer_payload)
    _validate_label_dataset(label_payload)

    inputs = [
        _input_record("transfer_readiness", transfer_readiness_path),
        _input_record("label_dataset", label_dataset_path),
        _input_record("conflict_policy", conflict_path),
    ]

    if cross_pool_path is not None:
        cross_payload = _load_json_object(Path(cross_pool_path))
        _validate_optional_artifact(
            cross_payload,
            name="cross-pool",
            artifact_type=CROSS_POOL_ARTIFACT_TYPE,
            version_key="baseline_version",
            version=CROSS_POOL_VERSION,
        )
        inputs.append(_input_record("cross_pool", cross_pool_path))
    if text_corpus_v2_path is not None:
        text_payload = _load_json_object(Path(text_corpus_v2_path))
        _validate_optional_artifact(
            text_payload,
            name="text-corpus-v2",
            artifact_type=TEXT_CORPUS_ARTIFACT_TYPE,
            version_key="corpus_version",
            version=TEXT_CORPUS_V2_VERSION,
        )
        inputs.append(_input_record("text_corpus_v2", text_corpus_v2_path))
    if embeddings_v1_path is not None:
        embeddings_payload = _load_json_object(Path(embeddings_v1_path))
        _validate_optional_artifact(
            embeddings_payload,
            name="embeddings-v1",
            artifact_type=EMBEDDINGS_ARTIFACT_TYPE,
            version_key="embedding_artifact_version",
            version=EMBEDDINGS_V1_VERSION,
        )
        inputs.append(_input_record("embeddings_v1", embeddings_v1_path))

    class_balance = _class_balance(transfer_payload, label_payload)
    label_gaps = build_label_gaps(transfer_payload, class_balance)
    targets = build_targets(transfer_payload, class_balance)
    gates = build_gates(transfer_payload=transfer_payload, class_balance=class_balance, label_gaps=label_gaps)
    status, rationale = _overall_status(transfer_payload, label_gaps)
    evaluation_rules = transfer_meta.get("thresholds") if isinstance(transfer_meta.get("thresholds"), Mapping) else DEFAULT_EVALUATION_RULES
    missing_gates = _merge_missing_gates(transfer_payload)

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "plan_version": PLAN_VERSION,
        "plan_schema_version": PLAN_SCHEMA_VERSION,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "overall_status": status,
        "overall_status_rationale": rationale,
        "evaluation_rules": dict(evaluation_rules),
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "targets": targets,
        "gates": gates,
        "label_gaps": label_gaps,
        "no_go_conditions": build_no_go_conditions(conflict_path),
        "next_artifacts": build_next_artifacts(),
        "production_recommender_missing_gates": missing_gates,
        "caveats": list(CAVEATS),
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    lines = [
        "# ML Production Readiness Plan v1",
        "",
        "## Executive Summary",
        "",
        f"- **overall_status:** `{metadata.get('overall_status')}`",
        f"- **rationale:** {metadata.get('overall_status_rationale')}",
        "- **primary target:** `good_or_acceptable` for offline ranker research only.",
        "- **deferred target:** `surprising_or_useful` for rubric and labeling work only.",
        "- No production ranking, shadow scoring, or default change is authorized by this plan.",
        "",
        "## Gate Checklist",
        "",
        "| gate_id | status | blocking | required_for | next_action |",
        "| --- | --- | --- | --- | --- |",
    ]
    for gate in payload.get("gates", []):
        if not isinstance(gate, Mapping):
            continue
        blocking = gate.get("blocking")
        blocking_text = json.dumps(blocking, sort_keys=True) if isinstance(blocking, Mapping) else str(blocking)
        required = ", ".join(str(item) for item in gate.get("required_for", []))
        lines.append(
            f"| `{gate.get('gate_id')}` | `{gate.get('status')}` | `{blocking_text}` | {required} | {gate.get('next_action')} |"
        )

    lines.extend(
        [
            "",
            "## Target Readiness",
            "",
            "| target | status | allowed_next_stage | production_eligible | rationale |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for target, block in payload.get("targets", {}).items():
        if not isinstance(block, Mapping):
            continue
        lines.append(
            f"| `{target}` | `{block.get('status')}` | `{block.get('allowed_next_stage')}` | "
            f"`{block.get('production_eligible')}` | {block.get('rationale')} |"
        )

    lines.extend(
        [
            "",
            "## Label Gaps",
            "",
            "| priority | target | pool | pos | neg | null | action |",
            "| --- | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for gap in payload.get("label_gaps", []):
        if not isinstance(gap, Mapping):
            continue
        lines.append(
            f"| `{gap.get('priority')}` | `{gap.get('target')}` | `{gap.get('pool')}` | "
            f"{gap.get('current_pos')} | {gap.get('current_neg')} | {gap.get('current_null')} | "
            f"{gap.get('recommended_action')} |"
        )

    lines.extend(["", "## No-Go Conditions", ""])
    for item in payload.get("no_go_conditions", []):
        if isinstance(item, Mapping):
            lines.append(f"- {item.get('condition')}")

    lines.extend(["", "## Ordered Next Artifacts", ""])
    for item in payload.get("next_artifacts", []):
        if isinstance(item, Mapping):
            lines.append(f"- `{item.get('name')}`: {item.get('purpose')}")

    lines.extend(["", "## Not Validation / Not Production Recommender Test", ""])
    for caveat in metadata.get("caveats", []):
        lines.append(f"- {caveat}")
    lines.append("")
    return "\n".join(lines)


def write_ml_production_readiness_plan(
    *,
    transfer_readiness_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path | None,
    cross_pool_path: Path | None,
    text_corpus_v2_path: Path | None,
    embeddings_v1_path: Path | None,
    output_path: Path,
    markdown_output_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_production_readiness_plan_payload(
        transfer_readiness_path=transfer_readiness_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        cross_pool_path=cross_pool_path,
        text_corpus_v2_path=text_corpus_v2_path,
        embeddings_v1_path=embeddings_v1_path,
    )
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if markdown_output_path is not None:
        md = Path(markdown_output_path)
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(render_markdown(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "CAVEATS",
    "PLAN_VERSION",
    "REQUIRED_GATE_IDS",
    "MLProductionReadinessPlanError",
    "build_gates",
    "build_label_gaps",
    "build_ml_production_readiness_plan_payload",
    "build_next_artifacts",
    "build_no_go_conditions",
    "build_targets",
    "render_markdown",
    "write_ml_production_readiness_plan",
]
