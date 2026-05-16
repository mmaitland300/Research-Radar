"""Read-only synthesis for text transfer readiness.

This consumes existing JSON audit artifacts and emits deterministic decision
support summaries. It does not call OpenAlex, embedding APIs, Postgres, ranking,
sklearn fitting, or mutate labels.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_text_transfer_readiness"
READINESS_VERSION = "ml-text-transfer-readiness-v1"
CROSS_POOL_ARTIFACT_TYPE = "ml_text_baseline_cross_pool"
CROSS_POOL_VERSION = "ml-text-baseline-cross-pool-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v7"
TEXT_CORPUS_ARTIFACT_TYPE = "ml_labeled_text_corpus"
TEXT_CORPUS_V2_VERSION = "ml-labeled-text-corpus-v2"
EMBEDDINGS_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDINGS_V1_VERSION = "ml-labeled-text-embeddings-v1"
TARGETS = ("good_or_acceptable", "surprising_or_useful")
EMBEDDING_MODEL_NAME = "embedding_logistic"
KEY_TRANSFER_PAIRS = (
    "external_near_miss_to_blind_snapshot",
    "blind_snapshot_to_external_near_miss",
)

THRESHOLDS: dict[str, float | int] = {
    "strong_in_pool_balanced_accuracy_threshold": 0.70,
    "strong_in_pool_roc_auc_threshold": 0.80,
    "weak_transfer_balanced_accuracy_threshold": 0.55,
    "weak_transfer_roc_auc_threshold": 0.60,
    "moderate_transfer_balanced_accuracy_threshold": 0.60,
    "moderate_transfer_roc_auc_threshold": 0.70,
    "sparse_class_min_count_threshold": 20,
}

CAVEATS = (
    "Not validation.",
    "Single-reviewer audit labels.",
    "Heuristic synthesis only.",
    "No production ranking implication.",
    "Observation-level duplicates/conflicts are preserved.",
    "Source selection, label context, class imbalance, and text source remain possible confounds.",
    "No new model training, embeddings, ranking, or splits were created.",
)

PRODUCTION_RECOMMENDER_MISSING_GATES = (
    "multi-reviewer or adjudication policy",
    "deliberate split policy",
    "product-matched candidate pools",
    "top-k workflow metrics",
    "leakage controls",
    "shadow/flagged experiment plan",
)

NO_REEMBED_NEEDED_CONCLUSION = (
    "Text-format normalization did not change text_for_embedding values in v2; regenerating embeddings solely for text "
    "formatting is unnecessary for this dataset. The observed transfer differences are unlikely to be explained by "
    "title/abstract string packaging alone, though source selection and label context remain confounds."
)


class MLTextTransferReadinessError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLTextTransferReadinessError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLTextTransferReadinessError(f"Expected JSON object in {path}")
    return payload


def _input_record(name: str, path: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    try:
        digest = sha256_file(resolved)
    except OSError as exc:
        raise MLTextTransferReadinessError(f"Failed to hash {name} input {path}: {exc}") from exc
    return {"name": name, "path": portable_repo_path(resolved), "sha256": digest}


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLTextTransferReadinessError(f"{name} JSON missing metadata object")
    return metadata


def _validate_cross_pool(
    payload: Mapping[str, Any],
    *,
    expected_cross_pool_version: str = CROSS_POOL_VERSION,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="cross-pool")
    if metadata.get("artifact_type") != CROSS_POOL_ARTIFACT_TYPE:
        raise MLTextTransferReadinessError(
            f"expected cross-pool metadata.artifact_type={CROSS_POOL_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("baseline_version") != expected_cross_pool_version:
        raise MLTextTransferReadinessError(
            "expected cross-pool metadata.baseline_version="
            f"{expected_cross_pool_version!r}, got {metadata.get('baseline_version')!r}"
        )
    if not isinstance(payload.get("per_target"), Mapping):
        raise MLTextTransferReadinessError("cross-pool JSON missing per_target object")
    return metadata


def _validate_label_dataset(
    payload: Mapping[str, Any],
    *,
    expected_label_dataset_version: str = LABEL_DATASET_VERSION,
) -> None:
    if payload.get("dataset_version") != expected_label_dataset_version:
        raise MLTextTransferReadinessError(
            f"expected label dataset_version={expected_label_dataset_version!r}, got {payload.get('dataset_version')!r}"
        )
    if not isinstance(payload.get("rows"), list):
        raise MLTextTransferReadinessError("label dataset missing rows array")


def _validate_text_corpus_v2(
    payload: Mapping[str, Any],
    *,
    expected_text_corpus_version: str = TEXT_CORPUS_V2_VERSION,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="text-corpus-v2")
    if metadata.get("artifact_type") != TEXT_CORPUS_ARTIFACT_TYPE:
        raise MLTextTransferReadinessError(
            f"expected text-corpus-v2 metadata.artifact_type={TEXT_CORPUS_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("corpus_version") != expected_text_corpus_version:
        raise MLTextTransferReadinessError(
            "expected text-corpus-v2 metadata.corpus_version="
            f"{expected_text_corpus_version!r}, got {metadata.get('corpus_version')!r}"
        )
    return metadata


def _validate_embeddings_v1(
    payload: Mapping[str, Any],
    *,
    expected_embeddings_version: str = EMBEDDINGS_V1_VERSION,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="embeddings-v1")
    if metadata.get("artifact_type") != EMBEDDINGS_ARTIFACT_TYPE:
        raise MLTextTransferReadinessError(
            f"expected embeddings-v1 metadata.artifact_type={EMBEDDINGS_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != expected_embeddings_version:
        raise MLTextTransferReadinessError(
            "expected embeddings-v1 metadata.embedding_artifact_version="
            f"{expected_embeddings_version!r}, got {metadata.get('embedding_artifact_version')!r}"
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


def build_class_balance(label_payload: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    rows = _explicit_audit_rows(label_payload)
    balances: dict[str, Any] = {}
    for target in TARGETS:
        by_variant: dict[str, dict[str, int]] = {}
        for row in rows:
            variant = _bucket(row.get("review_pool_variant"))
            counts = by_variant.setdefault(variant, {"true": 0, "false": 0, "null": 0, "total": 0})
            counts[_target_bucket(row.get(target))] += 1
            counts["total"] += 1
        balances[target] = dict(sorted(by_variant.items()))

    paper_ids = [str(row.get("paper_id") or "").strip() for row in rows if str(row.get("paper_id") or "").strip()]
    counts = Counter(paper_ids)
    duplicates = [{"paper_id": paper_id, "count": count} for paper_id, count in sorted(counts.items()) if count > 1]
    duplicate_summary = {
        "explicit_labeled_row_count": len(rows),
        "paper_id_count": len(paper_ids),
        "unique_paper_id_count": len(counts),
        "duplicate_paper_id_count": len(duplicates),
        "duplicate_observation_pressure_count": sum(item["count"] - 1 for item in duplicates),
        "duplicate_paper_ids": duplicates,
    }
    return {
        "total_explicit_labeled_row_count": len(rows),
        "by_target": balances,
    }, duplicate_summary


def _extract_model_metrics(model: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(model, Mapping):
        return {}
    keys = (
        "train_n",
        "train_pos",
        "train_neg",
        "test_n",
        "test_pos",
        "test_neg",
        "accuracy",
        "balanced_accuracy",
        "macro_f1",
        "roc_auc",
        "roc_auc_skip_reason",
        "confusion",
    )
    return {key: model.get(key) for key in keys if key in model}


def _summarize_comparison(comp: Mapping[str, Any]) -> dict[str, Any]:
    models = comp.get("models")
    model_summaries: dict[str, Any] = {}
    if isinstance(models, Mapping):
        for model_name, model_metrics in models.items():
            model_summaries[str(model_name)] = _extract_model_metrics(model_metrics if isinstance(model_metrics, Mapping) else None)
    out = {
        "comparison_name": comp.get("comparison_name"),
        "comparison_type": comp.get("comparison_type"),
        "target": comp.get("target"),
        "skipped": bool(comp.get("skipped")),
        "skip_reason": comp.get("skip_reason"),
        "models": model_summaries,
    }
    for key in ("effective_cv_folds", "train_histograms", "test_histograms"):
        if key in comp:
            out[key] = comp.get(key)
    return out


def _skip_reason_counts(*comparisons: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for group in comparisons:
        for comp in group.values():
            if comp.get("skipped"):
                counts[str(comp.get("skip_reason") or "unknown")] += 1
    return dict(sorted(counts.items()))


def build_cross_pool_synthesis(cross_pool_payload: Mapping[str, Any]) -> dict[str, Any]:
    per_target = cross_pool_payload.get("per_target")
    assert isinstance(per_target, Mapping)
    synthesis: dict[str, Any] = {}
    for target in TARGETS:
        block = per_target.get(target)
        if not isinstance(block, Mapping):
            synthesis[target] = {"missing": True, "reason": "target missing from cross-pool artifact"}
            continue
        in_pool = block.get("in_pool_cv") if isinstance(block.get("in_pool_cv"), Mapping) else {}
        transfer = block.get("source_transfer") if isinstance(block.get("source_transfer"), Mapping) else {}
        assert isinstance(in_pool, Mapping)
        assert isinstance(transfer, Mapping)
        synthesis[target] = {
            "target": target,
            "eligible_row_count": block.get("eligible_row_count"),
            "excluded_count": block.get("excluded_count"),
            "excluded_row_ids": block.get("excluded_row_ids", []),
            "slice_counts": block.get("slice_counts", {}),
            "in_pool_cv": {str(name): _summarize_comparison(comp) for name, comp in in_pool.items() if isinstance(comp, Mapping)},
            "source_transfer": {
                str(name): _summarize_comparison(comp) for name, comp in transfer.items() if isinstance(comp, Mapping)
            },
            "skipped_reason_counts": _skip_reason_counts(in_pool, transfer),
        }
    return synthesis


def _embedding_metrics(comp: Mapping[str, Any]) -> Mapping[str, Any] | None:
    models = comp.get("models")
    if not isinstance(models, Mapping):
        return None
    model = models.get(EMBEDDING_MODEL_NAME)
    return model if isinstance(model, Mapping) else None


def _metric_evidence(
    *,
    target: str,
    comparison_or_slice: str,
    metric_name: str,
    value: Any,
    threshold_name: str,
    threshold: float | int,
) -> dict[str, Any]:
    return {
        "target": target,
        "comparison_or_slice": comparison_or_slice,
        "model": EMBEDDING_MODEL_NAME,
        "metric_name": metric_name,
        "value": value,
        "threshold_name": threshold_name,
        "threshold": threshold,
    }


def _value_meets(value: Any, threshold: float | int) -> bool:
    return isinstance(value, (int, float)) and float(value) >= float(threshold)


def _value_below(value: Any, threshold: float | int) -> bool:
    return isinstance(value, (int, float)) and float(value) < float(threshold)


def _strong_in_pool_evidence(target: str, in_pool: Mapping[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for slice_name, comp in in_pool.items():
        if not isinstance(comp, Mapping) or comp.get("skipped"):
            continue
        metrics = _embedding_metrics(comp)
        if metrics is None:
            continue
        ba = metrics.get("balanced_accuracy")
        auc = metrics.get("roc_auc")
        if _value_meets(ba, THRESHOLDS["strong_in_pool_balanced_accuracy_threshold"]):
            evidence.append(
                _metric_evidence(
                    target=target,
                    comparison_or_slice=str(slice_name),
                    metric_name="balanced_accuracy",
                    value=ba,
                    threshold_name="strong_in_pool_balanced_accuracy_threshold",
                    threshold=THRESHOLDS["strong_in_pool_balanced_accuracy_threshold"],
                )
            )
        if _value_meets(auc, THRESHOLDS["strong_in_pool_roc_auc_threshold"]):
            evidence.append(
                _metric_evidence(
                    target=target,
                    comparison_or_slice=str(slice_name),
                    metric_name="roc_auc",
                    value=auc,
                    threshold_name="strong_in_pool_roc_auc_threshold",
                    threshold=THRESHOLDS["strong_in_pool_roc_auc_threshold"],
                )
            )
    return evidence


def _weak_transfer_evidence(target: str, transfer: Mapping[str, Any], *, key_only: bool) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    items = [(name, transfer.get(name)) for name in KEY_TRANSFER_PAIRS] if key_only else list(transfer.items())
    for comp_name, comp in items:
        if not isinstance(comp, Mapping) or comp.get("skipped"):
            continue
        metrics = _embedding_metrics(comp)
        if metrics is None:
            continue
        ba = metrics.get("balanced_accuracy")
        auc = metrics.get("roc_auc")
        if _value_below(ba, THRESHOLDS["weak_transfer_balanced_accuracy_threshold"]):
            evidence.append(
                _metric_evidence(
                    target=target,
                    comparison_or_slice=str(comp_name),
                    metric_name="balanced_accuracy",
                    value=ba,
                    threshold_name="weak_transfer_balanced_accuracy_threshold",
                    threshold=THRESHOLDS["weak_transfer_balanced_accuracy_threshold"],
                )
            )
        if _value_below(auc, THRESHOLDS["weak_transfer_roc_auc_threshold"]):
            evidence.append(
                _metric_evidence(
                    target=target,
                    comparison_or_slice=str(comp_name),
                    metric_name="roc_auc",
                    value=auc,
                    threshold_name="weak_transfer_roc_auc_threshold",
                    threshold=THRESHOLDS["weak_transfer_roc_auc_threshold"],
                )
            )
    return evidence


def _moderate_transfer_evidence(target: str, transfer: Mapping[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for comp_name, comp in transfer.items():
        if not isinstance(comp, Mapping) or comp.get("skipped"):
            continue
        metrics = _embedding_metrics(comp)
        if metrics is None:
            continue
        ba = metrics.get("balanced_accuracy")
        auc = metrics.get("roc_auc")
        if _value_meets(ba, THRESHOLDS["moderate_transfer_balanced_accuracy_threshold"]):
            evidence.append(
                _metric_evidence(
                    target=target,
                    comparison_or_slice=str(comp_name),
                    metric_name="balanced_accuracy",
                    value=ba,
                    threshold_name="moderate_transfer_balanced_accuracy_threshold",
                    threshold=THRESHOLDS["moderate_transfer_balanced_accuracy_threshold"],
                )
            )
        if _value_meets(auc, THRESHOLDS["moderate_transfer_roc_auc_threshold"]):
            evidence.append(
                _metric_evidence(
                    target=target,
                    comparison_or_slice=str(comp_name),
                    metric_name="roc_auc",
                    value=auc,
                    threshold_name="moderate_transfer_roc_auc_threshold",
                    threshold=THRESHOLDS["moderate_transfer_roc_auc_threshold"],
                )
            )
    return evidence


def _sparse_class_evidence(target: str, class_balance: Mapping[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    by_target = class_balance.get("by_target")
    if not isinstance(by_target, Mapping):
        return evidence
    by_variant = by_target.get(target)
    if not isinstance(by_variant, Mapping):
        return evidence
    threshold = THRESHOLDS["sparse_class_min_count_threshold"]
    for variant, counts in by_variant.items():
        if not isinstance(counts, Mapping):
            continue
        true_count = counts.get("true", 0)
        false_count = counts.get("false", 0)
        if not isinstance(true_count, int) or not isinstance(false_count, int):
            continue
        if true_count < int(threshold) or false_count < int(threshold):
            evidence.append(
                {
                    "target": target,
                    "comparison_or_slice": str(variant),
                    "metric_name": "min_class_count",
                    "value": min(true_count, false_count),
                    "threshold_name": "sparse_class_min_count_threshold",
                    "threshold": threshold,
                    "true_count": true_count,
                    "false_count": false_count,
                }
            )
    return evidence


def _skipped_slice_evidence(target: str, in_pool: Mapping[str, Any]) -> list[dict[str, Any]]:
    evidence = []
    for slice_name, comp in in_pool.items():
        if isinstance(comp, Mapping) and comp.get("skipped"):
            evidence.append(
                {
                    "target": target,
                    "comparison_or_slice": str(slice_name),
                    "metric_name": "skipped",
                    "value": True,
                    "threshold_name": "requires_evaluable_slice",
                    "threshold": True,
                    "reason": comp.get("skip_reason"),
                }
            )
    return evidence


def build_heuristic_readiness_flags(
    *,
    cross_pool_payload: Mapping[str, Any],
    class_balance: Mapping[str, Any],
) -> dict[str, Any]:
    per_target = cross_pool_payload.get("per_target")
    assert isinstance(per_target, Mapping)
    flags: dict[str, Any] = {}
    for target in TARGETS:
        block = per_target.get(target)
        if not isinstance(block, Mapping):
            flags[target] = {
                "in_pool_signal_strong": {"value": False, "evidence": []},
                "external_blind_transfer_weak": {"value": False, "evidence": []},
                "transfer_inconsistent": {"value": False, "evidence": []},
                "needs_more_labels": {
                    "value": True,
                    "evidence": [{"target": target, "metric_name": "target_missing", "value": True}],
                },
                "production_ready": {
                    "value": False,
                    "evidence": [{"target": target, "metric_name": "production_ready", "value": False}],
                },
            }
            continue
        in_pool = block.get("in_pool_cv") if isinstance(block.get("in_pool_cv"), Mapping) else {}
        transfer = block.get("source_transfer") if isinstance(block.get("source_transfer"), Mapping) else {}
        assert isinstance(in_pool, Mapping)
        assert isinstance(transfer, Mapping)

        strong_evidence = _strong_in_pool_evidence(target, in_pool)
        external_blind_weak = _weak_transfer_evidence(target, transfer, key_only=True)
        transfer_weak = _weak_transfer_evidence(target, transfer, key_only=False)
        transfer_moderate = _moderate_transfer_evidence(target, transfer)
        sparse_evidence = _sparse_class_evidence(target, class_balance)
        skipped_evidence = _skipped_slice_evidence(target, in_pool)
        inconsistent = bool(transfer_weak and transfer_moderate)
        needs_evidence = sparse_evidence + skipped_evidence
        if inconsistent:
            needs_evidence.append(
                {
                    "target": target,
                    "comparison_or_slice": "source_transfer",
                    "metric_name": "transfer_inconsistent",
                    "value": True,
                    "threshold_name": "weak_and_moderate_transfer_both_present",
                    "threshold": True,
                    "weak_examples": transfer_weak[:6],
                    "moderate_examples": transfer_moderate[:6],
                }
            )
        flags[target] = {
            "in_pool_signal_strong": {"value": bool(strong_evidence), "evidence": strong_evidence},
            "external_blind_transfer_weak": {"value": bool(external_blind_weak), "evidence": external_blind_weak},
            "transfer_inconsistent": {
                "value": inconsistent,
                "evidence": transfer_weak[:8] + transfer_moderate[:8] if inconsistent else [],
            },
            "needs_more_labels": {"value": bool(needs_evidence), "evidence": needs_evidence},
            "production_ready": {
                "value": False,
                "evidence": [
                    {
                        "target": target,
                        "comparison_or_slice": "production",
                        "metric_name": "production_ready",
                        "value": False,
                        "threshold_name": "offline_audit_only",
                        "threshold": True,
                    }
                ],
            },
        }
    return flags


def build_text_format_evidence(
    text_corpus_v2_payload: Mapping[str, Any] | None,
    *,
    expected_text_corpus_version: str = TEXT_CORPUS_V2_VERSION,
) -> dict[str, Any]:
    if text_corpus_v2_payload is None:
        return {
            "provided": False,
            "conclusion": f"No {expected_text_corpus_version} artifact was provided; text-format sensitivity evidence is unavailable.",
        }
    metadata = _validate_text_corpus_v2(
        text_corpus_v2_payload,
        expected_text_corpus_version=expected_text_corpus_version,
    )
    changed = metadata.get("n_text_changed_from_v1")
    if changed == 0:
        if expected_text_corpus_version == TEXT_CORPUS_V2_VERSION:
            conclusion = NO_REEMBED_NEEDED_CONCLUSION
        else:
            conclusion = (
                f"Text-format normalization did not change text_for_embedding values in {expected_text_corpus_version}; "
                "regenerating embeddings solely for text formatting is unnecessary for this dataset. The observed "
                "transfer differences are unlikely to be explained by title/abstract string packaging alone, though "
                "source selection and label context remain confounds."
            )
    else:
        if expected_text_corpus_version == TEXT_CORPUS_V2_VERSION:
            conclusion = (
                "Text-format normalization changed text_for_embedding values in v2; generate v2 embeddings before comparing "
                "cross-pool transfer again."
            )
        else:
            conclusion = (
                f"Text-format normalization changed text_for_embedding values in {expected_text_corpus_version}; generate "
                "matching embeddings before comparing cross-pool transfer again."
            )
    return {
        "provided": True,
        "counts_by_previous_embedding_text_format_version": metadata.get(
            "counts_by_previous_embedding_text_format_version", {}
        ),
        "counts_by_canonicalization_status": metadata.get("counts_by_canonicalization_status", {}),
        "n_text_changed_from_v1": changed,
        "conclusion": conclusion,
    }


def build_recommendations() -> list[dict[str, Any]]:
    return [
        {
            "priority": 1,
            "action": "Clarify the surprising_or_useful rubric and collect more balanced labels across external, blind, and rank-shaped sources.",
            "rationale": "This target shows stronger source sensitivity and weaker external/blind transfer in the current diagnostic.",
            "expected_impact": "Reduce label-context ambiguity before trusting transfer-style offline results.",
            "risk": "More labels without rubric alignment may amplify reviewer-specific noise.",
            "depends_on_artifact": "ml-text-baseline-cross-pool-v1",
        },
        {
            "priority": 2,
            "action": "Expand cross-source labeling for good_or_acceptable and use future offline ranker experiments only as research probes.",
            "rationale": "The target shows learnable in-pool text signal, but source-transfer evidence is not a production gate.",
            "expected_impact": "Improve relevance-boundary evidence across candidate sources before any product-matched evaluation.",
            "risk": "Pooling sources too early can hide source-selection effects.",
            "depends_on_artifact": "ml-label-dataset-v7",
        },
        {
            "priority": 3,
            "action": "Keep production recommender changes blocked until the missing gates are explicitly addressed.",
            "rationale": "The current artifacts are audit diagnostics, not live recommender validation.",
            "expected_impact": "Prevents over-reading offline text signals as product readiness.",
            "risk": "Slower path to deployment, but clearer evidence boundaries.",
            "depends_on_artifact": "ml-text-transfer-readiness-v1",
        },
    ]


def build_ml_text_transfer_readiness_payload(
    *,
    cross_pool_path: Path,
    label_dataset_path: Path,
    text_corpus_v2_path: Path | None = None,
    embeddings_v1_path: Path | None = None,
    expected_cross_pool_version: str = CROSS_POOL_VERSION,
    expected_label_dataset_version: str = LABEL_DATASET_VERSION,
    expected_text_corpus_version: str = TEXT_CORPUS_V2_VERSION,
    expected_embeddings_version: str = EMBEDDINGS_V1_VERSION,
    readiness_version: str = READINESS_VERSION,
    generated_at: str | None = None,
) -> dict[str, Any]:
    inputs = [
        _input_record("cross_pool", cross_pool_path),
        _input_record("label_dataset", label_dataset_path),
    ]
    cross_pool_payload = _load_json_object(Path(cross_pool_path))
    label_payload = _load_json_object(Path(label_dataset_path))
    _validate_cross_pool(cross_pool_payload, expected_cross_pool_version=expected_cross_pool_version)
    _validate_label_dataset(label_payload, expected_label_dataset_version=expected_label_dataset_version)

    text_corpus_payload = None
    if text_corpus_v2_path is not None:
        inputs.append(_input_record("text_corpus_v2", text_corpus_v2_path))
        text_corpus_payload = _load_json_object(Path(text_corpus_v2_path))
        _validate_text_corpus_v2(text_corpus_payload, expected_text_corpus_version=expected_text_corpus_version)

    if embeddings_v1_path is not None:
        inputs.append(_input_record("embeddings_v1", embeddings_v1_path))
        embeddings_payload = _load_json_object(Path(embeddings_v1_path))
        _validate_embeddings_v1(embeddings_payload, expected_embeddings_version=expected_embeddings_version)

    class_balance, duplicate_summary = build_class_balance(label_payload)
    cross_pool_synthesis = build_cross_pool_synthesis(cross_pool_payload)
    text_format_evidence = build_text_format_evidence(
        text_corpus_payload,
        expected_text_corpus_version=expected_text_corpus_version,
    )
    flags = build_heuristic_readiness_flags(cross_pool_payload=cross_pool_payload, class_balance=class_balance)

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "readiness_version": readiness_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "expected_versions": {
            "cross_pool": expected_cross_pool_version,
            "label_dataset": expected_label_dataset_version,
            "text_corpus": expected_text_corpus_version,
            "embeddings": expected_embeddings_version,
        },
        "thresholds": dict(THRESHOLDS),
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "class_balance_by_review_pool_variant": class_balance,
        "duplicate_paper_summary": duplicate_summary,
        "cross_pool_synthesis": cross_pool_synthesis,
        "text_format_evidence": text_format_evidence,
        "heuristic_readiness_flags": flags,
        "recommendations": build_recommendations(),
        "production_recommender_missing_gates": list(PRODUCTION_RECOMMENDER_MISSING_GATES),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _embedding_model_metrics(comp: Mapping[str, Any]) -> Mapping[str, Any]:
    models = comp.get("models")
    if isinstance(models, Mapping):
        model = models.get(EMBEDDING_MODEL_NAME)
        if isinstance(model, Mapping):
            return model
    return {}


def render_markdown(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    class_balance = payload.get("class_balance_by_review_pool_variant", {})
    synthesis = payload.get("cross_pool_synthesis", {})
    text_format = payload.get("text_format_evidence", {})
    flags = payload.get("heuristic_readiness_flags", {})

    lines = [
        f"# Text Transfer Readiness ({metadata.get('readiness_version')})",
        "",
        "## Executive Summary",
        "",
        "- Offline synthesis only: no new embeddings, model training, ranking, Postgres, or label mutation.",
        f"- Explicit labeled audit rows: `{class_balance.get('total_explicit_labeled_row_count')}`.",
        f"- Duplicate paper pressure: `{payload.get('duplicate_paper_summary', {}).get('duplicate_observation_pressure_count')}` duplicate observations beyond first paper IDs.",
        "- In-pool text signal exists for at least some slices, but cross-source transfer is uneven.",
        "- `surprising_or_useful` remains the least stable transfer target and needs rubric/label work before trust.",
        f"- Text-format evidence: {text_format.get('conclusion')}",
        "- Production readiness remains explicitly false.",
        "",
        "## Class Balance",
        "",
        "| target | review_pool_variant | true | false | null | total |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    by_target = class_balance.get("by_target", {}) if isinstance(class_balance, Mapping) else {}
    if isinstance(by_target, Mapping):
        for target in TARGETS:
            variants = by_target.get(target, {})
            if not isinstance(variants, Mapping):
                continue
            for variant, counts in variants.items():
                if not isinstance(counts, Mapping):
                    continue
                lines.append(
                    f"| `{target}` | `{variant}` | {counts.get('true', 0)} | {counts.get('false', 0)} | "
                    f"{counts.get('null', 0)} | {counts.get('total', 0)} |"
                )

    lines.extend(["", "## In-Pool Signal Summary", "", "| target | slice | skipped | balanced_accuracy | roc_auc | macro_f1 |", "| --- | --- | --- | ---: | ---: | ---: |"])
    if isinstance(synthesis, Mapping):
        for target in TARGETS:
            block = synthesis.get(target, {})
            if not isinstance(block, Mapping):
                continue
            in_pool = block.get("in_pool_cv", {})
            if not isinstance(in_pool, Mapping):
                continue
            for slice_name, comp in in_pool.items():
                if not isinstance(comp, Mapping):
                    continue
                metrics = _embedding_model_metrics(comp)
                lines.append(
                    f"| `{target}` | `{slice_name}` | `{comp.get('skipped')}` | {_fmt(metrics.get('balanced_accuracy'))} | "
                    f"{_fmt(metrics.get('roc_auc'))} | {_fmt(metrics.get('macro_f1'))} |"
                )

    lines.extend(["", "## Transfer Summary", "", "| target | comparison | skipped | balanced_accuracy | roc_auc | macro_f1 |", "| --- | --- | --- | ---: | ---: | ---: |"])
    if isinstance(synthesis, Mapping):
        for target in TARGETS:
            block = synthesis.get(target, {})
            if not isinstance(block, Mapping):
                continue
            transfer = block.get("source_transfer", {})
            if not isinstance(transfer, Mapping):
                continue
            for comp_name, comp in transfer.items():
                if not isinstance(comp, Mapping):
                    continue
                metrics = _embedding_model_metrics(comp)
                lines.append(
                    f"| `{target}` | `{comp_name}` | `{comp.get('skipped')}` | {_fmt(metrics.get('balanced_accuracy'))} | "
                    f"{_fmt(metrics.get('roc_auc'))} | {_fmt(metrics.get('macro_f1'))} |"
                )

    lines.extend(
        [
            "",
            "## Text-Format Evidence",
            "",
            f"- `n_text_changed_from_v1`: `{text_format.get('n_text_changed_from_v1', 'n/a')}`",
            f"- conclusion: {text_format.get('conclusion')}",
            "",
            "## Decisions / Next Steps",
            "",
        ]
    )
    for rec in payload.get("recommendations", []):
        if isinstance(rec, Mapping):
            lines.append(f"- P{rec.get('priority')}: {rec.get('action')} Rationale: {rec.get('rationale')}")

    lines.extend(["", "## Heuristic Flags", ""])
    if isinstance(flags, Mapping):
        for target in TARGETS:
            target_flags = flags.get(target, {})
            if isinstance(target_flags, Mapping):
                compact = ", ".join(f"{name}={details.get('value')}" for name, details in target_flags.items() if isinstance(details, Mapping))
                lines.append(f"- `{target}`: {compact}")

    lines.extend(["", "## Not Doing Yet", ""])
    for gate in payload.get("production_recommender_missing_gates", []):
        lines.append(f"- {gate}")

    lines.extend(["", "## Caveats", ""])
    for caveat in metadata.get("caveats", []):
        lines.append(f"- {caveat}")
    lines.append("")
    return "\n".join(lines)


def write_ml_text_transfer_readiness(
    *,
    cross_pool_path: Path,
    label_dataset_path: Path,
    text_corpus_v2_path: Path | None,
    embeddings_v1_path: Path | None,
    output_path: Path,
    markdown_output_path: Path | None,
    expected_cross_pool_version: str = CROSS_POOL_VERSION,
    expected_label_dataset_version: str = LABEL_DATASET_VERSION,
    expected_text_corpus_version: str = TEXT_CORPUS_V2_VERSION,
    expected_embeddings_version: str = EMBEDDINGS_V1_VERSION,
    readiness_version: str = READINESS_VERSION,
) -> dict[str, Any]:
    payload = build_ml_text_transfer_readiness_payload(
        cross_pool_path=cross_pool_path,
        label_dataset_path=label_dataset_path,
        text_corpus_v2_path=text_corpus_v2_path,
        embeddings_v1_path=embeddings_v1_path,
        expected_cross_pool_version=expected_cross_pool_version,
        expected_label_dataset_version=expected_label_dataset_version,
        expected_text_corpus_version=expected_text_corpus_version,
        expected_embeddings_version=expected_embeddings_version,
        readiness_version=readiness_version,
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
    "NO_REEMBED_NEEDED_CONCLUSION",
    "PRODUCTION_RECOMMENDER_MISSING_GATES",
    "READINESS_VERSION",
    "THRESHOLDS",
    "MLTextTransferReadinessError",
    "build_class_balance",
    "build_cross_pool_synthesis",
    "build_heuristic_readiness_flags",
    "build_ml_text_transfer_readiness_payload",
    "build_text_format_evidence",
    "render_markdown",
    "write_ml_text_transfer_readiness",
]
