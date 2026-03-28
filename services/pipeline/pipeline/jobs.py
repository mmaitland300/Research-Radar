from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from pipeline.config import IngestRun, RawBatchManifest, SnapshotCounts, SnapshotManifest, SourceSnapshotVersion, Watermark
from pipeline.openalex import SourceResolutionPlan, WorksPagePlan, build_bootstrap_work_plans, build_source_resolution_plans
from pipeline.policy import CorpusPolicy, PolicyDecision
from pipeline.source_resolution import SourceResolutionOutcome


def create_bootstrap_bundle(policy: CorpusPolicy, note: str) -> tuple[SourceSnapshotVersion, IngestRun]:
    snapshot = SourceSnapshotVersion.create(policy=policy, ingest_mode="api-bootstrap", note=note)
    ingest_run = IngestRun.start(
        snapshot=snapshot,
        config={
            "policy": policy.as_dict(),
            "resolution_plan_count": len(build_source_resolution_plans(policy)),
            "bootstrap_plan_count": len(build_bootstrap_work_plans(policy)),
        },
    )
    return snapshot, ingest_run


def write_ingest_artifacts(root_dir: Path, snapshot: SourceSnapshotVersion, ingest_run: IngestRun) -> tuple[Path, Path]:
    snapshot_dir = root_dir / snapshot.source_snapshot_version
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = snapshot_dir / "source-snapshot.json"
    snapshot_path.write_text(json.dumps(asdict(snapshot), indent=2, default=str), encoding="utf-8")

    ingest_path = snapshot_dir / f"{ingest_run.ingest_run_id}.json"
    ingest_path.write_text(json.dumps(asdict(ingest_run), indent=2, default=str), encoding="utf-8")
    return snapshot_path, ingest_path


def write_source_resolution_manifest(root_dir: Path, snapshot: SourceSnapshotVersion, plans: Iterable[SourceResolutionPlan]) -> Path:
    resolution_dir = root_dir / snapshot.source_snapshot_version
    resolution_dir.mkdir(parents=True, exist_ok=True)
    resolution_path = resolution_dir / "source-resolution-plan.json"
    resolution_path.write_text(
        json.dumps([asdict(plan) for plan in plans], indent=2, default=str),
        encoding="utf-8",
    )
    return resolution_path


def write_source_resolution_results(
    root_dir: Path,
    snapshot: SourceSnapshotVersion,
    outcomes: Iterable[SourceResolutionOutcome],
) -> Path:
    resolution_dir = root_dir / snapshot.source_snapshot_version
    resolution_dir.mkdir(parents=True, exist_ok=True)
    resolution_path = resolution_dir / "source-resolution-results.json"
    resolution_path.write_text(
        json.dumps([asdict(o) for o in outcomes], indent=2, default=str),
        encoding="utf-8",
    )
    return resolution_path


def write_bootstrap_plan(root_dir: Path, snapshot: SourceSnapshotVersion, plans: Iterable[WorksPagePlan]) -> Path:
    plan_dir = root_dir / snapshot.source_snapshot_version
    plan_dir.mkdir(parents=True, exist_ok=True)
    plan_path = plan_dir / "bootstrap-work-plan.json"
    serialized = [
        {
            "source_slug": plan.source_slug,
            "source_display_name": plan.source_display_name,
            "params": plan.params,
            "select_fields": list(plan.select_fields),
            "url": plan.url(),
        }
        for plan in plans
    ]
    plan_path.write_text(json.dumps(serialized, indent=2), encoding="utf-8")
    return plan_path


def record_raw_work_batch(
    raw_root: Path,
    snapshot: SourceSnapshotVersion,
    ingest_run: IngestRun,
    source_slug: str,
    page_index: int,
    page_cursor: str,
    payload: Mapping[str, Any],
) -> RawBatchManifest:
    batch_dir = raw_root / snapshot.source_snapshot_version / source_slug
    batch_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"page-{page_index:04d}.json"
    absolute_path = batch_dir / file_name

    envelope = {
        "snapshot_version": snapshot.source_snapshot_version,
        "ingest_run_id": ingest_run.ingest_run_id,
        "source_slug": source_slug,
        "page_index": page_index,
        "page_cursor": page_cursor,
        "recorded_at": datetime.now(UTC).isoformat(),
        "payload": payload,
    }
    encoded = json.dumps(envelope, indent=2, sort_keys=True).encode("utf-8")
    absolute_path.write_bytes(encoded)

    checksum = hashlib.sha256(encoded).hexdigest()
    results = payload.get("results") if isinstance(payload, Mapping) else ()
    work_count = len(results) if isinstance(results, list) else 0

    return RawBatchManifest(
        source_slug=source_slug,
        page_cursor=page_cursor,
        page_index=page_index,
        work_count=work_count,
        relative_payload_path=str(absolute_path.relative_to(raw_root)),
        checksum=checksum,
    )


def make_watermark(
    source_snapshot_version: str,
    entity_type: str,
    source_slug: str | None,
    cursor: str | None,
    updated_date: str | None,
) -> Watermark:
    key = "|".join([entity_type, source_slug or "global", cursor or "none", updated_date or "none"])
    return Watermark(
        watermark_key=key,
        entity_type=entity_type,
        source_slug=source_slug,
        cursor=cursor,
        updated_date=updated_date,
        source_snapshot_version=source_snapshot_version,
        recorded_at=datetime.now(UTC),
    )


def summarize_policy_decisions(decisions: Iterable[PolicyDecision]) -> SnapshotCounts:
    included = 0
    excluded = 0
    excluded_by_reason: dict[str, int] = {}
    for decision in decisions:
        if decision.included:
            included += 1
            continue
        excluded += 1
        excluded_by_reason[decision.reason] = excluded_by_reason.get(decision.reason, 0) + 1
    return SnapshotCounts(included_works=included, excluded_works=excluded, excluded_by_reason=excluded_by_reason)


def finalize_snapshot_manifest(
    output_dir: Path,
    snapshot: SourceSnapshotVersion,
    ingest_run: IngestRun,
    counts: SnapshotCounts,
    raw_batches: Iterable[RawBatchManifest],
    watermarks: Iterable[Watermark],
) -> Path:
    finalized_run = ingest_run.complete(counts)
    manifest = SnapshotManifest(
        snapshot=snapshot,
        ingest_run=finalized_run,
        counts=counts,
        raw_batches=tuple(raw_batches),
        watermarks=tuple(watermarks),
    )
    manifest_dir = output_dir / snapshot.source_snapshot_version
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "snapshot-manifest.json"
    manifest_path.write_text(manifest.to_json(), encoding="utf-8")
    return manifest_path


def fail_ingest_run(output_dir: Path, snapshot: SourceSnapshotVersion, ingest_run: IngestRun, message: str) -> Path:
    manifest_dir = output_dir / snapshot.source_snapshot_version
    manifest_dir.mkdir(parents=True, exist_ok=True)
    failure_path = manifest_dir / f"{ingest_run.ingest_run_id}-failed.json"
    failure_path.write_text(json.dumps(asdict(ingest_run.fail(message)), indent=2, default=str), encoding="utf-8")
    return failure_path


def write_bootstrap_preflight_failure(output_dir: Path, *, stage: str, message: str) -> Path:
    """
    Record bootstrap failures that happen before a snapshot / ingest_run exists
    (for example source resolution or early DB sync). Written at output_dir root.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "bootstrap-preflight-failure.json"
    payload = {
        "stage": stage,
        "error_message": message,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path
