from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from pipeline.policy import CorpusPolicy


@dataclass(frozen=True)
class SnapshotCounts:
    included_works: int = 0
    excluded_works: int = 0
    unique_authors: int = 0
    unique_sources: int = 0
    unique_topics: int = 0
    citation_edges: int = 0
    excluded_by_reason: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceSnapshotVersion:
    source_snapshot_version: str
    policy_name: str
    policy_hash: str
    ingest_mode: str
    created_at: datetime
    note: str

    @classmethod
    def create(cls, policy: CorpusPolicy, ingest_mode: str, note: str) -> "SourceSnapshotVersion":
        created_at = datetime.now(UTC)
        version = created_at.strftime("source-snapshot-%Y%m%d-%H%M%S")
        return cls(
            source_snapshot_version=version,
            policy_name=policy.name,
            policy_hash=policy.policy_hash,
            ingest_mode=ingest_mode,
            created_at=created_at,
            note=note,
        )


@dataclass(frozen=True)
class Watermark:
    watermark_key: str
    entity_type: str
    source_slug: str | None
    cursor: str | None
    updated_date: str | None
    source_snapshot_version: str
    recorded_at: datetime


@dataclass(frozen=True)
class IngestRun:
    ingest_run_id: str
    source_snapshot_version: str
    policy_hash: str
    status: str
    started_at: datetime
    config: dict[str, Any]
    counts: SnapshotCounts = field(default_factory=SnapshotCounts)
    finished_at: datetime | None = None
    error_message: str | None = None

    @classmethod
    def start(cls, snapshot: SourceSnapshotVersion, config: dict[str, Any]) -> "IngestRun":
        started_at = datetime.now(UTC)
        digest = hashlib.sha1(
            f"{snapshot.source_snapshot_version}:{started_at.isoformat()}".encode("utf-8")
        ).hexdigest()[:10]
        return cls(
            ingest_run_id=f"ingest-{digest}",
            source_snapshot_version=snapshot.source_snapshot_version,
            policy_hash=snapshot.policy_hash,
            status="running",
            started_at=started_at,
            config=config,
        )

    def complete(self, counts: SnapshotCounts) -> "IngestRun":
        return IngestRun(
            ingest_run_id=self.ingest_run_id,
            source_snapshot_version=self.source_snapshot_version,
            policy_hash=self.policy_hash,
            status="succeeded",
            started_at=self.started_at,
            config=self.config,
            counts=counts,
            finished_at=datetime.now(UTC),
            error_message=None,
        )

    def fail(self, message: str) -> "IngestRun":
        return IngestRun(
            ingest_run_id=self.ingest_run_id,
            source_snapshot_version=self.source_snapshot_version,
            policy_hash=self.policy_hash,
            status="failed",
            started_at=self.started_at,
            config=self.config,
            counts=self.counts,
            finished_at=datetime.now(UTC),
            error_message=message,
        )


@dataclass(frozen=True)
class RawBatchManifest:
    source_slug: str
    page_cursor: str
    page_index: int
    work_count: int
    relative_payload_path: str
    checksum: str


@dataclass(frozen=True)
class SnapshotManifest:
    snapshot: SourceSnapshotVersion
    ingest_run: IngestRun
    counts: SnapshotCounts
    raw_batches: tuple[RawBatchManifest, ...] = ()
    watermarks: tuple[Watermark, ...] = ()

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)
