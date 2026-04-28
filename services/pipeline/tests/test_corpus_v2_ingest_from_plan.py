from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.cli as cli_main
from pipeline.corpus_v2_ingest_from_plan import (
    CorpusV2IngestError,
    candidate_plan_sha256,
    load_candidate_plan,
    run_corpus_v2_ingest_from_plan,
    validate_candidate_plan,
)


def _candidate(
    wid: str = "https://openalex.org/W1",
    *,
    doi: str | None = "10.1234/example",
    title: str = "Music information retrieval paper",
    year: int = 2024,
    bucket_id: str = "core_mir_existing_sources",
) -> dict:
    return {
        "openalex_id": wid,
        "doi": doi,
        "title": title,
        "year": year,
        "citation_count": 3,
        "source_display_name": "Transactions of the International Society for Music Information Retrieval",
        "bucket_id": bucket_id,
        "inclusion_reason": "bucket_allow_signal",
        "matched_terms": ["core_source_query"],
        "exclusion_reason": None,
    }


def _plan(candidates: list[dict] | None = None, *, selected_total: int | None = None) -> dict:
    rows = candidates if candidates is not None else [_candidate()]
    return {
        "generated_at": "2026-04-28T05:33:40+00:00",
        "contact_provided": False,
        "contact_mode": "none",
        "api_key_provided": True,
        "auth_mode": "api_key",
        "policy_reference": {"name": "research-radar-v1", "policy_hash": "abc123"},
        "target_min": 1,
        "target_max": 10,
        "selected_total": len(rows) if selected_total is None else selected_total,
        "bucket_summaries": [],
        "selected_candidates": rows,
        "caveats": [
            "Dry-run only: no Postgres writes, no snapshot, no embeddings, clustering, or ranking.",
            "Candidate list is not a benchmark.",
        ],
    }


def _write_plan(path: Path, plan: dict) -> Path:
    path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8", newline="\n")
    return path


def test_valid_candidate_plan_parsing(tmp_path: Path) -> None:
    path = _write_plan(tmp_path / "plan.json", _plan())

    doc = load_candidate_plan(path)

    assert doc.sha256 == candidate_plan_sha256(path)
    assert doc.payload["selected_total"] == 1


def test_missing_selected_candidates_fails() -> None:
    plan = _plan()
    del plan["selected_candidates"]

    with pytest.raises(CorpusV2IngestError, match="selected_candidates"):
        validate_candidate_plan(plan)


def test_selected_total_mismatch_fails() -> None:
    plan = _plan([_candidate(), _candidate("https://openalex.org/W2")], selected_total=1)

    with pytest.raises(CorpusV2IngestError, match="does not match"):
        validate_candidate_plan(plan)


def test_selected_total_outside_target_range_fails() -> None:
    plan = _plan([_candidate()])
    plan["target_min"] = 2

    with pytest.raises(CorpusV2IngestError, match="inside target range"):
        validate_candidate_plan(plan)


@pytest.mark.parametrize(
    ("field", "value", "pattern"),
    [
        ("auth_mode", "no_key", "auth_mode"),
        ("api_key_provided", False, "api_key_provided"),
    ],
)
def test_missing_api_key_auth_metadata_fails(field: str, value: object, pattern: str) -> None:
    plan = _plan()
    plan[field] = value

    with pytest.raises(CorpusV2IngestError, match=pattern):
        validate_candidate_plan(plan)


def test_raw_secret_like_fields_are_rejected() -> None:
    plan = _plan()
    plan["api_key"] = "oa-secret-never-write"

    with pytest.raises(CorpusV2IngestError, match="raw secret"):
        validate_candidate_plan(plan)


def test_candidate_plan_sha256_is_stable_for_exact_bytes(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_bytes(b'{"selected_total":1}\n')

    assert candidate_plan_sha256(path) == candidate_plan_sha256(path)
    assert candidate_plan_sha256(path) == "7e70c1ffcc822b781b1ce5472473792b52157ce122fb66a99560547e28003688"


def test_cli_requires_explicit_snapshot_version(tmp_path: Path) -> None:
    path = _write_plan(tmp_path / "plan.json", _plan())
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "corpus-v2-ingest-from-plan",
            "--candidate-plan",
            str(path),
            "--output",
            str(tmp_path / "out.json"),
            "--markdown-output",
            str(tmp_path / "out.md"),
        ],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main()
    assert exc.value.code == 2


class _Result:
    def __init__(self, *, one: tuple | None = None, all_rows: list[tuple] | None = None) -> None:
        self._one = one
        self._all = all_rows or []

    def fetchone(self) -> tuple | None:
        return self._one

    def fetchall(self) -> list[tuple]:
        return self._all


class _Tx:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self.snapshot: dict | None = None

    def __enter__(self) -> "_Tx":
        self.snapshot = self.conn.snapshot_state()
        return self

    def __exit__(self, exc_type, _exc, _tb) -> bool:
        if exc_type is not None and self.snapshot is not None:
            self.conn.restore_state(self.snapshot)
            self.conn.rollback_count += 1
        return False


class _FakeConn:
    def __init__(self, *, fail_on_work_insert: bool = False) -> None:
        self.fail_on_work_insert = fail_on_work_insert
        self.source_policies: set[str] = set()
        self.source_snapshot_versions: dict[str, dict] = {}
        self.ingest_runs: dict[str, dict] = {}
        self.works: dict[int, dict] = {}
        self.raw_openalex_works: list[dict] = []
        self.next_work_id = 1
        self.sql: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False

    def snapshot_state(self) -> dict:
        return {
            "works": copy.deepcopy(self.works),
            "raw_openalex_works": copy.deepcopy(self.raw_openalex_works),
            "next_work_id": self.next_work_id,
            "ingest_runs": copy.deepcopy(self.ingest_runs),
        }

    def restore_state(self, state: dict) -> None:
        self.works = state["works"]
        self.raw_openalex_works = state["raw_openalex_works"]
        self.next_work_id = state["next_work_id"]
        self.ingest_runs = state["ingest_runs"]

    def transaction(self) -> _Tx:
        return _Tx(self)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def execute(self, sql: str, params: tuple | None = None) -> _Result:
        params = params or ()
        compact = " ".join(sql.split())
        self.sql.append(compact)
        if compact == "SELECT source_slug FROM source_policies":
            return _Result(all_rows=[(slug,) for slug in sorted(self.source_policies)])
        if compact.startswith("INSERT INTO source_snapshot_versions"):
            self.source_snapshot_versions[str(params[0])] = {
                "policy_name": params[1],
                "policy_hash": params[2],
                "ingest_mode": params[3],
                "note": params[4],
            }
            return _Result()
        if compact.startswith("INSERT INTO ingest_runs"):
            self.ingest_runs[str(params[0])] = {
                "source_snapshot_version": params[1],
                "policy_hash": params[2],
                "status": params[3],
                "config_json": params[5],
                "counts_json": None,
                "error_message": None,
            }
            return _Result()
        if compact.startswith("UPDATE ingest_runs"):
            run = self.ingest_runs[str(params[4])]
            run["status"] = params[0]
            run["counts_json"] = params[2]
            run["error_message"] = params[3]
            return _Result()
        if compact.startswith("SELECT id FROM works WHERE openalex_id"):
            for work_id, work in self.works.items():
                if work["openalex_id"] == params[0]:
                    return _Result(one=(work_id,))
            return _Result()
        if "lower(doi) = ANY" in compact:
            doi_set = set(params[0])
            for work_id, work in self.works.items():
                doi = work.get("doi")
                if doi and doi.casefold() in doi_set:
                    return _Result(one=(work_id,))
            return _Result()
        if compact.startswith("INSERT INTO raw_openalex_works"):
            self.raw_openalex_works.append(
                {"openalex_id": params[0], "ingest_run_id": params[1], "payload": json.loads(params[6])}
            )
            return _Result()
        if compact.startswith("INSERT INTO works"):
            if self.fail_on_work_insert:
                raise RuntimeError("controlled insert failure")
            work_id = self.next_work_id
            self.next_work_id += 1
            self.works[work_id] = {
                "openalex_id": params[0],
                "title": params[1],
                "abstract": params[2],
                "year": params[3],
                "doi": params[4],
                "type": params[5],
                "language": params[6],
                "source_slug": params[9],
                "citation_count": params[10],
                "is_core_corpus": params[11],
                "corpus_snapshot_version": params[13],
            }
            return _Result(one=(work_id,))
        if compact.startswith("UPDATE works"):
            work_id = int(params[-1])
            self.works[work_id].update(
                {
                    "openalex_id": params[0],
                    "title": params[1],
                    "abstract": params[2],
                    "year": params[3],
                    "doi": params[4],
                    "type": params[5],
                    "language": params[6],
                    "source_slug": params[9],
                    "citation_count": params[10],
                    "is_core_corpus": params[11],
                    "corpus_snapshot_version": params[13],
                }
            )
            return _Result()
        raise AssertionError(f"unhandled SQL: {compact}")


def _run_with_fake_db(tmp_path: Path, conn: _FakeConn, plan: dict) -> dict:
    plan_path = _write_plan(tmp_path / "plan.json", plan)
    with patch("pipeline.corpus_v2_ingest_from_plan.psycopg.connect", return_value=conn):
        return run_corpus_v2_ingest_from_plan(
            candidate_plan_path=plan_path,
            snapshot_version="source-snapshot-test",
            output_path=tmp_path / "summary.json",
            markdown_output_path=tmp_path / "summary.md",
            database_url="postgresql://example",
        )


def test_success_creates_snapshot_ingest_run_and_counts_by_bucket(tmp_path: Path) -> None:
    conn = _FakeConn()
    plan = _plan(
        [
            _candidate("https://openalex.org/W1", bucket_id="core_mir_existing_sources"),
            _candidate(
                "https://openalex.org/W2",
                doi=None,
                bucket_id="music_recommender_systems",
                title="Music recommender systems",
            ),
        ]
    )

    summary = _run_with_fake_db(tmp_path, conn, plan)

    assert conn.source_snapshot_versions["source-snapshot-test"]["ingest_mode"] == "snapshot-import"
    run = conn.ingest_runs[summary["ingest_run_id"]]
    assert run["status"] == "succeeded"
    assert summary["inserted_count"] == 2
    assert summary["updated_count"] == 0
    assert summary["counts_by_bucket"] == {
        "core_mir_existing_sources": 1,
        "music_recommender_systems": 1,
    }
    assert summary["missing_abstract_count"] == 2
    assert summary["missing_doi_count"] == 1
    assert summary["defaulted_language_count"] == 2
    assert summary["unknown_type_count"] == 2
    assert summary["embedding_ready_count"] == 0
    assert summary["embedding_blocked_count"] == 2
    assert summary["snapshot_embedding_ready"] is False
    assert summary["openalex_enrichment"] == "not_run"
    assert "metadata/text hydration" in summary["next_step"]
    assert "ranking" in summary["next_step"]
    raw_payload = conn.raw_openalex_works[0]["payload"]
    assert raw_payload["derived_work_fields"]["abstract"] is None
    assert raw_payload["derived_work_fields"]["language"] == "en"
    assert raw_payload["derived_work_fields"]["type"] == "unknown"
    assert raw_payload["derived_field_provenance"]["abstract"] == "missing_in_candidate_plan"
    assert raw_payload["derived_field_provenance"]["language"] == "candidate_plan_policy_default"
    assert raw_payload["derived_field_provenance"]["type"] == "candidate_plan_unknown_default_not_validated"
    markdown = (tmp_path / "summary.md").read_text(encoding="utf-8")
    assert "not an embedding-ready corpus" in markdown
    assert "title-only embedding version" in markdown
    assert (tmp_path / "summary.json").is_file()
    assert (tmp_path / "summary.md").is_file()


def test_deduplicates_by_doi_before_writes(tmp_path: Path) -> None:
    conn = _FakeConn()
    plan = _plan(
        [
            _candidate("https://openalex.org/W1", doi="https://doi.org/10.5555/dup"),
            _candidate("https://openalex.org/W2", doi="10.5555/dup", title="Duplicate DOI"),
        ]
    )

    summary = _run_with_fake_db(tmp_path, conn, plan)

    assert summary["inserted_count"] == 1
    assert summary["skipped_existing_count"] == 1
    assert len(conn.raw_openalex_works) == 1


def test_failed_insert_rolls_back_work_writes_and_marks_run_failed(tmp_path: Path) -> None:
    conn = _FakeConn(fail_on_work_insert=True)

    with pytest.raises(CorpusV2IngestError, match="controlled insert failure"):
        _run_with_fake_db(tmp_path, conn, _plan())

    assert conn.works == {}
    assert conn.raw_openalex_works == []
    assert len(conn.ingest_runs) == 1
    assert next(iter(conn.ingest_runs.values()))["status"] == "failed"
    assert conn.rollback_count >= 1


def test_raw_secret_like_fields_are_not_written_on_failure(tmp_path: Path) -> None:
    conn = _FakeConn()
    plan = _plan()
    plan["raw_mailto"] = "operator@example.invalid"
    path = _write_plan(tmp_path / "plan.json", plan)

    with patch("pipeline.corpus_v2_ingest_from_plan.psycopg.connect", return_value=conn):
        with pytest.raises(CorpusV2IngestError, match="raw secret"):
            run_corpus_v2_ingest_from_plan(
                candidate_plan_path=path,
                snapshot_version="source-snapshot-test",
                output_path=tmp_path / "summary.json",
                markdown_output_path=tmp_path / "summary.md",
                database_url="postgresql://example",
            )

    assert conn.source_snapshot_versions == {}
    assert conn.ingest_runs == {}
    assert conn.raw_openalex_works == []


def test_no_embeddings_clustering_or_ranking_tables_are_written(tmp_path: Path) -> None:
    conn = _FakeConn()

    _run_with_fake_db(tmp_path, conn, _plan())

    executed = "\n".join(conn.sql).casefold()
    assert "insert into embeddings" not in executed
    assert "clustering_runs" not in executed
    assert "insert into clusters" not in executed
    assert "ranking_runs" not in executed
    assert "paper_scores" not in executed

