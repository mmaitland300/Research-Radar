"""Corpus expansion preview: bucket definitions, schema, CLI mock mode, no DB calls."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.cli as cli_main
from pipeline.corpus_expansion_preview import (
    REQUIRED_BUCKET_KEYS,
    REQUIRED_PREVIEW_TOP_LEVEL_KEYS,
    REQUIRED_SAMPLE_WORK_KEYS,
    expansion_bucket_definitions,
    render_corpus_expansion_markdown,
    resolve_corpus_expansion_preview_mailto,
    run_corpus_expansion_preview,
    run_corpus_expansion_preview_from_cli,
    work_to_sample_row,
)
from pipeline.openalex_client import OPENALEX_API_KEY_ENV
from pipeline.policy import CorpusPolicy

EXPECTED_BUCKET_IDS: tuple[str, ...] = (
    "core_mir_existing_sources",
    "ismir_proceedings_or_mir_conference",
    "audio_ml_signal_processing",
    "music_recommender_systems",
    "cultural_computational_musicology",
    "ethics_law_fairness_user_studies",
    "symbolic_music_and_harmony",
    "source_separation_benchmarks",
)


def test_expansion_preview_mock_never_serializes_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "never-in-expansion-preview-json")
    out = run_corpus_expansion_preview(
        policy=CorpusPolicy(),
        mailto="p@test.invalid",
        per_bucket_sample=10,
        openalex_mode="mock",
    )
    blob = json.dumps(out, ensure_ascii=False) + render_corpus_expansion_markdown(out)
    assert "never-in-expansion-preview-json" not in blob
    assert out["auth_mode"] == "mock"
    assert out["api_key_provided"] is False


def test_expansion_bucket_definitions_exist() -> None:
    specs = expansion_bucket_definitions()
    assert len(specs) == 8
    assert tuple(s.bucket_id for s in specs) == EXPECTED_BUCKET_IDS
    for s in specs:
        assert s.rationale
        assert s.expected_role_in_bridge_discovery
        assert s.openalex_query_filter_strategy
        assert s.likely_risks_noise_sources
        p = CorpusPolicy()
        q = s.build_params(p)
        assert "filter" in q
        assert "per-page" in q


def test_output_schema_includes_required_fields() -> None:
    out = run_corpus_expansion_preview(
        policy=CorpusPolicy(),
        mailto="test@example.com",
        per_bucket_sample=10,
        openalex_mode="mock",
    )
    for k in REQUIRED_PREVIEW_TOP_LEVEL_KEYS:
        assert k in out
    assert out["recommendation"]["expand_before_next_bridge_weight_experiment"] is True
    assert "versioning_implications" in out
    vi = out["versioning_implications"]
    assert "new_corpus_snapshot_version" in vi
    assert "new_embedding_version" in vi
    assert "new_cluster_version" in vi
    assert "new_zero_bridge_ranking_version" in vi
    for b in out["buckets"]:
        for k in REQUIRED_BUCKET_KEYS:
            assert k in b, f"bucket {b.get('bucket_id')!r} missing {k!r}"
        for sw in b.get("sample_works") or []:
            for sk in REQUIRED_SAMPLE_WORK_KEYS:
                assert sk in sw, f"sample work missing {sk!r}"


def test_work_to_sample_row() -> None:
    w = {
        "id": "https://openalex.org/W1",
        "title": "Test",
        "publication_year": 2020,
        "cited_by_count": 3,
        "has_abstract": True,
        "primary_location": {"source": {"display_name": "Example Journal"}},
    }
    r = work_to_sample_row(w, has_extra_abstract=False)
    assert r["openalex_id"] == "https://openalex.org/W1"
    assert r["source_display_name"] == "Example Journal"
    assert r["abstract_present"] is True


def test_openalex_fetch_injectable() -> None:
    called: list[str] = []

    def fake(u: str) -> dict:
        called.append(u)
        return {
            "meta": {"count": 42},
            "results": [
                {
                    "id": "https://openalex.org/W99",
                    "title": "Fake",
                    "publication_year": 2021,
                    "cited_by_count": 0,
                    "has_abstract": True,
                    "primary_location": {"source": {"display_name": "Venue"}},
                }
            ],
        }

    out = run_corpus_expansion_preview(
        policy=CorpusPolicy(),
        mailto="a@b.c",
        per_bucket_sample=10,
        fetch=fake,
        openalex_mode="live",
    )
    assert len(called) == 8
    assert out["buckets"][0]["estimated_candidate_count"] == 42
    assert out["buckets"][0]["sample_works"]


@patch("pipeline.corpus_expansion_preview.write_corpus_expansion_artifacts", lambda *a, **k: None)
def test_no_db_path_when_running_preview_helper() -> None:
    run_corpus_expansion_preview_from_cli(
        output=Path("out.json"),
        markdown_output=Path("out.md"),
        mailto="x@y.z",
        per_bucket_sample=10,
        mock_openalex=True,
    )
    # No assertion on psycopg; helper never imports it — covered by not importing connect


@patch("pipeline.cli.psycopg.connect")
def test_corpus_expansion_cli_does_not_call_postgres(
    connect: MagicMock,
    tmp_path: Path,
) -> None:
    jp = tmp_path / "c.json"
    mp = tmp_path / "c.md"
    with patch.object(
        cli_main.sys,
        "argv",
        [
            "pipeline.cli",
            "corpus-expansion-preview",
            "--output",
            str(jp),
            "--markdown-output",
            str(mp),
            "--mailto",
            "cli-test@example.com",
            "--mock-openalex",
            "--per-bucket-sample",
            "10",
        ],
    ):
        cli_main.main()
    connect.assert_not_called()
    data = jp.read_text(encoding="utf-8")
    assert "expand_before_next_bridge_weight_experiment" in data
    assert "200" in data and "500" in data
    md = mp.read_text(encoding="utf-8")
    assert "Caveats" in md
    assert "200" in md
    assert "500" in md
    assert "versioning" in md.lower() or "Versioning" in md
    # Planning only — not a validity proof
    assert "scientific validation" in md or "ingest" in md.lower()


def test_markdown_scientific_validation_disclaimer() -> None:
    from pipeline.corpus_expansion_preview import render_corpus_expansion_markdown, run_corpus_expansion_preview

    prev = run_corpus_expansion_preview(
        policy=CorpusPolicy(),
        mailto="m@e.com",
        per_bucket_sample=10,
        openalex_mode="mock",
    )
    md = render_corpus_expansion_markdown(prev)
    assert "scientific validation" in md
    assert "benchmark result" in md  # disclaimed, not claimed


def test_resolve_mailto_mock_allows_placeholder() -> None:
    assert resolve_corpus_expansion_preview_mailto(mailto="", mock_openalex=True) == "research-radar-dev@local.invalid"


def test_resolve_mailto_mock_prefers_cli_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENALEX_MAILTO", "env@example.com")
    assert resolve_corpus_expansion_preview_mailto(mailto="cli@example.com", mock_openalex=True) == "cli@example.com"


def test_resolve_mailto_live_requires_contact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    with pytest.raises(ValueError, match="live_corpus_expansion_preview_requires_mailto"):
        resolve_corpus_expansion_preview_mailto(mailto="", mock_openalex=False)


def test_resolve_mailto_live_accepts_api_key_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "openalex-key-test")
    assert resolve_corpus_expansion_preview_mailto(mailto="", mock_openalex=False) == "research-radar-dev@local.invalid"


def test_resolve_mailto_live_accepts_env_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENALEX_MAILTO", " u@openalex.test ")
    assert resolve_corpus_expansion_preview_mailto(mailto="", mock_openalex=False) == "u@openalex.test"


def test_resolve_mailto_live_accepts_cli_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
    assert resolve_corpus_expansion_preview_mailto(mailto="  only@cli.test ", mock_openalex=False) == "only@cli.test"


@patch("pipeline.corpus_expansion_preview.write_corpus_expansion_artifacts", lambda *a, **k: None)
def test_from_cli_live_exits_without_mailto_or_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    with pytest.raises(SystemExit) as exc:
        run_corpus_expansion_preview_from_cli(
            output=tmp_path / "a.json",
            markdown_output=tmp_path / "b.md",
            mailto="",
            per_bucket_sample=10,
            mock_openalex=False,
        )
    assert exc.value.code == 2


@patch("pipeline.cli.psycopg.connect")
def test_corpus_expansion_cli_live_exits_without_contact(
    _connect: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    with patch.object(
        cli_main.sys,
        "argv",
        [
            "pipeline.cli",
            "corpus-expansion-preview",
            "--output",
            str(tmp_path / "x.json"),
            "--markdown-output",
            str(tmp_path / "x.md"),
            "--per-bucket-sample",
            "10",
        ],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main()
    assert exc.value.code == 2


def test_corpus_expansion_includes_suggested_range_in_markdown() -> None:
    from pipeline.corpus_expansion_preview import render_corpus_expansion_markdown, run_corpus_expansion_preview

    prev = run_corpus_expansion_preview(
        policy=CorpusPolicy(), mailto="a@b.c", per_bucket_sample=10, openalex_mode="mock"
    )
    md = render_corpus_expansion_markdown(prev)
    assert "200" in md and "500" in md
    assert prev["recommendation"]["suggested_target_corpus_size_range"] == [200, 500]
