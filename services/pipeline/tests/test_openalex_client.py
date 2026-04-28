"""OpenAlex HTTP client: API key query param, no secret leakage."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from pipeline.openalex_client import (
    OPENALEX_API_KEY_ENV,
    append_openalex_api_key_query,
    compute_contact_provenance,
    compute_openalex_auth_artifact_fields,
    fetch_openalex_json,
)


def test_append_openalex_api_key_query_adds_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "test-key-value")
    url = "https://api.openalex.org/works?filter=id:W1"
    out = append_openalex_api_key_query(url)
    assert "api_key=test-key-value" in out
    assert out.startswith("https://api.openalex.org/works?")


def test_append_openalex_api_key_query_replaces_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "new-secret")
    url = "https://api.openalex.org/works?filter=x&api_key=old"
    out = append_openalex_api_key_query(url)
    assert "api_key=new-secret" in out
    assert "api_key=old" not in out


def test_append_openalex_api_key_query_noop_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    url = "https://api.openalex.org/works?filter=id:W1"
    assert append_openalex_api_key_query(url) == url


def test_fetch_openalex_json_requests_url_with_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "k-from-env-unit-test")
    monkeypatch.setenv("OPENALEX_MAILTO", "contact-meta-only@test.invalid")
    captured: list[str] = []

    class _Resp:
        def read(self) -> bytes:
            return json.dumps({"meta": {}, "results": []}).encode("utf-8")

        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(req: object, timeout: float | None = None) -> _Resp:
        captured.append(getattr(req, "full_url", ""))
        return _Resp()

    with patch("urllib.request.urlopen", fake_urlopen):
        fetch_openalex_json("https://api.openalex.org/works?filter=id:W123")

    assert len(captured) == 1
    assert "api_key=k-from-env-unit-test" in captured[0]


def test_fetch_openalex_json_apply_api_key_false_skips_query(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "should-not-appear")
    captured: list[str] = []

    class _Resp:
        def read(self) -> bytes:
            return b"{}"

        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(req: object, timeout: float | None = None) -> _Resp:
        captured.append(getattr(req, "full_url", ""))
        return _Resp()

    with patch("urllib.request.urlopen", fake_urlopen):
        fetch_openalex_json(
            "https://api.openalex.org/works?filter=id:W9",
            apply_api_key=False,
        )
    assert "api_key" not in captured[0]


def test_compute_openalex_auth_artifact_fields_live(monkeypatch: pytest.MonkeyPatch) -> None:
    assert compute_openalex_auth_artifact_fields(mock_openalex=True) == (False, "mock")
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    assert compute_openalex_auth_artifact_fields(mock_openalex=False) == (False, "no_key")
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "artifact-test-key")
    assert compute_openalex_auth_artifact_fields(mock_openalex=False) == (True, "api_key")


def test_compute_contact_provenance_api_key_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "x")
    assert compute_contact_provenance(mailto_cli="", mock_openalex=False) == ("none", False)


def test_compute_contact_provenance_mock_ignores_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "x")
    assert compute_contact_provenance(mailto_cli="", mock_openalex=True) == ("mock", False)
