import json

import pytest

from pipeline.embedding_provider import OpenAIEmbeddingProvider


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_openai_embedding_provider_sends_batch_and_sorts_by_index(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: float):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["headers"] = {key.lower(): value for key, value in request.header_items()}
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse(
            {
                "data": [
                    {"index": 1, "embedding": [0.3, 0.4, 0.5]},
                    {"index": 0, "embedding": [0.0, 0.1, 0.2]},
                ]
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    provider = OpenAIEmbeddingProvider(
        api_key="test-key",
        model="test-model",
        base_url="https://example.test/v1",
        expected_dimensions=3,
        timeout_seconds=12.5,
    )

    vectors = provider.embed_texts(["first text", "second text"])

    assert captured["url"] == "https://example.test/v1/embeddings"
    assert captured["timeout"] == 12.5
    assert captured["headers"]["authorization"] == "Bearer test-key"
    assert captured["body"] == {"model": "test-model", "input": ["first text", "second text"]}
    assert vectors == [[0.0, 0.1, 0.2], [0.3, 0.4, 0.5]]


def test_openai_embedding_provider_rejects_dimension_mismatch(monkeypatch) -> None:
    def fake_urlopen(request, timeout: float):
        return _FakeResponse({"data": [{"index": 0, "embedding": [0.1, 0.2]}]})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    provider = OpenAIEmbeddingProvider(api_key="test-key", expected_dimensions=3)

    with pytest.raises(RuntimeError, match="dimension mismatch"):
        provider.embed_texts(["hello"])
