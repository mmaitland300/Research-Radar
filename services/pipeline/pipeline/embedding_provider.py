from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol, Sequence

DEFAULT_OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
EXPECTED_EMBEDDING_DIMENSIONS = 1536
RECOGNIZED_OPENAI_ERROR_CODES = frozenset({"insufficient_quota"})


class EmbeddingProvider(Protocol):
    def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        ...


@dataclass(frozen=True)
class OpenAIEmbeddingProvider:
    api_key: str
    model: str = DEFAULT_OPENAI_EMBEDDING_MODEL
    base_url: str = DEFAULT_OPENAI_BASE_URL
    expected_dimensions: int = EXPECTED_EMBEDDING_DIMENSIONS
    timeout_seconds: float = 60.0

    def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []

        payload = json.dumps(
            {
                "model": self.model,
                "input": list(texts),
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            url=f"{self.base_url.rstrip('/')}/embeddings",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw_body = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            try:
                parsed_detail = json.loads(detail)
            except json.JSONDecodeError:
                parsed_detail = None

            error_code = None
            if isinstance(parsed_detail, dict):
                error_obj = parsed_detail.get("error")
                if isinstance(error_obj, dict):
                    code = error_obj.get("code")
                    if isinstance(code, str) and code in RECOGNIZED_OPENAI_ERROR_CODES:
                        error_code = code

            if exc.code == 429 and error_code == "insufficient_quota":
                raise RuntimeError(
                    "OpenAI embeddings request failed with status 429 "
                    "(insufficient_quota): quota exhausted; no embeddings were written."
                ) from None

            status = exc.code if isinstance(exc.code, int) else "unknown"
            code_suffix = f" ({error_code})" if error_code else ""
            raise RuntimeError(
                f"OpenAI embeddings request failed with status {status}{code_suffix}."
            ) from None
        except urllib.error.URLError:
            raise RuntimeError(
                "OpenAI embeddings request failed: transport details redacted."
            ) from None

        body = json.loads(raw_body.decode("utf-8"))
        data = body.get("data")
        if not isinstance(data, list):
            raise RuntimeError("OpenAI embeddings response missing data list.")
        if len(data) != len(texts):
            raise RuntimeError(
                "OpenAI embeddings response count mismatch: "
                f"expected {len(texts)} items, got {len(data)}."
            )

        vectors: list[list[float]] = []
        for item in sorted(data, key=lambda value: int(value.get("index", 0))):
            embedding = item.get("embedding")
            if not isinstance(embedding, list) or not embedding:
                raise RuntimeError("OpenAI embeddings response item missing embedding array.")
            vector = [float(value) for value in embedding]
            if len(vector) != self.expected_dimensions:
                raise RuntimeError(
                    "OpenAI embeddings dimension mismatch: "
                    f"expected {self.expected_dimensions}, got {len(vector)}."
                )
            vectors.append(vector)
        return vectors


def openai_embedding_provider_from_env(
    *,
    model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
    expected_dimensions: int = EXPECTED_EMBEDDING_DIMENSIONS,
    timeout_seconds: float = 60.0,
) -> OpenAIEmbeddingProvider:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is required for embed-works. "
            "Set it in the environment before running the embedding job."
        )
    base_url = os.environ.get("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    return OpenAIEmbeddingProvider(
        api_key=api_key,
        model=model,
        base_url=base_url,
        expected_dimensions=expected_dimensions,
        timeout_seconds=timeout_seconds,
    )
