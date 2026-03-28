from __future__ import annotations

import io
import json
import time
import urllib.error
import urllib.request
from typing import Any, Mapping

DEFAULT_BACKOFF_SEC = 2.0
MAX_RETRIES = 4


def _default_mailto() -> str:
    import os

    return os.environ.get("OPENALEX_MAILTO", "research-radar-dev@local.invalid")


def fetch_openalex_json(url: str, *, mailto: str | None = None, timeout_sec: float = 60.0) -> Mapping[str, Any]:
    """GET a JSON document from OpenAlex with a polite User-Agent."""
    agent_mailto = mailto or _default_mailto()
    headers = {
        "User-Agent": f"ResearchRadarPipeline/0.1 (mailto:{agent_mailto})",
        "Accept": "application/json",
    }
    last_error: BaseException | None = None
    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read()
            return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (429, 500, 502, 503) and attempt < MAX_RETRIES - 1:
                time.sleep(DEFAULT_BACKOFF_SEC * (attempt + 1))
                continue
            if exc.code == 400:
                try:
                    detail = exc.read().decode("utf-8", errors="replace")[:2000]
                except Exception:
                    detail = ""
                if detail:
                    raise urllib.error.HTTPError(
                        exc.url,
                        exc.code,
                        f"{exc.msg} — {detail}",
                        exc.hdrs,
                        io.BytesIO(detail.encode("utf-8")),
                    ) from exc
            raise
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(DEFAULT_BACKOFF_SEC * (attempt + 1))
                continue
            raise
    assert last_error is not None
    raise last_error
