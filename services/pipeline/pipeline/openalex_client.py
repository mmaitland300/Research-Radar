from __future__ import annotations

import io
import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

DEFAULT_BACKOFF_SEC = 2.0
MAX_RETRIES = 4

OPENALEX_API_KEY_ENV = "OPENALEX_API_KEY"


def _default_mailto() -> str:
    return os.environ.get("OPENALEX_MAILTO", "research-radar-dev@local.invalid")


def openalex_api_key_from_env() -> str | None:
    raw = (os.environ.get(OPENALEX_API_KEY_ENV) or "").strip()
    return raw or None


def append_openalex_api_key_query(url: str, *, api_key: str | None = None) -> str:
    """Merge api_key=… into query string; replaces existing api_key. No-op if key absent."""
    key = api_key if api_key is not None else openalex_api_key_from_env()
    if not key:
        return url
    parsed = urlparse(url)
    pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "api_key"]
    pairs.append(("api_key", key))
    new_query = urlencode(pairs)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def compute_openalex_auth_artifact_fields(*, mock_openalex: bool) -> tuple[bool, str]:
    """Returns (api_key_provided, auth_mode) for artifacts only — never the secret."""
    if mock_openalex:
        return False, "mock"
    if openalex_api_key_from_env():
        return True, "api_key"
    return False, "no_key"


def compute_contact_provenance(*, mailto_cli: str, mock_openalex: bool) -> tuple[str, bool]:
    """
    Contact provenance for artifacts only (never stores raw mailto).

    - mock: --mock-openalex (may still pass mailto for placeholder resolution).
    - cli: non-empty --mailto (wins over OPENALEX_MAILTO when both set).
    - env: OPENALEX_MAILTO only.
    - none: live mode but no contact string (e.g. OPENALEX_API_KEY-only auth).
    """
    cli = (mailto_cli or "").strip()
    env = (os.environ.get("OPENALEX_MAILTO") or "").strip()
    if mock_openalex:
        return "mock", bool(cli or env)
    if cli:
        return "cli", True
    if env:
        return "env", True
    if openalex_api_key_from_env():
        return "none", False
    return "none", False


def fetch_openalex_json(
    url: str,
    *,
    mailto: str | None = None,
    timeout_sec: float = 60.0,
    apply_api_key: bool = True,
) -> Mapping[str, Any]:
    """GET JSON from OpenAlex. Optionally appends api_key from OPENALEX_API_KEY (preferred auth)."""
    agent_mailto = mailto or _default_mailto()
    headers = {
        "User-Agent": f"ResearchRadarPipeline/0.1 (mailto:{agent_mailto})",
        "Accept": "application/json",
    }
    req_url = append_openalex_api_key_query(url) if apply_api_key else url
    last_error: BaseException | None = None
    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(req_url, headers=headers, method="GET")
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
