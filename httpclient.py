"""Shared HTTP client factory for the MAX adapter.

Kept in its own module so the adapter and the standalone sender build the same
SSRF-guarded client. Tests monkeypatch ``adapter.new_http_client``.
"""

from __future__ import annotations

from typing import Any, Optional


def new_http_client(*, verify: Any, timeout: Any, headers: Optional[dict[str, str]] = None) -> Any:
    """SSRF-guarded ``httpx.AsyncClient`` (connect-time IP checks, redirect re-checks)."""
    from gateway.platforms.helpers import platform_http_client

    return platform_http_client(verify=verify, timeout=timeout, headers=headers or {})
