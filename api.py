"""Thin MAX Bot API client (https://platform-api2.max.ru).

Auth is the raw bot token in ``Authorization`` — no ``Bearer`` prefix, no query
parameter (the query form is deprecated by MAX). The client itself carries no
default headers: every API call passes the token explicitly, and uploads to the
pre-signed URL deliberately pass none.
"""

from __future__ import annotations

from typing import Any, Optional

import httpx


class MaxApiError(Exception):
    """One failed MAX API call, classified for the adapter's retry policy."""

    def __init__(
        self,
        status: Optional[int],
        code: Optional[str],
        message: str,
        *,
        retryable: bool = False,
        retry_after: Optional[float] = None,
        timed_out: bool = False,
    ):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after = retry_after
        self.timed_out = timed_out

    def __str__(self) -> str:  # keep tokens/signed URLs out of logs
        parts = []
        if self.status:
            parts.append(f"HTTP {self.status}")
        if self.code:
            parts.append(str(self.code))
        parts.append(self.message)
        return " / ".join(parts)


def _retry_after(response: httpx.Response) -> Optional[float]:
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _error_from_response(response: httpx.Response) -> MaxApiError:
    code: Optional[str] = None
    message = response.reason_phrase or "request failed"
    try:
        payload = response.json()
        if isinstance(payload, dict):
            code = payload.get("code") or None
            message = str(payload.get("message") or message)
    except ValueError:
        pass
    status = response.status_code
    retryable = status == 429 or status >= 500
    return MaxApiError(status, code, message, retryable=retryable, retry_after=_retry_after(response))


class MaxApi:
    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = client

    @property
    def _auth(self) -> dict[str, str]:
        return {"Authorization": self.token}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
        timeout: Optional[float] = None,
        auth: bool = True,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            response = await self.client.request(
                method,
                url,
                params=params,
                json=json,
                headers=self._auth if auth else None,
                timeout=timeout,
            )
        except httpx.TimeoutException as exc:
            raise MaxApiError(None, "timeout", type(exc).__name__, retryable=False, timed_out=True) from exc
        except httpx.HTTPError as exc:
            raise MaxApiError(None, "network", type(exc).__name__, retryable=True) from exc
        if response.status_code >= 400:
            raise _error_from_response(response)
        if response.status_code == 204 or not response.content:
            return {}
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

    # -- identity ---------------------------------------------------------

    async def get_me(self) -> dict[str, Any]:
        return await self._request("GET", "/me", timeout=30.0)

    # -- inbound ----------------------------------------------------------

    async def get_updates(self, *, limit: int, timeout: int, marker: Any = None) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": int(limit), "timeout": int(timeout)}
        if marker is not None:
            params["marker"] = marker
        return await self._request("GET", "/updates", params=params, timeout=float(timeout) + 30.0)

    # -- outbound ---------------------------------------------------------

    async def send_message(
        self,
        *,
        chat_id: str,
        text: str,
        fmt: Optional[str] = None,
        attachments: Optional[list[dict[str, Any]]] = None,
        link: Optional[dict[str, Any]] = None,
        notify: bool = True,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"text": text, "notify": bool(notify)}
        if fmt in {"html", "markdown"}:
            body["format"] = fmt
        if attachments:
            body["attachments"] = attachments
        if link:
            body["link"] = link
        return await self._request("POST", "/messages", params={"chat_id": chat_id}, json=body)

    async def edit_message(
        self,
        *,
        message_id: str,
        text: str,
        fmt: Optional[str] = None,
        attachments: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"text": text}
        if fmt in {"html", "markdown"}:
            body["format"] = fmt
        if attachments is not None:
            body["attachments"] = attachments
        return await self._request("PUT", "/messages", params={"message_id": message_id}, json=body)

    async def delete_message(self, *, message_id: str) -> None:
        await self._request("DELETE", "/messages", params={"message_id": message_id})

    async def answer_callback(
        self,
        *,
        callback_id: str,
        notification: Optional[str] = None,
        message: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if notification is not None:
            body["notification"] = notification
        if message is not None:
            body["message"] = message
        return await self._request("POST", "/answers", params={"callback_id": callback_id}, json=body)

    async def send_typing(self, chat_id: str) -> None:
        await self._request("POST", f"/chats/{chat_id}/actions", json={"action": "typing_on"}, timeout=5.0)

    # -- media ------------------------------------------------------------

    async def create_upload(self, kind: str) -> dict[str, Any]:
        return await self._request("POST", "/uploads", params={"type": kind}, timeout=15.0)

    async def upload_file(self, url: str, data: bytes, filename: str, mime: str) -> dict[str, Any]:
        """POST the bytes to the pre-signed URL. No Authorization: it is a CDN host."""
        try:
            response = await self.client.post(
                url,
                files={"data": (filename or "file", data, mime or "application/octet-stream")},
                timeout=120.0,
            )
        except httpx.TimeoutException as exc:
            raise MaxApiError(None, "timeout", "upload timed out", retryable=True, timed_out=True) from exc
        except httpx.HTTPError as exc:
            raise MaxApiError(None, "network", type(exc).__name__, retryable=True) from exc
        if response.status_code >= 400:
            raise _error_from_response(response)
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        return payload if isinstance(payload, dict) else {}

    # -- chats ------------------------------------------------------------

    async def get_chat(self, chat_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/chats/{chat_id}")

    async def get_chat_members(self, chat_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/chats/{chat_id}/members")
