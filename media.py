"""Inbound and outbound media for the MAX adapter.

Only documented attachment containers are inspected: ``message.body.attachments``
plus the named ``voice``/``audio``/``video``/``image``/``file`` containers of the
message. No free-form URL scanning: a signed URL from message text must never be
downloaded by the bot.
"""

from __future__ import annotations

import mimetypes
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Optional

import httpx

_KINDS = {"image", "video", "audio", "voice", "file"}
_AUDIO_KINDS = {"audio", "voice"}
_MEDIA_CONTAINERS = {"voice": "audio", "audio": "audio", "video": "video", "image": "image", "file": "file"}
_CONTAINER_KEYS = (
    "attachments",
    "voice",
    "audio",
    "video",
    "image",
    "file",
    "media",
    "payload",
    "message",
    "link",
    "reply",
    "body",
)
_DEFAULT_MIME = {
    "image": "image/jpeg",
    "video": "video/mp4",
    "audio": "audio/ogg",
    "file": "application/octet-stream",
}


@dataclass(frozen=True)
class Attachment:
    kind: str
    url: str = ""
    token: str = ""
    filename: str = ""
    size: Optional[int] = None
    mime: str = ""

    @property
    def is_image(self) -> bool:
        return self.kind == "image"


def _payload(item: dict[str, Any]) -> dict[str, Any]:
    payload = item.get("payload")
    return payload if isinstance(payload, dict) else {}


def _attachment_from(item: dict[str, Any]) -> Optional[Attachment]:
    if not isinstance(item, dict):
        return None
    kind = str(item.get("type") or "").lower()
    if kind not in _KINDS:
        return None
    payload = _payload(item)
    filename = str(payload.get("filename") or "")
    mime = str(payload.get("mime") or "") or mimetypes.guess_type(filename)[0] or ""
    # MAX has no distinct "voice" type: voice notes arrive as "audio". Files with
    # an audio MIME are promoted too, so the gateway's STT path sees them.
    if kind in _AUDIO_KINDS:
        kind = "audio"
    elif kind == "file" and mime.startswith("audio/"):
        kind = "audio"
    size: Optional[int]
    try:
        size = int(payload.get("size")) if payload.get("size") is not None else None
    except (TypeError, ValueError):
        size = None
    return Attachment(
        kind=kind,
        url=str(payload.get("url") or ""),
        token=str(payload.get("token") or ""),
        filename=filename,
        size=size,
        mime=mime or _DEFAULT_MIME.get(kind, ""),
    )


def collect_attachments(message: dict[str, Any]) -> list[Attachment]:
    body = message.get("body") if isinstance(message.get("body"), dict) else message
    items = body.get("attachments") or []
    found: list[Attachment] = []
    seen: set[tuple[str, str, str]] = set()
    if isinstance(items, list):
        for item in items:
            att = _attachment_from(item)
            if att is None:
                continue
            key = (att.kind, att.url, att.token)
            if key in seen:
                continue
            seen.add(key)
            found.append(att)
    return found


def collect_voice_fallback(message: dict[str, Any]) -> list[Attachment]:
    """Voice notes sometimes arrive outside ``body.attachments`` under a named key."""
    found: list[Attachment] = []
    for key in ("voice", "audio"):
        item = message.get(key)
        if isinstance(item, dict):
            att = _attachment_from({"type": "audio", "payload": item})
            if att and (att.url or att.token):
                found.append(att)
    return found


def collect_media_any(update: dict[str, Any], *, max_depth: int = 6) -> list[Attachment]:
    """Walk the documented containers of an update looking for attachments.

    MAX nests media differently for direct and forwarded messages (and voice
    notes may sit at the update root). Only attachment-shaped dicts with an
    explicit media ``type`` and a URL/token are taken — never arbitrary links.
    """
    found: list[Attachment] = []
    seen: set[tuple[str, str, str]] = set()

    def add(attachment: Optional[Attachment]) -> None:
        if attachment is None or not (attachment.url or attachment.token):
            return
        key = (attachment.kind, attachment.url, attachment.token)
        if key not in seen:
            seen.add(key)
            found.append(attachment)

    def walk(node: Any, depth: int) -> None:
        if depth > max_depth:
            return
        if isinstance(node, list):
            for item in node:
                walk(item, depth + 1)
            return
        if not isinstance(node, dict):
            return
        add(_attachment_from(node))
        for container in _CONTAINER_KEYS:
            if container not in node:
                continue
            value = node[container]
            hint = _MEDIA_CONTAINERS.get(container)
            if hint and isinstance(value, dict):
                add(_attachment_from({"type": hint, "payload": value}))
            walk(value, depth + 1)

    walk(update, 0)
    return found


async def download_attachment(
    client: httpx.AsyncClient,
    attachment: Attachment,
    *,
    max_bytes: int,
    base_url: str,
    token: str,
) -> Optional[bytes]:
    """Stream one attachment into memory, enforcing ``max_bytes``."""
    url = attachment.url
    headers: dict[str, str] = {}
    if not url and attachment.token:
        url = f"{base_url.rstrip('/')}/attachments/{attachment.token}"
        headers["Authorization"] = token
    if not url:
        return None
    try:
        async with client.stream("GET", url, headers=headers or None, timeout=60.0) as response:
            if response.status_code >= 400:
                return None
            declared = response.headers.get("Content-Length")
            if declared and declared.isdigit() and int(declared) > max_bytes:
                return None
            chunks: list[bytes] = []
            total = 0
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > max_bytes:
                    return None
                chunks.append(chunk)
            return b"".join(chunks)
    except httpx.HTTPError:
        return None


def _parse_upload_response(payload: dict[str, Any], fallback_token: str) -> Optional[dict[str, Any]]:
    if "photos" in payload and isinstance(payload["photos"], dict):
        return {"type": "image", "payload": {"photos": payload["photos"]}}
    token = str(payload.get("token") or fallback_token or "")
    if not token:
        return None
    return token


def _parse_xml(text: str) -> bool:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return False
    retval = root.findtext("retval")
    return str(retval or "").strip() in {"1", "true", "True"}


async def upload_attachment(
    api: Any,
    *,
    data: bytes,
    kind: str,
    filename: str,
    mime: str,
) -> Optional[dict[str, Any]]:
    """Create an upload slot, push the bytes, return the attachment descriptor."""
    if kind not in _KINDS:
        kind = "file"
    try:
        slot = await api.create_upload(kind)
    except Exception:
        return None
    url = str(slot.get("url") or "")
    slot_token = str(slot.get("token") or "")
    if not url:
        return None
    try:
        result = await api.upload_file(url, data, filename, mime)
    except Exception:
        return None
    parsed = _parse_upload_response(result, slot_token)
    if parsed is None:
        return None
    if isinstance(parsed, dict):
        return parsed
    # A bare token: for images MAX may also accept a plain token payload.
    return {"type": kind, "payload": {"token": parsed}}


__all__ = [
    "Attachment",
    "collect_attachments",
    "collect_media_any",
    "collect_voice_fallback",
    "download_attachment",
    "upload_attachment",
]
