"""Standalone send path: cron delivery and ``hermes send --to max:<chat_id>``.

Runs without a live gateway, so it builds its own HTTP client, uploads media and
posts directly through the MAX API. Failures go through ``send_error`` so a
token or signed URL from an httpx error never reaches the model transcript.
"""

from __future__ import annotations

import asyncio
import mimetypes
import ssl
from pathlib import Path
from typing import Any, Optional

import httpx
from gateway.platforms._shared import send_error

from .api import MaxApi, MaxApiError
from .formatting import render_chunks
from .httpclient import new_http_client
from .media import upload_attachment
from .settings import MaxSettings

MAX_MESSAGE_LENGTH = 4000


def _kind_for_suffix(suffix: str) -> str:
    if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic"}:
        return "image"
    if suffix in {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}:
        return "video"
    if suffix in {".ogg", ".oga", ".mp3", ".m4a", ".wav", ".opus", ".flac"}:
        return "audio"
    return "file"


async def standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> dict[str, Any]:
    """Send one message (with optional media) through the MAX Bot API."""
    settings = MaxSettings.from_config(pconfig)
    if not settings.token:
        return send_error("MAX_BOT_TOKEN is not set")

    verify = ssl.create_default_context()
    if settings.ca_cert_path and settings.ca_cert_path.exists():
        verify.load_verify_locations(cafile=str(settings.ca_cert_path))
    client = new_http_client(
        verify=verify,
        timeout=httpx.Timeout(
            connect=15.0, read=settings.http_timeout, write=settings.http_timeout, pool=15.0
        ),
    )
    api = MaxApi(settings.base_url, settings.token, client, sanitize_errors=settings.sanitize_errors)
    try:
        uploaded: list[dict[str, Any]] = []
        for item in media_files or []:
            path_str, is_voice = item if isinstance(item, (tuple, list)) and len(item) == 2 else (item, False)
            path = Path(str(path_str)).expanduser()
            if not path.is_file():
                continue
            data = await asyncio.to_thread(path.read_bytes)
            kind = (
                "audio" if is_voice else ("file" if force_document else _kind_for_suffix(path.suffix.lower()))
            )
            attachment = await upload_attachment(
                api,
                data=data,
                kind=kind,
                filename=path.name,
                mime=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
            )
            if attachment is not None:
                uploaded.append(attachment)

        chunks = render_chunks(message or "", settings.format, MAX_MESSAGE_LENGTH) or [""]
        delivered: list[str] = []
        for index, chunk in enumerate(chunks):
            result = await api.send_message(
                chat_id=str(chat_id),
                text=chunk,
                fmt=settings.format if settings.format in {"html", "markdown"} else None,
                attachments=uploaded if index == 0 else None,
                notify=settings.notify,
            )
            message_obj = result.get("message") if isinstance(result.get("message"), dict) else {}
            body = message_obj.get("body") if isinstance(message_obj.get("body"), dict) else {}
            if body.get("mid"):
                delivered.append(str(body["mid"]))
        return {
            "success": True,
            "platform": "max",
            "chat_id": str(chat_id),
            "message_id": delivered[-1] if delivered else None,
        }
    except MaxApiError as exc:
        return send_error(str(exc))
    finally:
        await client.aclose()
