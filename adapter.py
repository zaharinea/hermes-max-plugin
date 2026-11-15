"""MAX (max.ru) platform adapter for Hermes Agent.

Long polling (``GET /updates`` with a persisted marker) in, ``POST /messages``
out. Supports text, native media, group chats, typing indicators and
inline-keyboard approval/clarify/choice prompts. A ``user_id`` allowlist gates
every inbound message; group traffic additionally requires both an allowlisted
sender and (when configured) an allowlisted chat.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import logging
import random
import re
import ssl
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from gateway.config import Platform, PlatformConfig
from gateway.platform_registry import PlatformEntry
from gateway.platforms._shared import (
    apply_yaml_bridge,
    config_connected_extra,
    env_is_connected,
    extra_or_secret,
    get_scoped_secret,
    seed_extra_from_env,
)
from gateway.platforms.base import (
    BasePlatformAdapter,
    ExecApprovalPrompt,
    SendResult,
    cache_media_bytes_async,
)
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator

from .api import MaxApi, MaxApiError
from .callbacks import CallbackRegistry
from .formatting import iter_media_refs, render_chunks
from .httpclient import new_http_client
from .media import (
    Attachment,
    collect_attachments,
    collect_media_any,
    collect_voice_fallback,
    download_attachment,
    upload_attachment,
)
from .rate_limit import RateLimiter
from .settings import MaxSettings
from .state import MaxStateStore

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 4000
_PLACEHOLDERS = {
    "image": "[фото]",
    "video": "[видео]",
    "audio": "[аудио]",
    "file": "[файл]",
}
_PLATFORM_HINT = (
    "You are communicating through MAX (max.ru). Replies are limited to 4000 characters and "
    "are split automatically. Markdown is rendered as MAX HTML. You can send files natively by "
    "writing MEDIA:/absolute/path on its own line; images, video, audio and documents are "
    "uploaded as attachments. In group chats the bot answers only when mentioned unless "
    "MAX_REQUIRE_MENTION is disabled. Approval and clarify prompts use inline buttons."
)
# Curated menu: MAX allows 32 entries, but a short list is easier to use. The
# full set stays reachable by typing any command (and via /commands).
_MAX_PROVIDERS_PER_PAGE = 10
_MAX_MODELS_PER_PAGE = 20
_MAX_KNOWN_CHATS = 500
_MAX_ATTACHMENTS = 10
_MAX_OUTBOUND_BYTES = 50 * 1024 * 1024
_COMMAND_MENU = (
    {"name": "status", "description": "Состояние сессии, модели и контекста"},
    {"name": "new", "description": "Начать новую сессию"},
    {"name": "stop", "description": "Остановить фоновые процессы"},
    {"name": "context", "description": "Контекст: занято, категории, сжатие"},
    {"name": "model", "description": "Сменить модель (--global — навсегда)"},
    {"name": "reasoning", "description": "Глубина и показ рассуждений"},
    {"name": "cron", "description": "Задачи по расписанию"},
    {"name": "memory", "description": "Память: записи и одобрение"},
    {"name": "agents", "description": "Активные задачи и субагенты"},
    {"name": "tools", "description": "Инструменты: список, вкл/выкл"},
    {"name": "skills", "description": "Навыки: поиск и установка"},
    {"name": "approve", "description": "Одобрить ожидающую опасную команду"},
    {"name": "deny", "description": "Отклонить ожидающую опасную команду"},
    {"name": "sessions", "description": "Прошлые сессии: список и выбор"},
    {"name": "resume", "description": "Продолжить сессию по имени"},
    {"name": "queue", "description": "Очередь промптов: поставить или список"},
    {"name": "steer", "description": "Вставить сообщение после следующего шага"},
    {"name": "btw", "description": "Вопрос в сторону, не прерывая работу"},
    {"name": "commands", "description": "Полный список команд и скиллов"},
    {"name": "help", "description": "Справка по командам"},
)


def _update_ts_ms(update: dict[str, Any]) -> Optional[float]:
    raw = update.get("timestamp")
    if raw is None:
        message = update.get("message")
        if isinstance(message, dict):
            raw = message.get("timestamp")
    try:
        return float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _looks_like_tls_error(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ssl.SSLError):
            return True
        text = str(current)
        if "CERTIFICATE_VERIFY_FAILED" in text or "certificate verify failed" in text:
            return True
        current = current.__cause__ or current.__context__  # type: ignore[assignment]
    return False


class MaxAdapter(BasePlatformAdapter):
    """MAX platform adapter."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    splits_long_messages = True
    supports_code_blocks = True
    serial_receive_loop = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform("max"))
        self._settings = MaxSettings.from_config(config)
        self._store = MaxStateStore(self._settings.state_path)
        self._state = self._store.load()
        self._dedup = MessageDeduplicator(max_size=2000, ttl_seconds=self._settings.dedup_ttl)
        for key in list(self._state.recent):
            self._dedup.is_duplicate(key)
        self._callbacks = CallbackRegistry(ttl=self._settings.callback_ttl)
        self._limiter = RateLimiter(self._settings.per_chat_rps, self._settings.global_rps)
        self._api: Optional[MaxApi] = None
        self._http: Optional[httpx.AsyncClient] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._bot_id: Optional[str] = None
        self._bot_username: str = ""
        self._bot_name: str = ""
        self._known_chats: dict[str, dict[str, str]] = {}
        self._reveal_last: dict[str, float] = {}
        self._marker_failures: int = 0

    # -- lifecycle ---------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        settings = self._settings
        if not settings.token:
            self._set_fatal_error("max_no_token", "MAX_BOT_TOKEN is not set", retryable=False)
            await self._safe_notify_fatal()
            return False

        identity = hashlib.sha256(settings.token.encode()).hexdigest()[:16]
        if not self._acquire_platform_lock("max-bot-token", identity, "MAX bot token"):
            return False

        try:
            verify = ssl.create_default_context()
            if settings.ca_cert_path and settings.ca_cert_path.exists():
                verify.load_verify_locations(cafile=str(settings.ca_cert_path))
            self._http = new_http_client(
                verify=verify,
                timeout=httpx.Timeout(
                    connect=15.0,
                    read=settings.http_timeout + settings.poll_timeout,
                    write=settings.http_timeout,
                    pool=15.0,
                ),
            )
            self._api = MaxApi(
                settings.base_url, settings.token, self._http, sanitize_errors=settings.sanitize_errors
            )
            me = await self._api.get_me()
        except MaxApiError as exc:
            await self._close_http()
            self._release_platform_lock()
            if _looks_like_tls_error(exc):
                self._set_fatal_error(
                    "max_tls_failed",
                    "TLS verification failed for platform-api2.max.ru. Install the Russian "
                    "Trusted Root CA at MAX_CA_CERT_PATH (see the plugin README).",
                    retryable=False,
                )
            elif exc.status in (401, 403):
                self._set_fatal_error(
                    "max_unauthorized",
                    "MAX API rejected the bot token (401/403). Check MAX_BOT_TOKEN.",
                    retryable=False,
                )
            elif exc.retryable or exc.timed_out:
                self._set_fatal_error("max_unreachable", f"MAX API is unreachable: {exc}", retryable=True)
            else:
                self._set_fatal_error(
                    "max_connect_failed", f"MAX API connection failed: {exc}", retryable=False
                )
            await self._safe_notify_fatal()
            return False
        except Exception as exc:
            await self._close_http()
            self._release_platform_lock()
            self._set_fatal_error(
                "max_connect_failed", f"MAX API connection failed: {type(exc).__name__}", retryable=True
            )
            await self._safe_notify_fatal()
            return False

        self._bot_id = str(me.get("user_id") or "") or None
        self._bot_username = str(me.get("username") or "")
        self._bot_name = str(me.get("first_name") or me.get("name") or "")
        if self._state.bot_id and self._bot_id and self._state.bot_id != self._bot_id:
            logger.warning(
                "[max] bot identity changed (%s -> %s); resetting marker", self._state.bot_id, self._bot_id
            )
            self._state.marker = None
            self._state.recent.clear()
        self._state.bot_id = self._bot_id
        await self._save_state()
        await self._register_commands()

        self._mark_connected()
        self._stop_event = asyncio.Event()
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("[max] connected as @%s (user_id=%s)", self._bot_username or "?", self._bot_id or "?")
        return True

    async def _register_commands(self) -> None:
        """Best-effort native command menu (``PATCH /me/commands``)."""
        if not self._settings.register_commands or self._api is None:
            return
        commands = self._menu_commands()
        try:
            await self._api.set_commands(commands)
            logger.info("[max] command menu registered (%d commands)", len(commands))
        except MaxApiError as exc:
            logger.debug("[max] command menu registration failed: %s", exc)

    def _menu_commands(self) -> list[dict[str, str]]:
        """The curated static menu (no core imports: keeps ``plugins validate`` clean)."""
        return [dict(row) for row in _COMMAND_MENU]

    async def disconnect(self) -> None:
        self._running = False
        if self._stop_event is not None:
            self._stop_event.set()
        task, self._poll_task = self._poll_task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        await self._close_http()
        await self._save_state()
        self._release_platform_lock()
        self._mark_disconnected()

    async def _close_http(self) -> None:
        client, self._http = self._http, None
        self._api = None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.aclose()

    async def _safe_notify_fatal(self) -> None:
        try:
            await self._notify_fatal_error()
        except Exception as exc:  # pragma: no cover - notification is best effort
            logger.debug("[max] fatal-error notification failed: %s", type(exc).__name__)

    async def _save_state(self) -> None:
        await self._store.save(self._state)

    # -- polling -----------------------------------------------------------

    async def _poll_loop(self) -> None:
        backoff_index = 0
        while self._running:
            try:
                await self._poll_once()
                backoff_index = 0
            except asyncio.CancelledError:
                raise
            except MaxApiError as exc:
                if exc.status in (401, 403):
                    self._set_fatal_error(
                        "max_unauthorized", f"MAX API rejected the token ({exc.status})", retryable=False
                    )
                    await self._safe_notify_fatal()
                    return
                delay = self._backoff_delay(backoff_index, exc)
                backoff_index += 1
                logger.warning("[max] poll failed: %s; retry in %.1fs", exc, delay)
                await self._sleep_or_stop(delay)
                continue
            except Exception as exc:
                if not self._running:
                    return
                delay = self._backoff_delay(backoff_index, None)
                backoff_index += 1
                logger.warning("[max] poll error: %s; retry in %.1fs", type(exc).__name__, delay)
                await self._sleep_or_stop(delay)
                continue
            await self._sleep_or_stop(max(0.3, self._settings.poll_pause))

    def _backoff_delay(self, index: int, exc: Optional[MaxApiError]) -> float:
        base = min(self._settings.backoff_max, self._settings.backoff_base * (2 ** max(0, index)))
        if exc is not None and exc.retry_after:
            base = max(base, exc.retry_after)
        return base + random.uniform(0.0, min(0.5, base * 0.25))

    async def _sleep_or_stop(self, seconds: float) -> None:
        if self._stop_event is None:
            await asyncio.sleep(seconds)
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=max(0.0, seconds))

    async def _poll_once(self) -> None:
        api = self._api
        if api is None:
            return
        cold_start = self._state.marker is None
        data = await asyncio.wait_for(
            api.get_updates(
                limit=self._settings.poll_limit,
                timeout=self._settings.poll_timeout,
                marker=self._state.marker,
            ),
            timeout=self._settings.poll_timeout + 30.0,
        )
        updates = data.get("updates") or []
        cutoff_ms: Optional[float] = None
        if cold_start and self._settings.bootstrap_max_age > 0:
            cutoff_ms = (time.time() - self._settings.bootstrap_max_age) * 1000.0
        failed = False
        for update in updates:
            if not isinstance(update, dict):
                continue
            ts = _update_ts_ms(update)
            if cutoff_ms is not None and ts is not None and ts < cutoff_ms:
                logger.info(
                    "[max] bootstrap: skipping update older than %ss", self._settings.bootstrap_max_age
                )
                continue
            try:
                await self._process_update(update)
            except asyncio.CancelledError:
                raise
            except Exception:
                failed = True
                logger.exception("[max] update processing failed (type=%s)", update.get("update_type"))
            await self._save_state()
        marker = data.get("marker")
        if marker is not None:
            if failed and self._state.marker is not None and self._marker_failures < 3:
                # Keep the cursor so the failed update is retried, but do not loop
                # forever on a poison update.
                self._marker_failures += 1
                logger.warning(
                    "[max] holding marker after a failed update (attempt %d/3)", self._marker_failures
                )
            elif failed and self._state.marker is not None:
                logger.error(
                    "[max] dropping a batch that failed %d times; advancing marker", self._marker_failures
                )
                self._state.marker = marker
                self._marker_failures = 0
            else:
                self._state.marker = marker
                self._marker_failures = 0
        await self._save_state()

    # -- dispatch ----------------------------------------------------------

    async def _process_update(self, update: dict[str, Any]) -> None:
        utype = str(update.get("update_type") or update.get("type") or "")
        if self._settings.debug_updates:
            message = update.get("message") if isinstance(update.get("message"), dict) else {}
            body = message.get("body") if isinstance(message.get("body"), dict) else {}
            logger.info(
                "[max] update type=%s keys=%s message_keys=%s body_keys=%s",
                utype or "?",
                sorted(update)[:12],
                sorted(message)[:12],
                sorted(body)[:12],
            )
        if utype == "message_created":
            await self._handle_message_created(update)
        elif utype == "message_callback":
            await self._handle_message_callback(update)
        elif utype == "bot_started":
            await self._handle_bot_started(update)
        elif utype == "bot_added":
            await self._handle_bot_added(update)
        elif utype == "chat_title_changed":
            self._handle_title_changed(update)
        else:
            logger.debug("[max] ignoring update type %s", utype or "?")

    def _claim_update(self, key: str) -> bool:
        if not key:
            return False
        if self._dedup.is_duplicate(key):
            return True
        self._state.recent[key] = time.time()
        return False

    # -- inbound -----------------------------------------------------------

    async def _handle_message_created(self, update: dict[str, Any]) -> None:
        message = update.get("message") if isinstance(update.get("message"), dict) else {}
        sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
        sender_id = str(sender.get("user_id") or "")
        if not sender_id or sender.get("is_bot") or (self._bot_id and sender_id == self._bot_id):
            if not sender_id:
                logger.info(
                    "[max] message_created without sender (keys=%s) — cannot authorize; ignored",
                    sorted(update)[:8],
                )
            return
        recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
        chat_id = str(update.get("chat_id") or recipient.get("chat_id") or sender_id)
        native_type = str(recipient.get("chat_type") or "dialog")
        chat_type = "dm" if native_type == "dialog" else "group"
        body = message.get("body") if isinstance(message.get("body"), dict) else {}
        text = str(body.get("text") or "").strip()
        mid = str(body.get("mid") or "")

        key = f"msg:{mid}" if mid else f"msg:{chat_id}:{update.get('timestamp')}"
        sender_name = self._display_name(sender)
        if self._is_whoami(text) and chat_type == "dm":
            if self._claim_update(key):
                return
            self._remember_chat(chat_id, chat_type, sender_name)
            await self._reply_whoami(chat_id, sender_id)
            return

        if chat_type == "dm":
            if not self._settings.user_allowed(sender_id):
                logger.info("[max] ignoring DM from user_id=%s (not in MAX_ALLOWED_USERS)", sender_id)
                return
        else:
            if not self._settings.user_allowed(sender_id):
                logger.debug("[max] ignoring group message from user_id=%s (not allowlisted)", sender_id)
                return
            if self._settings.group_allowed_chats and chat_id not in self._settings.group_allowed_chats:
                logger.debug("[max] ignoring group chat %s (not in MAX_GROUP_ALLOWED_CHATS)", chat_id)
                return
            if self._settings.require_mention and not self._is_addressed(message, text):
                return

        # Journal only accepted updates: rejected senders must not dirty state.
        if self._claim_update(key):
            return

        # Remember chats only after authorization: an unauthenticated sender must
        # not grow adapter state.
        self._remember_chat(chat_id, chat_type, sender_name)

        attachments = collect_attachments(message)
        if not attachments:
            attachments = collect_voice_fallback(message)
        if not attachments:
            attachments = collect_media_any(update)

        # MAX ships the transcript with the audio attachment; prefer it over STT.
        # The local STT path stays as the fallback (and can be forced by setting
        # MAX_USE_BUILTIN_TRANSCRIPTION=false).
        transcription = ""
        if self._settings.use_builtin_transcription:
            transcription = next(
                (
                    item.transcription.strip()
                    for item in attachments
                    if item.kind == "audio" and item.transcription.strip()
                ),
                "",
            )
        if transcription:
            text = f"{text}\n\n[Расшифровка голосового] {transcription}" if text else transcription

        media_urls: list[str] = []
        media_types: list[str] = []
        media_filenames: list[str] = []
        kinds: list[str] = []
        total_media_bytes = 0
        for attachment in attachments[:_MAX_ATTACHMENTS]:
            kinds.append(attachment.kind)
            data = await download_attachment(
                self._http,  # type: ignore[arg-type]
                attachment,
                max_bytes=self._settings.download_max_bytes,
                base_url=self._settings.base_url,
                token=self._settings.token,
            )
            if not data:
                continue
            total_media_bytes += len(data)
            if total_media_bytes > self._settings.download_max_bytes:
                logger.warning(
                    "[max] inbound media batch exceeded %d MB; skipping the rest",
                    self._settings.download_max_mb,
                )
                break
            cached = await cache_media_bytes_async(
                data,
                filename=attachment.filename,
                mime_type=attachment.mime,
                default_kind=attachment.kind if attachment.kind in {"image", "video", "audio"} else None,
            )
            if cached is None:
                continue
            media_urls.append(cached.path)
            media_types.append(cached.media_type)
            media_filenames.append(cached.display_name)

        if not text and not media_urls:
            if kinds:
                text = " ".join(self._placeholder(attachments))
            else:
                logger.debug("[max] empty inbound update from %s", sender_id)
                return

        message_type = MessageType.TEXT
        if "image" in kinds:
            message_type = MessageType.PHOTO
        elif "video" in kinds:
            message_type = MessageType.VIDEO
        elif "audio" in kinds:
            # A built-in transcript makes the gateway STT pass redundant; keep the
            # audio available but mark it as a plain audio file.
            message_type = MessageType.AUDIO if transcription else MessageType.VOICE
        elif "file" in kinds:
            message_type = MessageType.DOCUMENT

        timestamp_ms = _update_ts_ms(update)
        event_time = (
            datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
            if timestamp_ms is not None
            else datetime.now(tz=timezone.utc)
        )
        source = self.build_source(
            chat_id=chat_id,
            chat_name=self._chat_name(chat_id, sender_name, chat_type),
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_name,
            message_id=mid or None,
        )
        event = MessageEvent(
            text=text,
            message_type=message_type,
            user_id=sender_id,
            user_name=sender_name,
            source=source,
            message_id=mid or None,
            media_urls=media_urls,
            media_types=media_types,
            media_filenames=media_filenames,
            timestamp=event_time,
        )
        await self.handle_message(event)

    async def _handle_bot_started(self, update: dict[str, Any]) -> None:
        user = update.get("user") if isinstance(update.get("user"), dict) else {}
        user_id = str(user.get("user_id") or update.get("user_id") or "")
        chat_id = str(update.get("chat_id") or user_id)
        if user_id and self._settings.user_allowed(user_id):
            self._remember_chat(chat_id, "dm", self._display_name(user))
        logger.info("[max] bot_started chat_id=%s user_id=%s", chat_id, user_id or "?")

    async def _handle_bot_added(self, update: dict[str, Any]) -> None:
        chat_id = str(update.get("chat_id") or "")
        if not chat_id:
            return
        self._remember_chat(chat_id, "group", str(update.get("title") or ""))
        logger.info("[max] bot added to chat %s (type=%s)", chat_id, update.get("chat_type") or "?")
        with contextlib.suppress(Exception):
            members = await self._api.get_chat_members(chat_id)  # type: ignore[union-attr]
            items = members.get("members") or []
            for member in items if isinstance(items, list) else []:
                if str(member.get("user_id") or "") == (self._bot_id or ""):
                    if not (member.get("is_admin") or member.get("is_owner")):
                        logger.warning(
                            "[max] bot is not an administrator of chat %s; MAX may not deliver all group events",
                            chat_id,
                        )
                    break

    def _handle_title_changed(self, update: dict[str, Any]) -> None:
        chat_id = str(update.get("chat_id") or "")
        title = str(update.get("title") or "")
        if chat_id and title:
            self._remember_chat(chat_id, "group", title)

    async def _handle_message_callback(self, update: dict[str, Any]) -> None:
        callback = update.get("callback") if isinstance(update.get("callback"), dict) else {}
        user = update.get("user") if isinstance(update.get("user"), dict) else {}
        if not user:
            user = callback.get("user") if isinstance(callback.get("user"), dict) else {}
        user_id = str(user.get("user_id") or "")
        callback_id = str(callback.get("callback_id") or "")
        payload = str(callback.get("payload") or "")
        message = update.get("message") if isinstance(update.get("message"), dict) else {}
        recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
        chat_id = str(update.get("chat_id") or recipient.get("chat_id") or user_id)

        key = f"cb:{callback_id}" if callback_id else f"cb:{payload}:{update.get('timestamp')}"

        if not user_id or not self._settings.user_allowed(user_id):
            logger.info("[max] ignoring button press from user_id=%s (not allowlisted)", user_id or "?")
            await self._answer_callback(callback_id, notification="Недостаточно прав.")
            return
        if self._claim_update(key):
            return

        parts = payload.split(":")
        if len(parts) < 3 or parts[0] != "mx":
            logger.debug("[max] unknown callback payload shape")
            await self._answer_callback(callback_id, notification="Кнопка не распознана.")
            return
        token, action = parts[1], ":".join(parts[2:])
        entry = self._callbacks.peek(token)
        if entry is None:
            await self._answer_callback(callback_id, notification="Кнопка устарела.")
            return
        # Check actor and chat BEFORE consuming the single-use token: a foreign
        # allowlisted user must not be able to burn the owner's button.
        if entry.user_id and entry.user_id != user_id:
            logger.info("[max] callback user mismatch for token owner")
            await self._answer_callback(callback_id, notification="Недостаточно прав.")
            return
        if entry.chat_id and str(entry.chat_id) != chat_id:
            logger.info("[max] callback chat mismatch (card=%s press=%s)", entry.chat_id, chat_id)
            await self._answer_callback(callback_id, notification="Недостаточно прав.")
            return
        native_type = str(recipient.get("chat_type") or ("dialog" if not chat_id.startswith("-") else "chat"))
        if native_type != "dialog":
            if self._settings.group_allowed_chats and chat_id not in self._settings.group_allowed_chats:
                logger.info("[max] callback from group %s not in MAX_GROUP_ALLOWED_CHATS", chat_id)
                await self._answer_callback(callback_id, notification="Недостаточно прав.")
                return
        entry = self._callbacks.pop(token)
        if entry is None:
            await self._answer_callback(callback_id, notification="Кнопка устарела.")
            return

        if entry.kind == "approval":
            await self._resolve_approval(entry, action, callback_id)
        elif entry.kind == "clarify":
            await self._resolve_clarify(entry, action, callback_id)
        elif entry.kind == "choice":
            await self._resolve_choice(entry, action, callback_id, chat_id, user_id)
        elif entry.kind == "model_provider":
            await self._resolve_model_provider(entry, action, callback_id)
        elif entry.kind == "model_choice":
            await self._resolve_model_choice(entry, action, callback_id, user_id)
        elif entry.kind == "scope_choice":
            await self._resolve_scope_choice(entry, action, callback_id, user_id)
        else:
            await self._answer_callback(callback_id, notification="Кнопка не распознана.")

    async def _resolve_approval(self, entry: Any, action: str, callback_id: str) -> None:
        from tools.approval import resolve_gateway_approval_card

        try:
            resolved = resolve_gateway_approval_card(entry.session_key or "", action, entry.request_id)
        except Exception as exc:
            logger.warning("[max] approval resolve failed: %s", type(exc).__name__)
            resolved = 0
        if resolved > 0:
            with contextlib.suppress(Exception):
                self.resume_typing_for_chat(entry.chat_id)
            await self._answer_callback(
                callback_id,
                notification="Принято.",
                message={"text": "✅ Решение принято.", "attachments": []},
            )
        else:
            await self._answer_callback(callback_id, notification="Подтверждение уже неактивно.")

    async def _resolve_clarify(self, entry: Any, action: str, callback_id: str) -> None:
        from tools.clarify_gateway import get_pending, mark_awaiting_text, resolve_gateway_clarify

        pending = get_pending(entry.clarify_id or "")
        if pending is None:
            await self._answer_callback(callback_id, notification="Вопрос уже неактивен.")
            return
        if action == "other":
            mark_awaiting_text(entry.clarify_id or "")
            await self._answer_callback(callback_id, notification="Напишите ответ в чат.")
            return
        choices = list(getattr(pending, "choices", None) or [])
        try:
            index = int(action)
        except (TypeError, ValueError):
            await self._answer_callback(callback_id, notification="Некорректный вариант.")
            return
        if index < 0 or index >= len(choices):
            await self._answer_callback(callback_id, notification="Некорректный вариант.")
            return
        value = str(choices[index])
        if resolve_gateway_clarify(entry.clarify_id or "", value):
            await self._answer_callback(
                callback_id,
                notification="Ответ принят.",
                message={"text": f"✅ Ответ: {value}", "attachments": []},
            )
        else:
            await self._answer_callback(callback_id, notification="Вопрос уже неактивен.")

    async def _resolve_choice(
        self, entry: Any, action: str, callback_id: str, chat_id: str, user_id: str = ""
    ) -> None:
        choices = list(entry.choices or [])
        try:
            index = int(action)
            choice = choices[index]
            value = str(choice.get("value") if isinstance(choice, dict) else choice)
        except (ValueError, TypeError, IndexError):
            await self._answer_callback(callback_id, notification="Некорректный вариант.")
            return
        callback = entry.on_choice_selected
        if (
            callback is not None
            and user_id
            and (entry.extra or {}).get("reasoning_picker")
            and self._is_reasoning_value(value)
        ):
            # Second-level menu: scope is chosen inside the card, not by a chat button.
            token = self._callbacks.put(
                "scope_choice",
                chat_id=str(chat_id),
                user_id=str(user_id) or None,
                extra={
                    "apply": callback,
                    "value": value,
                    "label": value,
                    "command": f"/reasoning {value} --global",
                },
            )
            rows = [
                [{"type": "callback", "text": "✅ Только эта сессия", "payload": f"mx:{token}:s"}],
                [{"type": "callback", "text": "🌐 Сделать глобальной", "payload": f"mx:{token}:g"}],
            ]
            await self._answer_callback(
                callback_id,
                message={
                    "text": f"⚙ Reasoning: {value}\n\nКак применить?",
                    "attachments": [self._keyboard(rows)],
                },
            )
            return
        result_text = "Готово."
        if callback is not None:
            try:
                result = callback(chat_id, value)
                if asyncio.iscoroutine(result):
                    result = await result
                if isinstance(result, str) and result.strip():
                    result_text = result.strip()
            except Exception as exc:
                logger.warning("[max] choice callback failed: %s", type(exc).__name__)
                result_text = "Не удалось применить выбор."
        await self._answer_callback(
            callback_id,
            notification=result_text[:120],
            message={"text": result_text, "attachments": []},
        )

    @staticmethod
    def _is_reasoning_value(value: str) -> bool:
        try:
            from hermes_constants import VALID_REASONING_EFFORTS

            levels = {str(level) for level in VALID_REASONING_EFFORTS}
        except Exception:
            levels = {"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}
        return str(value).lower() in levels

    async def _answer_callback(
        self,
        callback_id: str,
        *,
        notification: Optional[str] = None,
        message: Optional[dict[str, Any]] = None,
    ) -> None:
        if not callback_id or self._api is None:
            return
        try:
            await self._api.answer_callback(
                callback_id=callback_id, notification=notification, message=message
            )
        except MaxApiError as exc:
            logger.debug("[max] /answers failed: %s", exc)

    # -- outbound ----------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        if self._api is None:
            return SendResult(success=False, error="MAX adapter is not connected", retryable=True)
        media_paths = list(iter_media_refs(content))
        chunks = render_chunks(content, self._settings.format, self.MAX_MESSAGE_LENGTH)
        uploaded: list[dict[str, Any]] = []
        upload_failures = 0
        for path in media_paths:
            attachment = await self._upload_path(path)
            if attachment is not None:
                uploaded.append(attachment)
            else:
                upload_failures += 1
        if upload_failures:
            logger.warning("[max] %d of %d media uploads failed", upload_failures, len(media_paths))
        if upload_failures and not uploaded and not (content or "").strip():
            return SendResult(
                success=False, error="MAX media upload failed", retryable=True, error_kind="transient"
            )
        if not chunks and not uploaded:
            return SendResult(success=True, message_id=None)
        if not chunks:
            chunks = [""]
        delivered: list[str] = []
        tail_uncertain = False
        for index, chunk in enumerate(chunks):
            await self._limiter.acquire(str(chat_id))
            try:
                result = await self._api.send_message(
                    chat_id=str(chat_id),
                    text=chunk,
                    fmt=self._settings.format if self._settings.format in {"html", "markdown"} else None,
                    attachments=uploaded if index == 0 else None,
                    notify=self._settings.notify,
                )
            except MaxApiError as exc:
                failure = self._classify_send_error(exc)
                if delivered:
                    undelivered = chunks[index:]
                    return self._with_partial_send(
                        failure, undelivered, delivered, tail_certain=not tail_uncertain and not exc.timed_out
                    )
                return failure
            mid = (
                (((result.get("message") or {}).get("body") or {}).get("mid"))
                if isinstance(result.get("message"), dict)
                else None
            )
            if mid:
                delivered.append(str(mid))
            else:
                # A 2xx without an id still reaches the user; the tail is no longer
                # certain-undelivered, so never offer it for blind resumption.
                tail_uncertain = True
        return SendResult(
            success=True,
            message_id=delivered[-1] if delivered else None,
            continuation_message_ids=tuple(delivered[:-1]),
        )

    @staticmethod
    def _with_partial_send(
        result: SendResult,
        undelivered: list[str],
        delivered: list[str],
        *,
        tail_certain: bool = True,
    ) -> SendResult:
        if not delivered:
            return result
        raw = dict(result.raw_response) if isinstance(result.raw_response, dict) else {}
        raw.update(
            {
                "partial_overflow": True,
                "delivered_chunks": len(delivered),
                "total_chunks": len(delivered) + len(undelivered),
                "last_message_id": delivered[-1],
                "continuation_message_ids": tuple(delivered[1:]),
            }
        )
        if tail_certain and undelivered:
            raw["undelivered_chunks"] = tuple(undelivered)
            raw["delivered_message_ids"] = tuple(delivered)
        result.raw_response = raw
        return result

    async def _resume_partial_send(
        self,
        chat_id: str,
        result: SendResult,
        *,
        reply_to: Optional[str],
        metadata: Any,
    ) -> Optional[SendResult]:
        raw = result.raw_response if isinstance(result.raw_response, dict) else {}
        undelivered = list(raw.get("undelivered_chunks") or ())
        if not undelivered or self._api is None:
            return None
        delivered = list(raw.get("delivered_message_ids") or ())
        for index, chunk in enumerate(undelivered):
            await self._limiter.acquire(str(chat_id))
            try:
                sent = await self._api.send_message(
                    chat_id=str(chat_id),
                    text=chunk,
                    fmt=self._settings.format if self._settings.format in {"html", "markdown"} else None,
                    notify=self._settings.notify,
                )
            except MaxApiError as exc:
                failure = self._classify_send_error(exc)
                return self._with_partial_send(
                    failure, undelivered[index:], delivered, tail_certain=failure.retryable
                )
            mid = (
                (((sent.get("message") or {}).get("body") or {}).get("mid"))
                if isinstance(sent.get("message"), dict)
                else None
            )
            if mid:
                delivered.append(str(mid))
        return SendResult(
            success=True,
            message_id=delivered[-1] if delivered else None,
            continuation_message_ids=tuple(delivered[:-1]),
        )

    def _classify_send_error(self, exc: MaxApiError) -> SendResult:
        kind = "transient"
        retryable = bool(exc.retryable)
        if exc.status == 429:
            kind = "rate_limited"
        elif exc.status in (401, 403):
            kind = "forbidden"
            retryable = False
        elif exc.status == 404:
            kind = "not_found"
            retryable = False
        elif exc.code and "too long" in (exc.code or "").lower():
            kind = "too_long"
            retryable = False
        elif exc.status == 400:
            kind = "bad_format"
            retryable = False
        elif exc.timed_out:
            kind = "timeout"
            retryable = False  # delivery is unknown: never resend blindly
        return SendResult(
            success=False,
            error=f"MAX send failed: {exc}",
            retryable=retryable,
            retry_after=exc.retry_after,
            error_kind=kind,
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        if self._api is None:
            return
        try:
            await self._limiter.acquire(str(chat_id))
            await self._api.send_typing(str(chat_id))
        except Exception:
            logger.debug("[max] typing indicator failed", exc_info=False)

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        if self._api is None:
            return SendResult(success=False, error="MAX adapter is not connected", retryable=True)
        chunks = render_chunks(content, self._settings.format, self.MAX_MESSAGE_LENGTH)
        if not chunks:
            chunks = [""]
        text = chunks[0]
        try:
            await self._limiter.acquire(str(chat_id))
            await self._api.edit_message(
                message_id=str(message_id),
                text=text,
                fmt=self._settings.format if self._settings.format in {"html", "markdown"} else None,
            )
        except MaxApiError as exc:
            return self._classify_send_error(exc)
        if len(chunks) == 1:
            return SendResult(success=True, message_id=str(message_id))
        # Editing only the first chunk would silently drop the tail of a long
        # streamed answer, so the remainder goes out as follow-up messages.
        delivered = [str(message_id)]
        for index, chunk in enumerate(chunks[1:], start=1):
            await self._limiter.acquire(str(chat_id))
            try:
                result = await self._api.send_message(
                    chat_id=str(chat_id),
                    text=chunk,
                    fmt=self._settings.format if self._settings.format in {"html", "markdown"} else None,
                    notify=self._settings.notify,
                )
            except MaxApiError as exc:
                failure = self._classify_send_error(exc)
                return self._with_partial_send(
                    failure, chunks[index:], delivered, tail_certain=not exc.timed_out
                )
            message = result.get("message") if isinstance(result.get("message"), dict) else {}
            body = message.get("body") if isinstance(message.get("body"), dict) else {}
            if body.get("mid"):
                delivered.append(str(body["mid"]))
        return SendResult(
            success=True,
            message_id=delivered[-1],
            continuation_message_ids=tuple(delivered[:-1]),
        )

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        if self._api is None:
            return False
        try:
            await self._limiter.acquire(str(chat_id))
            await self._api.delete_message(message_id=str(message_id))
            return True
        except MaxApiError:
            return False

    # -- media -------------------------------------------------------------

    async def _upload_path(self, path: str, kind: Optional[str] = None) -> Optional[dict[str, Any]]:
        file_path = Path(path).expanduser()
        if not file_path.is_file() or self._api is None:
            return None
        try:
            if file_path.stat().st_size > _MAX_OUTBOUND_BYTES:
                logger.warning(
                    "[max] refusing to upload %s: larger than %d MB",
                    file_path.name,
                    _MAX_OUTBOUND_BYTES // (1024 * 1024),
                )
                return None
        except OSError:
            return None
        data = await asyncio.to_thread(file_path.read_bytes)
        guessed_kind = kind or self._kind_for_suffix(file_path.suffix.lower())
        mime = self._mime_for(file_path)
        return await upload_attachment(
            self._api, data=data, kind=guessed_kind, filename=file_path.name, mime=mime
        )

    @staticmethod
    def _kind_for_suffix(suffix: str) -> str:
        if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic"}:
            return "image"
        if suffix in {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}:
            return "video"
        if suffix in {".ogg", ".oga", ".mp3", ".m4a", ".wav", ".opus", ".flac"}:
            return "audio"
        return "file"

    @staticmethod
    def _mime_for(path: Path) -> str:
        import mimetypes

        return mimetypes.guess_type(path.name)[0] or "application/octet-stream"

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        data = await self._download_url(image_url)
        if data is None:
            return await self.send(
                chat_id=chat_id, content=caption or image_url, reply_to=reply_to, metadata=metadata
            )
        attachment = await upload_attachment(
            self._api, data=data, kind="image", filename="image.jpg", mime="image/jpeg"
        )
        if attachment is None:
            return SendResult(success=False, error="MAX image upload failed", retryable=True)
        return await self._send_payload(chat_id, caption or "", attachments=[attachment])

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_media(chat_id, image_path, "image", caption)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_media(chat_id, file_path, "file", caption, file_name=file_name)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_media(chat_id, audio_path, "audio", caption)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_media(chat_id, video_path, "video", caption)

    async def _send_media(
        self,
        chat_id: str,
        path: str,
        kind: str,
        caption: Optional[str],
        *,
        file_name: Optional[str] = None,
    ) -> SendResult:
        file_path = Path(path).expanduser()
        if not file_path.is_file():
            return SendResult(success=False, error="MAX media file not found", retryable=False)
        try:
            if file_path.stat().st_size > _MAX_OUTBOUND_BYTES:
                return SendResult(success=False, error="MAX media file is too large", retryable=False)
        except OSError:
            return SendResult(success=False, error="MAX media file is not readable", retryable=False)
        data = await asyncio.to_thread(file_path.read_bytes)
        attachment = await upload_attachment(
            self._api,
            data=data,
            kind=kind,
            filename=file_name or file_path.name,
            mime=self._mime_for(file_path),
        )
        if attachment is None:
            return SendResult(success=False, error="MAX media upload failed", retryable=True)
        return await self._send_payload(chat_id, caption or "", attachments=[attachment])

    async def _download_url(self, url: str) -> Optional[bytes]:
        if self._http is None:
            return None
        try:
            async with self._http.stream("GET", url, timeout=60.0) as response:
                if response.status_code >= 400:
                    return None
                total = 0
                chunks: list[bytes] = []
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > self._settings.download_max_bytes:
                        return None
                    chunks.append(chunk)
                return b"".join(chunks)
        except httpx.HTTPError:
            return None

    # -- prompts (buttons) -------------------------------------------------

    async def _send_payload(
        self,
        chat_id: str,
        content: str,
        *,
        attachments: Optional[list[dict[str, Any]]] = None,
        raw_text: bool = False,
    ) -> SendResult:
        if self._api is None:
            return SendResult(success=False, error="MAX adapter is not connected", retryable=True)
        if raw_text:
            chunks = [content] if content else [""]
            fmt = None
        else:
            chunks = render_chunks(content, self._settings.format, self.MAX_MESSAGE_LENGTH)
            fmt = self._settings.format if self._settings.format in {"html", "markdown"} else None
            if not chunks:
                chunks = [""]
        delivered: list[str] = []
        for index, chunk in enumerate(chunks):
            await self._limiter.acquire(str(chat_id))
            try:
                result = await self._api.send_message(
                    chat_id=str(chat_id),
                    text=chunk,
                    fmt=fmt,
                    attachments=attachments if index == 0 else None,
                    notify=self._settings.notify,
                )
            except MaxApiError as exc:
                failure = self._classify_send_error(exc)
                if delivered:
                    return self._with_partial_send(failure, chunks[index:], delivered, tail_certain=True)
                return failure
            message = result.get("message") if isinstance(result.get("message"), dict) else {}
            body = message.get("body") if isinstance(message.get("body"), dict) else {}
            if body.get("mid"):
                delivered.append(str(body["mid"]))
        return SendResult(
            success=True,
            message_id=delivered[-1] if delivered else None,
            continuation_message_ids=tuple(delivered[:-1]),
        )

    @staticmethod
    def _keyboard(rows: list[list[dict[str, Any]]]) -> dict[str, Any]:
        return {"type": "inline_keyboard", "payload": {"buttons": rows}}

    async def _send_exec_approval_prompt(self, prompt: ExecApprovalPrompt) -> SendResult:
        token = self._callbacks.put(
            "approval",
            chat_id=prompt.chat_id,
            session_key=prompt.session_key,
            request_id=prompt.request_id,
        )
        rows: list[list[dict[str, Any]]] = []
        row: list[dict[str, Any]] = []
        for label, choice, _style in prompt.actions:
            row.append({"type": "callback", "text": str(label)[:128], "payload": f"mx:{token}:{choice}"})
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        text = (prompt.text or prompt.command or "")[:3500]
        return await self._send_payload(
            prompt.chat_id, text, attachments=[self._keyboard(rows)], raw_text=True
        )

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        if not choices:
            return await self.send(chat_id=chat_id, content=f"❓ {question}", metadata=metadata)
        pending = None
        try:
            from tools.clarify_gateway import get_pending

            pending = get_pending(clarify_id)
        except Exception:
            pending = None
        if pending is not None and getattr(pending, "multi_select", False):
            from tools.clarify_gateway import mark_awaiting_text

            mark_awaiting_text(clarify_id)
            numbered = "\n".join(f"{i}. {choice}" for i, choice in enumerate(choices, start=1))
            return await self.send(
                chat_id=chat_id,
                content=f"❓ {question}\n\n{numbered}\n\nНапишите номера через запятую.",
                metadata=metadata,
            )
        token = self._callbacks.put(
            "clarify", chat_id=chat_id, session_key=session_key, clarify_id=clarify_id
        )
        rows: list[list[dict[str, Any]]] = []
        for index, choice in enumerate(choices):
            label = str(choice)[:128]
            rows.append([{"type": "callback", "text": label, "payload": f"mx:{token}:{index}"}])
        rows.append([{"type": "callback", "text": "✏️ Другой ответ", "payload": f"mx:{token}:other"}])
        numbered = "\n".join(f"{i}. {choice}" for i, choice in enumerate(choices, start=1))
        text = f"❓ {question}\n\n{numbered}"[:3500]
        return await self._send_payload(chat_id, text, attachments=[self._keyboard(rows)], raw_text=True)

    async def send_model_picker(
        self,
        chat_id: str,
        providers: list,
        current_model: str,
        current_provider: str,
        session_key: str,
        on_model_selected,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Paginated provider → model picker, edited in place through ``POST /answers``."""
        if not providers:
            return SendResult(success=False, error="No providers")
        state = {
            "chat_id": str(chat_id),
            "providers": list(providers),
            "current_model": current_model,
            "current_provider": current_provider,
            "session_key": session_key,
            "on_model_selected": on_model_selected,
        }
        text, keyboard = self._provider_card(state, 0)
        return await self._send_payload(chat_id, text, attachments=[keyboard], raw_text=True)

    def _provider_card(self, state: dict[str, Any], page: int) -> tuple[str, dict[str, Any]]:
        providers = list(state.get("providers") or [])
        total_pages = max(1, (len(providers) + _MAX_PROVIDERS_PER_PAGE - 1) // _MAX_PROVIDERS_PER_PAGE)
        page = max(0, min(int(page), total_pages - 1))
        token = self._callbacks.put(
            "model_provider",
            chat_id=state["chat_id"],
            session_key=state.get("session_key"),
            extra={**state, "page": page},
        )
        start = page * _MAX_PROVIDERS_PER_PAGE
        rows: list[list[dict[str, Any]]] = []
        for offset, provider in enumerate(providers[start : start + _MAX_PROVIDERS_PER_PAGE]):
            index = start + offset
            name = str(provider.get("name") or provider.get("slug") or "?")
            count = provider.get("total_models") or len(provider.get("models") or [])
            label = f"{name} ({count})"
            if provider.get("is_current") or str(provider.get("slug")) == str(state.get("current_provider")):
                label = f"✓ {label}"
            rows.append([{"type": "callback", "text": label[:128], "payload": f"mx:{token}:p:{index}"}])
        rows.append(self._pager_row(token, "pp", page, total_pages))
        rows.append([{"type": "callback", "text": "✖ Отмена", "payload": f"mx:{token}:x"}])
        text = (
            f"⚙ Выбор модели\nТекущая: {state.get('current_model') or '—'}\n"
            f"Провайдер: {state.get('current_provider') or '—'}\n\n"
            f"Выберите провайдера ({page + 1}/{total_pages}):"
        )
        return text, self._keyboard(rows)

    def _model_card(self, state: dict[str, Any], page: int) -> tuple[str, dict[str, Any]]:
        models = list(state.get("models") or [])
        total_pages = max(1, (len(models) + _MAX_MODELS_PER_PAGE - 1) // _MAX_MODELS_PER_PAGE)
        page = max(0, min(int(page), total_pages - 1))
        token = self._callbacks.put(
            "model_choice",
            chat_id=state["chat_id"],
            session_key=state.get("session_key"),
            extra={**state, "page": page},
        )
        start = page * _MAX_MODELS_PER_PAGE
        rows: list[list[dict[str, Any]]] = []
        for offset, model in enumerate(models[start : start + _MAX_MODELS_PER_PAGE]):
            rows.append(
                [{"type": "callback", "text": model[:128], "payload": f"mx:{token}:m:{start + offset}"}]
            )
        rows.append(self._pager_row(token, "mp", page, total_pages))
        rows.append([{"type": "callback", "text": "↩ К провайдерам", "payload": f"mx:{token}:bc"}])
        text = (
            f"⚙ {state.get('provider_name') or 'Провайдер'}\n"
            f"Модель (текущая: {state.get('current_model') or '—'}): {page + 1}/{total_pages}"
        )
        return text, self._keyboard(rows)

    @staticmethod
    def _pager_row(token: str, prefix: str, page: int, total_pages: int) -> list[dict[str, Any]]:
        row: list[dict[str, Any]] = []
        if page > 0:
            row.append({"type": "callback", "text": "◀", "payload": f"mx:{token}:{prefix}:{page - 1}"})
        row.append(
            {
                "type": "callback",
                "text": f"{page + 1}/{total_pages}",
                "payload": f"mx:{token}:{prefix}:{page}",
            }
        )
        if page < total_pages - 1:
            row.append({"type": "callback", "text": "▶", "payload": f"mx:{token}:{prefix}:{page + 1}"})
        return row

    async def _resolve_model_provider(self, entry: Any, action: str, callback_id: str) -> None:
        state = dict(entry.extra or {})
        providers = list(state.get("providers") or [])
        if action.startswith("pp:"):
            try:
                page = int(action.split(":", 1)[1])
            except (IndexError, ValueError):
                await self._answer_callback(callback_id, notification="Некорректная страница.")
                return
            text, keyboard = self._provider_card(state, page)
            await self._answer_callback(callback_id, message={"text": text, "attachments": [keyboard]})
            return
        if action == "x":
            await self._answer_callback(
                callback_id, notification="Отменено.", message={"text": "Отменено.", "attachments": []}
            )
            return
        if not action.startswith("p:"):
            await self._answer_callback(callback_id, notification="Некорректный провайдер.")
            return
        try:
            index = int(action.split(":", 1)[1])
        except (IndexError, ValueError):
            await self._answer_callback(callback_id, notification="Некорректный провайдер.")
            return
        if index < 0 or index >= len(providers):
            await self._answer_callback(callback_id, notification="Некорректный провайдер.")
            return
        provider = providers[index]
        models = [str(model) for model in (provider.get("models") or [])]
        if not models:
            await self._answer_callback(callback_id, notification="У провайдера нет моделей.")
            return
        state.update(
            {
                "models": models,
                "provider_slug": str(provider.get("slug") or ""),
                "provider_name": str(provider.get("name") or provider.get("slug") or ""),
                "provider_page": int(state.get("page") or 0),
            }
        )
        text, keyboard = self._model_card(state, 0)
        await self._answer_callback(callback_id, message={"text": text, "attachments": [keyboard]})

    async def _resolve_model_choice(
        self, entry: Any, action: str, callback_id: str, user_id: str = ""
    ) -> None:
        state = dict(entry.extra or {})
        if action.startswith("mp:"):
            try:
                page = int(action.split(":", 1)[1])
            except (IndexError, ValueError):
                await self._answer_callback(callback_id, notification="Некорректная страница.")
                return
            text, keyboard = self._model_card(state, page)
            await self._answer_callback(callback_id, message={"text": text, "attachments": [keyboard]})
            return
        if action == "bc":
            text, keyboard = self._provider_card(state, int(state.get("provider_page") or 0))
            await self._answer_callback(callback_id, message={"text": text, "attachments": [keyboard]})
            return
        try:
            index = int(action.split(":", 1)[1])
        except (IndexError, ValueError):
            await self._answer_callback(callback_id, notification="Некорректная модель.")
            return
        models = list(state.get("models") or [])
        if index < 0 or index >= len(models):
            await self._answer_callback(callback_id, notification="Некорректная модель.")
            return
        model = models[index]
        provider = str(state.get("provider_slug") or "")
        callback = state.get("on_model_selected")
        if callback is None:
            await self._answer_callback(callback_id, notification="Не удалось переключить модель.")
            return

        def _apply(_chat_id: str, _value: str) -> Any:
            return callback(entry.chat_id, model, provider)

        # A model id is interpolated into a slash command; refuse values that could
        # parse as flags or split into another command.
        can_global = bool(user_id) and self._safe_command_value(model)
        token = self._callbacks.put(
            "scope_choice",
            chat_id=entry.chat_id,
            user_id=str(user_id) or None,
            extra={
                "apply": _apply,
                "value": model,
                "label": model,
                "command": f"/model --global {model}" if can_global else "",
            },
        )
        rows = [[{"type": "callback", "text": "✅ Только эта сессия", "payload": f"mx:{token}:s"}]]
        if can_global:
            rows.append([{"type": "callback", "text": "🌐 Сделать глобальной", "payload": f"mx:{token}:g"}])
        await self._answer_callback(
            callback_id,
            message={
                "text": f"⚙ {provider or 'Провайдер'}\nМодель: {model}\n\nКак применить?",
                "attachments": [self._keyboard(rows)],
            },
        )

    async def _resolve_scope_choice(
        self, entry: Any, action: str, callback_id: str, user_id: str = ""
    ) -> None:
        """Apply a pick to this session, or promote it to the global default."""
        state = dict(entry.extra or {})
        apply = state.get("apply")
        command = str(state.get("command") or "")
        label = str(state.get("label") or "")
        if action == "s":
            result_text = "Готово."
            if apply is None:
                await self._answer_callback(callback_id, notification="Не удалось применить.")
                return
            try:
                result = apply(entry.chat_id, label)
                if asyncio.iscoroutine(result):
                    result = await result
                if isinstance(result, str) and result.strip():
                    result_text = result.strip()
            except Exception as exc:
                logger.warning("[max] scope apply failed: %s", type(exc).__name__)
                result_text = "Не удалось применить выбор."
            await self._answer_callback(
                callback_id,
                notification=result_text[:120],
                message={"text": result_text, "attachments": []},
            )
            return
        if action == "g":
            if not command or not user_id or not self._settings.user_allowed(user_id):
                await self._answer_callback(callback_id, notification="Недостаточно прав.")
                return
            await self._answer_callback(
                callback_id,
                notification="Применяю глобально…",
                message={"text": "🌐 Применяю глобально…", "attachments": []},
            )
            await self._dispatch_synthetic_command(entry.chat_id, command, user_id)
            return
        await self._answer_callback(callback_id, notification="Кнопка не распознана.")

    @staticmethod
    def _looks_like_reasoning(title: str) -> bool:
        lowered = str(title or "").lower()
        return "reasoning" in lowered or "рассужд" in lowered

    @staticmethod
    def _safe_command_value(value: str) -> bool:
        text = str(value or "")
        return (
            bool(text) and not text.startswith("-") and not any(ch.isspace() or ord(ch) < 32 for ch in text)
        )

    async def _dispatch_synthetic_command(self, chat_id: str, command: str, user_id: str) -> None:
        """Run a gateway command (e.g. ``/model --global …``) as if the owner typed it."""
        known = self._known_chats.get(str(chat_id)) or {}
        chat_type = known.get("type") or ("group" if str(chat_id).startswith("-") else "dm")
        if (
            chat_type == "group"
            and self._settings.group_allowed_chats
            and str(chat_id) not in self._settings.group_allowed_chats
        ):
            logger.warning("[max] refusing synthetic command in non-allowlisted group %s", chat_id)
            return
        source = self.build_source(
            chat_id=str(chat_id),
            chat_name=known.get("name") or None,
            chat_type=chat_type,
            user_id=str(user_id),
            user_name="",
            message_id=None,
        )
        event = MessageEvent(
            text=command,
            message_type=MessageType.TEXT,
            user_id=str(user_id),
            source=source,
            message_id=f"synthetic:{uuid.uuid4().hex[:8]}",
        )
        await self.handle_message(event)

    async def send_choice_picker(
        self,
        chat_id: str,
        title: str,
        choices: list,
        session_key: str,
        on_choice_selected,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        if not choices:
            return SendResult(success=False, error="No choices")
        token = self._callbacks.put(
            "choice",
            chat_id=chat_id,
            session_key=session_key,
            choices=list(choices),
            on_choice_selected=on_choice_selected,
            extra={"reasoning_picker": self._looks_like_reasoning(title)},
        )
        rows: list[list[dict[str, Any]]] = []
        for index, choice in enumerate(choices):
            label = (
                str(choice.get("label") or choice.get("value") or "")
                if isinstance(choice, dict)
                else str(choice)
            )
            if isinstance(choice, dict) and choice.get("is_current"):
                label = f"✓ {label}"
            rows.append([{"type": "callback", "text": label[:128], "payload": f"mx:{token}:{index}"}])
        return await self._send_payload(
            chat_id, title or "", attachments=[self._keyboard(rows)], raw_text=True
        )

    # -- chat info ---------------------------------------------------------

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        known = self._known_chats.get(str(chat_id))
        if known:
            return {"name": known.get("name") or str(chat_id), "type": known.get("type") or "group"}
        if self._api is not None:
            with contextlib.suppress(Exception):
                chat = await self._api.get_chat(str(chat_id))
                title = str(chat.get("title") or chat.get("name") or "")
                chat_type = "group" if str(chat.get("type") or "").lower() in {"chat", "channel"} else "dm"
                return {"name": title or str(chat_id), "type": chat_type}
        return {"name": str(chat_id), "type": "dm" if not str(chat_id).startswith("-") else "group"}

    def format_message(self, content: str) -> str:
        return content

    # -- small helpers -----------------------------------------------------

    @staticmethod
    def _display_name(user: dict[str, Any]) -> str:
        full = str(user.get("name") or "").strip()
        if full:
            return full
        parts = [str(user.get("first_name") or "").strip(), str(user.get("last_name") or "").strip()]
        joined = " ".join(part for part in parts if part)
        return joined or str(user.get("username") or user.get("user_id") or "")

    def _remember_chat(self, chat_id: str, chat_type: str, title: str) -> None:
        if not chat_id:
            return
        entry = self._known_chats.get(chat_id) or {}
        if title:
            entry["name"] = title
        entry.setdefault("type", chat_type)
        self._known_chats[chat_id] = entry
        # Hard bound: the map is diagnostic/only for titles, never worth unbounded growth.
        while len(self._known_chats) > _MAX_KNOWN_CHATS:
            self._known_chats.pop(next(iter(self._known_chats)), None)

    def _chat_name(self, chat_id: str, sender_name: str, chat_type: str) -> str:
        known = self._known_chats.get(chat_id) or {}
        if known.get("name") and chat_type != "dm":
            return str(known["name"])
        return sender_name or str(chat_id)

    @staticmethod
    def _is_whoami(text: str) -> bool:
        first = text.strip().split()[0].lower() if text.strip() else ""
        return first in {"/whoami", "/whoami@max"} or first.startswith("/whoami@")

    async def _reply_whoami(self, chat_id: str, user_id: str) -> None:
        now = time.monotonic()
        last = self._reveal_last.get(user_id, 0.0)
        if now - last < self._settings.reveal_cooldown:
            return
        self._reveal_last[user_id] = now
        if len(self._reveal_last) > 500:
            self._reveal_last = {
                key: ts for key, ts in self._reveal_last.items() if now - ts < self._settings.reveal_cooldown
            }
        await self._send_payload(
            chat_id,
            f"Ваш MAX user_id: {user_id}\nДобавьте его в MAX_ALLOWED_USERS, чтобы общаться с агентом.",
            raw_text=True,
        )

    @staticmethod
    def _placeholder(attachments: list[Attachment]) -> list[str]:
        labels: list[str] = []
        for attachment in attachments:
            label = _PLACEHOLDERS.get(attachment.kind, "[вложение]")
            if attachment.kind == "file" and attachment.filename:
                label = f"[файл: {attachment.filename}]"
            labels.append(label)
        return labels

    def _is_addressed(self, message: dict[str, Any], text: str) -> bool:
        lowered = (text or "").lower().lstrip()
        if lowered.startswith("/"):
            return True
        names = [self._bot_username, self._bot_name, *self._settings.bot_aliases]
        for name in names:
            candidate = str(name or "").strip().lower()
            if len(candidate) < 3:
                continue
            if f"@{candidate}" in lowered:
                return True
            if re.search(rf"(?<![\w@]){re.escape(candidate)}(?![\w@])", lowered):
                return True
        link = message.get("link") if isinstance(message.get("link"), dict) else {}
        sender = link.get("sender") if isinstance(link.get("sender"), dict) else {}
        if self._bot_id and str(sender.get("user_id") or "") == self._bot_id:
            return True
        return False


# -- plugin registration --------------------------------------------------

_ENV_SEED: tuple[tuple[str, str, Any], ...] = (
    ("MAX_BOT_TOKEN", "token", None),
    ("MAX_ALLOWED_USERS", "allowed_users", None),
    ("MAX_ALLOW_ALL_USERS", "allow_all", None),
    ("MAX_GROUP_ALLOWED_CHATS", "group_allowed_chats", None),
    ("MAX_REQUIRE_MENTION", "require_mention", None),
    ("MAX_BOT_ALIASES", "bot_aliases", None),
    ("MAX_CA_CERT_PATH", "ca_cert_path", None),
    ("MAX_API_BASE", "api_base", None),
    ("MAX_FORMAT", "format", None),
    ("MAX_NOTIFY", "notify", None),
    ("MAX_POLL_TIMEOUT", "poll_timeout", int),
    ("MAX_POLL_LIMIT", "poll_limit", int),
    ("MAX_POLL_PAUSE_SECONDS", "poll_pause", float),
    ("MAX_DEDUP_TTL_SECONDS", "dedup_ttl", float),
    ("MAX_BOOTSTRAP_MAX_AGE_SECONDS", "bootstrap_max_age", int),
    ("MAX_DOWNLOAD_MAX_MB", "download_max_mb", int),
    ("MAX_SEND_RATE_PER_CHAT", "send_rate_per_chat", float),
    ("MAX_GLOBAL_RPS", "global_rps", float),
    ("MAX_HTTP_TIMEOUT", "http_timeout", float),
    ("MAX_BACKOFF_BASE_SECONDS", "backoff_base", float),
    ("MAX_BACKOFF_MAX_SECONDS", "backoff_max", float),
    ("MAX_CALLBACK_TTL_SECONDS", "callback_ttl", float),
    ("MAX_REVEAL_USER_ID_COOLDOWN", "reveal_cooldown", int),
    ("MAX_STATE_PATH", "state_path", None),
)


def check_requirements() -> bool:
    """Passive dependency probe: only whether httpx is importable (it is a core dependency)."""
    try:
        import httpx  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config: Any) -> bool:
    extra = getattr(config, "extra", None) or {}
    token = (
        extra_or_secret(extra, "token", "MAX_BOT_TOKEN", "")
        or getattr(config, "token", None)
        or getattr(config, "api_key", None)
        or ""
    )
    return bool(str(token).strip())


def _env_enablement() -> Optional[dict[str, Any]]:
    if not str(get_scoped_secret("MAX_BOT_TOKEN", "") or "").strip():
        return None
    return seed_extra_from_env(_ENV_SEED, home_env="MAX_HOME_CHANNEL")


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict[str, Any]]:
    return apply_yaml_bridge(
        platform_cfg,
        (
            ("token", "MAX_BOT_TOKEN", "str"),
            ("api_base", "MAX_API_BASE", "str"),
            ("allowed_users", "MAX_ALLOWED_USERS", "csv"),
            ("allow_all", "MAX_ALLOW_ALL_USERS", "lower"),
            ("group_allowed_chats", "MAX_GROUP_ALLOWED_CHATS", "csv"),
            ("ca_cert_path", "MAX_CA_CERT_PATH", "str"),
            ("format", "MAX_FORMAT", "lower"),
            ("require_mention", "MAX_REQUIRE_MENTION", "lower"),
            ("bot_aliases", "MAX_BOT_ALIASES", "csv"),
        ),
    )


_env_configured = env_is_connected("MAX_BOT_TOKEN")


def _is_connected(config: Any) -> bool:
    return _env_configured(config) or validate_config(config)


def register(ctx: Any) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    kwargs: dict[str, Any] = dict(
        name="max",
        label="MAX",
        adapter_factory=MaxAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=_is_connected,
        config_connected_fn=config_connected_extra("token"),
        required_env=["MAX_BOT_TOKEN"],
        install_hint="Set MAX_BOT_TOKEN and install the Russian Trusted Root CA (see README).",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MAX_HOME_CHANNEL",
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=_PLATFORM_HINT,
        emoji="🟠",
        pii_safe=True,
        allow_update_command=True,
        dashboard_description="Connect to MAX (max.ru) through long polling.",
        docs_url="https://github.com/zaharinea/hermes-max-plugin",
        dashboard_env_vars=("MAX_BOT_TOKEN", "MAX_ALLOWED_USERS", "MAX_CA_CERT_PATH"),
    )

    try:
        from .setup import interactive_setup

        kwargs["setup_fn"] = interactive_setup
    except Exception:  # pragma: no cover - setup is optional
        pass
    try:
        from .standalone import standalone_send

        kwargs["standalone_sender_fn"] = standalone_send
    except Exception:  # pragma: no cover - standalone is optional
        pass

    known = {f.name for f in dataclasses.fields(PlatformEntry)}
    for key, value in (
        ("allowlist_suppresses_pairing", True),
        ("apply_yaml_config_fn", _apply_yaml_config),
        ("display_tier", "low"),
    ):
        if key in known:
            kwargs[key] = value

    ctx.register_platform(**kwargs)
