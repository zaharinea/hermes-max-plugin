"""Single-use callback tokens for MAX inline keyboards.

A button payload is ``mx:<token>:<action>``. The registry binds the token to the
context the click must be checked against (chat, session, expected user, the
approval request or clarify id) and removes the entry on the first lookup, so a
double click can never resolve the same prompt twice.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class CallbackEntry:
    kind: str
    chat_id: str
    session_key: Optional[str] = None
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    clarify_id: Optional[str] = None
    choices: Optional[list] = None
    on_choice_selected: Optional[Callable] = None
    created_at: float = field(default_factory=time.monotonic)
    extra: dict[str, Any] = field(default_factory=dict)


class CallbackRegistry:
    def __init__(self, ttl: float = 3600.0, max_size: int = 500):
        self._ttl = max(1.0, ttl)
        self._max_size = max(1, max_size)
        self._entries: dict[str, CallbackEntry] = {}

    def _purge(self) -> None:
        now = time.monotonic()
        expired = [token for token, entry in self._entries.items() if now - entry.created_at > self._ttl]
        for token in expired:
            self._entries.pop(token, None)
        if len(self._entries) > self._max_size:
            ordered = sorted(self._entries.items(), key=lambda item: item[1].created_at)
            for token, _ in ordered[: len(ordered) - self._max_size]:
                self._entries.pop(token, None)

    def put(self, kind: str, chat_id: str, **kwargs: Any) -> str:
        self._purge()
        token = secrets.token_hex(4)
        while token in self._entries:
            token = secrets.token_hex(4)
        self._entries[token] = CallbackEntry(kind=kind, chat_id=str(chat_id), **kwargs)
        self._purge()
        return token

    def pop(self, token: str) -> Optional[CallbackEntry]:
        self._purge()
        return self._entries.pop(token, None)

    def peek(self, token: str) -> Optional[CallbackEntry]:
        self._purge()
        return self._entries.get(token)

    def __len__(self) -> int:
        return len(self._entries)
