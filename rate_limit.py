"""Outgoing rate limiting for MAX.

The API allows about 2 messages/second per chat and 30 requests/second overall.
The limiter paces *all* API calls (sends, edits, answers, typing) so a burst of
typing indicators cannot push a reply over the per-chat budget.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Optional


class RateLimiter:
    """Simple next-slot scheduler: one global slot plus one slot per chat."""

    def __init__(
        self,
        per_chat_rps: float,
        global_rps: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        max_chats: int = 512,
    ):
        self._per_chat_interval = 1.0 / max(0.1, per_chat_rps)
        self._global_interval = 1.0 / max(0.1, global_rps)
        self._clock = clock
        self._sleep = sleep or asyncio.sleep
        self._next_global = 0.0
        self._next_chat: dict[str, float] = {}
        self._max_chats = max_chats
        self._lock = asyncio.Lock()

    def _evict(self, now: float) -> None:
        if len(self._next_chat) <= self._max_chats:
            return
        stale = [key for key, ts in self._next_chat.items() if ts <= now]
        for key in stale:
            self._next_chat.pop(key, None)
        if len(self._next_chat) > self._max_chats:
            ordered = sorted(self._next_chat.items(), key=lambda item: item[1])
            for key, _ in ordered[: len(ordered) - self._max_chats]:
                self._next_chat.pop(key, None)

    async def acquire(self, key: str) -> None:
        """Wait until the next slot for ``key`` (and globally) is available.

        The reservation happens under a lock: without it, concurrent senders read the
        same slot before sleeping and wake up together, bursting past the budget.
        """
        chat_key = str(key or "-")
        async with self._lock:
            now = self._clock()
            wait = max(0.0, self._next_global - now, self._next_chat.get(chat_key, 0.0) - now)
            if wait > 0:
                await self._sleep(wait)
                now = self._clock()
            self._next_global = max(now, self._next_global) + self._global_interval
            self._next_chat[chat_key] = max(now, self._next_chat.get(chat_key, 0.0)) + self._per_chat_interval
            self._evict(now)
