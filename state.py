"""Marker and duplicate-suppression journal for the MAX adapter.

The marker is committed only after a whole batch has been handled; each handled
update id is appended to ``recent`` immediately (off the event loop), so a crash
between updates replays at most the update that was in flight. Delivery is
therefore at-least-once, never exactly-once.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from utils import atomic_json_write

logger = logging.getLogger(__name__)

RECENT_LIMIT = 1000
RECENT_TTL_SECONDS = 24 * 3600


def _prune(recent: dict[str, float]) -> dict[str, float]:
    now = time.time()
    fresh = {key: ts for key, ts in recent.items() if now - ts <= RECENT_TTL_SECONDS}
    if len(fresh) > RECENT_LIMIT:
        ordered = sorted(fresh.items(), key=lambda item: item[1])
        fresh = dict(ordered[-RECENT_LIMIT:])
    return fresh


@dataclass
class MaxState:
    marker: Any = None
    bot_id: Optional[str] = None
    recent: dict[str, float] = field(default_factory=dict)


class MaxStateStore:
    """Tolerant reader, atomic writer for ``state.json``."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def load(self) -> MaxState:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return MaxState()
        if not isinstance(raw, dict):
            return MaxState()
        recent: dict[str, float] = {}
        items = raw.get("recent") or []
        if isinstance(items, list):
            for item in items:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    try:
                        recent[str(item[0])] = float(item[1])
                    except (TypeError, ValueError):
                        continue
        marker = raw.get("marker")
        bot_id = str(raw.get("bot_id") or "") or None
        return MaxState(marker=marker, bot_id=bot_id, recent=_prune(recent))

    def save_sync(self, state: MaxState) -> None:
        state.recent = _prune(state.recent)
        payload = {
            "version": 1,
            "bot_id": state.bot_id,
            "marker": state.marker,
            "saved_at": time.time(),
            "recent": [[key, ts] for key, ts in state.recent.items()],
        }
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json_write(self.path, payload, indent=None, mode=0o600)
        except OSError as exc:  # pragma: no cover - disk-level failure
            logger.warning("[max] state write failed: %s", exc)

    async def save(self, state: MaxState) -> None:
        """Persist off the event loop: ``atomic_json_write`` fsyncs."""
        await asyncio.to_thread(self.save_sync, state)
