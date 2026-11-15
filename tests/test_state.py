"""State journal: tolerant load, atomic save, pruning, TTL."""

from __future__ import annotations

import json
import os
import time

from state import RECENT_LIMIT, MaxState, MaxStateStore


def test_roundtrip(tmp_path):
    path = tmp_path / "state.json"
    store = MaxStateStore(path)
    state = MaxState(marker=42, bot_id="999000111", recent={"mid.1": time.time()})
    store.save_sync(state)

    loaded = MaxStateStore(path).load()
    assert loaded.marker == 42
    assert loaded.bot_id == "999000111"
    assert "mid.1" in loaded.recent


def test_corrupt_file_degrades_to_empty(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json", encoding="utf-8")
    state = MaxStateStore(path).load()
    assert state.marker is None
    assert state.recent == {}


def test_non_object_json_degrades(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")
    assert MaxStateStore(path).load().marker is None


def test_recent_is_pruned(tmp_path):
    now = time.time()
    state = MaxState(recent={f"id-{i}": now for i in range(RECENT_LIMIT + 50)})
    MaxStateStore(tmp_path / "state.json").save_sync(state)
    payload = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert len(payload["recent"]) == RECENT_LIMIT


def test_old_entries_dropped(tmp_path):
    old = time.time() - 48 * 3600
    state = MaxState(recent={"old": old, "fresh": time.time()})
    MaxStateStore(tmp_path / "state.json").save_sync(state)
    loaded = MaxStateStore(tmp_path / "state.json").load()
    assert "old" not in loaded.recent
    assert "fresh" in loaded.recent


def test_file_is_private(tmp_path):
    path = tmp_path / "state.json"
    MaxStateStore(path).save_sync(MaxState(marker=1))
    if os.name == "posix":
        assert (path.stat().st_mode & 0o777) == 0o600
