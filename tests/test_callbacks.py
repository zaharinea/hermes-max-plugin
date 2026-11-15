"""Callback registry: single use, TTL, capacity."""

from __future__ import annotations

from callbacks import CallbackRegistry


def test_put_pop_single_use():
    registry = CallbackRegistry()
    token = registry.put("approval", chat_id="1", session_key="s", request_id="r")
    entry = registry.pop(token)
    assert entry is not None and entry.request_id == "r"
    assert registry.pop(token) is None


def test_payload_fields_roundtrip():
    registry = CallbackRegistry()
    choices = [{"value": "a", "label": "A"}]
    token = registry.put(
        "choice", chat_id="1", session_key="s", choices=choices, on_choice_selected=lambda *_: "ok"
    )
    entry = registry.pop(token)
    assert entry.kind == "choice"
    assert entry.choices == choices
    assert entry.user_id is None


def test_capacity_is_bounded():
    registry = CallbackRegistry(max_size=3)
    for _ in range(10):
        registry.put("clarify", chat_id="1", clarify_id="c")
    assert len(registry) <= 3


def test_ttl_expiry(monkeypatch):
    registry = CallbackRegistry(ttl=1.0)
    token = registry.put("approval", chat_id="1")
    monkeypatch.setattr("callbacks.time.monotonic", lambda: 1e9)
    assert registry.pop(token) is None
