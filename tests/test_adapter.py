"""Adapter behaviour: allowlist, dedup, groups, outbound, buttons."""

from __future__ import annotations

import itertools
import sys
import types

import pytest

from api import MaxApiError

ALLOWED = 111111111
STRANGER = 222222222
CHAT = 333333333


def _capture(adapter):
    events = []

    async def capture(event):
        events.append(event)

    adapter.handle_message = capture  # type: ignore[method-assign]
    return events


def _callback_update(payload, *, user_id=ALLOWED, callback_id="cb-1", mid="mid-9"):
    return {
        "update_type": "message_callback",
        "timestamp": 1794725471000,
        "user": {"user_id": user_id},
        "callback": {"callback_id": callback_id, "payload": payload},
        "message": {
            "recipient": {"chat_id": CHAT, "chat_type": "dialog", "user_id": 999000111},
            "body": {"mid": mid, "text": "card"},
        },
    }


@pytest.mark.asyncio
async def test_dm_from_allowed_user_reaches_agent(adapter, message_update):
    events = _capture(adapter)
    await adapter._handle_message_created(message_update(text="Привет"))
    assert len(events) == 1
    event = events[0]
    assert event.text == "Привет"
    assert event.source.chat_type == "dm"
    assert event.source.user_id == str(ALLOWED)
    assert event.message_id == "mid.1"
    # Hermes' native admission keeps a closed metadata contract: anything except
    # the WhatsApp keys (or an empty dict) refuses the event with invalid_params.
    assert event.metadata == {}


@pytest.mark.asyncio
async def test_dm_from_stranger_is_ignored(adapter, message_update):
    events = _capture(adapter)
    await adapter._handle_message_created(message_update(user_id=STRANGER))
    assert events == []


@pytest.mark.asyncio
async def test_duplicate_mid_processed_once(adapter, message_update):
    events = _capture(adapter)
    update = message_update()
    await adapter._handle_message_created(update)
    await adapter._handle_message_created(update)
    assert len(events) == 1


@pytest.mark.asyncio
async def test_whoami_replies_with_user_id(adapter, message_update):
    events = _capture(adapter)
    await adapter._handle_message_created(message_update(text="/whoami", user_id=STRANGER))
    assert events == []
    assert adapter._api.sent
    assert "222222222" in adapter._api.sent[0]["text"]


@pytest.mark.asyncio
async def test_whoami_rate_limited(adapter, message_update):
    _capture(adapter)
    await adapter._handle_message_created(message_update(text="/whoami", mid="mid-a", user_id=STRANGER))
    await adapter._handle_message_created(message_update(text="/whoami", mid="mid-b", user_id=STRANGER))
    assert len(adapter._api.sent) == 1


@pytest.mark.asyncio
async def test_group_requires_mention(adapter, message_update):
    adapter._settings = type(adapter._settings)(
        **{**adapter._settings.__dict__, "require_mention": True, "allowed_users": frozenset({str(ALLOWED)})}
    )
    events = _capture(adapter)
    await adapter._handle_message_created(message_update(text="просто болтаю", chat_type="chat", mid="g1"))
    assert events == []
    await adapter._handle_message_created(
        message_update(text="@example_bot вопрос", chat_type="chat", mid="g2")
    )
    assert len(events) == 1


@pytest.mark.asyncio
async def test_group_chat_allowlist_is_conjunctive(adapter, message_update):
    adapter._settings = type(adapter._settings)(
        **{**adapter._settings.__dict__, "group_allowed_chats": frozenset({"-100500"})}
    )
    events = _capture(adapter)
    await adapter._handle_message_created(message_update(text="@example_bot hi", chat_type="chat", mid="g3"))
    assert events == []
    await adapter._handle_message_created(
        message_update(text="@example_bot hi", chat_type="chat", mid="g4", chat_id=-100500)
    )
    assert len(events) == 1


@pytest.mark.asyncio
async def test_group_member_outside_allowlist_is_ignored(adapter, message_update):
    events = _capture(adapter)
    await adapter._handle_message_created(
        message_update(text="@example_bot hi", chat_type="chat", user_id=STRANGER, mid="g5")
    )
    assert events == []


@pytest.mark.asyncio
async def test_send_splits_and_tracks_continuations(adapter):
    result = await adapter.send(str(CHAT), "слово " * 2000)
    assert result.success
    assert len(adapter._api.sent) > 1
    assert result.continuation_message_ids
    assert result.message_id == f"mid-{len(adapter._api.sent)}"


@pytest.mark.asyncio
async def test_partial_send_marks_overflow(adapter):
    original = adapter._api.send_message

    async def failing(**kwargs):
        if adapter._api.sent:
            raise MaxApiError(500, "boom", "server error", retryable=True)
        return await original(**kwargs)

    adapter._api.send_message = failing  # type: ignore[assignment]
    result = await adapter.send(str(CHAT), "слово " * 2000)
    assert result.success is False
    assert result.raw_response["partial_overflow"] is True
    assert result.raw_response["delivered_chunks"] == 1


@pytest.mark.asyncio
async def test_typing_indicator(adapter):
    await adapter.send_typing(str(CHAT))
    assert adapter._api.typing == [str(CHAT)]


@pytest.mark.asyncio
async def test_edit_and_delete(adapter):
    edited = await adapter.edit_message(str(CHAT), "mid-1", "new text")
    assert edited.success
    assert adapter._api.edits[0]["message_id"] == "mid-1"
    assert await adapter.delete_message(str(CHAT), "mid-1") is True
    assert adapter._api.deleted == ["mid-1"]


@pytest.mark.asyncio
async def test_approval_callback_resolves_with_request_id(adapter, monkeypatch):
    calls = []
    module = types.ModuleType("tools.approval")
    module.resolve_gateway_approval_card = lambda session, choice, request_id: (
        calls.append((session, choice, request_id)) or 1
    )
    monkeypatch.setitem(sys.modules, "tools.approval", module)

    token = adapter._callbacks.put("approval", chat_id=str(CHAT), session_key="s1", request_id="req-1")
    await adapter._handle_message_callback(_callback_update(f"mx:{token}:once"))
    assert calls == [("s1", "once", "req-1")]
    assert adapter._api.answers
    assert adapter._api.answers[0]["message"]["text"].startswith("✅")

    # a second press with the same token must not resolve anything
    await adapter._handle_message_callback(_callback_update(f"mx:{token}:always", callback_id="cb-2"))
    assert len(calls) == 1
    assert "устарела" in adapter._api.answers[-1]["notification"].lower()


@pytest.mark.asyncio
async def test_button_from_stranger_is_denied(adapter, monkeypatch):
    calls = []
    module = types.ModuleType("tools.approval")
    module.resolve_gateway_approval_card = lambda *a: calls.append(a) or 1
    monkeypatch.setitem(sys.modules, "tools.approval", module)

    token = adapter._callbacks.put("approval", chat_id=str(CHAT), session_key="s1", request_id="req-1")
    await adapter._handle_message_callback(_callback_update(f"mx:{token}:once", user_id=STRANGER))
    assert calls == []
    assert "прав" in adapter._api.answers[-1]["notification"].lower()


@pytest.mark.asyncio
async def test_clarify_callback(adapter, monkeypatch):
    resolved = []
    pending = types.SimpleNamespace(choices=["да", "нет"], multi_select=False)
    module = types.ModuleType("tools.clarify_gateway")
    module.get_pending = lambda clarify_id: pending
    module.mark_awaiting_text = lambda clarify_id: resolved.append(("text", clarify_id))
    module.resolve_gateway_clarify = lambda clarify_id, value: resolved.append((clarify_id, value)) or True
    monkeypatch.setitem(sys.modules, "tools.clarify_gateway", module)

    token = adapter._callbacks.put("clarify", chat_id=str(CHAT), clarify_id="c1", session_key="s1")
    await adapter._handle_message_callback(_callback_update(f"mx:{token}:1"))
    assert resolved == [("c1", "нет")]
    assert "нет" in adapter._api.answers[-1]["message"]["text"]


@pytest.mark.asyncio
async def test_choice_picker_callback(adapter):
    selected = []

    async def on_select(chat_id, value):
        selected.append((chat_id, value))
        return f"Выбрано: {value}"

    result = await adapter.send_choice_picker(
        str(CHAT),
        "Модель",
        [{"value": "a", "label": "A"}, {"value": "b", "label": "B", "is_current": True}],
        "s1",
        on_select,
    )
    assert result.success
    payload = adapter._api.sent[0]["attachments"][0]["payload"]["buttons"]
    token = payload[0][0]["payload"].split(":")[1]
    await adapter._handle_message_callback(_callback_update(f"mx:{token}:1"))
    assert selected == [(str(CHAT), "b")]
    assert "Выбрано: b" in adapter._api.answers[-1]["message"]["text"]


@pytest.mark.asyncio
async def test_builtin_transcription_becomes_text(adapter, message_update):
    from gateway.platforms.event import MessageType

    events = _capture(adapter)
    update = message_update(text="", mid="mid-voice")
    update["message"]["body"]["attachments"] = [
        {"type": "audio", "transcription": "Привет из MAX", "payload": {}}
    ]
    await adapter._handle_message_created(update)
    assert len(events) == 1
    event = events[0]
    assert event.text == "Привет из MAX"
    assert event.message_type == MessageType.AUDIO


@pytest.mark.asyncio
async def test_caption_and_transcription_are_merged(adapter, message_update):
    events = _capture(adapter)
    update = message_update(text="Расшифруй", mid="mid-voice-2")
    update["message"]["body"]["attachments"] = [
        {"type": "audio", "transcription": "Привет из MAX", "payload": {}}
    ]
    await adapter._handle_message_created(update)
    assert "Расшифруй" in events[0].text
    assert "Привет из MAX" in events[0].text


@pytest.mark.asyncio
async def test_model_picker_scope_step(adapter):
    selected = []

    async def on_select(chat_id, model, provider):
        selected.append((chat_id, model, provider))
        return f"Модель: {model}"

    providers = [
        {"slug": "p1", "name": "Provider 1", "models": ["m1", "m2"], "is_current": True},
        {"slug": "p2", "name": "Provider 2", "models": ["m3"], "total_models": 1},
    ]
    counter = itertools.count()

    async def open_scope_card():
        """provider step → model step → scope step; returns (session, global) tokens."""
        await adapter.send_model_picker(str(CHAT), providers, "m1", "p1", "s1", on_select)
        keyboard = adapter._api.sent[-1]["attachments"][0]["payload"]["buttons"]
        provider_token = keyboard[0][0]["payload"].split(":")[1]
        await adapter._handle_message_callback(
            _callback_update(f"mx:{provider_token}:p:0", callback_id=f"cb-p{next(counter)}")
        )
        model_rows = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
        assert [row[0]["text"] for row in model_rows] == ["m1", "m2"]
        model_token = model_rows[1][0]["payload"].split(":")[1]
        await adapter._handle_message_callback(
            _callback_update(f"mx:{model_token}:m:1", callback_id=f"cb-m{next(counter)}")
        )
        scope_rows = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
        assert "сессия" in scope_rows[0][0]["text"].lower()
        assert "глобальной" in scope_rows[1][0]["text"].lower()
        return scope_rows[0][0]["payload"].split(":")[1], scope_rows[1][0]["payload"].split(":")[1]

    session_token, _ = await open_scope_card()
    await adapter._handle_message_callback(_callback_update(f"mx:{session_token}:s", callback_id="cb-s"))
    assert selected == [(str(CHAT), "m2", "p1")]
    assert "Модель: m2" in adapter._api.answers[-1]["message"]["text"]

    _, global_token = await open_scope_card()
    synthetic = []
    adapter.handle_message = lambda event: synthetic.append(event)  # type: ignore[method-assign]
    await adapter._handle_message_callback(_callback_update(f"mx:{global_token}:g", callback_id="cb-g"))
    assert synthetic and synthetic[0].text == "/model --global m2"


@pytest.mark.asyncio
async def test_reasoning_choice_has_scope_step(adapter):
    applied = []

    async def on_select(chat_id, value):
        applied.append(value)
        return f"Reasoning: {value}"

    counter = itertools.count()

    async def open_scope_card():
        await adapter.send_choice_picker(
            str(CHAT), "Reasoning", [{"value": "low"}, {"value": "high"}], "s1", on_select
        )
        keyboard = adapter._api.sent[-1]["attachments"][0]["payload"]["buttons"]
        token = keyboard[1][0]["payload"].split(":")[1]
        await adapter._handle_message_callback(
            _callback_update(f"mx:{token}:1", callback_id=f"cb-r{next(counter)}")
        )
        rows = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
        return rows[0][0]["payload"].split(":")[1], rows[1][0]["payload"].split(":")[1]

    session_token, _ = await open_scope_card()
    await adapter._handle_message_callback(_callback_update(f"mx:{session_token}:s", callback_id="cb-rs"))
    assert applied == ["high"]
    assert "Reasoning: high" in adapter._api.answers[-1]["message"]["text"]

    _, global_token = await open_scope_card()
    synthetic = []
    adapter.handle_message = lambda event: synthetic.append(event)  # type: ignore[method-assign]
    await adapter._handle_message_callback(_callback_update(f"mx:{global_token}:g", callback_id="cb-rg"))
    assert synthetic and synthetic[0].text == "/reasoning high --global"


@pytest.mark.asyncio
async def test_model_picker_paginates_providers_and_models(adapter):
    from adapter import _MAX_MODELS_PER_PAGE, _MAX_PROVIDERS_PER_PAGE

    async def on_select(*_args):
        return "ok"

    providers = [
        {"slug": f"p{i}", "name": f"P{i}", "models": [f"m{i}-{j}" for j in range(45)]} for i in range(15)
    ]
    await adapter.send_model_picker(str(CHAT), providers, "", "p0", "s1", on_select)
    keyboard = adapter._api.sent[-1]["attachments"][0]["payload"]["buttons"]
    assert len(keyboard) == _MAX_PROVIDERS_PER_PAGE + 2  # page of providers + pager + cancel
    next_page = keyboard[_MAX_PROVIDERS_PER_PAGE][-1]
    assert next_page["text"] == "▶"
    provider_token = next_page["payload"].split(":")[1]
    await adapter._handle_message_callback(_callback_update(f"mx:{provider_token}:pp:1", callback_id="cb-pp"))
    rows = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
    assert len(rows) == 5 + 2  # 15 providers on two pages

    await adapter._handle_message_callback(_callback_update(rows[0][0]["payload"], callback_id="cb-p"))
    model_rows = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
    assert len(model_rows) == _MAX_MODELS_PER_PAGE + 2  # page of models + pager + back
    model_next = model_rows[_MAX_MODELS_PER_PAGE][-1]
    assert model_next["text"] == "▶"
    model_token = model_next["payload"].split(":")[1]
    await adapter._handle_message_callback(_callback_update(f"mx:{model_token}:mp:2", callback_id="cb-mp"))
    last_page = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
    assert len(last_page) == 5 + 2  # 45 models on three pages


@pytest.mark.asyncio
async def test_model_picker_back_to_providers(adapter):
    async def on_select(*_args):
        return "ok"

    providers = [{"slug": "p1", "name": "P1", "models": ["m1"]}]
    await adapter.send_model_picker(str(CHAT), providers, "", "p1", "s1", on_select)
    provider_token = adapter._api.sent[-1]["attachments"][0]["payload"]["buttons"][0][0]["payload"].split(
        ":"
    )[1]
    await adapter._handle_message_callback(_callback_update(f"mx:{provider_token}:p:0", callback_id="cb-p"))
    model_rows = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
    back = model_rows[-1][0]
    assert "провайдерам" in back["text"].lower()
    await adapter._handle_message_callback(_callback_update(back["payload"], callback_id="cb-bc"))
    rows = adapter._api.answers[-1]["message"]["attachments"][0]["payload"]["buttons"]
    assert rows[0][0]["text"].startswith("✓")


def test_menu_commands_are_capped_and_valid(adapter):
    rows = adapter._menu_commands()
    assert len(rows) == 20
    assert all(row["name"] and row["description"] for row in rows)
    assert all(len(row["name"]) <= 64 and len(row["description"]) <= 128 for row in rows)
    assert len({row["name"] for row in rows}) == len(rows)
    names = {row["name"] for row in rows}
    assert {"status", "new", "stop", "cron", "commands", "sessions", "steer"} <= names


def test_kind_detection():
    from adapter import MaxAdapter

    assert MaxAdapter._kind_for_suffix(".jpg") == "image"
    assert MaxAdapter._kind_for_suffix(".mp4") == "video"
    assert MaxAdapter._kind_for_suffix(".ogg") == "audio"
    assert MaxAdapter._kind_for_suffix(".pdf") == "file"
