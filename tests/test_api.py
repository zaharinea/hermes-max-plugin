"""MAX API client against ``httpx.MockTransport``."""

from __future__ import annotations

import json

import httpx
import pytest

from api import MaxApi, MaxApiError


def make_api(handler):
    return MaxApi("https://api.test", "raw-token", httpx.AsyncClient(transport=httpx.MockTransport(handler)))


@pytest.mark.asyncio
async def test_get_me_uses_raw_token_header():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"user_id": 1, "username": "bot"})

    api = make_api(handler)
    payload = await api.get_me()
    assert payload["user_id"] == 1
    assert seen["auth"] == "raw-token"
    assert seen["url"] == "https://api.test/me"


@pytest.mark.asyncio
async def test_updates_marker_is_optional():
    urls = []

    def handler(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(200, json={"updates": [], "marker": 7})

    api = make_api(handler)
    await api.get_updates(limit=10, timeout=1)
    await api.get_updates(limit=10, timeout=1, marker=7)
    assert "marker" not in urls[0]
    assert "marker=7" in urls[1]


@pytest.mark.asyncio
async def test_send_message_body_and_query():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"message": {"body": {"mid": "m1"}}})

    api = make_api(handler)
    result = await api.send_message(
        chat_id="333", text="hi", fmt="html", attachments=[{"type": "image"}], notify=False
    )
    assert result["message"]["body"]["mid"] == "m1"
    assert "chat_id=333" in captured["url"]
    assert captured["body"]["text"] == "hi"
    assert captured["body"]["format"] == "html"
    assert captured["body"]["notify"] is False
    assert captured["body"]["attachments"] == [{"type": "image"}]


@pytest.mark.asyncio
async def test_plain_format_omits_format_field():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"message": {"body": {"mid": "m"}}})

    api = make_api(handler)
    await api.send_message(chat_id="1", text="x", fmt=None)
    assert "format" not in captured["body"]


@pytest.mark.asyncio
async def test_answers_payload():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    api = make_api(handler)
    await api.answer_callback(callback_id="cb1", notification="ok", message={"text": "done"})
    assert "callback_id=cb1" in captured["url"]
    assert captured["body"] == {"notification": "ok", "message": {"text": "done"}}


@pytest.mark.asyncio
async def test_error_classification_401_and_429():
    def handler(request: httpx.Request) -> httpx.Response:
        if "updates" in str(request.url):
            return httpx.Response(
                429, headers={"Retry-After": "5"}, json={"code": "rate", "message": "slow down"}
            )
        return httpx.Response(401, json={"code": "verify.token", "message": "bad token"})

    api = make_api(handler)
    with pytest.raises(MaxApiError) as unauthorized:
        await api.get_me()
    assert unauthorized.value.status == 401
    assert unauthorized.value.code == "verify.token"
    assert unauthorized.value.retryable is False

    with pytest.raises(MaxApiError) as limited:
        await api.get_updates(limit=1, timeout=0)
    assert limited.value.status == 429
    assert limited.value.retryable is True
    assert limited.value.retry_after == 5.0


@pytest.mark.asyncio
async def test_upload_file_sends_no_authorization():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"token": "uploaded"})

    api = make_api(handler)
    payload = await api.upload_file("https://cdn.test/upload", b"data", "a.txt", "text/plain")
    assert payload["token"] == "uploaded"
    assert seen["auth"] is None
    assert seen["url"] == "https://cdn.test/upload"


@pytest.mark.asyncio
async def test_set_commands_patch_body():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    api = make_api(handler)
    await api.set_commands([{"name": "status", "description": "Состояние"}])
    assert captured["method"] == "PATCH"
    assert captured["url"].endswith("/me/commands")
    assert captured["body"] == {"commands": [{"name": "status", "description": "Состояние"}]}


@pytest.mark.asyncio
async def test_timeout_is_not_retryable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    api = make_api(handler)
    with pytest.raises(MaxApiError) as exc:
        await api.send_message(chat_id="1", text="x")
    assert exc.value.timed_out is True
    assert exc.value.retryable is False
