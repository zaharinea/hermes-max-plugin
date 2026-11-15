"""Shared fixtures for the MAX plugin tests.

The plugin imports ``gateway.*`` from a hermes-agent checkout. Point
``HERMES_REPO`` at one (default: ``~/Develop/github/hermes-agent``); when the
checkout is absent the gateway-dependent tests are skipped.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest

HERMES_REPO = Path(os.environ.get("HERMES_REPO", Path.home() / "Develop" / "github" / "hermes-agent"))
if HERMES_REPO.is_dir() and str(HERMES_REPO) not in sys.path:
    sys.path.insert(0, str(HERMES_REPO))

# Modules that import ``gateway.*`` at module level cannot even be collected
# without a hermes-agent checkout; keep them out of CI instead of failing.
_GATEWAY_TEST_FILES = ("test_adapter.py", "test_registration.py", "test_settings.py", "test_state.py")
collect_ignore = (
    [] if HERMES_REPO.is_dir() else [str(Path(__file__).parent / name) for name in _GATEWAY_TEST_FILES]
)


class FakeConfig:
    """Minimal ``PlatformConfig`` stand-in (only the fields the adapter reads)."""

    def __init__(self, extra: dict[str, Any] | None = None, token: str | None = None):
        self.extra = dict(extra or {})
        self.token = token
        self.api_key = None
        self.home_channel = None


@pytest.fixture
def fake_config(tmp_path):
    def make(**extra):
        values = {
            "token": "test-token",
            "state_path": str(tmp_path / "state.json"),
            "ca_cert_path": "",
        }
        values.update(extra)
        return FakeConfig(extra=values)

    return make


@pytest.fixture
def adapter(fake_config):
    from adapter import MaxAdapter

    instance = MaxAdapter(fake_config())
    instance._api = FakeApi()
    instance._http = None
    instance._bot_id = "999000111"
    instance._bot_username = "example_bot"
    instance._bot_name = "ExampleBot"
    return instance


class FakeApi:
    """Records every API call; responses are configurable per test."""

    def __init__(self):
        self.sent: list[dict[str, Any]] = []
        self.answers: list[dict[str, Any]] = []
        self.edits: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.typing: list[str] = []
        self.updates: list[dict[str, Any]] = []
        self.send_error: Exception | None = None
        self._counter = 0

    async def send_message(self, **kwargs):
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(kwargs)
        self._counter += 1
        return {"message": {"body": {"mid": f"mid-{self._counter}"}}}

    async def edit_message(self, **kwargs):
        self.edits.append(kwargs)
        return {}

    async def delete_message(self, *, message_id: str):
        self.deleted.append(message_id)
        return {}

    async def answer_callback(self, **kwargs):
        self.answers.append(kwargs)
        return {}

    async def send_typing(self, chat_id: str):
        self.typing.append(chat_id)

    async def get_chat(self, chat_id: str):
        return {"title": "Test chat", "type": "chat"}

    async def get_chat_members(self, chat_id: str):
        return {"members": [{"user_id": 999000111, "is_admin": True}]}

    async def get_me(self):
        return {"user_id": 999000111, "username": "example_bot", "first_name": "ExampleBot"}

    async def get_updates(self, **kwargs):
        return {"updates": list(self.updates), "marker": 1}


def make_message_update(
    *,
    text: str = "Привет",
    user_id: int = 111111111,
    chat_id: int = 333333333,
    chat_type: str = "dialog",
    mid: str = "mid.1",
    ts: int = 1794725470905,
) -> dict[str, Any]:
    return {
        "update_type": "message_created",
        "timestamp": ts,
        "message": {
            "recipient": {"chat_id": chat_id, "chat_type": chat_type, "user_id": 999000111},
            "timestamp": ts,
            "body": {"mid": mid, "seq": 1, "text": text},
            "sender": {"user_id": user_id, "first_name": "Test", "last_name": "User", "is_bot": False},
        },
    }


@pytest.fixture
def message_update():
    return make_message_update
