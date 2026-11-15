"""Plugin registration and the public platform contract."""

from __future__ import annotations

import dataclasses
import inspect

from gateway.platform_registry import PlatformEntry
from gateway.platforms.base import BasePlatformAdapter

from adapter import MaxAdapter, check_requirements, register, validate_config


class FakeCtx:
    def __init__(self):
        self.kwargs = None

    def register_platform(self, **kwargs):
        self.kwargs = kwargs


def test_register_produces_platform_entry():
    ctx = FakeCtx()
    register(ctx)
    kwargs = ctx.kwargs
    assert kwargs["name"] == "max"
    assert kwargs["label"] == "MAX"
    assert kwargs["adapter_factory"] is MaxAdapter
    assert kwargs["allowed_users_env"] == "MAX_ALLOWED_USERS"
    assert kwargs["allow_all_env"] == "MAX_ALLOW_ALL_USERS"
    assert kwargs["max_message_length"] == 4000
    assert kwargs["cron_deliver_env_var"] == "MAX_HOME_CHANNEL"
    assert kwargs["pii_safe"] is True
    # The group chat allowlist must NOT be granted by the core: it authorizes any
    # member of an allowed chat before the user allowlist (authz_mixin.py).
    assert "group_allowed_chats_env" not in kwargs
    assert all(key in {f.name for f in dataclasses.fields(PlatformEntry)} for key in kwargs)


def test_adapter_implements_abstract_contract():
    assert not inspect.isabstract(MaxAdapter)
    assert issubclass(MaxAdapter, BasePlatformAdapter)
    assert MaxAdapter.MAX_MESSAGE_LENGTH == 4000
    assert MaxAdapter.splits_long_messages is True
    assert MaxAdapter.supports_exec_approval_buttons() is True


def test_passive_checks_do_not_require_a_token(monkeypatch):
    monkeypatch.delenv("MAX_BOT_TOKEN", raising=False)
    assert check_requirements() is True


def test_validate_config_sees_extra_token():
    class Cfg:
        token = None
        api_key = None
        extra = {"token": "from-extra"}

    assert validate_config(Cfg()) is True

    class Empty:
        token = None
        api_key = None
        extra = {}

    assert validate_config(Empty()) is False
