"""Settings parsing from config.extra (no process env needed)."""

from __future__ import annotations

from settings import DEFAULT_API_BASE, MaxSettings
from tests.conftest import FakeConfig


def test_defaults_without_extra():
    settings = MaxSettings.from_config(FakeConfig(extra={}, token="tok"))
    assert settings.token == "tok"
    assert settings.api_base == DEFAULT_API_BASE
    assert settings.format == "html"
    assert settings.poll_timeout == 30
    assert settings.poll_limit == 100
    assert settings.per_chat_rps == 2.0
    assert settings.allow_all is False
    assert settings.require_mention is True


def test_parses_csv_and_bools():
    settings = MaxSettings.from_config(
        FakeConfig(
            extra={
                "allowed_users": "111, 222;333",
                "group_allowed_chats": "-100, -200",
                "allow_all": "true",
                "require_mention": "false",
                "bot_aliases": "бот, ассистент",
            }
        )
    )
    assert settings.allowed_users == frozenset({"111", "222", "333"})
    assert settings.group_allowed_chats == frozenset({"-100", "-200"})
    assert settings.allow_all is True
    assert settings.require_mention is False
    assert settings.bot_aliases == ("ассистент", "бот")


def test_clamps_out_of_range_values():
    settings = MaxSettings.from_config(
        FakeConfig(extra={"poll_timeout": "999", "poll_limit": "0", "poll_pause": "0.1"})
    )
    assert settings.poll_timeout == 90
    assert settings.poll_limit == 1
    assert settings.poll_pause == 0.3


def test_invalid_format_falls_back_to_html():
    assert MaxSettings.from_config(FakeConfig(extra={"format": "wat"})).format == "html"
    assert MaxSettings.from_config(FakeConfig(extra={"format": "PLAIN"})).format == "plain"


def test_user_allowed_wildcard_and_list():
    assert MaxSettings(allowed_users=frozenset({"111"})).user_allowed("111")
    assert not MaxSettings(allowed_users=frozenset({"111"})).user_allowed("222")
    assert MaxSettings(allowed_users=frozenset({"*"})).user_allowed("222")
    assert MaxSettings(allow_all=True).user_allowed("222")


def test_token_falls_back_to_platform_token():
    settings = MaxSettings.from_config(FakeConfig(extra={}, token="from-config"))
    assert settings.token == "from-config"


def test_register_commands_flag():
    assert MaxSettings.from_config(FakeConfig(extra={})).register_commands is True
    assert (
        MaxSettings.from_config(FakeConfig(extra={"register_commands": "false"})).register_commands is False
    )


def test_debug_updates_flag():
    assert MaxSettings.from_config(FakeConfig(extra={})).debug_updates is False
    assert MaxSettings.from_config(FakeConfig(extra={"debug_updates": "true"})).debug_updates is True


def test_builtin_transcription_flag():
    assert MaxSettings.from_config(FakeConfig(extra={})).use_builtin_transcription is True
    assert (
        MaxSettings.from_config(
            FakeConfig(extra={"use_builtin_transcription": "false"})
        ).use_builtin_transcription
        is False
    )


def test_sanitize_errors_flag():
    assert MaxSettings.from_config(FakeConfig(extra={})).sanitize_errors is True
    assert MaxSettings.from_config(FakeConfig(extra={"sanitize_errors": "false"})).sanitize_errors is False
