"""Settings for the MAX platform adapter.

Every value is read through the profile-scoped helpers from
``gateway.platforms._shared`` — never ``os.getenv`` — so a secondary gateway
profile sees its own ``.env`` and never the default profile's credentials.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from gateway.platforms._shared import extra_or_secret
from hermes_constants import get_hermes_home

DEFAULT_API_BASE = "https://platform-api2.max.ru"
CA_FILENAME = "russian_trusted_root_ca_pem.crt"

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}
_FORMATS = {"html", "markdown", "plain"}


def _csv(raw: Any) -> frozenset[str]:
    if raw is None:
        return frozenset()
    if isinstance(raw, (list, tuple, set)):
        items = [str(item) for item in raw]
    else:
        items = str(raw).replace(";", ",").split(",")
    return frozenset(item.strip() for item in items if str(item).strip())


def _bool(raw: Any, default: bool) -> bool:
    text = str(raw if raw is not None else "").strip().lower()
    if text in _TRUTHY:
        return True
    if text in _FALSY:
        return False
    return default


def _float(raw: Any, default: float) -> float:
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return default


def _int(raw: Any, default: int) -> int:
    try:
        return int(float(str(raw).strip()))
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def default_state_path() -> Path:
    return get_hermes_home() / "max" / "state.json"


def default_ca_path() -> Path:
    return get_hermes_home() / "max" / "certs" / CA_FILENAME


@dataclass(frozen=True)
class MaxSettings:
    """The adapter's immutable configuration snapshot."""

    token: str = ""
    api_base: str = DEFAULT_API_BASE
    allowed_users: frozenset[str] = frozenset()
    allow_all: bool = False
    group_allowed_chats: frozenset[str] = frozenset()
    require_mention: bool = True
    bot_aliases: tuple[str, ...] = ()
    ca_cert_path: Optional[Path] = None
    format: str = "html"
    notify: bool = True
    poll_timeout: int = 30
    poll_limit: int = 100
    poll_pause: float = 0.5
    dedup_ttl: float = 300.0
    bootstrap_max_age: int = 300
    download_max_mb: int = 100
    per_chat_rps: float = 2.0
    global_rps: float = 30.0
    http_timeout: float = 30.0
    backoff_base: float = 1.0
    backoff_max: float = 60.0
    callback_ttl: float = 3600.0
    reveal_cooldown: int = 3600
    register_commands: bool = True
    debug_updates: bool = False
    state_path: Path = field(default_factory=default_state_path)

    @property
    def download_max_bytes(self) -> int:
        return max(1, self.download_max_mb) * 1024 * 1024

    @property
    def base_url(self) -> str:
        return self.api_base.rstrip("/")

    def user_allowed(self, user_id: str) -> bool:
        if self.allow_all:
            return True
        return "*" in self.allowed_users or str(user_id) in self.allowed_users

    @classmethod
    def from_config(cls, config: Any) -> "MaxSettings":
        extra = getattr(config, "extra", None) or {}

        def get(key: str, env: str, default: Any = "") -> Any:
            return extra_or_secret(extra, key, env, default)

        token = str(
            get("token", "MAX_BOT_TOKEN", "")
            or getattr(config, "token", None)
            or getattr(config, "api_key", None)
            or ""
        ).strip()

        api_base = str(get("api_base", "MAX_API_BASE", DEFAULT_API_BASE) or DEFAULT_API_BASE).strip()

        raw_ca = str(get("ca_cert_path", "MAX_CA_CERT_PATH", "") or "").strip()
        if raw_ca:
            ca_path: Optional[Path] = Path(raw_ca).expanduser()
        else:
            candidate = default_ca_path()
            ca_path = candidate if candidate.exists() else None

        fmt = str(get("format", "MAX_FORMAT", "html") or "html").strip().lower()
        if fmt not in _FORMATS:
            fmt = "html"

        state_raw = str(get("state_path", "MAX_STATE_PATH", "") or "").strip()
        state_path = Path(state_raw).expanduser() if state_raw else default_state_path()

        return cls(
            token=token,
            api_base=api_base,
            allowed_users=_csv(get("allowed_users", "MAX_ALLOWED_USERS", "")),
            allow_all=_bool(get("allow_all", "MAX_ALLOW_ALL_USERS", ""), False),
            group_allowed_chats=_csv(get("group_allowed_chats", "MAX_GROUP_ALLOWED_CHATS", "")),
            require_mention=_bool(get("require_mention", "MAX_REQUIRE_MENTION", ""), True),
            bot_aliases=tuple(sorted(_csv(get("bot_aliases", "MAX_BOT_ALIASES", "")))),
            ca_cert_path=ca_path,
            format=fmt,
            notify=_bool(get("notify", "MAX_NOTIFY", ""), True),
            poll_timeout=int(_clamp(_int(get("poll_timeout", "MAX_POLL_TIMEOUT", ""), 30), 0, 90)),
            poll_limit=int(_clamp(_int(get("poll_limit", "MAX_POLL_LIMIT", ""), 100), 1, 1000)),
            poll_pause=max(0.3, _float(get("poll_pause", "MAX_POLL_PAUSE_SECONDS", ""), 0.5)),
            dedup_ttl=max(1.0, _float(get("dedup_ttl", "MAX_DEDUP_TTL_SECONDS", ""), 300.0)),
            bootstrap_max_age=max(
                0, _int(get("bootstrap_max_age", "MAX_BOOTSTRAP_MAX_AGE_SECONDS", ""), 300)
            ),
            download_max_mb=max(1, _int(get("download_max_mb", "MAX_DOWNLOAD_MAX_MB", ""), 100)),
            per_chat_rps=max(0.1, _float(get("per_chat_rps", "MAX_SEND_RATE_PER_CHAT", ""), 2.0)),
            global_rps=max(0.1, _float(get("global_rps", "MAX_GLOBAL_RPS", ""), 30.0)),
            http_timeout=max(1.0, _float(get("http_timeout", "MAX_HTTP_TIMEOUT", ""), 30.0)),
            backoff_base=max(0.1, _float(get("backoff_base", "MAX_BACKOFF_BASE_SECONDS", ""), 1.0)),
            backoff_max=max(1.0, _float(get("backoff_max", "MAX_BACKOFF_MAX_SECONDS", ""), 60.0)),
            callback_ttl=max(1.0, _float(get("callback_ttl", "MAX_CALLBACK_TTL_SECONDS", ""), 3600.0)),
            reveal_cooldown=max(0, _int(get("reveal_cooldown", "MAX_REVEAL_USER_ID_COOLDOWN", ""), 3600)),
            register_commands=_bool(get("register_commands", "MAX_REGISTER_COMMANDS", ""), True),
            debug_updates=_bool(get("debug_updates", "MAX_DEBUG_UPDATES", ""), False),
            state_path=state_path,
        )
