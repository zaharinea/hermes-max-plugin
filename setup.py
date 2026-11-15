"""Minimal ``hermes gateway setup`` wizard step for MAX.

Everything is optional: the adapter is fully configured through ``.env`` and
``config.yaml``. The wizard only saves the token and the home channel.
"""

from __future__ import annotations

from typing import Any


def _ask(prompt: str) -> str:
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        return ""


def interactive_setup(ctx: Any = None) -> bool:
    """Ask for MAX_BOT_TOKEN (and optionally the home chat id) and save them."""
    print("\nMAX (max.ru) — шлюз мессенджера")
    print("Токен бота: business.max.ru → «Чат-боты» → Configure.")
    print("Для platform-api2.max.ru нужен Russian Trusted Root CA (см. README).")

    try:
        from hermes_cli.config import get_env_value, save_env_value
    except Exception:
        print("Не удалось открыть управление .env — задайте MAX_BOT_TOKEN вручную.")
        return False

    current_token = str(get_env_value("MAX_BOT_TOKEN") or "").strip()
    token_prompt = "MAX bot token" + (" [оставить текущий]: " if current_token else ": ")
    token = _ask(token_prompt) or current_token
    if token:
        save_env_value("MAX_BOT_TOKEN", token)

    current_home = str(get_env_value("MAX_HOME_CHANNEL") or "").strip()
    home_prompt = "Home chat_id" + (
        f" [оставить {current_home}]: " if current_home else " (можно пропустить): "
    )
    home = _ask(home_prompt) or current_home
    if home:
        save_env_value("MAX_HOME_CHANNEL", home)

    allow = _ask(
        "Разрешённые user_id через запятую"
        + (" [оставить текущие]: " if str(get_env_value("MAX_ALLOWED_USERS") or "").strip() else ": ")
    )
    if allow:
        save_env_value("MAX_ALLOWED_USERS", allow)

    print("Готово. Проверьте: hermes plugins doctor <путь> --ci, затем hermes gateway restart.")
    return True
