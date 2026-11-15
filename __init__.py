"""MAX (max.ru) platform plugin entry point.

Keep this module light: the adapter is imported lazily so the plugin stays
cheap to discover. When the directory is imported outside a package context
(some test collectors do that), ``register`` is simply unavailable instead of
failing the whole import.
"""

if __package__:
    from .adapter import register
else:  # pragma: no cover - tooling imports the file as a top-level module
    register = None  # type: ignore[assignment]

__all__ = ["register"]
