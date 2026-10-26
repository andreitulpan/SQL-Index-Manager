"""Tracks running jobs so the same one is never started twice.

Shared by the UI (app.py) and the rule runner (rules.py): a rebuild is refused, or skipped by a rule, while
someone else is rebuilding that index.
"""
from __future__ import annotations

import threading

_running: set[tuple] = set()
_lock = threading.Lock()


def claim(key: tuple) -> bool:
    """Marks the job as running; False if it already was."""
    with _lock:
        if key in _running:
            return False
        _running.add(key)
        return True


def release(key: tuple) -> None:
    with _lock:
        _running.discard(key)
