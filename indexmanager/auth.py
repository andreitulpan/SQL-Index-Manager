"""Sign-in for the UI. Users are checked against the app database (store.py).

The first start on an empty database creates the administrator (see announce_admin). A user who signs in with a
password an administrator set must change it first. Sessions are in memory only: a restart signs everyone out.
"""
from __future__ import annotations

import secrets
import threading
import time

from . import store

SESSION_COOKIE = "session"
SESSION_SECONDS = 12 * 3600
MAX_NAME_LENGTH = 64

_sessions: dict[str, tuple[str, float, bool]] = {}   # token -> (username, expires at, must change password)
_lock = threading.Lock()


def clean_username(username: str) -> str | None:
    """The user name with whitespace tidied, or None if it isn't usable."""
    name = " ".join(username.split())
    if not name or len(name) > MAX_NAME_LENGTH or not name.isprintable():
        return None
    return name


def announce_admin() -> None:
    """Creates the administrator if there is none and prints the password once (stdout, so `docker logs` shows it)."""
    password = store.ensure_admin()
    if password is None:
        return
    lines = [
        "An administrator account was created because the database had none.",
        f"    username: {store.ADMIN_USERNAME}",
        f"    password: {password}",
        "This password is shown only this once. Save it now.",
        "You will be asked to choose a new one when you first sign in.",
    ]
    width = max(map(len, lines)) + 4
    print("\n".join(["", "=" * width, *(f"  {line}" for line in lines), "=" * width, ""]), flush=True)


def login(username: str, password: str) -> str | None:
    """A new session token if the credentials are valid, else None."""
    name = clean_username(username)
    user = store.check_credentials(name, password) if name is not None and password else None
    if user is None:
        return None
    now = time.time()
    token = secrets.token_urlsafe(32)
    with _lock:
        for old in [t for t, (_, expires, _) in _sessions.items() if expires < now]:
            del _sessions[old]
        _sessions[token] = (user["username"], now + SESSION_SECONDS, user["must_change_password"])
    return token


def logout(token: str | None) -> None:
    with _lock:
        _sessions.pop(token or "", None)


def session_for(token: str | None) -> tuple[str, bool] | None:
    """(username, must change password) of a live session, or None."""
    with _lock:
        session = _sessions.get(token or "")
        if session is None:
            return None
        if session[1] < time.time():
            del _sessions[token]
            return None
        return session[0], session[2]


def user_for(token: str | None) -> str | None:
    session = session_for(token)
    return session[0] if session else None


def end_sessions(username: str) -> None:
    """Signs the user out everywhere."""
    with _lock:
        for token in [t for t, session in _sessions.items() if session[0] == username]:
            del _sessions[token]


def password_changed(token: str, username: str) -> None:
    """Clears the must-change flag on this session and ends the user's other sessions."""
    with _lock:
        for other in [t for t, session in _sessions.items() if session[0] == username and t != token]:
            del _sessions[other]
        if token in _sessions:
            name, expires, _ = _sessions[token]
            _sessions[token] = (name, expires, False)
