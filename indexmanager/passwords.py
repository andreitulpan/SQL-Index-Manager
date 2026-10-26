"""Passwords of UI users. Only a salted PBKDF2-HMAC-SHA256 hash is stored, as
"pbkdf2_sha256$<iterations>$<salt>$<hash>" (base64 salt and hash), so the iteration count can be raised later
and old hashes still verify.

Passwords of the accounts used to reach managed databases are different: the app must send them to the
server, so they are stored as typed (see store.py).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets

ITERATIONS = 600_000        # OWASP recommendation for PBKDF2-HMAC-SHA256
_PREFIX = "pbkdf2_sha256"
MIN_LENGTH = 8             # for chosen passwords; generated ones are longer


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _derive(password: str, salt: bytes, iterations: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    return f"{_PREFIX}${ITERATIONS}${_b64(salt)}${_b64(_derive(password, salt, ITERATIONS))}"


def verify_password(password: str, stored: str) -> bool:
    """Whether the password matches the hash. A malformed hash is just no match."""
    try:
        prefix, iterations, salt, expected = stored.split("$")
        if prefix != _PREFIX:
            return False
        count = int(iterations)
        if not 1 <= count <= 10_000_000:
            return False
        return hmac.compare_digest(_derive(password, base64.b64decode(salt), count), base64.b64decode(expected))
    except ValueError:      # wrong part count, bad base64 or bad number
        return False


# Checked for unknown user names so they take as long as a wrong password.
_DUMMY_HASH = hash_password(secrets.token_urlsafe(16))


def verify_nothing(password: str) -> None:
    verify_password(password, _DUMMY_HASH)


def generate_password() -> str:
    """A random URL-safe password (about 128 bits)."""
    return secrets.token_urlsafe(16)
