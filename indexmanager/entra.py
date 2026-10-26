"""Microsoft Entra ID sign-in for accounts that use it instead of a SQL login.

Opens the browser sign-in on the server machine and keeps tokens in an encrypted cache (msal-extensions).
One credential per user lives for the process; its AuthenticationRecord is saved in .auth/ so sign-in stays
silent after a restart. azure-identity is imported on first use.
"""
from __future__ import annotations

import hashlib
import struct
import threading
import warnings
from pathlib import Path

TOKEN_CACHE_NAME = "db_tool_msal_cache"
AZURE_SQL_SCOPE = "https://database.windows.net/.default"
SQL_COPT_SS_ACCESS_TOKEN = 1256                  # pyodbc attribute for the access token
SIGN_IN_TIMEOUT_SECONDS = 120
AUTH_RECORD_DIR = Path(__file__).resolve().parent.parent / ".auth"

# Serialized so concurrent first sign-ins don't open two browser windows.
_lock = threading.Lock()
_credentials: dict[str, object] = {}


class SignInError(Exception):
    """Entra ID sign-in failed; the message is safe to show in the UI."""


def _record_path(username: str) -> Path:
    return AUTH_RECORD_DIR / f"{hashlib.sha256(username.lower().encode()).hexdigest()[:16]}.json"


def _credential(username: str):
    """The user's credential. Call with _lock held."""
    credential = _credentials.get(username)
    if credential is not None:
        return credential
    try:
        from azure.identity import AuthenticationRecord, InteractiveBrowserCredential, TokenCachePersistenceOptions
    except ImportError as exc:
        raise SignInError("Microsoft Entra ID sign-in needs the azure-identity and msal-extensions packages "
                          "(pip install -r requirements.txt).") from exc

    path = _record_path(username)
    record = None
    if path.is_file():
        try:
            record = AuthenticationRecord.deserialize(path.read_text(encoding="utf-8"))
        except (ValueError, KeyError):
            record = None                         # unreadable: sign in again and overwrite
    credential = InteractiveBrowserCredential(
        login_hint=username,
        cache_persistence_options=TokenCachePersistenceOptions(name=TOKEN_CACHE_NAME),
        authentication_record=record,
        timeout=SIGN_IN_TIMEOUT_SECONDS,
    )
    if record is None:
        record = credential.authenticate(scopes=[AZURE_SQL_SCOPE])
        AUTH_RECORD_DIR.mkdir(exist_ok=True)
        path.write_text(record.serialize(), encoding="utf-8")
    _credentials[username] = credential
    return credential


def access_token_struct(username: str) -> bytes:
    """An Azure SQL access token in the layout the ODBC driver expects (SQL_COPT_SS_ACCESS_TOKEN)."""
    with _lock, warnings.catch_warnings():
        # azure-identity can't request response_mode='form_post', so MSAL warns every time
        warnings.filterwarnings("ignore", message="response_mode='form_post' is recommended", category=UserWarning)
        try:
            token = _credential(username).get_token(AZURE_SQL_SCOPE).token
        except SignInError:
            raise
        except Exception as exc:
            _credentials.pop(username, None)      # rebuild from the saved record next time
            raise SignInError(f"Sign-in did not complete within {SIGN_IN_TIMEOUT_SECONDS}s, or failed: {exc}") from exc
    raw = token.encode("utf-16-le")
    return struct.pack(f"<I{len(raw)}s", len(raw), raw)
