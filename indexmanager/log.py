"""Log lines on stdout, as `docker logs` shows them: 2026-10-19T15:04:34Z INF message key=value."""
import json
import sys
import threading
from datetime import datetime, timezone

_lock = threading.Lock()
_MAX_VALUE = 300

_BANNER = """\
 ####  ###  #         ##### #   # ####  ##### #   #
#     #   # #           #   ##  # #   # #      # #
 ###  # # # #           #   # # # #   # ####    #
    # #  #  #           #   #  ## #   # #      # #
####   ## # #####     ##### #   # ####  ##### #   #

#   #  ###  #   #  ###   #### ##### ####
## ## #   # ##  # #   # #     #     #   #
# # # ##### # # # ##### # ### ####  ####
#   # #   # #  ## #   # #   # #     #  #
#   # #   # #   # #   #  ###  ##### #   #
"""


def _value(value) -> str:
    text = str(value)
    if len(text) > _MAX_VALUE:
        text = text[:_MAX_VALUE] + "..."
    return json.dumps(text) if not text or any(c in text for c in ' ="\r\n\t') else text


def _emit(level: str, message: str, fields: dict) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    extra = "".join(f" {key}={_value(value)}" for key, value in fields.items() if value is not None)
    with _lock:
        print(f"{stamp} {level} {message}{extra}", file=sys.stdout if level == "INF" else sys.stderr, flush=True)


def info(message: str, **fields) -> None:
    _emit("INF", message, fields)


def warning(message: str, **fields) -> None:
    _emit("WRN", message, fields)


def error(message: str, **fields) -> None:
    _emit("ERR", message, fields)


def banner(version: str, commit: str | None) -> None:
    lines = [
        "",
        *_BANNER.splitlines(),
        "",
        f"SQL Index Manager {version}" + (f" ({commit})" if commit else ""),
        "Source: https://github.com/andreitulpan/SQLIndexManager",
        "",
    ]
    with _lock:
        print("\n".join(lines), flush=True)
