"""App version shown in the page footer. Raise VERSION when you release."""
import subprocess
from pathlib import Path

VERSION = "1.0.0"


def _commit() -> str | None:
    """Short id of the git commit the code runs from, or None without git (e.g. a Docker image). A trailing "+" means uncommitted changes."""
    here = Path(__file__).resolve().parent

    def git(*args: str) -> str | None:
        try:
            done = subprocess.run(["git", *args], cwd=here, capture_output=True, text=True, timeout=5)
        except (OSError, subprocess.SubprocessError):
            return None
        return done.stdout.strip() if done.returncode == 0 else None

    commit = git("rev-parse", "--short", "HEAD")
    if not commit:
        return None
    return commit + ("+" if git("status", "--porcelain", "--untracked-files=no") else "")


# Read once at startup so the footer shows what is running, not what is on disk.
COMMIT = _commit()
