"""UI colour theme, chosen at startup.

    APP_THEME  azure (default) | ocean                                            --theme

azure is dark blue with a bright blue accent; ocean is deep sea-blue with a lime-yellow accent. Each theme is a
stylesheet of colour variables in static/themes/, served as /theme.css. The flag wins over the variable; empty
counts as unset.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Mapping

from . import dbconfig

THEMES = ("azure", "ocean")
DEFAULT = THEMES[0]
THEMES_DIR = Path(__file__).resolve().parent / "static" / "themes"

_current = DEFAULT


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--theme", metavar="THEME", help=f"colour theme: {', '.join(THEMES)} (default {DEFAULT}); or set APP_THEME")


def load(args: argparse.Namespace | None = None, env: Mapping[str, str] | None = None) -> str:
    """The theme from the command line and environment. Raises dbconfig.ConfigError if unknown."""
    env = os.environ if env is None else env
    value = (getattr(args, "theme", None) or env.get("APP_THEME", "")).strip().lower()
    if not value:
        return DEFAULT
    if value not in THEMES:
        raise dbconfig.ConfigError(f"APP_THEME '{value}' is not a theme. Use one of {', '.join(THEMES)}.")
    return value


def configure(name: str) -> None:
    global _current
    _current = name


def current() -> str:
    return _current


def stylesheet() -> Path:
    return THEMES_DIR / f"{_current}.css"
