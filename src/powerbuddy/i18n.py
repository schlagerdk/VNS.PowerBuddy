from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

LOCALES_DIR = Path(__file__).resolve().parent / "locales"
DEFAULT_LANGUAGE = "da"

# Env value (POWERBUDDY_UI_LANGUAGE) -> locale file code. Add new languages here
# together with a matching locales/<code>.json file.
_LANGUAGE_ALIASES = {
    "dk": "da",
    "da": "da",
    "en": "en",
    "gb": "en",
    "de": "de",
}


def normalize_language(value: str | None) -> str:
    code = _LANGUAGE_ALIASES.get((value or "").strip().lower(), DEFAULT_LANGUAGE)
    return code if (LOCALES_DIR / f"{code}.json").exists() else DEFAULT_LANGUAGE


@lru_cache(maxsize=None)
def load_catalog(language: str) -> dict[str, str]:
    path = LOCALES_DIR / f"{language}.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def get_catalog(language: str | None = None) -> dict[str, str]:
    """Return the full catalog for a language, falling back to Danish for missing keys."""
    code = normalize_language(language)
    catalog = dict(load_catalog(DEFAULT_LANGUAGE))
    if code != DEFAULT_LANGUAGE:
        catalog.update(load_catalog(code))
    return catalog


def current_language() -> str:
    from powerbuddy.config import settings

    return normalize_language(settings.ui_language)


def t(key: str, **params: object) -> str:
    text = get_catalog(current_language()).get(key, key)
    for name, value in params.items():
        text = text.replace("{" + name + "}", str(value))
    return text
