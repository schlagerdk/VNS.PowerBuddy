import json
import re
from pathlib import Path

from powerbuddy import i18n
from powerbuddy.dashboard_ui import render_dashboard_html

ROOT = Path(__file__).resolve().parents[1] / "src" / "powerbuddy"


def _catalog(code: str) -> dict[str, str]:
    return json.loads((i18n.LOCALES_DIR / f"{code}.json").read_text(encoding="utf-8"))


def test_locales_have_same_keys_and_placeholders():
    base = _catalog("da")
    for code in ("en", "de"):
        other = _catalog(code)
        assert set(other) == set(base), code
        for key, text in base.items():
            assert set(re.findall(r"\{(\w+)\}", text)) == set(re.findall(r"\{(\w+)\}", other[key])), (code, key)


def test_all_used_keys_exist():
    base = _catalog("da")
    sources = [
        ROOT / "static" / "powerbuddy.html",
        ROOT / "static" / "scripts" / "powerbuddy.js",
        ROOT / "main.py",
    ]
    used: set[str] = set()
    for path in sources:
        text = path.read_text(encoding="utf-8")
        used |= set(re.findall(r"\{\{t:([\w.]+)\}\}", text))
        used |= set(re.findall(r"\bpbT\('([\w.]+)'", text))
        used |= set(re.findall(r"\bt\(\"([\w.]+)\"", text))
    assert used - set(base) == set()


def test_normalize_language():
    assert i18n.normalize_language("DK") == "da"
    assert i18n.normalize_language("en") == "en"
    assert i18n.normalize_language("DE") == "de"
    assert i18n.normalize_language("xx") == "da"
    assert i18n.normalize_language(None) == "da"


def test_render_uses_selected_language(monkeypatch):
    monkeypatch.setattr(i18n, "current_language", lambda: "de")
    monkeypatch.setattr("powerbuddy.dashboard_ui.current_language", lambda: "de")
    html = render_dashboard_html(title="T", icon_url="/i.png", favicon_url="/f.ico", apple_touch_icon_url="/a.png")
    assert '<html lang="de">' in html
    assert "{{t:" not in html
    assert "Einstellungen" in html
    assert "window.PB_I18N" in html


def test_persist_env_value_replaces_or_appends(tmp_path, monkeypatch):
    from powerbuddy import main
    from powerbuddy.config import settings

    env_file = tmp_path / ".env"
    env_file.write_bytes(b"A=1\r\nPOWERBUDDY_UI_LANGUAGE=DK\r\nB=2\r\n")
    monkeypatch.setitem(settings.model_config, "env_file", str(env_file))
    main._persist_env_value("POWERBUDDY_UI_LANGUAGE", "DE")
    assert env_file.read_bytes() == b"A=1\r\nPOWERBUDDY_UI_LANGUAGE=DE\r\nB=2\r\n"
    main._persist_env_value("NEW_KEY", "x")
    assert env_file.read_bytes().endswith(b"NEW_KEY=x\r\n")
