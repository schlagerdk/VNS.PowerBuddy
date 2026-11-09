from __future__ import annotations

import asyncio

from powerbuddy import __version__
from powerbuddy.dashboard_ui import render_dashboard_html
from powerbuddy.services import updater


def test_version_comparison():
    assert updater.parse_version("v1.0.16") == (1, 0, 16)
    assert updater.is_newer("1.0.17", "1.0.16")
    assert updater.is_newer("v1.1.0", "1.0.16")
    assert not updater.is_newer("1.0.16", "1.0.16")
    assert not updater.is_newer("1.0.9", "1.0.16")
    assert not updater.is_newer(None, "1.0.16")


def test_update_script_installs_tag_and_restarts_only_on_success():
    script = updater.build_update_script("1.0.17")
    assert "archive/refs/tags/v1.0.17.tar.gz" in script
    assert script.index("pip install") < script.index("systemctl restart")
    assert "echo failed" in script.split("else", 1)[1]


def test_start_update_rejects_invalid_version():
    try:
        updater.start_update("1.0.17; rm -rf /")
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_check_for_update_uses_cached_release(monkeypatch):
    monkeypatch.setattr(updater, "_cache", {"checked_at": 9e18, "latest": "99.0.0", "url": "u", "error": None})
    status = asyncio.run(updater.check_for_update())
    assert status["current"] == __version__
    assert status["update_available"] is True


def test_dashboard_renders_running_version():
    html = render_dashboard_html(title="PB", icon_url="/i.png", favicon_url="/f.ico", apple_touch_icon_url="/a.png")
    assert "{{version}}" not in html
    assert f'data-version="{__version__}"' in html


def test_forced_check_refreshes_after_short_throttle(monkeypatch):
    calls = []
    real_client = updater.httpx.AsyncClient

    def handle(request):
        calls.append(request.url.path)
        return updater.httpx.Response(200, json={"tag_name": "v99.0.0", "html_url": "u"})

    monkeypatch.setattr(updater.httpx, "AsyncClient", lambda **kwargs: real_client(
        **kwargs, transport=updater.httpx.MockTransport(handle)
    ))
    clock = {"now": 10_000.0}
    monkeypatch.setattr(updater.time, "time", lambda: clock["now"])
    monkeypatch.setattr(updater, "_cache", {"checked_at": 10_000.0 - 120, "latest": "0.0.1", "url": None, "error": None})

    assert asyncio.run(updater.check_for_update())["latest"] == "0.0.1"
    assert calls == []
    assert asyncio.run(updater.check_for_update(force=True))["latest"] == "99.0.0"
    assert len(calls) == 1
    clock["now"] += 30
    asyncio.run(updater.check_for_update(force=True))
    assert len(calls) == 1
