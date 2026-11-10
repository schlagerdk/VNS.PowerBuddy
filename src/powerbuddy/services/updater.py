from __future__ import annotations

import logging
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import time

import httpx

from powerbuddy import __version__
from powerbuddy.config import settings

logger = logging.getLogger(__name__)

UPDATE_REPO = os.environ.get("POWERBUDDY_UPDATE_REPO", "schlagerdk/VNS.PowerBuddy")
SERVICE_NAME = os.environ.get("POWERBUDDY_SERVICE_NAME", "powerbuddy.service")
# Conditional requests (If-None-Match -> 304) do not count against GitHub's anonymous
# rate limit, so releases can be polled often without risking a lockout.
CHECK_INTERVAL_SECONDS = 120
FORCE_MIN_INTERVAL_SECONDS = 10

_cache: dict[str, object] = {
    "checked_at": 0.0, "latest": None, "url": None, "error": None, "etag": None, "blocked_until": 0.0,
}


def parse_version(value: str | None) -> tuple[int, ...]:
    parts = re.findall(r"\d+", (value or "").strip().lstrip("vV").split("-")[0])
    return tuple(int(p) for p in parts)


def is_newer(latest: str | None, current: str) -> bool:
    latest_parts = parse_version(latest)
    return bool(latest_parts) and latest_parts > parse_version(current)


def _state_dir() -> Path:
    return Path(settings.db_path).expanduser().resolve().parent


def _state_file() -> Path:
    return _state_dir() / "update.state"


def _log_file() -> Path:
    return _state_dir() / "update.log"


def read_update_state() -> str:
    try:
        return _state_file().read_text(encoding="utf-8").strip()
    except OSError:
        return ""


async def check_for_update(force: bool = False) -> dict[str, object]:
    now = time.time()
    age = now - float(_cache.get("checked_at") or 0.0)
    blocked = now < float(_cache.get("blocked_until") or 0.0)
    if not blocked and age > (FORCE_MIN_INTERVAL_SECONDS if force else CHECK_INTERVAL_SECONDS):
        _cache["checked_at"] = now
        await _refresh_latest_release(now)

    latest = _cache["latest"] if isinstance(_cache["latest"], str) else None
    return {
        "current": __version__,
        "latest": latest,
        "update_available": is_newer(latest, __version__),
        "release_url": _cache["url"],
        "error": _cache["error"],
        "state": read_update_state(),
    }


async def _refresh_latest_release(now: float) -> None:
    headers = {"Accept": "application/vnd.github+json", "Cache-Control": "no-cache"}
    etag = _cache.get("etag")
    if isinstance(etag, str) and etag and _cache.get("latest"):
        headers["If-None-Match"] = etag
    try:
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
            response = await client.get(f"https://api.github.com/repos/{UPDATE_REPO}/releases/latest")
        if response.status_code == 304:
            _cache["error"] = None
            return
        if response.status_code in {403, 429} and response.headers.get("x-ratelimit-remaining") == "0":
            reset = float(response.headers.get("x-ratelimit-reset") or 0.0)
            _cache["blocked_until"] = max(now + 60.0, min(reset, now + 3600.0))
            logger.warning("Update check rate limited by GitHub; retrying after %.0f s", _cache["blocked_until"] - now)
            _cache["error"] = "rate limited"
            return
        response.raise_for_status()
        data = response.json()
        latest = str(data.get("tag_name") or "").lstrip("vV") or None
        if latest and latest != _cache.get("latest") and is_newer(latest, __version__):
            logger.info("Update available: %s -> %s", __version__, latest)
        _cache.update(latest=latest, url=data.get("html_url"), error=None, etag=response.headers.get("etag"))
    except Exception as exc:
        logger.warning("Update check failed: %s", exc)
        _cache["error"] = str(exc)


def build_update_script(version: str) -> str:
    source = f"https://github.com/{UPDATE_REPO}/archive/refs/tags/v{version}.tar.gz"
    pip = shlex.quote(sys.executable)
    log = shlex.quote(str(_log_file()))
    state = shlex.quote(str(_state_file()))
    return (
        f'echo "$(date -Is) updating to {version}" >> {log}; '
        f"if {pip} -m pip install --upgrade --no-cache-dir {shlex.quote(source)} >> {log} 2>&1; then "
        f"echo done > {state}; systemctl restart {shlex.quote(SERVICE_NAME)}; "
        f"else echo failed > {state}; fi"
    )


def start_update(version: str) -> None:
    if not re.fullmatch(r"\d+(\.\d+)*", version or ""):
        raise ValueError("invalid version")
    _state_dir().mkdir(parents=True, exist_ok=True)
    _state_file().write_text("running\n", encoding="utf-8")
    script = build_update_script(version)
    # systemd-run keeps the installer alive when the service itself is restarted.
    if shutil.which("systemd-run"):
        command = ["systemd-run", "--collect", "--quiet", "/bin/sh", "-c", script]
    else:
        command = ["/bin/sh", "-c", script]
    subprocess.Popen(command, start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info("Started update from %s to %s", __version__, version)
