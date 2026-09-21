from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time
from typing import Any

import httpx

from powerbuddy.config import settings


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WattpilotStatus:
    available: bool
    charging: bool
    power_w: float
    stale: bool
    updated_at_epoch: float
    error: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "charger_available": self.available,
            "charger_charging": self.charging,
            "charger_power_w": self.power_w,
            "charger_stale": self.stale,
            "charger_updated_at_epoch": self.updated_at_epoch,
            "charger_error": self.error,
        }


def _normalized_key(value: object) -> str:
    return "".join(char for char in str(value or "").lower() if char.isalnum())


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on", "charging", "charge"}:
        return True
    if normalized in {"0", "false", "no", "off", "idle", "stopped", "disconnected"}:
        return False
    return None


def _extract_status(payload: object) -> tuple[float, bool]:
    power_w: float | None = None
    charging: bool | None = None
    power_keys = {"power", "powerw", "activepower", "chargingpower", "totalpower", "currentpower"}
    charging_keys = {"charging", "ischarging", "chargingenabled", "carstate", "chargestate", "state"}

    def visit(value: object) -> None:
        nonlocal power_w, charging
        if isinstance(value, dict):
            for key, item in value.items():
                normalized_key = _normalized_key(key)
                if normalized_key in power_keys and power_w is None:
                    parsed_power = _as_float(item)
                    if parsed_power is not None:
                        power_w = parsed_power * 1000.0 if 0.0 < parsed_power < 50.0 else parsed_power
                elif normalized_key in charging_keys and charging is None:
                    charging = _as_bool(item)
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(payload)
    resolved_power_w = max(0.0, float(power_w or 0.0))
    return resolved_power_w, bool(charging) or resolved_power_w >= 150.0


class WattpilotClient:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._last_fetch_epoch = 0.0
        self._status = WattpilotStatus(False, False, 0.0, True, 0.0, None)

    def is_enabled(self) -> bool:
        return bool(settings.wattpilot_enabled and settings.wattpilot_status_url.strip())

    async def get_status(self) -> WattpilotStatus:
        now = time.time()
        if not self.is_enabled():
            return WattpilotStatus(False, False, 0.0, True, now, "wattpilot_disabled_or_unconfigured")

        poll_interval = max(5.0, float(settings.wattpilot_poll_interval_seconds))
        max_age = max(poll_interval, float(settings.wattpilot_state_max_age_seconds))
        if now - self._last_fetch_epoch < poll_interval:
            return self._with_staleness(now, max_age)

        async with self._lock:
            now = time.time()
            if now - self._last_fetch_epoch < poll_interval:
                return self._with_staleness(now, max_age)

            try:
                async with httpx.AsyncClient(timeout=max(2.0, float(settings.wattpilot_timeout_seconds))) as client:
                    response = await client.get(settings.wattpilot_status_url.strip())
                    response.raise_for_status()
                    power_w, charging = _extract_status(response.json())
                self._status = WattpilotStatus(True, charging, power_w, False, now, None)
            except Exception as exc:
                logger.warning("Wattpilot status fetch failed: %s", exc)
                previous = self._status
                self._status = WattpilotStatus(
                    previous.available,
                    previous.charging,
                    previous.power_w,
                    True,
                    previous.updated_at_epoch or now,
                    "wattpilot_fetch_failed",
                )
            finally:
                self._last_fetch_epoch = now

            return self._status

    def _with_staleness(self, now: float, max_age: float) -> WattpilotStatus:
        status = self._status
        return WattpilotStatus(
            status.available,
            status.charging,
            status.power_w,
            (now - status.updated_at_epoch) > max_age,
            status.updated_at_epoch,
            status.error,
        )


_client = WattpilotClient()


async def get_wattpilot_status() -> dict[str, Any]:
    return (await _client.get_status()).to_payload()
