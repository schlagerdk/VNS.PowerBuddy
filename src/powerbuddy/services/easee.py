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
class EaseeStatus:
    available: bool
    charging: bool
    power_w: float
    charger_op_mode: str
    stale: bool
    updated_at_epoch: float
    error: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "ev_available": bool(self.available),
            "ev_charging": bool(self.charging),
            "ev_power_w": float(self.power_w),
            "ev_charger_op_mode": self.charger_op_mode,
            "ev_stale": bool(self.stale),
            "ev_updated_at_epoch": float(self.updated_at_epoch),
            "ev_error": self.error,
        }


def _normalize_key(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    return "".join(ch for ch in text if ch.isalnum())


def _as_float(raw: Any, default: float = 0.0) -> float:
    if raw is None:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _as_bool(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    text = str(raw or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _extract_from_state_payload(payload: Any) -> tuple[float, str]:
    # Accept both flat dict payloads and list-based state payloads used by Easee APIs.
    total_power: float | None = None
    op_mode: str | None = None

    def absorb_from_mapping(mapping: dict[str, Any]) -> None:
        nonlocal total_power, op_mode
        for key, value in mapping.items():
            nkey = _normalize_key(key)
            if nkey in {"totalpower", "activepower", "chargerpower", "outputpower"} and total_power is None:
                total_power = _as_float(value, default=0.0)
            elif nkey in {"chargeropmode", "opmode", "mode", "chargermode"} and op_mode is None:
                op_mode = str(value or "")

    if isinstance(payload, dict):
        absorb_from_mapping(payload)
        data = payload.get("data")
        if isinstance(data, dict):
            absorb_from_mapping(data)
        if isinstance(data, list):
            payload = data

    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            key = item.get("key") or item.get("name") or item.get("stateName") or item.get("id")
            nkey = _normalize_key(key)
            value = item.get("value")
            if nkey in {"totalpower", "activepower", "chargerpower", "outputpower"} and total_power is None:
                total_power = _as_float(value, default=0.0)
            elif nkey in {"chargeropmode", "opmode", "mode", "chargermode"} and op_mode is None:
                op_mode = str(value or "")

    return (float(total_power or 0.0), str(op_mode or ""))


class EaseeClient:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._access_token: str = ""
        self._token_expiry_epoch: float = 0.0
        self._resolved_charger_id: str = ""
        self._status = EaseeStatus(
            available=False,
            charging=False,
            power_w=0.0,
            charger_op_mode="",
            stale=True,
            updated_at_epoch=0.0,
            error=None,
        )
        self._last_fetch_epoch: float = 0.0

    def is_enabled(self) -> bool:
        return bool(
            settings.easee_enabled
            and settings.easee_username.strip()
            and settings.easee_password.strip()
        )

    async def get_status(self) -> EaseeStatus:
        if not self.is_enabled():
            return EaseeStatus(
                available=False,
                charging=False,
                power_w=0.0,
                charger_op_mode="",
                stale=True,
                updated_at_epoch=time.time(),
                error="easee_disabled_or_unconfigured",
            )

        now = time.time()
        poll_interval = max(5.0, float(settings.easee_poll_interval_seconds))
        max_age = max(poll_interval, float(settings.easee_state_max_age_seconds))

        cached_age = now - self._last_fetch_epoch
        if self._last_fetch_epoch > 0 and cached_age < poll_interval:
            cached = self._status
            return EaseeStatus(
                available=cached.available,
                charging=cached.charging,
                power_w=cached.power_w,
                charger_op_mode=cached.charger_op_mode,
                stale=(now - cached.updated_at_epoch) > max_age,
                updated_at_epoch=cached.updated_at_epoch,
                error=cached.error,
            )

        async with self._lock:
            now = time.time()
            cached_age = now - self._last_fetch_epoch
            if self._last_fetch_epoch > 0 and cached_age < poll_interval:
                cached = self._status
                return EaseeStatus(
                    available=cached.available,
                    charging=cached.charging,
                    power_w=cached.power_w,
                    charger_op_mode=cached.charger_op_mode,
                    stale=(now - cached.updated_at_epoch) > max_age,
                    updated_at_epoch=cached.updated_at_epoch,
                    error=cached.error,
                )

            try:
                power_w, op_mode = await self._fetch_state()
                mode_norm = _normalize_key(op_mode)
                charging_from_mode = mode_norm in {
                    "charge",
                    "charging",
                    "smartcharging",
                    "awaitingstart",
                    "2",
                    "3",
                }
                charging_from_power = power_w > 100.0
                charging = charging_from_mode or charging_from_power

                self._status = EaseeStatus(
                    available=True,
                    charging=charging,
                    power_w=float(power_w),
                    charger_op_mode=str(op_mode),
                    stale=False,
                    updated_at_epoch=now,
                    error=None,
                )
            except Exception as exc:
                logger.warning("Easee state fetch failed: %s", exc)
                last_good = self._status
                self._status = EaseeStatus(
                    available=last_good.available,
                    charging=last_good.charging,
                    power_w=last_good.power_w,
                    charger_op_mode=last_good.charger_op_mode,
                    stale=True,
                    updated_at_epoch=last_good.updated_at_epoch or now,
                    error="easee_fetch_failed",
                )
            finally:
                self._last_fetch_epoch = now

            return self._status

    async def _fetch_state(self) -> tuple[float, str]:
        charger_id = await self._resolve_charger_id()
        payload = await self._api_get(f"/chargers/{charger_id}/state")
        power_w, op_mode = _extract_from_state_payload(payload)

        # Some account setups return nested/alternate shape.
        if not op_mode and isinstance(payload, dict):
            power_w = _as_float(payload.get("totalPower"), default=power_w)
            op_mode = str(payload.get("chargerOpMode") or "")

        return power_w, op_mode

    async def _resolve_charger_id(self) -> str:
        configured_id = settings.easee_charger_id.strip()
        if configured_id:
            self._resolved_charger_id = configured_id
            return configured_id

        if self._resolved_charger_id:
            return self._resolved_charger_id

        payload = await self._api_get("/chargers")
        charger_id = ""

        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict):
                charger_id = str(first.get("id") or first.get("chargerId") or "").strip()
        elif isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list) and data:
                first = data[0]
                if isinstance(first, dict):
                    charger_id = str(first.get("id") or first.get("chargerId") or "").strip()

        if not charger_id:
            raise RuntimeError("Easee charger ID could not be discovered from /chargers")

        self._resolved_charger_id = charger_id
        logger.info("Easee charger auto-discovered: %s", charger_id)
        return charger_id

    async def _api_get(self, path: str) -> Any:
        token = await self._ensure_token()
        headers = {"Authorization": f"Bearer {token}"}
        base_url = settings.easee_base_url.rstrip("/")
        timeout = max(2.0, float(settings.easee_timeout_seconds))

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(f"{base_url}{path}", headers=headers)
            if response.status_code == 401:
                token = await self._ensure_token(force_refresh=True)
                headers = {"Authorization": f"Bearer {token}"}
                response = await client.get(f"{base_url}{path}", headers=headers)
            response.raise_for_status()
            return response.json()

    async def _ensure_token(self, force_refresh: bool = False) -> str:
        now = time.time()
        if (
            not force_refresh
            and self._access_token
            and now < (self._token_expiry_epoch - 60.0)
        ):
            return self._access_token

        base_url = settings.easee_base_url.rstrip("/")
        timeout = max(2.0, float(settings.easee_timeout_seconds))
        payload = {
            "userName": settings.easee_username.strip(),
            "password": settings.easee_password,
        }

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(f"{base_url}/accounts/login", json=payload)
            response.raise_for_status()
            data = response.json()

        token = str(data.get("accessToken") or data.get("token") or "").strip()
        if not token:
            raise RuntimeError("Easee login response missing access token")

        expires_in = _as_float(data.get("expiresIn"), default=3600.0)
        self._access_token = token
        self._token_expiry_epoch = now + max(300.0, expires_in)
        return token


_client = EaseeClient()


async def get_easee_status() -> dict[str, Any]:
    status = await _client.get_status()
    return status.to_payload()
