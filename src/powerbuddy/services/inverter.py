from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import hashlib
import re
import secrets
from urllib.parse import urlsplit

import httpx

from powerbuddy.config import settings

logger = logging.getLogger(__name__)

_inverter_client_singleton: InverterClient | None = None


@dataclass(slots=True)
class RealtimePowerData:
    timestamp: datetime
    grid_power_w: float
    load_power_w: float
    pv_power_w: float
    battery_power_w: float
    battery_soc: float


class InverterClient:
    async def get_realtime(self) -> RealtimePowerData:
        raise NotImplementedError

    async def apply_action(self, action: str, charge_power_w: float | None = None) -> bool:
        raise NotImplementedError

    async def get_battery_capacity_kwh(self) -> float | None:
        return None


class FroniusClient(InverterClient):
    def __init__(self, url: str) -> None:
        self.url = url
        self._realtime_cache: RealtimePowerData | None = None
        self._realtime_cache_until: datetime = datetime.min.replace(tzinfo=timezone.utc)
        self._realtime_lock = asyncio.Lock()

    @property
    def _origin(self) -> str:
        parsed = urlsplit(self.url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _realtime_cache_ttl_seconds(self) -> float:
        return max(0.0, float(settings.inverter_realtime_cache_seconds))

    def _is_realtime_cache_valid(self, now_utc: datetime) -> bool:
        return self._realtime_cache is not None and now_utc < self._realtime_cache_until

    async def _fetch_realtime_uncached(self) -> RealtimePowerData:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(self.url)
            response.raise_for_status()
            payload = response.json()

        data = payload.get("Body", {}).get("Data", {})
        site = data.get("Site", {})
        inverters = data.get("Inverters", {})

        battery_soc = 0.0
        if inverters:
            first_inverter = next(iter(inverters.values()))
            battery_soc = float(first_inverter.get("SOC", 0.0) or 0.0)

        grid_power = float(site.get("P_Grid", 0.0) or 0.0)
        load_power = abs(float(site.get("P_Load", 0.0) or 0.0))
        pv_power = max(0.0, float(site.get("P_PV", 0.0) or 0.0))
        battery_power = float(site.get("P_Akku", 0.0) or 0.0)

        return RealtimePowerData(
            timestamp=datetime.now(timezone.utc),
            grid_power_w=grid_power,
            load_power_w=load_power,
            pv_power_w=pv_power,
            battery_power_w=battery_power,
            battery_soc=battery_soc,
        )

    async def get_realtime(self) -> RealtimePowerData:
        ttl = self._realtime_cache_ttl_seconds()
        now_utc = datetime.now(timezone.utc)
        if ttl > 0.0 and self._is_realtime_cache_valid(now_utc):
            return self._realtime_cache  # type: ignore[return-value]

        async with self._realtime_lock:
            now_utc = datetime.now(timezone.utc)
            if ttl > 0.0 and self._is_realtime_cache_valid(now_utc):
                return self._realtime_cache  # type: ignore[return-value]

            realtime = await self._fetch_realtime_uncached()
            if ttl > 0.0:
                self._realtime_cache = realtime
                self._realtime_cache_until = realtime.timestamp + timedelta(seconds=ttl)
            return realtime

    @staticmethod
    def _extract_battery_capacity_kwh(payload: object) -> float | None:
        if not isinstance(payload, (dict, list)):
            return None

        candidates: list[tuple[int, float]] = []

        def _visit(node: object, path: str = "") -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    next_path = f"{path}.{key}" if path else str(key)
                    _visit(value, next_path)
                return
            if isinstance(node, list):
                for idx, value in enumerate(node):
                    next_path = f"{path}[{idx}]"
                    _visit(value, next_path)
                return

            value_f: float | None = None
            if isinstance(node, (int, float)):
                value_f = float(node)
            elif isinstance(node, str):
                stripped = node.strip()
                if not stripped:
                    return
                try:
                    value_f = float(stripped)
                except Exception:
                    return
            if value_f is None or value_f <= 0.0:
                return

            lower_path = path.lower()
            if any(token in lower_path for token in ("soc", "percent", "power", "current", "remaining", "available", "temp")):
                return

            score = 0
            if "capacity" in lower_path:
                score += 4
            if "energyfull" in lower_path or "energy_full" in lower_path:
                score += 5
            if any(token in lower_path for token in ("nominal", "rated", "design", "installed", "usable")):
                score += 2
            if any(token in lower_path for token in ("battery", "bat_", "akku")):
                score += 1
            if score < 4:
                return

            if "kwh" in lower_path:
                value_kwh = value_f
            elif "wh" in lower_path:
                value_kwh = value_f / 1000.0
            else:
                value_kwh = value_f / 1000.0 if value_f > 100.0 else value_f

            if 3.0 <= value_kwh <= 30.0:
                candidates.append((score, float(value_kwh)))

        _visit(payload)
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return float(candidates[0][1])

    async def _get_fronius_json(self, path: str, timeout_seconds: int = 5) -> dict | None:
        try:
            async with httpx.AsyncClient(timeout=max(2, timeout_seconds)) as client:
                response = await client.get(f"{self._origin}{path}")
                response.raise_for_status()
                data = response.json()
                return data if isinstance(data, dict) else None
        except Exception:
            return None

    async def get_battery_capacity_kwh(self) -> float | None:
        # Prefer authenticated Fronius battery config, then fallback to storage realtime endpoint.
        config_payload = await self._fronius_digest_request("GET", "/api/config/batteries")
        capacity = self._extract_battery_capacity_kwh(config_payload)
        if capacity is not None:
            return capacity

        storage_payload = await self._get_fronius_json("/solar_api/v1/GetStorageRealtimeData.fcgi")
        capacity = self._extract_battery_capacity_kwh(storage_payload)
        if capacity is not None:
            return capacity

        storage_payload_cgi = await self._get_fronius_json("/solar_api/v1/GetStorageRealtimeData.cgi")
        capacity = self._extract_battery_capacity_kwh(storage_payload_cgi)
        if capacity is not None:
            return capacity

        return None

    @staticmethod
    def _action_url(action: str) -> str:
        if action == "charge":
            return settings.fronius_charge_url.strip()
        if action in {"auto", "discharge", "discharge_force"}:
            return settings.fronius_discharge_url.strip()
        return settings.fronius_hold_url.strip()

    @staticmethod
    def _action_modbus_writes(action: str) -> list[tuple[int, int]]:
        if action == "charge":
            raw = settings.modbus_charge_writes_json.strip()
        elif action in {"auto", "discharge", "discharge_force"}:
            raw = settings.modbus_discharge_writes_json.strip()
        else:
            raw = settings.modbus_hold_writes_json.strip()

        if not raw:
            return []

        try:
            payload = json.loads(raw)
        except Exception as exc:
            logger.error("Invalid modbus writes JSON for action=%s: %s", action, exc)
            return []

        writes: list[tuple[int, int]] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            address = row.get("address")
            value = row.get("value")
            if address is None or value is None:
                continue
            try:
                writes.append((int(address), int(value)))
            except Exception:
                continue
        return writes

    async def _apply_action_modbus(self, action: str, charge_power_w: float | None = None) -> bool:
        host = settings.modbus_host.strip()
        writes = self._action_modbus_writes(action)
        if not host or not writes:
            return False

        if action == "charge" and charge_power_w is not None:
            scale_w = max(0.001, float(settings.modbus_charge_power_setpoint_scale_w))
            setpoint_address = int(settings.modbus_charge_power_setpoint_address)
            target_raw = int(round(max(0.0, charge_power_w) / scale_w))
            target_raw = max(0, min(65535, target_raw))
            writes = [
                (address, target_raw if address == setpoint_address else value)
                for address, value in writes
            ]

        try:
            from pymodbus.client import ModbusTcpClient
        except Exception as exc:
            logger.error("pymodbus not available for Modbus execution: %s", exc)
            return False

        client = ModbusTcpClient(host=host, port=int(settings.modbus_port), timeout=max(1, settings.fronius_action_timeout_seconds))
        try:
            if not client.connect():
                logger.error("Modbus connect failed to %s:%s", host, settings.modbus_port)
                return False

            for address, value in writes:
                # Configured addresses are documented as 1-based Modbus register numbers.
                result = client.write_register(address=address - 1, value=value, device_id=int(settings.modbus_unit_id))
                if getattr(result, "isError", lambda: True)():
                    logger.error("Modbus write failed action=%s address=%s value=%s", action, address, value)
                    return False

            logger.info("Applied inverter action=%s via Modbus (%s writes)", action, len(writes))
            return True
        except Exception as exc:
            logger.error("Failed applying inverter action=%s via Modbus: %s", action, exc)
            return False
        finally:
            try:
                client.close()
            except Exception:
                pass

    @staticmethod
    def _digest_parts(challenge: str) -> dict[str, str]:
        return {k: v for k, v in re.findall(r'(\w+)="?([^",]+)"?', challenge or "")}

    @staticmethod
    def _fronius_result_ok(result: dict | None) -> bool:
        return (
            isinstance(result, dict)
            and not result.get("errors")
            and not result.get("validationErrors")
            and not result.get("writeFailure")
        )

    async def _fronius_digest_request(self, method: str, path: str, payload: dict | None = None) -> dict | None:
        user = settings.fronius_action_auth_user.strip()
        password = settings.fronius_action_auth_pass
        if not user or not password:
            return None

        url = f"{self._origin}{path}"
        method_u = method.upper()
        timeout = max(2, settings.fronius_action_timeout_seconds)

        async with httpx.AsyncClient(timeout=timeout) as client:
            # Unauthenticated probe to receive x-www-authenticate challenge.
            try:
                challenge_resp = await client.request(method_u, url, json=payload)
            except Exception:
                challenge_resp = None
            if challenge_resp is None:
                logger.error("Fronius digest auth probe failed for path=%s", path)
                return None

            challenge = challenge_resp.headers.get("x-www-authenticate", "")
            parts = self._digest_parts(challenge)
            nonce = parts.get("nonce", "")
            realm = parts.get("realm", "Webinterface area")
            qop = (parts.get("qop", "auth").split(",")[0] or "auth").strip()
            if not nonce:
                logger.error("Fronius digest auth challenge missing nonce for path=%s", path)
                return None

            # Fronius UI uses md5(username:realm:password) as pre-hashed password, then SHA256 digest auth.
            hashed_pw = hashlib.md5(f"{user}:{realm}:{password}".encode()).hexdigest()
            cnonce = secrets.token_hex(8)
            nc = "00000001"
            ha2 = hashlib.sha256(f"{method_u}:{path}".encode()).hexdigest()
            response_hash = hashlib.sha256(
                f"{hashed_pw}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}".encode()
            ).hexdigest()

            auth_header = (
                "Digest "
                f'username="{user}", '
                f'realm="{realm}", '
                f'nonce="{nonce}", '
                f'uri="{path}", '
                f'response="{response_hash}", '
                f"qop={qop}, nc={nc}, "
                f'cnonce="{cnonce}"'
            )

            req_headers = {"Authorization": auth_header}
            if method_u == "GET":
                response = await client.get(url, headers=req_headers)
            else:
                response = await client.request(method_u, url, headers=req_headers, json=payload)
            response.raise_for_status()

            if not response.text:
                return None
            try:
                return response.json()
            except Exception:
                return None

    @staticmethod
    def _sanitize_tou_entry(entry: dict) -> dict:
        weekdays = entry.get("Weekdays") or {}
        timetable = entry.get("TimeTable") or {}
        return {
            "Active": bool(entry.get("Active", False)),
            "Power": int(entry.get("Power", 0) or 0),
            "ScheduleType": str(entry.get("ScheduleType", "CHARGE_MAX") or "CHARGE_MAX"),
            "TimeTable": {
                "Start": str(timetable.get("Start", "00:00") or "00:00"),
                "End": str(timetable.get("End", "23:59") or "23:59"),
            },
            "Weekdays": {
                "Mon": bool(weekdays.get("Mon", True)),
                "Tue": bool(weekdays.get("Tue", True)),
                "Wed": bool(weekdays.get("Wed", True)),
                "Thu": bool(weekdays.get("Thu", True)),
                "Fri": bool(weekdays.get("Fri", True)),
                "Sat": bool(weekdays.get("Sat", True)),
                "Sun": bool(weekdays.get("Sun", True)),
            },
        }

    async def _apply_action_fronius_timeofuse(self, action: str, charge_power_w: float | None = None) -> bool:
        user = settings.fronius_action_auth_user.strip()
        if not user:
            return False

        try:
            # Keep inverter running for all actions; avoid standby so PV/inverter stays active.
            standby_req = 1
            await self._fronius_digest_request(
                "POST",
                "/api/commands/StandbyRequestState",
                {"requestState": standby_req},
            )

            is_charge = action == "charge"
            is_hold = action == "hold"
            is_auto = action == "auto"
            # Keep discharge_force as a backwards-compatible alias.
            is_discharge = action in {"discharge", "discharge_force"}
            max_charge_w = int(max(0.0, float(settings.max_charge_kw) * 1000.0))
            max_discharge_w = max(0, int(settings.force_discharge_power_w))
            force_load_power_w = max_charge_w

            if is_charge and settings.force_load_solar_aware_enabled:
                try:
                    realtime = await self.get_realtime()
                    pv_w = max(0.0, float(realtime.pv_power_w))
                    grid_import_w = max(0.0, float(realtime.grid_power_w))
                    high_solar = pv_w >= float(settings.force_load_high_solar_pv_w_threshold)
                    grid_import_limit_w = max(0.0, float(settings.force_load_grid_import_limit_w))

                    if high_solar and grid_import_w > grid_import_limit_w:
                        reduction = int(round(grid_import_w - grid_import_limit_w))
                        force_load_power_w = max(0, max_charge_w - reduction)
                        logger.info(
                            "Solar-aware force load adjusted from %sW to %sW (pv=%.1fW, grid_import=%.1fW)",
                            max_charge_w,
                            force_load_power_w,
                            pv_w,
                            grid_import_w,
                        )
                except Exception:
                    force_load_power_w = max_charge_w

            # charge/auto/discharge: automatic SoC mode; behavior is controlled by
            # time-dependent battery control rules (CHARGE_MIN / DISCHARGE_MIN).
            # hold: manual SoC mode locked to current SoC.
            em_mode = 0
            em_power = force_load_power_w if is_charge else 0

            allow_external_charge_sources = is_charge or is_auto or is_discharge
            allow_grid_charge = is_charge or is_auto or is_discharge
            soc_mode = "manual" if is_hold else "auto"
            soc_min = int(settings.battery_min_soc)
            soc_max = 100

            if is_hold:
                try:
                    realtime = await self.get_realtime()
                    current_soc = int(round(realtime.battery_soc))
                except Exception:
                    current_soc = int(settings.battery_min_soc)
                soc_min = current_soc
                soc_max = 100

            battery_cfg_result = await self._fronius_digest_request(
                "POST",
                "/api/config/batteries",
                {
                    "HYB_BM_CHARGEFROMAC": allow_external_charge_sources,
                    "HYB_EVU_CHARGEFROMGRID": allow_grid_charge,
                    "HYB_EM_MODE": em_mode,
                    "HYB_EM_POWER": em_power,
                    "BAT_M0_SOC_MODE": soc_mode,
                    "BAT_M0_SOC_MIN": soc_min,
                    "BAT_M0_SOC_MAX": soc_max,
                },
            )
            battery_cfg_ok = self._fronius_result_ok(battery_cfg_result)
            if not battery_cfg_ok:
                logger.warning(
                    "Fronius battery config API returned non-success for action=%s: %s",
                    action,
                    battery_cfg_result,
                )

            cfg = await self._fronius_digest_request("GET", "/api/config/timeofuse")
            if not cfg or not isinstance(cfg, dict):
                return False

            raw_entries = cfg.get("timeofuse")
            if not isinstance(raw_entries, list):
                return False

            entries = [self._sanitize_tou_entry(e) for e in raw_entries if isinstance(e, dict)]
            # PowerBuddy manages charge/discharge limits explicitly.
            allowed_types = {"CHARGE_MIN", "CHARGE_MAX", "DISCHARGE_MIN"}
            compacted: list[dict] = []
            seen_types: set[str] = set()
            for entry in entries:
                schedule_type = str(entry.get("ScheduleType", "") or "")
                if schedule_type not in allowed_types:
                    continue
                if schedule_type in seen_types:
                    continue
                seen_types.add(schedule_type)
                compacted.append(entry)
            entries = compacted

            if not entries:
                entries = [
                    {
                        "Active": False,
                        "Power": 0,
                        "ScheduleType": "CHARGE_MIN",
                        "TimeTable": {"Start": "00:00", "End": "23:59"},
                        "Weekdays": {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True},
                    },
                    {
                        "Active": False,
                        "Power": 0,
                        "ScheduleType": "CHARGE_MAX",
                        "TimeTable": {"Start": "00:00", "End": "23:59"},
                        "Weekdays": {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True},
                    },
                    {
                        "Active": False,
                        "Power": 0,
                        "ScheduleType": "DISCHARGE_MIN",
                        "TimeTable": {"Start": "00:00", "End": "23:59"},
                        "Weekdays": {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True},
                    },
                ]

            for e in entries:
                e["Active"] = False

            if is_charge:
                schedule_type = "CHARGE_MIN"
                power_w = force_load_power_w
                # Keep rule active for full day; scheduler updates this action frequently.
                slot_start = "00:00"
                slot_end = "23:59"

                charge_max = next((e for e in entries if e.get("ScheduleType") == "CHARGE_MAX"), None)
                if charge_max is not None:
                    charge_max["Active"] = False
                    charge_max["Power"] = 0
                    charge_max["TimeTable"] = {"Start": slot_start, "End": slot_end}
                    charge_max["Weekdays"] = {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True}

                target = None
                for e in entries:
                    if e.get("ScheduleType") == schedule_type:
                        target = e
                        break
                if target is None:
                    target = {
                        "Active": False,
                        "Power": 0,
                        "ScheduleType": schedule_type,
                        "TimeTable": {"Start": slot_start, "End": slot_end},
                        "Weekdays": {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True},
                    }
                    entries.append(target)

                target["Active"] = True
                target["Power"] = power_w
                target["TimeTable"] = {"Start": slot_start, "End": slot_end}
                target["Weekdays"] = {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True}

            if is_discharge:
                schedule_type = "DISCHARGE_MIN"
                power_w = max_discharge_w
                slot_start = "00:00"
                slot_end = "23:59"

                target = None
                for e in entries:
                    if e.get("ScheduleType") == schedule_type:
                        target = e
                        break
                if target is None:
                    target = {
                        "Active": False,
                        "Power": 0,
                        "ScheduleType": schedule_type,
                        "TimeTable": {"Start": slot_start, "End": slot_end},
                        "Weekdays": {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True},
                    }
                    entries.append(target)

                target["Active"] = True
                target["Power"] = power_w
                target["TimeTable"] = {"Start": slot_start, "End": slot_end}
                target["Weekdays"] = {"Mon": True, "Tue": True, "Wed": True, "Thu": True, "Fri": True, "Sat": True, "Sun": True}

                # In charge mode we force minimum charge only (no CHARGE_MAX override).

            result = await self._fronius_digest_request("POST", "/api/config/timeofuse", {"timeofuse": entries})
            ok = self._fronius_result_ok(result)
            if ok:
                logger.info("Applied inverter action=%s via Fronius time-of-use API", action)
            else:
                logger.warning("Fronius time-of-use API returned non-success for action=%s: %s", action, result)
            return bool(ok)
        except Exception as exc:
            logger.error("Failed applying inverter action=%s via Fronius time-of-use API: %s", action, exc)
            return False

    async def apply_action(self, action: str, charge_power_w: float | None = None) -> bool:
        method = settings.fronius_action_method.strip().upper() or "POST"
        url = self._action_url(action)
        if not url:
            if await self._apply_action_fronius_timeofuse(action, charge_power_w=charge_power_w):
                return True
            if await self._apply_action_modbus(action, charge_power_w=charge_power_w):
                return True
            logger.warning("Execution requested but no Fronius URL or Modbus mapping configured for action=%s", action)
            return False

        auth = None
        if settings.fronius_action_auth_user:
            auth = (settings.fronius_action_auth_user, settings.fronius_action_auth_pass)

        try:
            async with httpx.AsyncClient(timeout=max(1, settings.fronius_action_timeout_seconds)) as client:
                response = await client.request(method, url, auth=auth)
                response.raise_for_status()
            logger.info("Applied inverter action=%s using %s %s", action, method, url)
            return True
        except Exception as exc:
            logger.error("Failed applying inverter action=%s via %s %s: %s", action, method, url, exc)
            return False


def get_inverter_client() -> InverterClient:
    global _inverter_client_singleton
    if _inverter_client_singleton is not None:
        return _inverter_client_singleton

    if settings.inverter_type.lower() == "fronius":
        if not settings.fronius_url:
            raise ValueError("Missing POWERBUDDY_FRONIUS_URL in environment")
        _inverter_client_singleton = FroniusClient(settings.fronius_url)
        return _inverter_client_singleton
    raise ValueError(f"Unsupported inverter type: {settings.inverter_type}")
