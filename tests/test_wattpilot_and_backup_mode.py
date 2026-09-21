from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from powerbuddy.main import inverter_realtime
from powerbuddy.services.inverter import RealtimePowerData
from powerbuddy.services.wattpilot import _extract_status


def test_wattpilot_status_extracts_power_and_charging_state():
    power_w, charging = _extract_status({"status": {"activePower": 7.4, "charging": True}})

    assert power_w == 7400.0
    assert charging is True


def test_inverter_realtime_includes_fronius_backup_mode(monkeypatch):
    class Client:
        async def get_realtime(self):
            return RealtimePowerData(
                timestamp=datetime(2026, 9, 21, 10, 0, tzinfo=timezone.utc),
                grid_power_w=0.0,
                load_power_w=1200.0,
                pv_power_w=2500.0,
                battery_power_w=-1300.0,
                battery_soc=80.0,
                is_backup_mode=True,
            )

    async def discover_capacity():
        return None

    monkeypatch.setattr("powerbuddy.main._discover_battery_capacity", discover_capacity)
    monkeypatch.setattr("powerbuddy.main.get_inverter_client", lambda: Client())

    result = asyncio.run(inverter_realtime())

    assert result.is_backup_mode is True
