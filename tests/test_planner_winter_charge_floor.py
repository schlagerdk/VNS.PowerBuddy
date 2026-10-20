from __future__ import annotations

from datetime import date, datetime, timedelta

from powerbuddy.models import PricePoint
from powerbuddy.services.planner import DayPlanner, PlannerInput


def test_winter_charge_floor_includes_global_and_additional_overnight_slots(monkeypatch):
    planner = DayPlanner()
    monkeypatch.setattr(
        planner,
        "resolve_expected_daily_consumption",
        lambda reference_day: (60.0, "test"),
    )

    prices = [100.0 + hour for hour in range(24)]
    prices[6] = 1.0
    prices[7] = 2.0
    prices[12] = 3.0
    prices[18] = 4.0
    prices[2] = 5.0
    prices[3] = 6.0
    points = [
        PricePoint(
            timestamp=datetime(2026, 10, 20) + timedelta(hours=hour),
            area="DK2",
            price_ore_per_kwh=prices[hour],
            currency="DKK",
            source="test",
        )
        for hour in range(24)
    ]

    actions = planner.plan(
        PlannerInput(
            day=date(2026, 10, 20),
            price_points=points,
            start_soc=50.0,
        )
    )
    charge_hours = {action.start_time.hour for action in actions if action.action == "charge"}

    assert {2, 3, 6, 7, 12, 18}.issubset(charge_hours)
