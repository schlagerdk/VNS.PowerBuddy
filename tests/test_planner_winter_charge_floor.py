from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.services.planning_sanity import apply_planning_sanity
from powerbuddy.services.planner import DayPlanner, PlannerInput, is_winter_charge_floor


pytestmark = pytest.mark.usefixtures("isolated_forecasts")


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


def test_winter_charge_floor_survives_sanity_and_redundant_charge_trim(monkeypatch):
    monkeypatch.setattr(settings, "winter_cheap_charge_hours", 4)
    monkeypatch.setattr(settings, "winter_additional_night_charge_hours", 2)
    monkeypatch.setattr(settings, "winter_cheap_charge_night_start_hour", 0)
    monkeypatch.setattr(settings, "winter_cheap_charge_night_end_hour", 5)
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_precheap_arbitrage_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_target_soc_percent", 5)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 0)
    monkeypatch.setattr(settings, "must_charge_window_min_soc_percent", 5)
    monkeypatch.setattr(settings, "reserve_soc_min_percent", 5)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    day = date(2026, 11, 12)
    levels = [100.0 + hour for hour in range(24)]
    for hour, price in ((6, 1), (7, 2), (12, 3), (13, 4), (14, 5), (2, 50), (3, 51)):
        levels[hour] = price
    points = [
        PricePoint(
            timestamp=datetime.combine(day, datetime.min.time()) + timedelta(hours=hour),
            area="DK2", price_ore_per_kwh=price, currency="DKK", source="elprisenligenu",
        )
        for hour, price in enumerate(levels)
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "resolve_expected_daily_consumption", lambda day: (60.0, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_consumption_profile", lambda day: ([1 / 24] * 24, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_pv_profile", lambda day: ([0.0] * 24, "test"))
    actions = planner.plan(PlannerInput(day=day, price_points=points, start_soc=50.0))
    required = {2, 3, 6, 7, 12, 13}
    assert required.issubset({a.start_time.hour for a in actions if a.action == "charge"})

    extra = next(action for action in actions if action.start_time.hour == 14)
    extra.action = "charge"
    extra.charge_power_w = planner.charge_setpoint_w
    extra.reason = "optional charge"
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
            projected_soc=100.0, projected_grid_kwh=0.0,
        )
        for action in actions
    ])

    checked, _ = apply_planning_sanity(planner, day, actions, points, 100.0)
    checked_charges = {a.start_time.hour for a in checked if a.action == "charge"}
    assert required.issubset(checked_charges)
    assert 14 not in checked_charges

    trimmed = planner.trim_redundant_charge_actions(day, checked, 100.0, price_points=points)
    assert required.issubset({a.start_time.hour for a in trimmed if a.action == "charge"})


def test_winter_floor_in_remaining_day_uses_tariff_for_each_local_hour(monkeypatch):
    monkeypatch.setattr(settings, "winter_cheap_charge_hours", 4)
    monkeypatch.setattr(settings, "winter_additional_night_charge_hours", 2)
    day = date(2026, 11, 12)
    midnight = datetime.combine(day, datetime.min.time())
    levels = {8: 1.0, 9: 2.0, 10: 3.0, 11: 4.0, 12: 5.0, 13: 6.0}
    points = [
        PricePoint(
            timestamp=midnight + timedelta(hours=hour), area="DK2",
            price_ore_per_kwh=levels.get(hour, 100.0), currency="DKK", source="test",
        )
        for hour in range(24)
    ]
    tariffs = [0.0] * 24
    tariffs[8] = tariffs[9] = 100.0
    planner = DayPlanner()
    monkeypatch.setattr(planner, "resolve_expected_daily_consumption", lambda day: (60.0, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_consumption_profile", lambda day: ([1 / 24] * 24, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_pv_profile", lambda day: ([0.0] * 24, "test"))
    actions = planner.plan(PlannerInput(
        day=day, price_points=points, start_soc=50.0,
        planning_start_time=midnight + timedelta(hours=8), tariff_ore_per_hour=tariffs,
    ))
    assert {a.start_time.hour for a in actions if is_winter_charge_floor(a)} == {10, 11, 12, 13}


def test_winter_planner_accepts_25_price_slots_with_24_hour_tariffs(monkeypatch):
    monkeypatch.setattr(settings, "timezone", "Europe/Copenhagen")
    day = date(2026, 10, 25)
    first_utc = datetime(2026, 10, 24, 22, tzinfo=timezone.utc)
    points = [
        PricePoint(
            timestamp=(first_utc + timedelta(hours=hour)).astimezone(ZoneInfo(settings.timezone)),
            area="DK2", price_ore_per_kwh=float(hour), currency="DKK", source="test",
        )
        for hour in range(25)
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "resolve_expected_daily_consumption", lambda day: (60.0, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_consumption_profile", lambda day: ([1 / 24] * 24, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_pv_profile", lambda day: ([0.0] * 24, "test"))
    actions = planner.plan(PlannerInput(
        day=day, price_points=points, start_soc=50.0, tariff_ore_per_hour=[0.0] * 24,
    ))
    assert len(actions) == 25


def test_sanity_preserves_provisional_winter_floor_slots(monkeypatch):
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_precheap_arbitrage_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 0)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    day = date(2026, 11, 12)
    midnight = datetime.combine(day, datetime.min.time())
    prices = [
        PricePoint(
            timestamp=midnight + timedelta(hours=hour), area="DK2",
            price_ore_per_kwh=float(hour), currency="DKK", source="provisional",
        )
        for hour in range(6)
    ]
    actions = [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1), action="charge",
            charge_power_w=1000.0, reason="provisional fallback: winter cheap-hour charging floor",
            is_manual_override=False,
        )
        for point in prices
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
            projected_soc=50.0, projected_grid_kwh=1.0,
        )
        for action in actions
    ])
    checked, _ = apply_planning_sanity(planner, day, actions, prices, 50.0)
    assert all(action.action == "charge" for action in checked)


def test_sanity_does_not_change_manual_target_soc(monkeypatch):
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_precheap_arbitrage_enabled", False)
    day = date(2026, 11, 12)
    start = datetime.combine(day, datetime.min.time())
    actions = [PlanAction(
        date_key=day.isoformat(), start_time=start, end_time=start + timedelta(hours=1),
        action="auto", target_soc=42.0, is_manual_override=True,
    )]
    prices = [PricePoint(
        timestamp=start, area="DK2", price_ore_per_kwh=20.0, currency="DKK", source="elprisenligenu",
    )]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=actions[0].start_time, action=actions[0].action,
            projected_soc=75.0, projected_grid_kwh=1.0,
        )
    ])
    checked, _ = apply_planning_sanity(planner, day, actions, prices, 75.0)
    assert checked[0].target_soc == 42.0


def test_summer_sanity_keeps_existing_two_charge_cap(monkeypatch):
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 0)
    day = date(2026, 7, 12)
    start = datetime.combine(day, datetime.min.time())
    prices = [
        PricePoint(
            timestamp=start + timedelta(hours=hour), area="DK2", price_ore_per_kwh=float(hour),
            currency="DKK", source="elprisenligenu",
        )
        for hour in range(10, 13)
    ]
    actions = [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1), action="charge",
            charge_power_w=1000.0, reason="optional charge", is_manual_override=False,
        )
        for point in prices
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
            projected_soc=50.0, projected_grid_kwh=1.0,
        )
        for action in actions
    ])
    checked, _ = apply_planning_sanity(planner, day, actions, prices, 50.0)
    assert {a.start_time.hour for a in checked if a.action == "charge"} == {10, 11}


def test_trim_preserves_manual_hold_in_night_and_after_charge(monkeypatch):
    day = date(2026, 11, 12)
    start = datetime.combine(day, datetime.min.time())
    actions = [
        PlanAction(
            date_key=day.isoformat(), start_time=start, end_time=start + timedelta(hours=1),
            action="charge", charge_power_w=1000.0, is_manual_override=False,
        ),
        PlanAction(
            date_key=day.isoformat(), start_time=start + timedelta(hours=1),
            end_time=start + timedelta(hours=2), action="hold", target_soc=42.0,
            is_manual_override=True,
        ),
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
            projected_soc=50.0, projected_grid_kwh=1.0,
        )
        for action in actions
    ])
    trimmed = planner.trim_redundant_charge_actions(day, actions, 50.0)
    assert trimmed[1].action == "hold"
    assert trimmed[1].target_soc == 42.0
