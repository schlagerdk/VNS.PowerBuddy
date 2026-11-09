from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from powerbuddy.main import _hourly_simulation_values
from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.services.plan_processing import process_actions
from powerbuddy.services.planner import DayPlanner, PlannerInput, SimulationInputs
from powerbuddy.services.scheduler import PowerBuddyScheduler


@pytest.fixture
def forecast_planner(monkeypatch):
    planner = DayPlanner()
    monkeypatch.setattr(planner, "resolve_expected_daily_consumption", lambda day: (60.0, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_consumption_profile", lambda day: ([1 / 24] * 24, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_pv_profile", lambda day: ([0.0] * 24, "test"))
    return planner


def action(day: date, hour: int, minutes: int = 0, duration: int = 60, mode: str = "hold") -> PlanAction:
    start = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour, minutes=minutes)
    return PlanAction(
        date_key=day.isoformat(),
        start_time=start,
        end_time=start + timedelta(minutes=duration),
        action=mode,
        is_manual_override=False,
    )


def test_remaining_hours_use_only_their_share_of_daily_consumption(forecast_planner):
    day = date(2030, 11, 12)
    full_day = [action(day, hour) for hour in range(24)]
    remaining = full_day[10:]

    full_simulation = forecast_planner.simulate(day, full_day, start_soc=50.0)
    remaining_simulation = forecast_planner.simulate(day, remaining, start_soc=50.0)

    assert sum(point.projected_grid_kwh for point in full_simulation) == pytest.approx(60.0)
    assert sum(point.projected_grid_kwh for point in remaining_simulation) == pytest.approx(35.0)


def test_two_half_hours_use_the_same_consumption_as_one_full_hour(forecast_planner):
    day = date(2030, 11, 12)
    whole = [action(day, 10)]
    halves = [action(day, 10, duration=30), action(day, 10, minutes=30, duration=30)]

    full_simulation = forecast_planner.simulate(day, whole, start_soc=50.0)
    half_simulation = forecast_planner.simulate(day, halves, start_soc=50.0)

    assert full_simulation[0].projected_grid_kwh == pytest.approx(2.5)
    assert [point.projected_grid_kwh for point in half_simulation] == pytest.approx([1.25, 1.25])


def test_half_hour_charge_scales_energy_and_battery_change(forecast_planner, monkeypatch):
    day = date(2030, 11, 12)
    monkeypatch.setattr(forecast_planner, "resolve_expected_daily_consumption", lambda day: (0.0, "test"))
    forecast_planner.capacity_kwh = 20.0
    forecast_planner.max_charge_kwh = 3.0
    forecast_planner.charge_efficiency = 1.0
    halves = [action(day, 10, duration=30, mode="charge"), action(day, 10, minutes=30, duration=30, mode="charge")]
    for item in halves:
        item.charge_power_w = 1000.0

    simulation = forecast_planner.simulate(day, halves, start_soc=50.0)

    assert [point.projected_grid_kwh for point in simulation] == pytest.approx([0.5, 0.5])
    assert [point.projected_soc for point in simulation] == pytest.approx([52.5, 55.0])


def test_auto_stores_solar_surplus_in_the_simulation(forecast_planner, monkeypatch):
    day = date(2030, 11, 12)
    forecast_planner.capacity_kwh = 20.0
    forecast_planner.max_charge_kwh = 3.0
    forecast_planner.charge_efficiency = 1.0
    monkeypatch.setattr(forecast_planner, "resolve_expected_daily_consumption", lambda day: (24.0, "test"))
    pv = [0.0] * 24
    pv[12] = 3.0
    monkeypatch.setattr(forecast_planner, "resolve_hourly_pv_profile", lambda day: (pv, "test"))

    simulation = forecast_planner.simulate(day, [action(day, 12, mode="auto")], start_soc=50.0)

    assert simulation[0].projected_soc == pytest.approx(60.0)
    assert simulation[0].projected_grid_kwh == pytest.approx(0.0)


@pytest.mark.parametrize(
    ("start_soc", "charge_limit", "expected_soc", "expected_export"),
    [(95.0, 3.0, 100.0, -1.0), (50.0, 1.0, 55.0, -1.0)],
)
def test_auto_exports_solar_that_the_battery_cannot_store(
    forecast_planner, monkeypatch, start_soc, charge_limit, expected_soc, expected_export,
):
    day = date(2030, 11, 12)
    forecast_planner.capacity_kwh = 20.0
    forecast_planner.max_charge_kwh = charge_limit
    forecast_planner.charge_efficiency = 1.0
    monkeypatch.setattr(forecast_planner, "resolve_expected_daily_consumption", lambda day: (24.0, "test"))
    pv = [0.0] * 24
    pv[12] = 3.0
    monkeypatch.setattr(forecast_planner, "resolve_hourly_pv_profile", lambda day: (pv, "test"))

    simulation = forecast_planner.simulate(day, [action(day, 12, mode="auto")], start_soc=start_soc)

    assert simulation[0].projected_soc == pytest.approx(expected_soc)
    assert simulation[0].projected_grid_kwh == pytest.approx(expected_export)


@pytest.mark.parametrize(
    ("start_soc", "pv_kwh", "expected_soc", "expected_grid"),
    [(5.4, 0.0, 5.0, 0.92), (99.6, 3.0, 100.0, -1.92)],
)
def test_auto_uses_fractional_soc_for_remaining_battery_capacity(
    forecast_planner, monkeypatch, start_soc, pv_kwh, expected_soc, expected_grid,
):
    day = date(2030, 11, 12)
    forecast_planner.capacity_kwh = 20.0
    forecast_planner.min_soc = 5.0
    forecast_planner.max_soc = 100.0
    forecast_planner.max_charge_kwh = 3.0
    forecast_planner.max_discharge_kwh = 3.0
    forecast_planner.charge_efficiency = 1.0
    forecast_planner.discharge_efficiency = 1.0
    monkeypatch.setattr(forecast_planner, "resolve_expected_daily_consumption", lambda day: (24.0, "test"))
    pv = [0.0] * 24
    pv[12] = pv_kwh
    monkeypatch.setattr(forecast_planner, "resolve_hourly_pv_profile", lambda day: (pv, "test"))

    simulation = forecast_planner.simulate(day, [action(day, 12, mode="auto")], start_soc=start_soc)

    assert simulation[0].projected_soc == pytest.approx(expected_soc)
    assert simulation[0].projected_grid_kwh == pytest.approx(expected_grid)


@pytest.mark.parametrize("mode", ["hold", "auto", "charge"])
def test_target_soc_hint_cannot_artificially_lower_simulated_battery(
    forecast_planner, monkeypatch, mode,
):
    day = date(2030, 11, 12)
    forecast_planner.capacity_kwh = 20.0
    forecast_planner.charge_efficiency = 1.0
    forecast_planner.discharge_efficiency = 1.0
    monkeypatch.setattr(forecast_planner, "resolve_expected_daily_consumption", lambda day: (24.0, "test"))
    without_hint = action(day, 12, mode=mode)
    with_hint = action(day, 12, mode=mode)
    with_hint.target_soc = 20.0
    if mode == "charge":
        without_hint.charge_power_w = with_hint.charge_power_w = 1000.0

    reference = forecast_planner.simulate(day, [without_hint], start_soc=50.0)[0]
    with_target = forecast_planner.simulate(day, [with_hint], start_soc=50.0)[0]

    assert with_target.projected_soc == pytest.approx(reference.projected_soc)
    assert with_target.projected_grid_kwh == pytest.approx(reference.projected_grid_kwh)


def test_legacy_discharge_force_alias_matches_manual_discharge_simulation(forecast_planner):
    day = date(2030, 11, 12)
    forecast_planner.capacity_kwh = 20.0
    forecast_planner.discharge_efficiency = 1.0
    regular = forecast_planner.simulate(day, [action(day, 12, mode="discharge")], start_soc=50.0)[0]
    alias = forecast_planner.simulate(day, [action(day, 12, mode="discharge_force")], start_soc=50.0)[0]

    assert alias.projected_soc == pytest.approx(regular.projected_soc)
    assert alias.projected_grid_kwh == pytest.approx(regular.projected_grid_kwh)


def test_current_interval_only_simulates_time_after_frozen_start(forecast_planner):
    day = date(2030, 11, 12)
    actions = [action(day, 9), action(day, 10), action(day, 11)]
    actions[0].target_soc = 20.0
    as_of = datetime.combine(day, datetime.min.time()) + timedelta(hours=10, minutes=15)

    simulation = forecast_planner.simulate(day, actions, start_soc=50.0, simulation_start_time=as_of)

    assert [point.projected_grid_kwh for point in simulation] == pytest.approx([0.0, 1.875, 2.5])
    assert [point.projected_soc for point in simulation] == pytest.approx([50.0, 50.0, 50.0])


def test_frozen_forecast_is_reused_after_historical_values_change(forecast_planner, monkeypatch):
    day = date(2030, 11, 12)
    forecast = forecast_planner.capture_simulation_inputs(day)
    monkeypatch.setattr(forecast_planner, "resolve_expected_daily_consumption", lambda day: (120.0, "changed"))
    actions = [action(day, 10), action(day, 11)]

    baseline = forecast_planner.simulate(day, actions, start_soc=50.0, simulation_inputs=forecast)
    candidate = forecast_planner.simulate(day, actions, start_soc=50.0, simulation_inputs=forecast)

    assert [point.projected_grid_kwh for point in baseline] == pytest.approx([2.5, 2.5])
    assert [point.projected_grid_kwh for point in candidate] == pytest.approx([2.5, 2.5])


def test_chart_aggregates_half_hours_and_uses_end_of_hour_soc():
    day = date(2030, 11, 12)
    start = datetime.combine(day, datetime.min.time()) + timedelta(hours=10)
    points = [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=start, action="charge",
            projected_soc=51.0, projected_grid_kwh=1.1,
        ),
        SimulationPoint(
            date_key=day.isoformat(), timestamp=start + timedelta(minutes=30), action="hold",
            projected_soc=53.0, projected_grid_kwh=0.4,
        ),
    ]

    assert _hourly_simulation_values(points) == {start: (53.0, 1.5)}


@pytest.mark.parametrize("day", [date(2030, 3, 31), date(2030, 10, 27)])
def test_full_day_forecast_remains_normalized_on_dst_transition(day, monkeypatch):
    zone = ZoneInfo("Europe/Copenhagen")
    monkeypatch.setattr("powerbuddy.services.planner.settings.timezone", "Europe/Copenhagen")
    planner = DayPlanner()
    monkeypatch.setattr(planner, "resolve_expected_daily_consumption", lambda day: (60.0, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_consumption_profile", lambda day: ([1 / 24] * 24, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_pv_profile", lambda day: ([0.0] * 24, "test"))
    start = datetime.combine(day, datetime.min.time(), tzinfo=zone).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=zone).astimezone(timezone.utc)
    actions = []
    while start < end:
        actions.append(PlanAction(
            date_key=day.isoformat(), start_time=start, end_time=start + timedelta(hours=1),
            action="hold", is_manual_override=False,
        ))
        start += timedelta(hours=1)

    simulation = planner.simulate(day, actions, start_soc=50.0)

    assert len(simulation) in {23, 25}
    assert sum(point.projected_grid_kwh for point in simulation) == pytest.approx(60.0, abs=0.013)


@pytest.mark.parametrize("manual_first", [True, False])
def test_effective_plan_keeps_both_halves_and_manual_override(manual_first):
    day = date(2030, 11, 12)
    planned = action(day, 10, mode="charge")
    manual = action(day, 10, minutes=0 if manual_first else 30, duration=30, mode="hold")
    manual.is_manual_override = True

    effective = PowerBuddyScheduler._dedupe_effective_actions([planned, manual])

    assert [(item.start_time.minute, item.end_time.minute, item.action, item.is_manual_override) for item in effective] == (
        [(0, 30, "hold", True), (30, 0, "charge", False)]
        if manual_first else [(0, 30, "charge", False), (30, 0, "hold", True)]
    )


def test_effective_plan_splits_actions_at_hourly_price_boundaries():
    day = date(2030, 11, 12)
    long_action = action(day, 9, minutes=30, duration=60)

    effective = PowerBuddyScheduler._dedupe_effective_actions([long_action])

    assert [(item.start_time.hour, item.start_time.minute, item.end_time.hour, item.end_time.minute)
            for item in effective] == [(9, 30, 10, 0), (10, 0, 10, 30)]


@pytest.mark.parametrize("stage", ["sanity", "trim"])
def test_frozen_actions_survive_sanity_and_trim(stage, forecast_planner, monkeypatch):
    day = date(2030, 11, 12)
    locked_charge = action(day, 10, mode="charge")
    locked_charge.charge_power_w = 1000.0
    locked_charge.target_soc = 70.0
    locked_hold = action(day, 11, mode="hold")
    later = action(day, 12, mode="auto")
    actions = [locked_charge, locked_hold, later]
    prices = [
        PricePoint(
            timestamp=item.start_time, area="DK2", price_ore_per_kwh=float(hour),
            currency="DKK", source="elprisenligenu",
        )
        for item, hour in zip(actions, (10, 20, 30))
    ]
    monkeypatch.setattr(settings, "planning_sanity_enabled", stage == "sanity")
    monkeypatch.setattr(settings, "planning_sanity_autofix_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_start_hour_local", 11)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_end_hour_local", 12)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 1.0)
    monkeypatch.setattr(settings, "planning_variant_search_enabled", False)
    data = PlannerInput(
        day=day, price_points=prices, start_soc=50.0,
        locally_protected_actions=[locked_charge, locked_hold],
        simulation_inputs=SimulationInputs(24.0, tuple([1 / 24] * 24), tuple([0.0] * 24)),
        simulation_start_time=datetime.combine(day, datetime.min.time()) + timedelta(hours=10, minutes=15),
    )

    processed, _, _ = process_actions(forecast_planner, data, actions, trim_redundant=stage == "trim")

    assert (processed[0].action, processed[0].charge_power_w, processed[0].target_soc) == (
        "charge", 1000.0, 70.0,
    )
    assert processed[1].action == "hold"


def test_frozen_actions_survive_variant_search(forecast_planner, monkeypatch):
    day = date(2030, 11, 12)
    actions = [action(day, 10), action(day, 11), action(day, 12, mode="auto")]
    prices = [
        PricePoint(
            timestamp=item.start_time, area="DK2", price_ore_per_kwh=price,
            currency="DKK", source="elprisenligenu",
        )
        for item, price in zip(actions, (50.0, 10.0, 100.0))
    ]
    monkeypatch.setattr(settings, "planning_sanity_enabled", False)
    monkeypatch.setattr(settings, "planning_variant_search_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_start_hour_local", 12)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_end_hour_local", 13)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 0.0)
    monkeypatch.setattr(settings, "reserve_soc_min_percent", 5.0)
    monkeypatch.setattr(settings, "must_charge_window_min_soc_percent", 5.0)
    monkeypatch.setattr(settings, "planning_variant_search_max_precheap_auto_hours", 1)
    monkeypatch.setattr(settings, "planning_variant_search_max_cheap_charge_hours", 0)
    monkeypatch.setattr(forecast_planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=item.start_time, action=item.action,
            projected_soc=start_soc, projected_grid_kwh=0.0 if item.action == "auto" else 1.0,
        )
        for item in actions
    ])
    data = PlannerInput(
        day=day, price_points=prices, start_soc=50.0,
        locally_protected_actions=[actions[0]],
        simulation_inputs=SimulationInputs(24.0, tuple([1 / 24] * 24), tuple([0.0] * 24)),
    )

    processed, _, _ = process_actions(forecast_planner, data, actions, trim_redundant=False)

    assert processed[0].action == "hold"
