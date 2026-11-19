from __future__ import annotations

import itertools
from datetime import date, datetime, timedelta

import pytest

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint
from powerbuddy.services.optimizer import HOLD_MIN_GAIN_ORE, carry_over_value_ore_per_kwh
from powerbuddy.services.planner import DayPlanner, PlannerInput, SimulationInputs, is_winter_charge_floor

DAY = date(2026, 11, 20)
# Prices from a real winter day (kr/kWh incl. tariff and VAT).
WINTER_DAY_KR = [
    1.76, 1.79, 1.77, 1.76, 1.76, 1.86, 2.14, 2.47, 2.51, 2.48, 2.34, 2.17,
    2.09, 2.14, 2.26, 2.44, 2.49, 2.49, 2.47, 2.41, 2.30, 2.09, 2.03, 1.93,
]
EVENING_PROFILE = [
    0.6, 0.5, 0.5, 0.5, 0.5, 0.6, 0.9, 1.3, 1.2, 1.0, 0.9, 0.9,
    0.9, 0.9, 0.9, 1.0, 1.3, 1.6, 1.7, 1.5, 1.3, 1.1, 0.9, 0.7,
]


@pytest.fixture
def planner(monkeypatch):
    monkeypatch.setattr(settings, "objective_include_vat", False)
    planner = DayPlanner()
    monkeypatch.setattr(planner, "capacity_kwh", 13.8)
    monkeypatch.setattr(planner, "max_charge_kwh", 6.8)
    monkeypatch.setattr(planner, "charge_setpoint_w", 6800.0)
    monkeypatch.setattr(planner, "max_discharge_kwh", 6.8)
    monkeypatch.setattr(planner, "min_soc", 5.0)
    monkeypatch.setattr(planner, "max_soc", 100.0)
    return planner


def _points(kr_per_kwh: list[float], start_hour: int = 0) -> list[PricePoint]:
    midnight = datetime.combine(DAY, datetime.min.time())
    return [
        PricePoint(
            timestamp=midnight + timedelta(hours=start_hour + index), area="DK2",
            price_ore_per_kwh=price * 100.0, currency="DKK", source="elprisenligenu",
        )
        for index, price in enumerate(kr_per_kwh)
    ]


def _inputs(daily_kwh: float, pv: list[float] | None = None) -> SimulationInputs:
    total = sum(EVENING_PROFILE)
    return SimulationInputs(daily_kwh, tuple(v / total for v in EVENING_PROFILE), tuple(pv or [0.0] * 24))


def _objective(planner: DayPlanner, data: PlannerInput, actions: list[PlanAction]) -> float:
    simulation = planner.simulate(data.day, actions, data.start_soc, simulation_inputs=data.simulation_inputs)
    prices = {point.timestamp: point.price_ore_per_kwh for point in data.price_points}
    cost, soc = 0.0, data.start_soc
    for point in simulation:
        cost += planner._cost_ore(point.projected_grid_kwh, prices[point.timestamp])
        cost += abs(point.projected_soc - soc) * planner.capacity_kwh / 100.0 * planner.degradation_cost_ore_per_kwh
        soc = point.projected_soc
    carry = carry_over_value_ore_per_kwh(planner, data, data.price_points)
    return cost - (soc - planner.min_soc) / 100.0 * planner.capacity_kwh * carry


def test_battery_that_covers_the_day_runs_auto_without_holds(planner):
    data = PlannerInput(DAY, _points(WINTER_DAY_KR), start_soc=15.0, simulation_inputs=_inputs(12.0))

    actions = planner.plan(data)

    assert not [a.start_time.hour for a in actions if a.action == "hold"]
    assert {a.start_time.hour for a in actions if a.action == "charge"} == {0, 1, 2, 3, 4}


def test_short_battery_never_holds_in_expensive_hours(planner):
    data = PlannerInput(DAY, _points(WINTER_DAY_KR), start_soc=15.0, simulation_inputs=_inputs(20.0))

    actions = planner.plan(data)

    holds = [WINTER_DAY_KR[a.start_time.hour] for a in actions if a.action == "hold"]
    assert holds, "an evening-heavy day with a too-small battery should save energy for the peak"
    # Holds land in the cheap middle of the day, never among the ten priciest hours.
    assert max(holds) < sorted(WINTER_DAY_KR)[-10]
    for hour in (7, 8, 9, 15, 16, 17, 18, 19):
        assert actions[hour].action == "auto"


@pytest.mark.parametrize("start_soc", [5.0, 40.0, 90.0])
@pytest.mark.parametrize("pv", [None, [0, 0, 1.5, 3.0, 1.5, 0]])
def test_plan_matches_brute_force_optimum(planner, monkeypatch, start_soc, pv):
    monkeypatch.setattr(settings, "winter_cheap_charge_hours", 0)
    prices = [1.2, 2.6, 0.9, 2.4, 3.1, 1.5]
    pv_profile = [0.0] * 6 + [float(v) for v in (pv or [0] * 6)] + [0.0] * 12
    data = PlannerInput(
        DAY, _points(prices, start_hour=6), start_soc=start_soc, simulation_inputs=_inputs(30.0, pv_profile),
    )
    planned = planner.plan(data)

    def actions_for(choice: tuple[str, ...]) -> list[PlanAction]:
        return [
            PlanAction(
                date_key=DAY.isoformat(), start_time=point.timestamp, end_time=point.timestamp + timedelta(hours=1),
                action=mode, charge_power_w=planner.charge_setpoint_w if mode == "charge" else None,
            )
            for point, mode in zip(data.price_points, choice)
        ]

    best = min(
        _objective(planner, data, actions_for(choice))
        for choice in itertools.product(("auto", "hold", "charge"), repeat=len(prices))
    )
    holds = sum(1 for a in planned if a.action == "hold")
    # Grid discretisation (0.25 % SOC) plus the deliberate hold threshold.
    assert _objective(planner, data, planned) <= best + 2.0 + holds * HOLD_MIN_GAIN_ORE


def test_carry_over_uses_next_day_prices_when_released(planner):
    today = _points([2.0] * 24)
    tomorrow = [
        PricePoint(
            timestamp=point.timestamp + timedelta(days=1), area="DK2", price_ore_per_kwh=50.0,
            currency="DKK", source="elprisenligenu",
        )
        for point in today
    ]
    data = PlannerInput(DAY, today, start_soc=50.0, next_day_price_points=tomorrow)
    expected = 50.0 * planner.charge_efficiency * planner.discharge_efficiency
    assert carry_over_value_ore_per_kwh(planner, data, today) == pytest.approx(expected)
    # Dummy/provisional next-day prices are ignored.
    for point in tomorrow:
        point.source = "dummy-future-fallback"
    assert carry_over_value_ore_per_kwh(planner, data, today) == pytest.approx(
        200.0 * planner.charge_efficiency * planner.discharge_efficiency
    )


def test_winter_floor_charges_the_cheapest_hours(planner):
    data = PlannerInput(DAY, _points(WINTER_DAY_KR), start_soc=15.0, simulation_inputs=_inputs(20.0))

    floor = {a.start_time.hour for a in planner.plan(data) if is_winter_charge_floor(a)}

    assert floor == {0, 1, 2, 3, 4}


def test_full_battery_is_never_labelled_charge_outside_the_floor(planner):
    data = PlannerInput(DAY, _points(WINTER_DAY_KR), start_soc=15.0, simulation_inputs=_inputs(20.0))

    actions = planner.plan(data)

    assert all(a.action != "charge" or is_winter_charge_floor(a) for a in actions[5:])
