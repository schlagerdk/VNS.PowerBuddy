"""Exact cost-minimising day planner.

Dynamic programming over battery state of charge, using the very same step model as
``DayPlanner.simulate``. The objective is the real household bill for the horizon:

    import (spot + tariff) * VAT  -  export * feed-in  +  battery wear
    -  value of energy still stored at the end of the horizon

There are no heuristic penalties. The only hard rules are physical limits, manual or
frozen intervals (kept exactly as they are) and the configured winter charge floor.
A ``hold`` is a bet on the forecast (it buys power now on the expectation that the
battery would otherwise run out in a pricier hour), so it is only chosen when it saves
more than ``POWERBUDDY_HOLD_MIN_GAIN_ORE`` per held hour; ties always prefer ``auto``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint
from powerbuddy.services.planner import WINTER_CHEAP_CHARGE_REASON, DayPlanner, PlannerInput

SOC_STEP_PERCENT = 0.25
PUBLIC_PRICE_SOURCES = frozenset({"energidataservice", "elprisenligenu"})

REASON_AUTO = "optimizer: self-consumption"
REASON_HOLD = "optimizer: save battery for pricier hours"
REASON_CHARGE = "optimizer: charge at cheap hour"
REASON_FIXED = "optimizer: locked interval"

# Preference on exact ties (lower wins).
_TIE_ORDER = {"auto": 0, "charge": 1, "hold": 2}


@dataclass(frozen=True, slots=True)
class _Segment:
    duration_hours: float
    fixed: PlanAction | None


def hold_min_gain_ore() -> float:
    return max(0.0, float(settings.hold_min_gain_ore))


def _naive(dt: datetime) -> datetime:
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def _vat_factor() -> float:
    return float(settings.tariff_vat_factor) if settings.objective_include_vat else 1.0


def _tariff_for(planner: DayPlanner, data: PlannerInput, point: PricePoint) -> float:
    tariffs = data.tariff_ore_per_hour
    if tariffs is None or len(tariffs) != 24:
        return 0.0
    return float(tariffs[planner._local_hour(point.timestamp)])


def _import_cost_ore(planner: DayPlanner, data: PlannerInput, point: PricePoint) -> float:
    return (float(point.price_ore_per_kwh) + _tariff_for(planner, data, point)) * _vat_factor()


def released_next_day_prices(day: date) -> list[PricePoint]:
    """Released (non-dummy, non-provisional) prices for the day after ``day``; empty if unknown."""
    from powerbuddy.repositories import PriceRepository

    try:
        points = PriceRepository.get_by_day(day + timedelta(days=1), settings.price_area)
    except Exception:  # optional context; never block planning
        return []
    return [point for point in points if point.source in PUBLIC_PRICE_SOURCES]


def carry_over_value_ore_per_kwh(planner: DayPlanner, data: PlannerInput, points: Sequence[PricePoint]) -> float:
    """Value of one stored kWh left at the end of the horizon.

    Leftover energy saves refilling it later, at best at the cheapest known import price.
    It is deliberately discounted by the round-trip efficiency so the planner only
    carries energy past midnight when it is clearly worth it, never at the cost of
    covering today's load from the battery.
    """
    reference = [
        point for point in (data.next_day_price_points or [])
        if point.source in PUBLIC_PRICE_SOURCES
    ] or list(points)
    if not reference:
        return 0.0
    cheapest = min(_import_cost_ore(planner, data, point) for point in reference)
    return max(0.0, cheapest * planner.charge_efficiency * planner.discharge_efficiency)


def winter_floor_indices(planner: DayPlanner, data: PlannerInput, points: Sequence[PricePoint]) -> set[int]:
    """Cheapest-hour charging floor for the non-summer season.

    Applies when the horizon contains the configured night window: the N cheapest slots
    plus additional cheap night slots. A horizon that starts after the night window
    (an intraday replan) is not forced to charge in the evening.
    """
    if data.day.month in {5, 6, 7, 8, 9} or not points:
        return set()
    count = max(0, min(len(points), int(settings.winter_cheap_charge_hours)))
    if not count:
        return set()
    night_start = int(settings.winter_cheap_charge_night_start_hour) % 24
    night_end = int(settings.winter_cheap_charge_night_end_hour) % 24

    def in_night(idx: int) -> bool:
        return planner._is_hour_in_window(planner._local_hour(points[idx].timestamp), night_start, night_end)

    if not any(in_night(idx) for idx in range(len(points))):
        return set()

    levels = [_import_cost_ore(planner, data, point) for point in points]
    ranked = sorted(range(len(points)), key=lambda idx: (levels[idx], idx))
    selected = ranked[:count]
    extra = max(0, int(settings.winter_additional_night_charge_hours))
    selected += [idx for idx in ranked if in_night(idx) and idx not in selected][:extra]
    return set(selected)


def _segments(
    slot_start: datetime,
    slot_end: datetime,
    simulation_start: datetime | None,
    protected: Sequence[PlanAction],
) -> list[_Segment]:
    start = max(slot_start, simulation_start) if simulation_start is not None else slot_start
    if start >= slot_end:
        return []
    cuts = {start, slot_end}
    overlapping = []
    for action in protected:
        p_start, p_end = _naive(action.start_time), _naive(action.end_time)
        if p_start < slot_end and p_end > start:
            overlapping.append((p_start, p_end, action))
            cuts.update(t for t in (p_start, p_end) if start < t < slot_end)
    # Manual overrides win over frozen actions when both cover the same instant.
    overlapping.sort(key=lambda item: not item[2].is_manual_override)
    ordered = sorted(cuts)
    segments = []
    for seg_start, seg_end in zip(ordered, ordered[1:]):
        fixed = next((a for p_start, p_end, a in overlapping if p_start <= seg_start and p_end >= seg_end), None)
        segments.append(_Segment((seg_end - seg_start).total_seconds() / 3600.0, fixed))
    return segments


def _interp(values: list[float], soc: float, soc_min: float, step: float) -> float:
    pos = (soc - soc_min) / step
    if pos <= 0:
        return values[0]
    last = len(values) - 1
    if pos >= last:
        return values[last]
    low = int(pos)
    frac = pos - low
    return values[low] + (values[low + 1] - values[low]) * frac


def optimize_day(planner: DayPlanner, data: PlannerInput) -> list[PlanAction]:
    points = sorted(data.price_points, key=lambda point: point.timestamp)
    if data.planning_start_time is not None:
        planning_start = _naive(data.planning_start_time)
        points = [point for point in points if _naive(point.timestamp) >= planning_start]
    if not points:
        return []

    inputs = data.simulation_inputs or planner.capture_simulation_inputs(data.day)
    net = planner._hourly_consumption_for_points(
        points=points,
        expected_daily_consumption_kwh=inputs.expected_daily_consumption_kwh,
        hourly_profile=inputs.hourly_consumption_profile,
        pv_profile_kwh=inputs.hourly_pv_profile_kwh,
        pv_weather_factor_24h=data.pv_weather_factor_24h,
        reference_day=data.day,
    )
    simulation_start = None
    if data.simulation_start_time is not None:
        simulation_start = data.simulation_start_time
        if simulation_start.tzinfo:
            simulation_start = simulation_start.astimezone(planner.timezone).replace(tzinfo=None)
    protected = list(data.locally_protected_actions or [])

    slots = [
        _segments(_naive(point.timestamp), _naive(point.timestamp) + timedelta(hours=1), simulation_start, protected)
        for point in points
    ]
    prices = [float(point.price_ore_per_kwh) for point in points]
    tariffs = [_tariff_for(planner, data, point) for point in points]
    floor = winter_floor_indices(planner, data, points)

    def options(t: int, soc: float) -> tuple[str, ...]:
        if slots[t] and all(segment.fixed is not None for segment in slots[t]):
            return ("fixed",)
        if t in floor:
            return ("charge",)
        if soc >= planner.max_soc - 1e-6:
            # A full battery cannot charge; "charge" would just be a mislabelled hold.
            return ("auto", "hold")
        return ("auto", "hold", "charge")

    hold_gain = hold_min_gain_ore()
    wear = planner.degradation_cost_ore_per_kwh * planner.capacity_kwh / 100.0

    def run_slot(t: int, soc: float, choice: str) -> tuple[float, float]:
        cost = hold_gain if choice == "hold" else 0.0
        for segment in slots[t]:
            if segment.fixed is not None:
                action, power = segment.fixed.action, segment.fixed.charge_power_w
            else:
                action = choice
                power = planner.charge_setpoint_w if choice == "charge" else None
            soc_after, grid_kwh = planner.step(action, soc, net[t], segment.duration_hours, power)
            cost += planner._cost_ore(grid_kwh, prices[t], tariffs[t]) + abs(soc_after - soc) * wear
            soc = soc_after
        return soc, cost

    soc_min, soc_max = planner.min_soc, planner.max_soc
    count = max(1, int(round((soc_max - soc_min) / SOC_STEP_PERCENT)))
    step = (soc_max - soc_min) / count if soc_max > soc_min else 1.0
    grid = [soc_min + i * step for i in range(count + 1)]

    carry = carry_over_value_ore_per_kwh(planner, data, points)
    value = [-(soc - soc_min) / 100.0 * planner.capacity_kwh * carry for soc in grid]
    values: list[list[float]] = [value]
    for t in range(len(points) - 1, -1, -1):
        following = values[0]
        current = []
        for soc in grid:
            best = float("inf")
            for choice in options(t, soc):
                soc_after, cost = run_slot(t, soc, choice)
                best = min(best, cost + _interp(following, soc_after, soc_min, step))
            current.append(best)
        values.insert(0, current)

    actions: list[PlanAction] = []
    soc = max(soc_min, min(soc_max, float(data.start_soc)))
    for t, point in enumerate(points):
        best_choice, best_total, best_soc = "auto", float("inf"), soc
        for choice in options(t, soc):
            soc_after, cost = run_slot(t, soc, choice)
            total = cost + _interp(values[t + 1], soc_after, soc_min, step)
            if total < best_total - 1e-9 or (
                abs(total - best_total) <= 1e-9 and _TIE_ORDER.get(choice, 3) < _TIE_ORDER.get(best_choice, 3)
            ):
                best_choice, best_total, best_soc = choice, total, soc_after
        actions.append(_to_action(planner, data.day, point, best_choice, slots[t], t in floor, soc, best_soc))
        soc = best_soc
    return actions


def _to_action(
    planner: DayPlanner,
    day: date,
    point: PricePoint,
    choice: str,
    segments: list[_Segment],
    is_floor: bool,
    soc_before: float,
    soc_after: float,
) -> PlanAction:
    action, power, target, reason = choice, None, None, REASON_AUTO
    if choice == "fixed":
        fixed = segments[0].fixed
        assert fixed is not None
        action, power, target, reason = fixed.action, fixed.charge_power_w, fixed.target_soc, REASON_FIXED
    elif choice == "charge":
        power = planner.charge_setpoint_w
        target = round(soc_after, 1)
        reason = WINTER_CHEAP_CHARGE_REASON if is_floor else REASON_CHARGE
    elif choice == "hold":
        reason = REASON_HOLD
    else:
        target = round(soc_after, 1)
    return PlanAction(
        date_key=day.isoformat(),
        start_time=point.timestamp,
        end_time=point.timestamp + timedelta(hours=1),
        action=action,
        charge_power_w=power,
        target_soc=target,
        reason=reason,
        is_manual_override=False,
    )
