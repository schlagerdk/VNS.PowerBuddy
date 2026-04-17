from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint
from powerbuddy.services.planner import DayPlanner


@dataclass(slots=True)
class VariantResult:
    score_ore: float
    soc_before_expensive: float
    expensive_auto_share: float
    changes: int
    actions: list[PlanAction]


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    qq = max(0.0, min(1.0, float(q)))
    sorted_values = sorted(values)
    idx = int(round((len(sorted_values) - 1) * qq))
    idx = max(0, min(len(sorted_values) - 1, idx))
    return float(sorted_values[idx])


def _is_hour_in_window(hour: int, start: int, end: int) -> bool:
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _clone_actions(actions: list[PlanAction]) -> list[PlanAction]:
    return [
        PlanAction(
            date_key=a.date_key,
            start_time=a.start_time,
            end_time=a.end_time,
            action=a.action,
            charge_power_w=a.charge_power_w,
            target_soc=a.target_soc,
            reason=a.reason,
            is_manual_override=a.is_manual_override,
        )
        for a in actions
    ]


def _scenario_factors(pv_weather_factor_24h: list[float] | None) -> list[tuple[float, list[float] | None]]:
    if pv_weather_factor_24h is None or len(pv_weather_factor_24h) != 24:
        return [(1.0, None)]

    w_base = max(0.0, float(settings.planning_variant_search_base_solar_weight))
    w_low = max(0.0, float(settings.planning_variant_search_low_solar_weight))
    w_high = max(0.0, float(settings.planning_variant_search_high_solar_weight))
    w_sum = w_base + w_low + w_high
    if w_sum <= 1e-9:
        w_base, w_low, w_high = 0.55, 0.30, 0.15
        w_sum = 1.0
    w_base /= w_sum
    w_low /= w_sum
    w_high /= w_sum

    base = [max(0.0, float(v)) for v in pv_weather_factor_24h]
    low_solar = [max(0.0, v * 0.2) for v in base]
    high_solar = [max(0.0, v * 1.2) for v in base]
    return [
        (w_base, base),
        (w_low, low_solar),
        (w_high, high_solar),
    ]


def _total_cost_ore(
    planner: DayPlanner,
    day: date,
    actions: list[PlanAction],
    start_soc: float,
    prices_by_hour: dict[int, float],
    tariff_ore_per_hour: list[float] | None,
    pv_weather_factor_24h: list[float] | None,
) -> float:
    sim = planner.simulate(day, actions, start_soc=start_soc, pv_weather_factor_24h=pv_weather_factor_24h)
    total = 0.0
    for point in sim:
        hour = int(point.timestamp.hour)
        grid_kwh = max(0.0, float(point.projected_grid_kwh))
        price = prices_by_hour.get(hour, 0.0)
        tariff = float(tariff_ore_per_hour[hour]) if tariff_ore_per_hour and 0 <= hour < len(tariff_ore_per_hour) else 0.0
        total += planner._cost_ore(grid_kwh, price, tariff)
    return total


def _soc_before_expensive(
    planner: DayPlanner,
    day: date,
    actions: list[PlanAction],
    start_soc: float,
    first_expensive_idx: int,
    pv_weather_factor_24h: list[float] | None,
) -> float:
    if first_expensive_idx <= 0:
        return float(start_soc)
    sim = planner.simulate(day, actions, start_soc=start_soc, pv_weather_factor_24h=pv_weather_factor_24h)
    idx = min(first_expensive_idx - 1, len(sim) - 1)
    if idx < 0:
        return float(start_soc)
    return float(sim[idx].projected_soc)


def choose_best_plan_variant(
    planner: DayPlanner,
    day: date,
    actions: list[PlanAction],
    prices: list[PricePoint],
    start_soc: float,
    tariff_ore_per_hour: list[float] | None = None,
    pv_weather_factor_24h: list[float] | None = None,
) -> tuple[list[PlanAction], dict[str, object]]:
    report: dict[str, object] = {
        "enabled": bool(settings.planning_variant_search_enabled),
        "candidates_evaluated": 0,
        "best_score_ore": None,
        "best_changes": 0,
        "best_soc_before_expensive": None,
        "best_expensive_auto_share": None,
    }

    if not settings.planning_variant_search_enabled:
        return actions, report

    if not actions or not prices:
        return actions, report

    actions_sorted = sorted(actions, key=lambda a: a.start_time)
    prices_by_hour = {int(p.timestamp.hour): float(p.price_ore_per_kwh) for p in prices}
    valid_indices = [idx for idx, action in enumerate(actions_sorted) if int(action.start_time.hour) in prices_by_hour]
    if not valid_indices:
        return actions_sorted, report

    total_levels = []
    for idx in valid_indices:
        hour = int(actions_sorted[idx].start_time.hour)
        tariff = float(tariff_ore_per_hour[hour]) if tariff_ore_per_hour and hour < len(tariff_ore_per_hour) else 0.0
        total_levels.append(prices_by_hour.get(hour, 0.0) + tariff)

    if not total_levels:
        return actions_sorted, report

    window_enabled = bool(settings.planning_sanity_expensive_window_enabled)
    window_start = max(0, min(23, int(settings.planning_sanity_expensive_window_start_hour_local)))
    window_end = max(1, min(24, int(settings.planning_sanity_expensive_window_end_hour_local)))

    expensive_indices = [
        idx for idx in valid_indices
        if window_enabled and _is_hour_in_window(int(actions_sorted[idx].start_time.hour), window_start, window_end)
    ]
    if not expensive_indices:
        threshold = _quantile(total_levels, settings.planning_sanity_expensive_quantile)
        expensive_indices = [
            idx for idx in valid_indices
            if (
                prices_by_hour.get(int(actions_sorted[idx].start_time.hour), 0.0)
                + (float(tariff_ore_per_hour[int(actions_sorted[idx].start_time.hour)]) if tariff_ore_per_hour else 0.0)
            ) >= threshold
        ]

    if not expensive_indices:
        return actions_sorted, report

    first_expensive_idx = min(expensive_indices)

    pre_exp = [idx for idx in valid_indices if idx < first_expensive_idx and not actions_sorted[idx].is_manual_override]
    if not pre_exp:
        return actions_sorted, report

    def slot_total(idx: int) -> float:
        hour = int(actions_sorted[idx].start_time.hour)
        tariff = float(tariff_ore_per_hour[hour]) if tariff_ore_per_hour and hour < len(tariff_ore_per_hour) else 0.0
        return prices_by_hour.get(hour, 0.0) + tariff

    cheap_cutoff = _quantile([slot_total(idx) for idx in pre_exp], settings.planning_sanity_charge_candidate_quantile)
    cheap_indices = [idx for idx in pre_exp if slot_total(idx) <= cheap_cutoff + 1e-6]
    cheap_indices = sorted(cheap_indices, key=slot_total)

    if not cheap_indices:
        return actions_sorted, report

    first_cheap_idx = min(cheap_indices)
    precheap_indices = [idx for idx in pre_exp if idx < first_cheap_idx]
    precheap_by_price = sorted(precheap_indices, key=slot_total, reverse=True)
    precheap_by_late = sorted(precheap_indices, reverse=True)

    max_precheap_auto = max(0, min(12, int(settings.planning_variant_search_max_precheap_auto_hours)))
    max_cheap_charge = max(0, min(8, int(settings.planning_variant_search_max_cheap_charge_hours)))

    auto_options = sorted({0, 2, 4, 5, 6, 8, max_precheap_auto})
    auto_options = [value for value in auto_options if 0 <= value <= max_precheap_auto]
    charge_options = sorted({0, 1, 2, 3, 4, max_cheap_charge})
    charge_options = [value for value in charge_options if 0 <= value <= max_cheap_charge]
    max_candidates = max(5, min(20, int(settings.planning_variant_search_candidate_count)))

    candidate_defs: list[tuple[str, int, int]] = [("price", 0, 0)]
    for mode in ("price", "late"):
        for a in auto_options:
            for c in charge_options:
                if a == 0 and c == 0:
                    continue
                candidate_defs.append((mode, a, c))
    candidate_defs = candidate_defs[:max_candidates]

    min_auto_share = max(0.0, min(1.0, float(settings.planning_sanity_min_expensive_auto_share)))
    required_soc = max(
        float(settings.reserve_soc_min_percent),
        float(settings.must_charge_window_min_soc_percent),
    )

    scenarios = _scenario_factors(pv_weather_factor_24h)
    best: VariantResult | None = None

    for mode, auto_n, charge_n in candidate_defs:
        cand = _clone_actions(actions_sorted)
        changes = 0

        auto_source = precheap_by_late if mode == "late" else precheap_by_price
        for idx in auto_source[:max(0, auto_n)]:
            if cand[idx].action == "hold":
                cand[idx].action = "auto"
                cand[idx].charge_power_w = None
                cand[idx].reason = f"variant search: pre-cheap auto ({mode})"
                changes += 1

        for idx in cheap_indices[:max(0, charge_n)]:
            if cand[idx].action != "charge":
                cand[idx].action = "charge"
                cand[idx].charge_power_w = planner.charge_setpoint_w
                cand[idx].reason = "variant search: cheap-window charge"
                changes += 1

        expensive_auto_count = sum(1 for idx in expensive_indices if cand[idx].action == "auto")
        expensive_share = expensive_auto_count / max(1, len(expensive_indices))
        if expensive_share + 1e-6 < min_auto_share:
            continue

        soc_before = _soc_before_expensive(
            planner,
            day,
            cand,
            start_soc,
            first_expensive_idx,
            pv_weather_factor_24h,
        )
        if soc_before + 1e-6 < required_soc:
            continue

        weighted_cost = 0.0
        for weight, scenario_factors in scenarios:
            weighted_cost += weight * _total_cost_ore(
                planner,
                day,
                cand,
                start_soc,
                prices_by_hour,
                tariff_ore_per_hour,
                scenario_factors,
            )

        report["candidates_evaluated"] = int(report["candidates_evaluated"]) + 1
        result = VariantResult(
            score_ore=weighted_cost,
            soc_before_expensive=soc_before,
            expensive_auto_share=expensive_share,
            changes=changes,
            actions=cand,
        )
        if best is None or result.score_ore < best.score_ore:
            best = result

    if best is None:
        return actions_sorted, report

    report["best_score_ore"] = round(best.score_ore, 3)
    report["best_changes"] = int(best.changes)
    report["best_soc_before_expensive"] = round(best.soc_before_expensive, 3)
    report["best_expensive_auto_share"] = round(best.expensive_auto_share, 3)
    report["auto_options"] = auto_options
    report["charge_options"] = charge_options
    report["auto_modes"] = ["price", "late"]
    return best.actions, report
