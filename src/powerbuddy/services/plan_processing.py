from __future__ import annotations

from dataclasses import dataclass

from powerbuddy.config import settings
from powerbuddy.models import PlanAction
from powerbuddy.services.planner import DayPlanner, PlannerInput
from powerbuddy.services.planning_sanity import apply_planning_sanity
from powerbuddy.services.planning_variants import choose_best_plan_variant


@dataclass(slots=True)
class PreparedPlan:
    actions_to_store: list[PlanAction]
    simulation_actions: list[PlanAction]
    sanity_report: dict[str, object]
    variant_report: dict[str, object]


def process_actions(
    planner: DayPlanner,
    data: PlannerInput,
    actions: list[PlanAction],
    *,
    trim_redundant: bool,
    protected_actions: list[PlanAction] | None = None,
) -> tuple[list[PlanAction], dict[str, object], dict[str, object]]:
    protected_intervals = (data.locally_protected_actions or []) + (protected_actions or [])
    protected_starts = {
        action.start_time
        for action in actions
        if any(
            action.start_time.replace(tzinfo=None) < protected.end_time.replace(tzinfo=None)
            and action.end_time.replace(tzinfo=None) > protected.start_time.replace(tzinfo=None)
            for protected in protected_intervals
        )
    }
    actions, sanity_report = apply_planning_sanity(
        planner=planner,
        day=data.day,
        actions=actions,
        prices=data.price_points,
        start_soc=data.start_soc,
        tariff_ore_per_hour=data.tariff_ore_per_hour,
        pv_weather_factor_24h=data.pv_weather_factor_24h,
        auto_fix=bool(settings.planning_sanity_autofix_enabled),
        protected_action_starts=protected_starts,
        simulation_inputs=data.simulation_inputs,
        simulation_start_time=data.simulation_start_time,
    )
    actions, variant_report = choose_best_plan_variant(
        planner=planner,
        day=data.day,
        actions=actions,
        prices=data.price_points,
        start_soc=data.start_soc,
        tariff_ore_per_hour=data.tariff_ore_per_hour,
        pv_weather_factor_24h=data.pv_weather_factor_24h,
        simulation_inputs=data.simulation_inputs,
        simulation_start_time=data.simulation_start_time,
        protected_action_starts=protected_starts,
    )
    if trim_redundant:
        actions = planner.trim_redundant_charge_actions(
            day=data.day,
            actions=actions,
            start_soc=data.start_soc,
            pv_weather_factor_24h=data.pv_weather_factor_24h,
            price_points=data.price_points,
            tariff_ore_per_hour=data.tariff_ore_per_hour,
            simulation_inputs=data.simulation_inputs,
            simulation_start_time=data.simulation_start_time,
            protected_action_starts=protected_starts,
        )
    return actions, sanity_report, variant_report
