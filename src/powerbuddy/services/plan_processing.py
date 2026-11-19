from __future__ import annotations

from dataclasses import dataclass

from powerbuddy.models import PlanAction


@dataclass(slots=True)
class PreparedPlan:
    actions_to_store: list[PlanAction]
    simulation_actions: list[PlanAction]
