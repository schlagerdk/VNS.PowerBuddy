from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from powerbuddy.config import settings
from powerbuddy.models import PlanAction
from powerbuddy.services.planner import DayPlanner, PlannerInput
from powerbuddy.services.planning_sanity import apply_planning_sanity

logger = logging.getLogger(__name__)


class ProposalRejected(ValueError):
    """A local, non-sensitive explanation of why a proposal cannot be used."""


_outcomes: ContextVar[dict[str, str] | None] = ContextVar("claude_planning_outcomes", default=None)


def claude_configured() -> bool:
    return bool(settings.claude_api_key.get_secret_value().strip())


@contextmanager
def collect_outcomes() -> Iterator[dict[str, str]]:
    """Collect per-day Claude outcomes (accepted/rejected/unavailable) for the current task."""
    collected: dict[str, str] = {}
    token = _outcomes.set(collected)
    try:
        yield collected
    finally:
        _outcomes.reset(token)


def summarize_outcomes(collected: dict[str, str]) -> dict[str, object]:
    values = list(collected.values())
    return {
        "enabled": claude_configured(),
        "days": len(values),
        "accepted": values.count("accepted"),
        "rejected": values.count("rejected"),
        "unavailable": values.count("unavailable"),
    }


def _record(data: PlannerInput, outcome: str) -> None:
    collected = _outcomes.get()
    if collected is not None:
        collected[data.day.isoformat()] = outcome


async def verify_api_key(api_key: str) -> dict[str, object]:
    """Check a key against the free model-list endpoint; never logs or returns the key."""
    async with httpx.AsyncClient(timeout=min(float(settings.claude_timeout_seconds), 15.0)) as client:
        response = await client.get(
            "https://api.anthropic.com/v1/models",
            params={"limit": 1000},
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"},
        )
    if response.status_code in {401, 403}:
        return {"valid": False, "model_available": False}
    response.raise_for_status()
    models = response.json().get("data") or []
    ids = {str(item.get("id")) for item in models if isinstance(item, dict)}
    return {"valid": True, "model_available": settings.claude_model in ids}


_SYSTEM = """Propose an hourly battery schedule from public spot prices and general rules.
Use charge for cheap grid charging, auto for normal solar/battery operation, and hold
to preserve stored energy for more expensive hours. Minimize electricity cost; avoid
unnecessary charging or holding. In winter, charge the cheapest hours plus additional
cheap overnight hours as specified in the rules. Return every supplied slot exactly
once, using its slot index. You have no private household data. A local engine will
apply consumption, solar forecasts, tariffs, state of charge and safety constraints.
Call submit_plan with your proposal. Do not request private data or invoke other tools."""


class SlotProposal(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    slot: int = Field(ge=0)
    action: Literal["auto", "charge", "hold"]


class Proposal(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    slots: list[SlotProposal] = Field(min_length=1, max_length=25)


class _ContentBlock(BaseModel):
    type: str
    name: str | None = None
    input: dict[str, object] | None = None


class _Response(BaseModel):
    stop_reason: str
    content: list[_ContentBlock]


def _public_payload(data: PlannerInput) -> dict[str, object]:
    # Explicit allowlist: never serialize PlannerInput, settings or ORM objects.
    points = sorted(data.price_points, key=lambda point: point.timestamp)
    if not points or len(points) > 25:
        raise ProposalRejected("unsupported price horizon")
    if any(point.source not in {"energidataservice", "elprisenligenu"} for point in points):
        raise ProposalRejected("Claude requires public, non-provisional price data")
    if any(not math.isfinite(point.price_ore_per_kwh) for point in points):
        raise ProposalRejected("invalid public price")
    return {
        "slots": [
            {
                "slot": index,
                "time": point.timestamp.isoformat(),
                "spot_ore_per_kwh": point.price_ore_per_kwh,
            }
            for index, point in enumerate(points)
        ],
        "rules": {
            "winter": data.day.month not in {5, 6, 7, 8, 9},
            "winter_cheapest_hours": settings.winter_cheap_charge_hours,
            "winter_additional_night_hours": settings.winter_additional_night_charge_hours,
            "night_start_hour": settings.winter_cheap_charge_night_start_hour,
            "night_end_hour": settings.winter_cheap_charge_night_end_hour,
        },
    }


async def _request_proposal(payload: dict[str, object]) -> Proposal:
    async with httpx.AsyncClient(timeout=settings.claude_timeout_seconds) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": settings.claude_api_key.get_secret_value(),
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": settings.claude_model,
                "max_tokens": 2048,
                "system": _SYSTEM,
                "messages": [{"role": "user", "content": json.dumps(payload, allow_nan=False)}],
                "tools": [{
                    "name": "submit_plan",
                    "description": "Submit a complete hourly price-based battery proposal for local validation.",
                    "input_schema": Proposal.model_json_schema(),
                }],
                "tool_choice": {"type": "tool", "name": "submit_plan"},
            },
        )
        response.raise_for_status()
        result = _Response.model_validate(response.json())
    tools = [block for block in result.content if block.type == "tool_use"]
    if result.stop_reason != "tool_use" or len(tools) != 1 or tools[0].name != "submit_plan":
        raise ProposalRejected("missing or incomplete Claude proposal")
    return Proposal.model_validate(tools[0].input)


def _candidate(
    planner: DayPlanner, data: PlannerInput, baseline: list[PlanAction], proposal: Proposal
) -> list[PlanAction]:
    points = sorted(data.price_points, key=lambda point: point.timestamp)
    if sorted(slot.slot for slot in proposal.slots) != list(range(len(points))):
        raise ProposalRejected("Claude proposal must cover every public price slot exactly once")
    proposed = {points[slot.slot].timestamp.replace(tzinfo=None): slot.action for slot in proposal.slots}
    candidate: list[PlanAction] = []
    for action in baseline:
        next_action = proposed[action.start_time.replace(tzinfo=None)]
        # Keep locally required charge slots, including the additive winter floor.
        if action.is_manual_override or action.action == "charge":
            next_action = action.action
        changed = next_action != action.action
        candidate.append(PlanAction(
            date_key=action.date_key,
            start_time=action.start_time,
            end_time=action.end_time,
            action=next_action,
            charge_power_w=(
                action.charge_power_w if not changed
                else planner.charge_setpoint_w if next_action == "charge" else None
            ),
            target_soc=action.target_soc if not changed else None,
            reason="Claude price proposal (locally validated)" if changed else action.reason,
            is_manual_override=action.is_manual_override,
        ))
    candidate, _ = apply_planning_sanity(
        planner, data.day, candidate, data.price_points, data.start_soc,
        tariff_ore_per_hour=data.tariff_ore_per_hour,
        pv_weather_factor_24h=data.pv_weather_factor_24h,
        auto_fix=True,
    )
    return candidate


def _validate_locally(
    planner: DayPlanner, data: PlannerInput, baseline: list[PlanAction], candidate: list[PlanAction]
) -> None:
    if len(candidate) != len(baseline):
        raise ProposalRejected("local validation changed plan coverage")
    prices = {point.timestamp.replace(tzinfo=None): point.price_ore_per_kwh for point in data.price_points}
    results = [
        planner.simulate(data.day, actions, data.start_soc, pv_weather_factor_24h=data.pv_weather_factor_24h)
        for actions in (baseline, candidate)
    ]
    base_sim, candidate_sim = results
    if len(base_sim) != len(baseline) or len(candidate_sim) != len(baseline):
        raise ProposalRejected("simulation coverage mismatch")
    costs = []
    for simulation in results:
        cost = 0.0
        previous_soc = data.start_soc
        for point in simulation:
            if not all(math.isfinite(value) for value in (point.projected_soc, point.projected_grid_kwh)):
                raise ProposalRejected("non-finite simulation result")
            hour = point.timestamp.hour
            tariff = data.tariff_ore_per_hour[hour] if data.tariff_ore_per_hour else 0.0
            cost += planner._cost_ore(
                point.projected_grid_kwh, prices[point.timestamp.replace(tzinfo=None)], tariff
            )
            cost += abs(point.projected_soc - previous_soc) * planner.capacity_kwh / 100 * planner.degradation_cost_ore_per_kwh
            previous_soc = point.projected_soc
        costs.append(cost)
    if costs[1] > costs[0] + 0.01:
        raise ProposalRejected("Claude proposal has higher locally simulated cost")
    if candidate_sim[-1].projected_soc + 0.01 < base_sim[-1].projected_soc:
        raise ProposalRejected("Claude proposal leaves a lower final battery reserve")
    for original, updated, before, after in zip(baseline, candidate, base_sim, candidate_sim):
        if (original.start_time, original.end_time) != (updated.start_time, updated.end_time):
            raise ProposalRejected("Claude proposal changed plan periods")
        if original.action == "charge" and updated.action != "charge":
            raise ProposalRejected("Claude proposal removed a locally required charge slot")
        if original.is_manual_override and (
            original.action, original.charge_power_w, original.target_soc
        ) != (updated.action, updated.charge_power_w, updated.target_soc):
            raise ProposalRejected("Claude proposal changed a manual action")
        if not planner.min_soc <= after.projected_soc <= planner.max_soc:
            raise ProposalRejected("Claude proposal violates battery limits")
        if planner._is_reserve_hour(updated.start_time):
            floor = min(before.projected_soc, float(settings.reserve_soc_min_percent))
            if after.projected_soc + 0.01 < floor:
                raise ProposalRejected("Claude proposal reduces the required expensive-hour reserve")
        hour = updated.start_time.hour
        if settings.planning_sanity_expensive_window_enabled and planner._is_hour_in_window(
            hour, settings.planning_sanity_expensive_window_start_hour_local,
            settings.planning_sanity_expensive_window_end_hour_local,
        ) and after.projected_grid_kwh > before.projected_grid_kwh + 0.001:
            raise ProposalRejected("Claude proposal increases expensive-hour grid purchases")


async def plan_with_claude(planner: DayPlanner, data: PlannerInput) -> list[PlanAction]:
    baseline = planner.plan(data)
    if not claude_configured() or not baseline:
        return baseline
    try:
        proposal = await _request_proposal(_public_payload(data))
        candidate = _candidate(planner, data, baseline, proposal)
        _validate_locally(planner, data, baseline, candidate)
    except ProposalRejected as exc:
        logger.warning("Claude planning rejected for %s: %s; using existing planner", data.day, exc)
        _record(data, "rejected")
        return baseline
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "Claude planning HTTP %s for %s; using existing planner",
            exc.response.status_code, data.day,
        )
        _record(data, "unavailable")
        return baseline
    except (httpx.RequestError, ValueError, KeyError) as exc:
        # Never log API response bodies, prompts, headers or validation input.
        logger.warning(
            "Claude planning unavailable/rejected for %s (%s); using existing planner",
            data.day, type(exc).__name__,
        )
        _record(data, "unavailable")
        return baseline
    logger.info("Claude price proposal accepted after local validation for %s", data.day)
    _record(data, "accepted")
    return candidate
