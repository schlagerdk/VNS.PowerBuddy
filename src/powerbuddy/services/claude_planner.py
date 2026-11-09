from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import math
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.services.planner import DayPlanner, PlannerInput
from powerbuddy.services.planning_sanity import apply_planning_sanity

logger = logging.getLogger(__name__)


class ProposalRejected(ValueError):
    """A local, non-sensitive explanation of why a proposal cannot be used."""


class ProposalRegression(ProposalRejected):
    """A proposal can be retried locally with fewer suggested changes."""


_outcomes: ContextVar[dict[str, str] | None] = ContextVar("claude_planning_outcomes", default=None)
_COST_TOLERANCE_ORE = 0.01
_CLAUDE_REASON = "Claude price proposal (locally validated)"


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


def claude_action_signatures(actions: list[PlanAction]) -> set[tuple[datetime, datetime, str]]:
    return {
        (action.start_time.replace(tzinfo=None), action.end_time.replace(tzinfo=None), action.action)
        for action in actions
        if not action.is_manual_override and action.reason == _CLAUDE_REASON
    }


def reconcile_outcome(
    day: date,
    proposed_actions: set[tuple[datetime, datetime, str]],
    final_actions: list[PlanAction],
) -> None:
    if not proposed_actions or proposed_actions.intersection(claude_action_signatures(final_actions)):
        return
    logger.info("No Claude changes survived final plan processing for %s", day)
    collected = _outcomes.get()
    day_key = day.isoformat()
    if collected is not None and collected.get(day_key) == "accepted":
        collected[day_key] = "rejected"


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


def _unique_price_times(points: list[PricePoint]) -> set[datetime]:
    times = {point.timestamp.replace(tzinfo=None) for point in points}
    if len(times) != len(points):
        raise ProposalRejected("ambiguous public price timestamps")
    return times


def _overlaps_locally_protected(data: PlannerInput, action: PlanAction) -> bool:
    start = action.start_time.replace(tzinfo=None)
    end = action.end_time.replace(tzinfo=None)
    return any(
        start < protected.end_time.replace(tzinfo=None)
        and end > protected.start_time.replace(tzinfo=None)
        for protected in (data.locally_protected_actions or ())
    )


def _locally_protected(data: PlannerInput, action: PlanAction) -> bool:
    return (
        action.is_manual_override
        or action.action == "charge"
        or _overlaps_locally_protected(data, action)
    )


def _has_applied_change(baseline: list[PlanAction], candidate: list[PlanAction]) -> bool:
    original_by_period = {(action.start_time, action.end_time): action for action in baseline}
    return any(
        updated.reason == _CLAUDE_REASON
        and (original := original_by_period.get((updated.start_time, updated.end_time))) is not None
        and original.action != updated.action
        for updated in candidate
    )


def _public_payload(data: PlannerInput) -> dict[str, object]:
    # Explicit allowlist: never serialize PlannerInput, settings or ORM objects.
    points = sorted(data.price_points, key=lambda point: point.timestamp)
    if not points or len(points) > 25:
        raise ProposalRejected("unsupported price horizon")
    _unique_price_times(points)
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


def _map_candidate(
    planner: DayPlanner,
    data: PlannerInput,
    baseline: list[PlanAction],
    proposal: Proposal,
    selected_slots: set[int] | None = None,
) -> list[PlanAction]:
    points = sorted(data.price_points, key=lambda point: point.timestamp)
    if sorted(slot.slot for slot in proposal.slots) != list(range(len(points))):
        raise ProposalRejected("Claude proposal must cover every public price slot exactly once")
    available = _unique_price_times(points)
    proposed = {
        points[slot.slot].timestamp.replace(tzinfo=None): slot.action
        for slot in proposal.slots
        if selected_slots is None or slot.slot in selected_slots
    }
    candidate: list[PlanAction] = []
    for action in baseline:
        start = action.start_time.replace(tzinfo=None)
        if start not in available:
            raise ProposalRejected("local plan period does not match a public price slot")
        next_action = proposed.get(start, action.action)
        # Manual/locked intervals and local charge decisions remain local.
        if _locally_protected(data, action):
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
            reason=_CLAUDE_REASON if changed else action.reason,
            is_manual_override=action.is_manual_override,
        ))
    return candidate


def _candidate(
    planner: DayPlanner,
    data: PlannerInput,
    baseline: list[PlanAction],
    proposal: Proposal,
    selected_slots: set[int] | None = None,
) -> list[PlanAction]:
    candidate = _map_candidate(planner, data, baseline, proposal, selected_slots)
    candidate, _ = apply_planning_sanity(
        planner, data.day, candidate, data.price_points, data.start_soc,
        tariff_ore_per_hour=data.tariff_ore_per_hour,
        pv_weather_factor_24h=data.pv_weather_factor_24h,
        auto_fix=bool(settings.planning_sanity_autofix_enabled),
        protected_charge_starts={action.start_time for action in baseline if action.action == "charge"},
        protected_action_starts={
            action.start_time for action in baseline if _overlaps_locally_protected(data, action)
        },
        simulation_inputs=data.simulation_inputs,
        simulation_start_time=data.simulation_start_time,
    )
    return candidate


@dataclass(frozen=True)
class _LocalEvaluation:
    simulation: list[SimulationPoint]
    cost_ore: float


def _evaluate_locally(planner: DayPlanner, data: PlannerInput, actions: list[PlanAction]) -> _LocalEvaluation:
    prices = {point.timestamp.replace(tzinfo=None): point.price_ore_per_kwh for point in data.price_points}
    simulation = planner.simulate(
        data.day, actions, data.start_soc, pv_weather_factor_24h=data.pv_weather_factor_24h,
        simulation_inputs=data.simulation_inputs, simulation_start_time=data.simulation_start_time,
    )
    if len(simulation) != len(actions):
        raise ProposalRejected("simulation coverage mismatch")

    cost = 0.0
    previous_soc = data.start_soc
    for point in simulation:
        if not all(math.isfinite(value) for value in (point.projected_soc, point.projected_grid_kwh)):
            raise ProposalRejected("non-finite simulation result")
        hour = planner._local_hour(point.timestamp)
        tariff = data.tariff_ore_per_hour[hour] if data.tariff_ore_per_hour else 0.0
        price_time = point.timestamp.replace(tzinfo=None, minute=0, second=0, microsecond=0)
        if price_time not in prices:
            raise ProposalRejected("simulation period has no matching price")
        cost += planner._cost_ore(
            point.projected_grid_kwh, prices[price_time], tariff
        )
        cost += abs(point.projected_soc - previous_soc) * planner.capacity_kwh / 100 * planner.degradation_cost_ore_per_kwh
        previous_soc = point.projected_soc
    if not math.isfinite(cost):
        raise ProposalRejected("non-finite simulation result")
    return _LocalEvaluation(simulation=simulation, cost_ore=cost)


def _validate_locally(
    planner: DayPlanner,
    data: PlannerInput,
    baseline: list[PlanAction],
    candidate: list[PlanAction],
    *,
    baseline_evaluation: _LocalEvaluation | None = None,
    log_comparison: bool = False,
) -> float:
    if len(candidate) != len(baseline):
        raise ProposalRejected("local validation changed plan coverage")
    for original, updated in zip(baseline, candidate):
        if (original.start_time, original.end_time) != (updated.start_time, updated.end_time):
            raise ProposalRejected("Claude proposal changed plan periods")
        if original.action == "charge" and updated.action != "charge":
            raise ProposalRejected("Claude proposal removed a locally required charge slot")
        if original.is_manual_override and (
            original.action, original.charge_power_w, original.target_soc
        ) != (updated.action, updated.charge_power_w, updated.target_soc):
            raise ProposalRejected("Claude proposal changed a manual action")

    baseline_evaluation = baseline_evaluation or _evaluate_locally(planner, data, baseline)
    candidate_evaluation = (
        baseline_evaluation if candidate is baseline else _evaluate_locally(planner, data, candidate)
    )
    base_sim = baseline_evaluation.simulation
    candidate_sim = candidate_evaluation.simulation
    if log_comparison:
        logger.info(
            "Claude local comparison for %s (ore, including battery wear): existing=%.2f candidate=%.2f",
            data.day, baseline_evaluation.cost_ore, candidate_evaluation.cost_ore,
        )
    if candidate_evaluation.cost_ore > baseline_evaluation.cost_ore + _COST_TOLERANCE_ORE:
        raise ProposalRegression("Claude proposal has higher locally simulated cost")
    if candidate_sim[-1].projected_soc + 0.01 < base_sim[-1].projected_soc:
        raise ProposalRegression("Claude proposal leaves a lower final battery reserve")
    for updated, before, after in zip(candidate, base_sim, candidate_sim):
        if not planner.min_soc <= after.projected_soc <= planner.max_soc:
            raise ProposalRegression("Claude proposal violates battery limits")
        if planner._is_reserve_hour(updated.start_time):
            floor = min(before.projected_soc, float(settings.reserve_soc_min_percent))
            if after.projected_soc + 0.01 < floor:
                raise ProposalRegression("Claude proposal reduces the required expensive-hour reserve")
        hour = planner._local_hour(updated.start_time)
        if settings.planning_sanity_expensive_window_enabled and planner._is_hour_in_window(
            hour, settings.planning_sanity_expensive_window_start_hour_local,
            settings.planning_sanity_expensive_window_end_hour_local,
        ) and after.projected_grid_kwh > before.projected_grid_kwh + 0.001:
            raise ProposalRegression("Claude proposal increases expensive-hour grid purchases")
    return candidate_evaluation.cost_ore


def _safe_partial_candidate(
    planner: DayPlanner,
    data: PlannerInput,
    baseline: list[PlanAction],
    proposal: Proposal,
    prepare_for_comparison: Callable[[list[PlanAction]], list[PlanAction]],
    baseline_compared: list[PlanAction],
    baseline_evaluation: _LocalEvaluation,
) -> tuple[list[PlanAction], list[PlanAction], float] | None:
    _validate_locally(
        planner, data, baseline_compared, baseline_compared, baseline_evaluation=baseline_evaluation
    )
    points = sorted(data.price_points, key=lambda point: point.timestamp)
    baseline_by_start = {action.start_time.replace(tzinfo=None): action for action in baseline}
    slots = [
        slot for slot in sorted(proposal.slots, key=lambda item: item.slot)
        if (original := baseline_by_start.get(points[slot.slot].timestamp.replace(tzinfo=None))) is not None
        and not _locally_protected(data, original)
        and original.action != slot.action
    ]

    selected: set[int] = set()
    best_cost = baseline_evaluation.cost_ore
    best_candidate: tuple[list[PlanAction], list[PlanAction], float] | None = None
    for slot in slots:
        trial_slots = selected | {slot.slot}
        trial = _map_candidate(planner, data, baseline, proposal, trial_slots)
        trial_compared = prepare_for_comparison(trial)
        try:
            cost = _validate_locally(
                planner, data, baseline_compared, trial_compared, baseline_evaluation=baseline_evaluation
            )
        except ProposalRegression:
            continue
        if cost <= best_cost:
            selected = trial_slots
            best_cost = cost
            if (
                cost + _COST_TOLERANCE_ORE < baseline_evaluation.cost_ore
                and _has_applied_change(baseline_compared, trial_compared)
            ):
                best_candidate = (trial, trial_compared, cost)
    return best_candidate


def _http_error_diagnostic(response: httpx.Response) -> str | None:
    if response.status_code == 402:
        return "API billing issue; check Anthropic API billing"
    if response.status_code != 400:
        return None

    unknown = "invalid request; exact cause unavailable"
    try:
        body = response.json()
    except ValueError:
        return unknown
    error = body.get("error") if isinstance(body, dict) else None
    message = error.get("message") if isinstance(error, dict) else None
    if not isinstance(message, str):
        return unknown

    message = message.casefold()
    if "credit balance" in message and ("too low" in message or "insufficient" in message):
        return "API credit balance is too low; check Anthropic API billing"
    if message.startswith((
        "you have reached your specified api usage limits",
        "you have reached your specified workspace api usage limits",
    )):
        return "configured API spending limit reached; check Anthropic API billing"
    if "input_schema" in message or "tool_choice" in message:
        return "tool schema or tool choice was rejected"
    if "model" in message and any(
        phrase in message for phrase in ("does not exist", "not found", "not available", "not supported")
    ):
        return "configured model is unavailable or unsupported"
    return unknown


@dataclass(frozen=True, slots=True)
class PlanSelection:
    planned_actions: list[PlanAction]
    comparison_actions: list[PlanAction]


async def select_plan_with_claude(
    planner: DayPlanner,
    data: PlannerInput,
    prepare_for_comparison: Callable[[list[PlanAction]], list[PlanAction]],
) -> PlanSelection:
    configured = claude_configured()
    if data.price_points and data.simulation_inputs is None:
        data.simulation_inputs = planner.capture_simulation_inputs(data.day)
    baseline = planner.plan(data)
    if not configured or not baseline:
        return PlanSelection(baseline, prepare_for_comparison(baseline))

    try:
        proposal = await _request_proposal(_public_payload(data))
    except ProposalRejected as exc:
        logger.warning("Claude planning rejected for %s: %s; using existing planner", data.day, exc)
        _record(data, "rejected")
        return PlanSelection(baseline, prepare_for_comparison(baseline))
    except httpx.HTTPStatusError as exc:
        diagnostic = _http_error_diagnostic(exc.response)
        logger.warning(
            "Claude planning HTTP %s for %s%s; using existing planner",
            exc.response.status_code, data.day, f": {diagnostic}" if diagnostic else "",
        )
        _record(data, "unavailable")
        return PlanSelection(baseline, prepare_for_comparison(baseline))
    except (httpx.RequestError, ValueError, KeyError) as exc:
        # Never log API response bodies, prompts, headers or validation input.
        logger.warning(
            "Claude planning unavailable/rejected for %s (%s); using existing planner",
            data.day, type(exc).__name__,
        )
        _record(data, "unavailable")
        return PlanSelection(baseline, prepare_for_comparison(baseline))

    baseline_compared = prepare_for_comparison(baseline)
    partial = False
    try:
        if not baseline_compared:
            raise ProposalRejected("no remaining action periods to compare")
        candidate = _map_candidate(planner, data, baseline, proposal)
        candidate_compared = prepare_for_comparison(candidate)
        baseline_evaluation = _evaluate_locally(planner, data, baseline_compared)
        try:
            _validate_locally(
                planner, data, baseline_compared, candidate_compared,
                baseline_evaluation=baseline_evaluation, log_comparison=True,
            )
            if not _has_applied_change(baseline_compared, candidate_compared):
                raise ProposalRegression("Claude proposal made no surviving eligible changes")
        except ProposalRegression:
            partial_candidate = _safe_partial_candidate(
                planner, data, baseline, proposal, prepare_for_comparison,
                baseline_compared, baseline_evaluation,
            )
            if partial_candidate is None:
                logger.info("No safe cost-saving subset of the Claude proposal was found for %s", data.day)
                raise
            candidate, candidate_compared, partial_cost = partial_candidate
            logger.info(
                "Partial Claude local comparison for %s (ore, including battery wear): existing=%.2f candidate=%.2f",
                data.day, baseline_evaluation.cost_ore, partial_cost,
            )
            partial = True
        if not _has_applied_change(baseline_compared, candidate_compared):
            raise ProposalRejected("Claude proposal made no surviving eligible changes")
    except ProposalRejected as exc:
        logger.warning("Claude planning rejected for %s: %s; using existing planner", data.day, exc)
        _record(data, "rejected")
        return PlanSelection(baseline, baseline_compared)

    if partial:
        logger.info("Partial Claude price proposal accepted after local validation for %s", data.day)
    else:
        logger.info("Claude price proposal accepted after local validation for %s", data.day)
    _record(data, "accepted")
    return PlanSelection(candidate, candidate_compared)


async def plan_with_claude(planner: DayPlanner, data: PlannerInput) -> list[PlanAction]:
    if not claude_configured():
        return planner.plan(data)

    baseline: list[PlanAction] | None = None
    protected_charge_starts: set[datetime] = set()

    def prepare(actions: list[PlanAction]) -> list[PlanAction]:
        nonlocal baseline, protected_charge_starts
        if baseline is None:
            baseline = actions
            protected_charge_starts = {action.start_time for action in actions if action.action == "charge"}
        if not settings.planning_sanity_enabled:
            return actions
        copied = [
            PlanAction(
                date_key=action.date_key, start_time=action.start_time, end_time=action.end_time,
                action=action.action, charge_power_w=action.charge_power_w,
                target_soc=action.target_soc, reason=action.reason,
                is_manual_override=action.is_manual_override,
            )
            for action in actions
        ]
        copied, _ = apply_planning_sanity(
            planner, data.day, copied, data.price_points, data.start_soc,
            tariff_ore_per_hour=data.tariff_ore_per_hour,
            pv_weather_factor_24h=data.pv_weather_factor_24h,
            auto_fix=bool(settings.planning_sanity_autofix_enabled),
            protected_charge_starts=protected_charge_starts,
            protected_action_starts={
                action.start_time for action in copied if _overlaps_locally_protected(data, action)
            },
            simulation_inputs=data.simulation_inputs,
            simulation_start_time=data.simulation_start_time,
        )
        return copied

    selection = await select_plan_with_claude(planner, data, prepare)
    return selection.planned_actions if selection.planned_actions is baseline else selection.comparison_actions
