from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
import json
import logging

import httpx
import pytest
from pydantic import SecretStr

from powerbuddy.config import Settings, settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.services import claude_planner as ai
from powerbuddy.services.planner import DayPlanner, PlannerInput
from powerbuddy.services.scheduler import PowerBuddyScheduler


pytestmark = pytest.mark.usefixtures("isolated_forecasts")


@pytest.fixture
def planning(monkeypatch):
    monkeypatch.setattr(settings, "claude_api_key", SecretStr("test-key-not-real"))
    monkeypatch.setattr(settings, "planning_sanity_enabled", False)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", False)
    day = date(2026, 11, 12)
    points = [
        PricePoint(
            timestamp=datetime.combine(day, datetime.min.time()) + timedelta(hours=hour),
            area="DK2", price_ore_per_kwh=float(10 + hour),
            currency="DKK", source="elprisenligenu",
        )
        for hour in range(3)
    ]
    data = PlannerInput(
        day=day, price_points=points, start_soc=57.3,
        tariff_ore_per_hour=[99.7] * 24, pv_weather_factor_24h=[0.763] * 24,
    )
    baseline = [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1),
            action="hold", reason="local baseline", is_manual_override=False,
        )
        for point in points
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "plan", lambda supplied: baseline)
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
            projected_soc=50.0, projected_grid_kwh=1.0,
        )
        for action in actions
    ])
    return planner, data, baseline


def proposal(actions=("auto", "auto", "auto")):
    return ai.Proposal(slots=[
        ai.SlotProposal(slot=index, action=action) for index, action in enumerate(actions)
    ])


def mock_api(monkeypatch, response=None, status=200, error=None):
    captured = []
    real_client = httpx.AsyncClient

    def handle(request):
        captured.append(request)
        if error is not None:
            raise error
        return httpx.Response(status, json=response)

    monkeypatch.setattr(ai.httpx, "AsyncClient", lambda **kwargs: real_client(
        **kwargs, transport=httpx.MockTransport(handle)
    ))
    return captured


def tool_response(value=None):
    return {
        "stop_reason": "tool_use",
        "content": [{"type": "tool_use", "name": "submit_plan", "input": value or proposal().model_dump()}],
    }


def test_no_key_preserves_old_planner_without_request(planning, monkeypatch):
    planner, data, baseline = planning
    monkeypatch.setattr(settings, "claude_api_key", SecretStr(""))
    requests = mock_api(monkeypatch)
    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert not requests


def test_no_key_selection_reuses_snapshot_for_baseline_and_processing(planning, monkeypatch):
    planner, data, baseline = planning
    monkeypatch.setattr(settings, "claude_api_key", SecretStr(""))
    reads = []
    original_capture = planner.capture_simulation_inputs

    def capture(day):
        reads.append(day)
        return original_capture(day)

    def prepare(actions):
        assert data.simulation_inputs is not None
        planner.simulate(
            data.day, actions, data.start_soc, simulation_inputs=data.simulation_inputs,
        )
        return actions

    monkeypatch.setattr(planner, "capture_simulation_inputs", capture)
    selection = asyncio.run(ai.select_plan_with_claude(planner, data, prepare))

    assert selection.planned_actions is baseline
    assert reads == [data.day]


@pytest.mark.parametrize("manual", [True, False])
def test_protected_half_hour_is_not_sent_or_counted_as_an_ai_change(planning, monkeypatch, manual):
    planner, original_data, baseline = planning
    protected = PlanAction(
        date_key=original_data.day.isoformat(),
        start_time=baseline[0].start_time + timedelta(minutes=30),
        end_time=baseline[0].end_time,
        action="hold", reason="private-protected-action", target_soc=71.234,
        is_manual_override=manual,
    )
    data = PlannerInput(
        day=original_data.day, price_points=original_data.price_points,
        start_soc=original_data.start_soc, locally_protected_actions=[protected],
    )
    requests = mock_api(monkeypatch, tool_response(proposal(("auto", "hold", "hold")).model_dump()))

    async def run():
        with ai.collect_outcomes() as outcomes:
            result = await ai.plan_with_claude(planner, data)
        return result, outcomes

    result, outcomes = asyncio.run(run())
    assert result is baseline
    assert outcomes[data.day.isoformat()] == "rejected"
    assert "private-protected-action" not in requests[0].content.decode()
    assert "71.234" not in requests[0].content.decode()


def test_ai_can_change_unprotected_hours_but_not_manual_hours(planning, monkeypatch):
    planner, original_data, baseline = planning
    protected = PlanAction(
        date_key=original_data.day.isoformat(),
        start_time=baseline[0].start_time, end_time=baseline[0].end_time,
        action="hold", is_manual_override=True,
    )
    data = PlannerInput(
        day=original_data.day, price_points=original_data.price_points,
        start_soc=original_data.start_soc, locally_protected_actions=[protected],
    )
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))
    result = asyncio.run(ai.plan_with_claude(planner, data))
    assert [action.action for action in result] == ["hold", "auto", "hold"]


def test_sanity_does_not_change_a_manual_half_hours_baseline(planning, monkeypatch):
    planner, original_data, baseline = planning
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_autofix_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_precheap_arbitrage_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_start_hour_local", 1)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_end_hour_local", 2)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 1.0)
    protected = PlanAction(
        date_key=original_data.day.isoformat(),
        start_time=baseline[1].start_time, end_time=baseline[1].start_time + timedelta(minutes=30),
        action="hold", is_manual_override=True,
    )
    data = PlannerInput(
        day=original_data.day, price_points=original_data.price_points,
        start_soc=original_data.start_soc, locally_protected_actions=[protected],
    )
    candidate = ai._candidate(planner, data, baseline, proposal(("auto", "hold", "hold")))
    assert candidate[1].action == baseline[1].action == "hold"


def test_outcome_is_reconciled_after_final_postprocessing(planning, caplog):
    _, data, baseline = planning
    accepted = [PlanAction(
        date_key=data.day.isoformat(), start_time=baseline[0].start_time,
        end_time=baseline[0].end_time, action="auto",
        reason="Claude price proposal (locally validated)", is_manual_override=False,
    )]
    with ai.collect_outcomes() as outcomes:
        ai._record(data, "accepted")
        ai.reconcile_outcome(data.day, ai.claude_action_signatures(accepted), baseline)
    assert outcomes[data.day.isoformat()] == "rejected"
    caplog.clear()
    with caplog.at_level(logging.INFO):
        ai.reconcile_outcome(data.day, ai.claude_action_signatures(accepted), baseline)
    assert "No Claude changes survived final plan processing" in caplog.text


def test_request_allowlist_and_valid_plan(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    requests = mock_api(monkeypatch, tool_response())
    with caplog.at_level(logging.INFO):
        result = asyncio.run(ai.plan_with_claude(planner, data))
    assert [action.action for action in result] == ["auto"] * 3
    assert [action.action for action in baseline] == ["hold"] * 3
    body = json.loads(requests[0].content)
    public = json.loads(body["messages"][0]["content"])
    assert set(public) == {"slots", "rules"}
    assert set(public["slots"][0]) == {"slot", "time", "spot_ore_per_kwh"}
    assert set(public["rules"]) == {
        "winter", "winter_cheapest_hours", "winter_additional_night_hours",
        "night_start_hour", "night_end_hour",
    }
    assert "57.3" not in json.dumps(body)
    assert "99.7" not in json.dumps(body)
    assert "0.763" not in json.dumps(body)
    assert "test-key-not-real" not in json.dumps(body) + caplog.text
    assert requests[0].headers["x-api-key"] == "test-key-not-real"
    assert body["tool_choice"] == {"type": "tool", "name": "submit_plan"}
    assert "accepted after local validation" in caplog.text


@pytest.mark.parametrize("status", [401, 429, 500])
def test_http_error_falls_back_and_logs_no_response_body(planning, monkeypatch, caplog, status):
    planner, data, baseline = planning
    mock_api(monkeypatch, {"error": "private-response-must-not-be-logged"}, status=status)
    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert "using existing planner" in caplog.text
    assert "private-response-must-not-be-logged" not in caplog.text


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        (
            "Your credit balance is too low to access the Anthropic API.",
            "API credit balance is too low",
        ),
        (
            "You have reached your specified API usage limits: private details.",
            "configured API spending limit reached",
        ),
        (
            "You have reached your specified workspace API usage limits: private details.",
            "configured API spending limit reached",
        ),
        (
            "tools.0.input_schema: invalid schema containing private details",
            "tool schema or tool choice was rejected",
        ),
        (
            "model: the requested model does not exist: private details",
            "configured model is unavailable or unsupported",
        ),
        ("unexpected private details", "invalid request; exact cause unavailable"),
    ],
)
def test_http_400_reports_safe_diagnostic(planning, monkeypatch, caplog, message, reason):
    planner, data, baseline = planning
    response = {
        "type": "error",
        "error": {"type": "invalid_request_error", "message": message},
        "private": "do-not-log-this-response",
    }
    mock_api(monkeypatch, response, status=400)

    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert "Claude planning HTTP 400" in caplog.text
    assert reason in caplog.text
    assert message not in caplog.text
    assert "do-not-log-this-response" not in caplog.text
    assert "test-key-not-real" not in caplog.text


@pytest.mark.parametrize("body", [b"invalid JSON", b'{"error": null}', b"[]"])
def test_http_400_without_usable_error_body_is_safe(body):
    response = httpx.Response(400, content=body)
    assert ai._http_error_diagnostic(response) == "invalid request; exact cause unavailable"


def test_http_402_reports_billing_without_response_body(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    mock_api(monkeypatch, {"error": "private billing response"}, status=402)
    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert "API billing issue" in caplog.text
    assert "private billing response" not in caplog.text


def test_timeout_falls_back(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    mock_api(monkeypatch, error=httpx.ReadTimeout("timeout"))
    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert "ReadTimeout" in caplog.text


@pytest.mark.parametrize("response", [
    {"stop_reason": "max_tokens", "content": []},
    {"stop_reason": "end_turn", "content": [{"type": "text", "text": "No plan"}]},
    tool_response({"slots": [{"slot": 0, "action": "discharge"}]}),
    tool_response({"slots": [{"slot": 0, "action": "auto"}]}),
    tool_response({"slots": [{"slot": 0, "action": "auto"}] * 3}),
    tool_response({"slots": [{"slot": 99, "action": "auto"}]}),
    tool_response({"slots": [{"slot": True, "action": "auto"}]}),
])
def test_invalid_proposal_preserves_baseline(planning, monkeypatch, caplog, response):
    planner, data, baseline = planning
    mock_api(monkeypatch, response)
    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert "using existing planner" in caplog.text


def test_provisional_prices_not_sent(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    data.price_points[0].source = "dummy"
    requests = mock_api(monkeypatch)
    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert not requests
    assert "using existing planner" in caplog.text
    assert "non-provisional price data" in caplog.text


def test_locally_worse_proposal_uses_baseline(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    mock_api(monkeypatch, tool_response())

    def simulate(day, actions, start_soc, **kwargs):
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0, projected_grid_kwh=1.0 if actions is baseline else 2.0,
            )
            for action in actions
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    assert asyncio.run(ai.plan_with_claude(planner, data)) is baseline
    assert "higher locally simulated cost" in caplog.text


def test_worse_full_proposal_uses_only_cost_saving_parts(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    requests = mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        grids = [
            0.0 if actions[0].action == "auto" else 1.0,
            3.0 if actions[1].action == "auto" else 1.0,
            1.0,
        ]
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0, projected_grid_kwh=grids[index],
            )
            for index, action in enumerate(actions)
        ]

    monkeypatch.setattr(planner, "simulate", simulate)

    async def run():
        with ai.collect_outcomes() as outcomes:
            result = await ai.plan_with_claude(planner, data)
        return result, outcomes

    with caplog.at_level(logging.INFO):
        result, outcomes = asyncio.run(run())
    assert [action.action for action in result] == ["auto", "hold", "hold"]
    assert outcomes[data.day.isoformat()] == "accepted"
    assert "Partial Claude price proposal" in caplog.text
    assert [action.action for action in baseline] == ["hold", "hold", "hold"]
    assert len(requests) == 1


def test_comparison_reuses_one_snapshot_for_both_plans(planning, monkeypatch):
    planner, data, baseline = planning
    reads = {"daily": 0, "consumption": 0, "pv": 0}
    simulated = []

    def daily(day):
        reads["daily"] += 1
        return (51.234 if reads["daily"] == 1 else 120.0, "test")

    def consumption(day):
        reads["consumption"] += 1
        return ([1 / 24] * 24, "test")

    def pv(day):
        reads["pv"] += 1
        return ([0.0] * 24, "test")

    def simulate(day, actions, start_soc, **kwargs):
        simulated.append((day, start_soc, kwargs["simulation_inputs"], kwargs["pv_weather_factor_24h"]))
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0, projected_grid_kwh=0.5 if action.action == "auto" else 1.0,
            )
            for action in actions
        ]

    monkeypatch.setattr(planner, "resolve_expected_daily_consumption", daily)
    monkeypatch.setattr(planner, "resolve_hourly_consumption_profile", consumption)
    monkeypatch.setattr(planner, "resolve_hourly_pv_profile", pv)
    monkeypatch.setattr(planner, "simulate", simulate)
    requests = mock_api(monkeypatch, tool_response(proposal(("auto", "hold", "hold")).model_dump()))

    selection = asyncio.run(ai.select_plan_with_claude(
        planner, data, PowerBuddyScheduler._dedupe_effective_actions,
    ))

    assert selection.planned_actions is not baseline
    assert reads == {"daily": 1, "consumption": 1, "pv": 1}
    assert len(simulated) >= 2
    assert all(day == data.day and soc == data.start_soc for day, soc, _, _ in simulated)
    assert all(snapshot is data.simulation_inputs and weather is data.pv_weather_factor_24h
               for _, _, snapshot, weather in simulated)
    assert data.simulation_inputs is not None
    assert data.simulation_inputs.expected_daily_consumption_kwh == 51.234
    assert "51.234" not in requests[0].content.decode()


def test_no_remaining_periods_falls_back_without_simulating(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    mock_api(monkeypatch, tool_response())

    with caplog.at_level(logging.WARNING):
        selection = asyncio.run(ai.select_plan_with_claude(planner, data, lambda actions: []))

    assert selection.planned_actions is baseline
    assert selection.comparison_actions == []
    assert "no remaining action periods to compare" in caplog.text


def test_legacy_wrapper_returns_sanity_processed_accepted_proposal(planning, monkeypatch):
    planner, data, _ = planning
    mock_api(monkeypatch, tool_response(proposal(("auto", "hold", "hold")).model_dump()))
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    calls = []

    def sanity(planner, day, actions, prices, start_soc, **kwargs):
        calls.append([action.action for action in actions])
        for action in actions:
            action.target_soc = 70.0
        return actions, {}

    monkeypatch.setattr(ai, "apply_planning_sanity", sanity)
    result = asyncio.run(ai.plan_with_claude(planner, data))

    assert calls[:2] == [["hold", "hold", "hold"], ["auto", "hold", "hold"]]
    assert [action.action for action in result] == ["auto", "hold", "hold"]
    assert all(action.target_soc == 70.0 for action in result)


@pytest.mark.parametrize("candidate_worse_after_processing", [True, False])
def test_comparison_scores_both_plans_after_same_processing(planning, monkeypatch, candidate_worse_after_processing):
    planner, data, baseline = planning
    mock_api(monkeypatch, tool_response(proposal(("auto", "hold", "hold")).model_dump()))
    monkeypatch.setattr(planner, "degradation_cost_ore_per_kwh", 0.0)
    monkeypatch.setattr(planner, "_cost_ore", lambda grid, price, tariff=0.0: grid)
    prepared = []

    def simulate(day, actions, start_soc, **kwargs):
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0,
                projected_grid_kwh=(
                    10.0 if action.start_time == baseline[2].start_time and action.action == "hold"
                    else 0.0 if action.action == "auto" else 1.0
                ),
            )
            for action in actions
        ]

    def prepare(actions):
        result = PowerBuddyScheduler._dedupe_effective_actions(actions)
        if (result[0].action == "auto") == candidate_worse_after_processing:
            result[2].action = "hold"
        else:
            result[2].action = "auto"
        result[2].reason = "local postprocessing"
        prepared.append(result)
        return result

    monkeypatch.setattr(planner, "simulate", simulate)
    selection = asyncio.run(ai.select_plan_with_claude(planner, data, prepare))

    assert prepared[0][0].action == "hold"
    assert prepared[1][0].action == "auto"
    assert (selection.planned_actions is baseline) is candidate_worse_after_processing


def test_half_hour_simulations_use_the_hourly_price(planning, monkeypatch):
    planner, data, baseline = planning
    monkeypatch.setattr(planner, "degradation_cost_ore_per_kwh", 0.0)
    monkeypatch.setattr(planner, "_cost_ore", lambda grid, price, tariff=0.0: grid * (price + tariff))
    start = baseline[0].start_time
    halves = [
        PlanAction(
            date_key=data.day.isoformat(), start_time=start + timedelta(minutes=offset),
            end_time=start + timedelta(minutes=offset + 30), action="hold", is_manual_override=False,
        )
        for offset in (0, 30)
    ]
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
            projected_soc=50.0, projected_grid_kwh=0.5,
        )
        for action in actions
    ])

    result = ai._evaluate_locally(planner, data, halves)

    assert result.cost_ore == pytest.approx(data.price_points[0].price_ore_per_kwh + 99.7)


def test_safe_partial_survives_full_scheduler_pipeline_and_manual_half(monkeypatch):
    monkeypatch.setattr(settings, "claude_api_key", SecretStr("test-key-not-real"))
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_autofix_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_precheap_arbitrage_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_pv_credit_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_expensive_quantile", 0.9)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 0)
    monkeypatch.setattr(settings, "planning_sanity_target_soc_percent", 5)
    monkeypatch.setattr(settings, "must_charge_window_min_soc_percent", 5)
    monkeypatch.setattr(settings, "reserve_soc_min_percent", 5)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    monkeypatch.setattr(settings, "planning_variant_search_enabled", True)
    monkeypatch.setattr(settings, "planning_variant_search_max_precheap_auto_hours", 0)
    monkeypatch.setattr(settings, "planning_variant_search_max_cheap_charge_hours", 0)
    day = date(2030, 11, 12)
    start = datetime.combine(day, datetime.min.time())
    prices = [
        PricePoint(
            timestamp=start + timedelta(hours=hour), area="DK2",
            price_ore_per_kwh=float(hour + 1), currency="DKK", source="elprisenligenu",
        )
        for hour in range(12)
    ]
    baseline = [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1),
            action="charge" if hour < 6 else "hold" if hour == 8 else "auto",
            charge_power_w=1000.0 if hour < 6 else None,
            reason="winter cheap-hour charging floor" if hour < 6 else "local baseline",
            is_manual_override=False,
        )
        for hour, point in enumerate(prices)
    ]
    manual = PlanAction(
        date_key=day.isoformat(), start_time=start, end_time=start + timedelta(minutes=30),
        action="hold", reason="manual", is_manual_override=True,
    )
    stored = [manual]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "plan", lambda data: baseline)
    simulated = []

    def simulate(day, actions, start_soc, **kwargs):
        simulated.append(([
            (action.start_time.hour, action.start_time.minute, action.action, action.is_manual_override)
            for action in actions
        ], kwargs.get("simulation_inputs")))
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0,
                projected_grid_kwh=(
                    0.0 if action.start_time.hour == 8 and action.action == "auto"
                    else 3.0 if action.start_time.hour == 9 and action.action == "hold"
                    else 1.0
                ),
            )
            for action in sorted(actions, key=lambda action: action.start_time)
        ]

    async def no_tariff():
        return [0.0] * 24

    async def no_weather(day):
        return None

    def replace_plan(day_key, actions):
        stored[:] = [manual, *actions]

    monkeypatch.setattr(planner, "simulate", simulate)
    monkeypatch.setattr("powerbuddy.services.scheduler.tariff_service.get_network_tariff_24h", no_tariff)
    monkeypatch.setattr("powerbuddy.services.scheduler.tariff_service.total_tariff_ore_24h", lambda tariffs: tariffs)
    monkeypatch.setattr("powerbuddy.services.scheduler.weather_forecast_service.get_hourly_pv_factor_24h", no_weather)
    monkeypatch.setattr("powerbuddy.services.scheduler.PlanRepository.get_plan", lambda day_key: list(stored))
    monkeypatch.setattr("powerbuddy.services.scheduler.PlanRepository.replace_plan", replace_plan)
    monkeypatch.setattr("powerbuddy.services.scheduler.SimulationRepository.replace_points", lambda *args: None)
    requests = mock_api(
        monkeypatch, tool_response(proposal(
            ("charge",) * 6 + ("auto", "auto", "auto", "hold", "auto", "auto")
        ).model_dump())
    )
    scheduler = object.__new__(PowerBuddyScheduler)
    scheduler.planner = planner

    async def run():
        with ai.collect_outcomes() as outcomes:
            await scheduler._plan_and_simulate(day, prices, 50.0)
        return outcomes

    outcomes = asyncio.run(run())
    assert outcomes[day.isoformat()] == "accepted"
    assert len(requests) == 1
    assert next(action for action in stored if action.start_time.hour == 8).action == "auto"
    assert next(action for action in stored if action.start_time.hour == 9).action == "auto"
    assert manual in stored and manual.action == "hold"
    remainder = next(action for action in stored if action.start_time == start + timedelta(minutes=30))
    assert remainder.action == "charge" and remainder.reason == "winter cheap-hour charging floor"
    assert {action.start_time.hour for action in stored if action.action == "charge"} == set(range(6))
    snapshots = [snapshot for _, snapshot in simulated]
    assert snapshots and snapshots[0] is not None and all(snapshot is snapshots[0] for snapshot in snapshots)
    assert all(
        any(hour == 0 and minute == 30 and mode == "charge" for hour, minute, mode, _ in actions)
        for actions, _ in simulated
    )
    final_actions = simulated[-1][0]
    assert [entry[1:] for entry in final_actions if entry[0] == 0] == [
        (0, "hold", True), (30, "charge", False),
    ]
    assert any(
        any(hour == 8 and mode == "hold" for hour, _, mode, _ in actions)
        and any(hour == 0 and minute == 30 for hour, minute, _, _ in actions)
        for actions, _ in simulated
    )
    assert any(
        any(hour == 8 and mode == "auto" for hour, _, mode, _ in actions)
        and any(hour == 0 and minute == 30 for hour, minute, _, _ in actions)
        for actions, _ in simulated
    )


@pytest.mark.parametrize(("saving", "uses_partial"), [(0.009, False), (0.01, False), (0.011, True)])
def test_partial_proposal_requires_more_than_001_ore_saving(planning, monkeypatch, saving, uses_partial):
    planner, data, baseline = planning
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))
    monkeypatch.setattr(planner, "degradation_cost_ore_per_kwh", 0.0)
    monkeypatch.setattr(planner, "_cost_ore", lambda grid, price, tariff=0.0: grid)

    def simulate(day, actions, start_soc, **kwargs):
        grids = [
            1.0 - saving if actions[0].action == "auto" else 1.0,
            2.0 if actions[1].action == "auto" else 1.0,
            1.0,
        ]
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0, projected_grid_kwh=grids[index],
            )
            for index, action in enumerate(actions)
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    result = asyncio.run(ai.plan_with_claude(planner, data))
    if uses_partial:
        assert [action.action for action in result] == ["auto", "hold", "hold"]
    else:
        assert result is baseline


def test_partial_proposal_never_reduces_final_reserve(planning, monkeypatch):
    planner, data, _ = planning
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        final_soc = 49.0 if actions[0].action == "auto" else 50.0
        grids = [
            0.0 if actions[0].action == "auto" else 1.0,
            0.0 if actions[1].action == "auto" else 1.0,
            1.0,
        ]
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=final_soc if index == 2 else 50.0,
                projected_grid_kwh=grids[index],
            )
            for index, action in enumerate(actions)
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    result = asyncio.run(ai.plan_with_claude(planner, data))
    assert [action.action for action in result] == ["hold", "auto", "hold"]


def test_partial_proposal_does_not_increase_expensive_window_import(planning, monkeypatch):
    planner, data, _ = planning
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_start_hour_local", 0)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_end_hour_local", 1)
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        grids = [
            1.5 if actions[0].action == "auto" else 1.0,
            0.0 if actions[1].action == "auto" else 1.0,
            0.0 if actions[0].action == "auto" else 1.0,
        ]
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0, projected_grid_kwh=grids[index],
            )
            for index, action in enumerate(actions)
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    result = asyncio.run(ai.plan_with_claude(planner, data))
    assert [action.action for action in result] == ["hold", "auto", "hold"]


def test_partial_proposal_never_violates_battery_limits(planning, monkeypatch):
    planner, data, _ = planning
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=101.0 if index == 0 and action.action == "auto" else 50.0,
                projected_grid_kwh=0.0 if action.action == "auto" else 1.0,
            )
            for index, action in enumerate(actions)
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    result = asyncio.run(ai.plan_with_claude(planner, data))
    assert [action.action for action in result] == ["hold", "auto", "hold"]


def test_partial_search_handles_remaining_day_horizon(planning, monkeypatch):
    planner, data, baseline = planning
    data.planning_start_time = baseline[1].start_time
    monkeypatch.setattr(planner, "plan", lambda supplied: baseline[1:])
    mock_api(monkeypatch, tool_response(proposal().model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0,
                projected_grid_kwh=(
                    0.0 if index == 0 and action.action == "auto"
                    else 3.0 if index == 1 and action.action == "auto" else 1.0
                ),
            )
            for index, action in enumerate(actions)
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    result = asyncio.run(ai.plan_with_claude(planner, data))
    assert [action.start_time for action in result] == [baseline[1].start_time, baseline[2].start_time]
    assert [action.action for action in result] == ["auto", "hold"]


def test_local_cost_uses_local_hour_for_tariffs(monkeypatch):
    monkeypatch.setattr(settings, "timezone", "Europe/Copenhagen")
    day = date(2026, 11, 12)
    start = datetime(2026, 11, 12, 17, tzinfo=timezone.utc)
    point = PricePoint(timestamp=start, area="DK2", price_ore_per_kwh=0.0, source="elprisenligenu")
    action = PlanAction(
        date_key=day.isoformat(), start_time=start, end_time=start + timedelta(hours=1),
        action="hold", is_manual_override=False,
    )
    tariffs = [0.0] * 24
    tariffs[17], tariffs[18] = 10.0, 77.0
    data = PlannerInput(day=day, price_points=[point], start_soc=50.0, tariff_ore_per_hour=tariffs)
    planner = DayPlanner()
    monkeypatch.setattr(planner, "_cost_ore", lambda grid, price, tariff: tariff)
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=start, action=actions[0].action,
            projected_soc=50.0, projected_grid_kwh=1.0,
        )
    ])
    assert ai._evaluate_locally(planner, data, [action]).cost_ore == 77.0


def test_local_expensive_window_validation_uses_local_hour(monkeypatch):
    monkeypatch.setattr(settings, "timezone", "Europe/Copenhagen")
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_start_hour_local", 18)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_end_hour_local", 19)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    day = date(2026, 11, 12)
    start = datetime(2026, 11, 12, 17, tzinfo=timezone.utc)
    point = PricePoint(timestamp=start, area="DK2", price_ore_per_kwh=0.0, source="elprisenligenu")
    baseline = [PlanAction(
        date_key=day.isoformat(), start_time=start, end_time=start + timedelta(hours=1),
        action="hold", is_manual_override=False,
    )]
    candidate = [PlanAction(
        date_key=day.isoformat(), start_time=start, end_time=start + timedelta(hours=1),
        action="auto", is_manual_override=False,
    )]
    data = PlannerInput(day=day, price_points=[point], start_soc=50.0)
    planner = DayPlanner()
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=start, action=actions[0].action,
            projected_soc=50.0, projected_grid_kwh=2.0 if actions[0].action == "auto" else 1.0,
        )
    ])
    with pytest.raises(ai.ProposalRegression, match="expensive-hour grid purchases"):
        ai._validate_locally(planner, data, baseline, candidate)


def test_public_payload_rejects_ambiguous_wall_clock_slots(planning):
    _, data, _ = planning
    day = date(2026, 10, 25)
    data.day = day
    data.price_points = [
        PricePoint(
            timestamp=datetime(2026, 10, 25, 2, tzinfo=timezone(timedelta(hours=offset))),
            area="DK2", price_ore_per_kwh=float(offset), source="elprisenligenu",
        )
        for offset in (2, 1)
    ]
    with pytest.raises(ai.ProposalRejected, match="ambiguous public price timestamps"):
        ai._public_payload(data)


def test_partial_search_has_bounded_simulations_and_one_api_request(monkeypatch):
    monkeypatch.setattr(settings, "claude_api_key", SecretStr("test-key-not-real"))
    monkeypatch.setattr(settings, "planning_sanity_enabled", False)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", False)
    day = date(2026, 11, 12)
    points = [
        PricePoint(
            timestamp=datetime.combine(day, datetime.min.time()) + timedelta(hours=hour),
            area="DK2", price_ore_per_kwh=100.0, currency="DKK", source="elprisenligenu",
        )
        for hour in range(24)
    ]
    baseline = [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1),
            action="hold", reason="local baseline", is_manual_override=False,
        )
        for point in points
    ]
    data = PlannerInput(day=day, price_points=points, start_soc=50.0)
    planner = DayPlanner()
    monkeypatch.setattr(planner, "plan", lambda supplied: baseline)
    calls = []

    def simulate(day, actions, start_soc, **kwargs):
        calls.append(1)
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0,
                projected_grid_kwh=(
                    5.0 if index == 23 and action.action == "auto"
                    else 0.9 if action.action == "auto" else 1.0
                ),
            )
            for index, action in enumerate(actions)
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    requests = mock_api(monkeypatch, tool_response(proposal(("auto",) * len(points)).model_dump()))
    result = asyncio.run(ai.plan_with_claude(planner, data))
    assert [action.action for action in result] == ["auto"] * 23 + ["hold"]
    assert len(requests) == 1
    assert len(calls) <= 2 * len(points) + 2


def test_partial_proposal_is_revalidated_after_sanity(planning, monkeypatch, caplog):
    planner, data, baseline = planning
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        grids = [
            0.0 if actions[0].action == "auto" else 1.0,
            3.0 if actions[1].action == "auto" else 1.0,
            10.0 if actions[2].action == "charge" else 1.0,
        ]
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0, projected_grid_kwh=grids[index],
            )
            for index, action in enumerate(actions)
        ]

    def unsafe_sanity(planner, day, actions, prices, start_soc, **kwargs):
        if actions[0].action == "auto":
            actions[2].action = "charge"
        return actions, {}

    monkeypatch.setattr(planner, "simulate", simulate)
    monkeypatch.setattr(ai, "apply_planning_sanity", unsafe_sanity)

    def prepare(actions):
        copied = PowerBuddyScheduler._dedupe_effective_actions(actions)
        return ai.apply_planning_sanity(planner, data.day, copied, data.price_points, data.start_soc)[0]

    selection = asyncio.run(ai.select_plan_with_claude(planner, data, prepare))
    assert selection.planned_actions is baseline
    assert "using existing planner" in caplog.text


def test_partial_proposal_is_not_reported_used_if_sanity_removes_its_changes(planning, monkeypatch):
    planner, data, baseline = planning
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "hold")).model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        grids = [
            0.9 if actions[0].action == "auto" else 1.0,
            2.0 if actions[1].action == "auto" else 1.0,
            0.5 if actions[2].action == "auto" else 1.0,
        ]
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0, projected_grid_kwh=grids[index],
            )
            for index, action in enumerate(actions)
        ]

    def unrelated_sanity(planner, day, actions, prices, start_soc, **kwargs):
        actions[0].action = "hold"
        actions[2].action = "auto"
        return actions, {}

    monkeypatch.setattr(planner, "simulate", simulate)
    monkeypatch.setattr(ai, "apply_planning_sanity", unrelated_sanity)

    def prepare(actions):
        copied = PowerBuddyScheduler._dedupe_effective_actions(actions)
        return ai.apply_planning_sanity(planner, data.day, copied, data.price_points, data.start_soc)[0]

    selection = asyncio.run(ai.select_plan_with_claude(planner, data, prepare))
    assert selection.planned_actions is baseline


def test_partial_search_keeps_manual_and_required_charges(planning, monkeypatch):
    planner, data, baseline = planning
    baseline[0].action = "charge"
    baseline[0].charge_power_w = 1234.0
    baseline[0].target_soc = 80.0
    baseline[1].is_manual_override = True
    mock_api(monkeypatch, tool_response(proposal(("auto", "auto", "auto")).model_dump()))

    def simulate(day, actions, start_soc, **kwargs):
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=50.0,
                projected_grid_kwh=2.0 if actions[2].action == "auto" else 1.0,
            )
            for action in actions
        ]

    monkeypatch.setattr(planner, "simulate", simulate)
    result = asyncio.run(ai.plan_with_claude(planner, data))
    assert result is baseline
    assert result[0].charge_power_w == 1234.0 and result[0].target_soc == 80.0
    assert result[1].action == "hold" and result[1].is_manual_override


def test_partial_replan_maps_full_public_day_locally(planning):
    planner, data, baseline = planning
    data.planning_start_time = baseline[1].start_time
    candidate = ai._candidate(planner, data, baseline[1:], proposal(("hold", "auto", "charge")))
    assert [action.action for action in candidate] == ["auto", "charge"]


def test_required_charge_and_manual_actions_preserved(planning):
    planner, data, baseline = planning
    baseline[0].action = "charge"
    baseline[0].charge_power_w = 1234.0
    baseline[0].target_soc = 80.0
    baseline[1].is_manual_override = True
    candidate = ai._candidate(planner, data, baseline, proposal())
    ai._validate_locally(planner, data, baseline, candidate)
    assert candidate[0].action == "charge"
    assert candidate[0].charge_power_w == 1234.0
    assert candidate[0].target_soc == 80.0
    assert candidate[1].action == "hold"
    assert candidate[1].is_manual_override


def test_sanity_preserves_baseline_charge_actions_and_respects_disabled_autofix(planning, monkeypatch):
    planner, data, baseline = planning
    baseline[0].action = "charge"
    baseline[0].charge_power_w = 1234.0
    calls = []

    def capture_sanity(planner, day, actions, prices, start_soc, **kwargs):
        calls.append(kwargs)
        return actions, {}

    monkeypatch.setattr(settings, "planning_sanity_autofix_enabled", False)
    monkeypatch.setattr(ai, "apply_planning_sanity", capture_sanity)
    ai._candidate(planner, data, baseline, proposal())
    assert calls[0]["auto_fix"] is False
    assert calls[0]["protected_charge_starts"] == {baseline[0].start_time}


def test_real_sanity_preserves_all_baseline_charge_slots(monkeypatch):
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_autofix_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_precheap_arbitrage_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_target_soc_percent", 5)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 0)
    monkeypatch.setattr(settings, "must_charge_window_min_soc_percent", 5)
    monkeypatch.setattr(settings, "reserve_soc_min_percent", 5)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    day = date(2026, 11, 12)
    points = [
        PricePoint(
            timestamp=datetime.combine(day, datetime.min.time()) + timedelta(hours=hour),
            area="DK2", price_ore_per_kwh=float(hour + 1), currency="DKK", source="elprisenligenu",
        )
        for hour in range(8)
    ]
    baseline = [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1),
            action="charge" if index < 6 else "auto",
            charge_power_w=1000.0 if index < 6 else None,
            reason="local baseline", is_manual_override=False,
        )
        for index, point in enumerate(points)
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "simulate", lambda day, actions, start_soc, **kwargs: [
        SimulationPoint(
            date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
            projected_soc=50.0, projected_grid_kwh=1.0,
        )
        for action in actions
    ])
    data = PlannerInput(day=day, price_points=points, start_soc=50.0)
    proposed = proposal(("hold",) * len(points))
    candidate = ai._candidate(planner, data, baseline, proposed)
    assert [action.action for action in candidate[:6]] == ["charge"] * 6
    ai._validate_locally(planner, data, baseline, candidate)


@pytest.mark.parametrize("mode", ["cost", "final_soc", "limits", "expensive_grid", "reserve"])
def test_local_regression_rejected(planning, monkeypatch, mode):
    planner, data, baseline = planning
    candidate = ai._candidate(planner, data, baseline, proposal())

    def simulate(day, actions, start_soc, **kwargs):
        changed = actions is candidate
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=(
                    49.0 if changed and mode == "final_soc"
                    else 101.0 if changed and mode == "limits"
                    else 20.0 if changed and mode == "reserve" and index == 0
                    else 50.0
                ),
                projected_grid_kwh=2.0 if changed and mode == "cost" else (
                    1.5 if changed and mode == "expensive_grid" and index == 0
                    else 0.0 if changed and mode == "expensive_grid" else 1.0
                ),
            )
            for index, action in enumerate(actions)
        ]

    if mode == "reserve":
        monkeypatch.setattr(settings, "reserve_soc_enabled", True)
        monkeypatch.setattr(settings, "reserve_soc_start_hour_local", 0)
        monkeypatch.setattr(settings, "reserve_soc_end_hour_local", 3)
        monkeypatch.setattr(settings, "reserve_soc_min_percent", 50)
    if mode == "expensive_grid":
        monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", True)
        monkeypatch.setattr(settings, "planning_sanity_expensive_window_start_hour_local", 0)
        monkeypatch.setattr(settings, "planning_sanity_expensive_window_end_hour_local", 1)
    monkeypatch.setattr(planner, "simulate", simulate)
    with pytest.raises(ValueError):
        ai._validate_locally(planner, data, baseline, candidate)


def test_key_excluded_from_settings_serialization_and_config():
    from powerbuddy.main import get_config

    config = Settings(_env_file=None, POWERBUDDY_CLAUDE_API_KEY="test-value-not-a-secret")
    assert "test-value-not-a-secret" not in repr(config)
    assert "claude_api_key" not in config.model_dump()
    assert "claude_api_key" not in config.model_dump(by_alias=True)
    assert "test-value-not-a-secret" not in config.model_dump_json()
    assert not any("api_key" in key.lower() for key in get_config())


def test_real_winter_plan_keeps_additive_charge_floor(monkeypatch):
    monkeypatch.setattr(settings, "claude_api_key", SecretStr("test-key-not-real"))
    monkeypatch.setattr(settings, "winter_cheap_charge_hours", 4)
    monkeypatch.setattr(settings, "winter_additional_night_charge_hours", 2)
    monkeypatch.setattr(settings, "winter_cheap_charge_night_start_hour", 0)
    monkeypatch.setattr(settings, "winter_cheap_charge_night_end_hour", 5)
    day = date(2026, 11, 12)
    levels = [100.0 + hour for hour in range(24)]
    for hour, price in ((6, 1), (7, 2), (12, 3), (18, 4), (2, 5), (3, 6)):
        levels[hour] = price
    points = [
        PricePoint(
            timestamp=datetime.combine(day, datetime.min.time()) + timedelta(hours=hour),
            area="DK2", price_ore_per_kwh=level, currency="DKK", source="elprisenligenu",
        )
        for hour, level in enumerate(levels)
    ]
    planner = DayPlanner()
    monkeypatch.setattr(planner, "resolve_expected_daily_consumption", lambda day: (60.0, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_consumption_profile", lambda day: ([1 / 24] * 24, "test"))
    monkeypatch.setattr(planner, "resolve_hourly_pv_profile", lambda day: ([0.0] * 24, "test"))
    # Even an AI proposal of all-auto cannot remove mandatory charging.
    mock_api(monkeypatch, tool_response({
        "slots": [{"slot": hour, "action": "auto"} for hour in range(24)],
    }))
    result = asyncio.run(ai.plan_with_claude(
        planner, PlannerInput(day=day, price_points=points, start_soc=50.0),
    ))
    charges = {action.start_time.hour for action in result if action.action == "charge"}
    assert {2, 3, 6, 7, 12, 18}.issubset(charges)


def test_outcomes_are_collected_per_day(planning, monkeypatch):
    planner, data, _ = planning
    mock_api(monkeypatch, tool_response())

    async def run():
        with ai.collect_outcomes() as outcomes:
            await ai.plan_with_claude(planner, data)
        return outcomes

    assert asyncio.run(run()) == {data.day.isoformat(): "accepted"}


def test_failed_outcome_is_collected(planning, monkeypatch):
    planner, data, _ = planning
    mock_api(monkeypatch, {"error": "x"}, status=500)

    async def run_failed():
        with ai.collect_outcomes() as outcomes:
            await ai.plan_with_claude(planner, data)
        return ai.summarize_outcomes(outcomes)

    assert asyncio.run(run_failed())["unavailable"] == 1
