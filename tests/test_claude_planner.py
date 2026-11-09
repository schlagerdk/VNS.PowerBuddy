from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
import json
import logging

import httpx
import pytest
from pydantic import SecretStr

from powerbuddy.config import Settings, settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.services import claude_planner as ai
from powerbuddy.services.planner import DayPlanner, PlannerInput


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
