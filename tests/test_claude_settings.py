from __future__ import annotations

import asyncio
from datetime import date, datetime
import os
import stat

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from powerbuddy import main
from powerbuddy.config import settings
from powerbuddy.models import PricePoint
from powerbuddy.services import claude_planner as ai
from powerbuddy.services.scheduler import PowerBuddyScheduler

VALID_KEY = "sk-ant-api03-" + "A" * 40


@pytest.fixture
def client(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_bytes(b"POWERBUDDY_UI_LANGUAGE=DK\r\nPOWERBUDDY_CLAUDE_API_KEY=\r\n")
    os.chmod(env_file, 0o600)
    monkeypatch.setitem(type(settings).model_config, "env_file", str(env_file))
    monkeypatch.setattr(settings, "claude_api_key", SecretStr(""))
    monkeypatch.setattr(main, "_dashboard_is_authorized", lambda request: True)
    return TestClient(main.app), env_file


def _post(client, payload):
    return client.post("/dashboard/settings/action", json=payload)


def test_save_key_persists_env_and_never_returns_key(client, monkeypatch):
    http, env_file = client

    async def verified(api_key):
        assert api_key == VALID_KEY
        return {"valid": True, "model_available": True}

    monkeypatch.setattr(main, "verify_api_key", verified)
    response = _post(http, {"action": "set-claude-key", "api_key": f"  {VALID_KEY}  "})
    assert response.status_code == 200
    body = response.json()
    assert body["configured"] is True and body["verified"] is True
    assert VALID_KEY not in response.text
    raw = env_file.read_bytes()
    assert f"POWERBUDDY_CLAUDE_API_KEY={VALID_KEY}\r\n".encode() in raw
    assert b"POWERBUDDY_UI_LANGUAGE=DK\r\n" in raw
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert settings.claude_api_key.get_secret_value() == VALID_KEY

    status = _post(http, {"action": "claude-status"}).json()
    assert status["configured"] is True and VALID_KEY not in str(status)

    removed = _post(http, {"action": "set-claude-key", "api_key": ""}).json()
    assert removed["configured"] is False
    assert b"POWERBUDDY_CLAUDE_API_KEY=\r\n" in env_file.read_bytes()


@pytest.mark.parametrize("api_key", ["not-a-key", "sk-ant-short", VALID_KEY + "\nPOWERBUDDY_X=1", 123])
def test_malformed_key_rejected_without_writing(client, api_key):
    http, env_file = client
    before = env_file.read_bytes()
    assert _post(http, {"action": "set-claude-key", "api_key": api_key}).status_code == 400
    assert env_file.read_bytes() == before


def test_key_rejected_by_anthropic_is_not_saved(client, monkeypatch):
    http, env_file = client
    before = env_file.read_bytes()

    async def rejected(api_key):
        return {"valid": False, "model_available": False}

    monkeypatch.setattr(main, "verify_api_key", rejected)
    response = _post(http, {"action": "set-claude-key", "api_key": VALID_KEY})
    assert response.status_code == 400 and response.json()["detail"] == "invalid_api_key"
    assert env_file.read_bytes() == before
    assert not ai.claude_configured()


def test_unreachable_verification_still_saves(client, monkeypatch):
    http, _ = client

    async def offline(api_key):
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(main, "verify_api_key", offline)
    body = _post(http, {"action": "set-claude-key", "api_key": VALID_KEY}).json()
    assert body["configured"] is True and body["verified"] is None


@pytest.mark.parametrize("status,valid,model_available", [(401, False, False), (200, True, True)])
def test_verify_api_key(monkeypatch, status, valid, model_available):
    captured = []
    real_client = httpx.AsyncClient

    def handle(request):
        captured.append(request)
        return httpx.Response(status, json={"data": [{"id": settings.claude_model}]})

    monkeypatch.setattr(ai.httpx, "AsyncClient", lambda **kwargs: real_client(
        **kwargs, transport=httpx.MockTransport(handle)
    ))
    result = asyncio.run(ai.verify_api_key(VALID_KEY))
    assert result == {"valid": valid, "model_available": model_available}
    assert captured[0].url.path == "/v1/models"
    assert captured[0].method == "GET"


def test_plan_actions_report_ai_outcomes(client, monkeypatch):
    http, _ = client
    monkeypatch.setattr(settings, "claude_api_key", SecretStr(VALID_KEY))

    async def fake_generate():
        ai._record(type("D", (), {"day": date(2026, 11, 9)})(), "accepted")
        ai._record(type("D", (), {"day": date(2026, 11, 10)})(), "rejected")
        return 2

    forced = {}

    async def fake_refresh(force=False):
        forced["force"] = force
        ai._record(type("D", (), {"day": date(2026, 11, 9)})(), "accepted")
        return 1

    monkeypatch.setattr(main.scheduler, "manual_generate_plan", fake_generate)
    monkeypatch.setattr(main.scheduler, "refresh_prices_and_replan", fake_refresh)

    ai_result = _post(http, {"action": "generate-plan"}).json()["ai"]
    assert ai_result == {"enabled": True, "days": 2, "accepted": 1, "rejected": 1, "unavailable": 0}
    refresh = _post(http, {"action": "refresh-and-plan"}).json()
    assert forced["force"] is True
    assert refresh["days"] == 1 and refresh["ai"]["accepted"] == 1


def test_forced_refresh_replans_days_with_unchanged_prices(monkeypatch):
    day = date(2026, 11, 9)
    prices = [PricePoint(
        timestamp=datetime(2026, 11, 9, hour), area=settings.price_area,
        price_ore_per_kwh=float(hour), currency="DKK", source="elprisenligenu",
    ) for hour in range(24)]

    class Provider:
        async def get_day_prices(self, requested_day, area):
            return list(prices)

    scheduler = object.__new__(PowerBuddyScheduler)
    scheduler.price_provider = Provider()
    planned = []

    async def fake_plan(requested_day, requested_prices, soc, lock_hours=0):
        planned.append(requested_day)

    async def fake_soc(requested_day):
        return 50.0

    scheduler._plan_and_simulate = fake_plan
    scheduler._resolve_start_soc_for_day = fake_soc
    scheduler._horizon_days_from_now = lambda: [day]
    scheduler._should_fetch_day = lambda *args: True
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.get_by_day", lambda d, area: list(prices))
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.clean_stale_dummy_prices", lambda now=None: 0)

    assert asyncio.run(scheduler.refresh_prices_and_replan()) == 0
    assert planned == []
    assert asyncio.run(scheduler.refresh_prices_and_replan(force=True)) == 1
    assert planned == [day]


def test_fetch_prices_uses_ai_only_when_configured(client, monkeypatch):
    http, _ = client
    calls = []

    async def fake_fetch(replan_fetched=False):
        calls.append(replan_fetched)
        if replan_fetched:
            ai._record(type("D", (), {"day": date(2026, 11, 9)})(), "accepted")
        return 1

    monkeypatch.setattr(main.scheduler, "manual_fetch_prices", fake_fetch)

    plain = _post(http, {"action": "fetch-prices"}).json()
    assert plain == {"ok": True, "action": "fetch-prices", "days": 1}
    monkeypatch.setattr(settings, "claude_api_key", SecretStr(VALID_KEY))
    with_ai = _post(http, {"action": "fetch-prices"}).json()
    assert with_ai["days"] == 1 and with_ai["ai"]["accepted"] == 1
    assert calls == [False, True]


def test_manual_fetch_replans_only_fetched_days_when_requested(monkeypatch):
    fetched_day, skipped_day = date(2026, 11, 9), date(2026, 11, 10)
    prices = [PricePoint(
        timestamp=datetime(2026, 11, 9, hour), area=settings.price_area,
        price_ore_per_kwh=float(hour), currency="DKK", source="elprisenligenu",
    ) for hour in range(24)]

    class Provider:
        async def get_day_prices(self, requested_day, area):
            return list(prices) if requested_day == fetched_day else []

    scheduler = object.__new__(PowerBuddyScheduler)
    scheduler.price_provider = Provider()
    planned = []

    async def fake_plan(requested_day, requested_prices, soc, lock_hours=0):
        planned.append(requested_day)

    async def fake_soc(requested_day):
        return 50.0

    scheduler._plan_and_simulate = fake_plan
    scheduler._resolve_start_soc_for_day = fake_soc
    scheduler._horizon_days_from_now = lambda: [fetched_day, skipped_day]
    scheduler._should_fetch_day = lambda *args: True
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.get_by_day", lambda d, area: list(prices))
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.upsert_prices", lambda points: None)

    assert asyncio.run(scheduler.manual_fetch_prices()) == 1
    assert planned == []
    assert asyncio.run(scheduler.manual_fetch_prices(replan_fetched=True)) == 1
    assert planned == [fetched_day]
