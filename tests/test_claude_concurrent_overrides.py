import asyncio
from datetime import date, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from powerbuddy import main, repositories
from powerbuddy.config import settings
from powerbuddy.database import Base
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.services import claude_planner
from powerbuddy.services import scheduler as scheduler_module
from powerbuddy.services.planner import DayPlanner, SimulationInputs
from powerbuddy.services.scheduler import PowerBuddyScheduler


@pytest.fixture
def isolated_repository(monkeypatch):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    monkeypatch.setattr(
        repositories, "SessionLocal",
        sessionmaker(bind=engine, autoflush=False, expire_on_commit=False),
    )
    yield
    engine.dispose()


@pytest.mark.parametrize(
    ("entrypoint", "override_change"),
    [
        ("scheduler", "add"), ("materialize", "add"), ("chart", "add"),
        ("scheduler", "quarter"), ("materialize", "quarter"), ("chart", "quarter"),
        ("scheduler", "move"), ("materialize", "move"),
    ],
)
@pytest.mark.parametrize("request_succeeds", [True, False])
def test_override_changed_while_claude_waits_is_compared_and_saved(
    entrypoint, override_change, request_succeeds, isolated_repository, monkeypatch,
):
    day = date(2030, 11, 12)
    midnight = datetime.combine(day, datetime.min.time())
    prices = [
        PricePoint(
            timestamp=midnight + timedelta(hours=hour), area="DK2",
            price_ore_per_kwh=20.0 + hour, currency="DKK", source="elprisenligenu",
        )
        for hour in range(3)
    ]
    monkeypatch.setattr(settings, "claude_api_key", SecretStr("fake-local-test-key"))
    monkeypatch.setattr(settings, "planning_sanity_enabled", False)
    monkeypatch.setattr(settings, "planning_variant_search_enabled", False)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", False)
    monkeypatch.setattr(
        DayPlanner, "capture_simulation_inputs",
        lambda self, day: SimulationInputs(24.0, tuple([1 / 24] * 24), tuple([0.0] * 24)),
    )
    monkeypatch.setattr(DayPlanner, "resolve_expected_daily_consumption", lambda self, day: (24.0, "test"))
    monkeypatch.setattr(
        DayPlanner, "trim_redundant_charge_actions",
        lambda self, day, actions, start_soc, **kwargs: actions,
    )
    monkeypatch.setattr(DayPlanner, "plan", lambda self, data: [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1),
            action="hold", reason="baseline", is_manual_override=False,
        )
        for point in data.price_points
    ])
    simulated = []

    def simulate(self, day, actions, start_soc, **kwargs):
        simulated.append([
            (action.start_time, action.end_time, action.action, bool(action.is_manual_override))
            for action in actions
        ])
        return [
            SimulationPoint(
                date_key=day.isoformat(), timestamp=action.start_time, action=action.action,
                projected_soc=start_soc, projected_grid_kwh=0.0 if action.action == "auto" else 1.0,
            )
            for action in actions
        ]

    monkeypatch.setattr(DayPlanner, "simulate", simulate)

    async def tariff():
        return [0.0] * 24

    async def weather(day):
        return None

    async def start_soc(day):
        return 50.0

    async def prices_with_provisional(day):
        return prices, False

    async def prices_with_fallback(day):
        return day, prices, False

    monkeypatch.setattr(main.tariff_service, "get_network_tariff_24h", tariff)
    monkeypatch.setattr(main.tariff_service, "total_tariff_ore_24h", lambda network: network)
    monkeypatch.setattr(main.weather_forecast_service, "get_hourly_pv_factor_24h", weather)
    monkeypatch.setattr(main, "_resolve_start_soc_for_day", start_soc)
    monkeypatch.setattr(main, "_get_day_prices_with_provisional_fallback", prices_with_provisional)
    monkeypatch.setattr(main, "_ensure_prices_with_fallback", prices_with_fallback)

    old_manual = None
    if override_change == "move":
        old_manual = repositories.PlanRepository.add_manual_override(PlanAction(
            date_key=day.isoformat(), start_time=midnight,
            end_time=midnight + timedelta(minutes=30),
            action="hold", reason="old manual action", is_manual_override=True,
        ))

    simulations_before_request = []

    async def run():
        requested = asyncio.Event()
        continue_request = asyncio.Event()

        async def request(payload):
            # Earlier simulations only build the request context; comparisons happen afterwards.
            simulations_before_request.append(len(simulated))
            requested.set()
            await continue_request.wait()
            if not request_succeeds:
                raise httpx.ConnectError("local mock")
            return claude_planner.Proposal(slots=[
                claude_planner.SlotProposal(slot=0, action="auto"),
                claude_planner.SlotProposal(slot=1, action="auto"),
                claude_planner.SlotProposal(slot=2, action="hold"),
            ])

        monkeypatch.setattr(claude_planner, "_request_proposal", request)
        if entrypoint == "scheduler":
            scheduler = object.__new__(PowerBuddyScheduler)
            scheduler.planner = DayPlanner()
            coroutine = scheduler._plan_and_simulate(day, prices, 50.0)
        elif entrypoint == "materialize":
            coroutine = main._materialize_day_plan_if_missing(day)
        else:
            coroutine = main.planning_chart_data(day)
        task = asyncio.create_task(coroutine)
        await asyncio.wait_for(requested.wait(), timeout=5)
        if old_manual is not None:
            assert repositories.PlanRepository.delete_action(old_manual.id)
        manual_start = midnight + timedelta(
            hours=2 if override_change == "move" else 0,
            minutes=15 if override_change == "quarter" else 0,
        )
        repositories.PlanRepository.add_manual_override(PlanAction(
            date_key=day.isoformat(), start_time=manual_start,
            end_time=manual_start + timedelta(minutes=30),
            action="hold", reason="new manual action", is_manual_override=True,
        ))
        continue_request.set()
        await task

    asyncio.run(run())
    effective = PowerBuddyScheduler._dedupe_effective_actions(
        repositories.PlanRepository.get_plan(day.isoformat())
    )
    proposed = "auto" if request_succeeds else "hold"
    if override_change == "move":
        expected = [
            (midnight, midnight + timedelta(hours=1), proposed, False),
            (midnight + timedelta(hours=1), midnight + timedelta(hours=2), proposed, False),
            (midnight + timedelta(hours=2), midnight + timedelta(hours=2, minutes=30), "hold", True),
            (midnight + timedelta(hours=2, minutes=30), midnight + timedelta(hours=3), "hold", False),
        ]
    elif override_change == "quarter":
        expected = [
            (midnight, midnight + timedelta(minutes=15), "hold", False),
            (midnight + timedelta(minutes=15), midnight + timedelta(minutes=45), "hold", True),
            (midnight + timedelta(minutes=45), midnight + timedelta(hours=1), "hold", False),
            (midnight + timedelta(hours=1), midnight + timedelta(hours=2), proposed, False),
            (midnight + timedelta(hours=2), midnight + timedelta(hours=3), "hold", False),
        ]
    else:
        expected = [
            (midnight, midnight + timedelta(minutes=30), "hold", True),
            (midnight + timedelta(minutes=30), midnight + timedelta(hours=1), "hold", False),
            (midnight + timedelta(hours=1), midnight + timedelta(hours=2), proposed, False),
            (midnight + timedelta(hours=2), midnight + timedelta(hours=3), "hold", False),
        ]
    assert [
        (action.start_time, action.end_time, action.action, bool(action.is_manual_override))
        for action in effective
    ] == expected
    assert simulated[-1] == expected
    manual_segment = next(segment for segment in expected if segment[3])
    assert all(manual_segment in actions for actions in simulated[simulations_before_request[0]:])
    points = repositories.SimulationRepository.get_points(day.isoformat())
    assert [(point.timestamp, point.action) for point in points] == [
        (start, mode) for start, _, mode, _ in expected
    ]


def test_locked_current_hours_are_included_in_comparison_and_final_simulation(
    isolated_repository, monkeypatch,
):
    day = date(2030, 11, 12)
    midnight = datetime.combine(day, datetime.min.time())

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2030, 11, 12, 10, 15, tzinfo=tz)

    monkeypatch.setattr(scheduler_module, "datetime", FrozenDateTime)
    monkeypatch.setattr(settings, "timezone", "Europe/Copenhagen")
    monkeypatch.setattr(settings, "claude_api_key", SecretStr("fake-local-test-key"))
    monkeypatch.setattr(settings, "planning_sanity_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_autofix_enabled", True)
    monkeypatch.setattr(settings, "planning_variant_search_enabled", True)
    monkeypatch.setattr(settings, "reserve_soc_enabled", False)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_enabled", True)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_start_hour_local", 11)
    monkeypatch.setattr(settings, "planning_sanity_expensive_window_end_hour_local", 12)
    monkeypatch.setattr(settings, "planning_sanity_min_expensive_auto_share", 1.0)
    monkeypatch.setattr(
        DayPlanner, "capture_simulation_inputs",
        lambda self, day: SimulationInputs(24.0, tuple([1 / 24] * 24), tuple([0.0] * 24)),
    )
    prices = [
        PricePoint(
            timestamp=midnight + timedelta(hours=hour), area="DK2",
            price_ore_per_kwh=20.0 + hour, currency="DKK", source="elprisenligenu",
        )
        for hour in range(10, 13)
    ]
    monkeypatch.setattr(DayPlanner, "plan", lambda self, data: [
        PlanAction(
            date_key=day.isoformat(), start_time=point.timestamp,
            end_time=point.timestamp + timedelta(hours=1),
            action="hold", reason="baseline", is_manual_override=False,
        )
        for point in data.price_points
    ])
    repositories.PlanRepository.replace_full_plan(day.isoformat(), [
        PlanAction(
            date_key=day.isoformat(), start_time=midnight + timedelta(hours=hour),
            end_time=midnight + timedelta(hours=hour + 1),
            action="charge" if hour == 10 else "hold",
            charge_power_w=1000.0 if hour == 10 else None,
            reason="locked", is_manual_override=False,
        )
        for hour in (10, 11)
    ])

    async def request(payload):
        return claude_planner.Proposal(slots=[
            claude_planner.SlotProposal(slot=hour, action="auto") for hour in range(3)
        ])

    async def tariff():
        return [0.0] * 24

    async def weather(day):
        return None

    monkeypatch.setattr(claude_planner, "_request_proposal", request)
    monkeypatch.setattr(main.tariff_service, "get_network_tariff_24h", tariff)
    monkeypatch.setattr(main.tariff_service, "total_tariff_ore_24h", lambda network: network)
    monkeypatch.setattr(main.weather_forecast_service, "get_hourly_pv_factor_24h", weather)
    scheduler = object.__new__(PowerBuddyScheduler)
    scheduler.planner = DayPlanner()
    scheduler.planner.capacity_kwh = 20.0
    scheduler.planner.max_charge_kwh = 3.0
    scheduler.planner.charge_efficiency = 1.0

    asyncio.run(scheduler._plan_and_simulate(day, prices, 50.0, lock_hours=2))

    points = repositories.SimulationRepository.get_points(day.isoformat())
    assert [point.timestamp.hour for point in points] == [10, 11, 12]
    assert points[0].action == "charge"
    assert points[1].action == "hold"
    assert points[0].projected_soc == pytest.approx(53.75)
    assert points[0].projected_grid_kwh == pytest.approx(1.5)


def test_materialized_plan_with_no_remaining_periods_saves_no_fake_simulation(
    isolated_repository, monkeypatch,
):
    day = date(2030, 11, 12)
    start = datetime.combine(day, datetime.min.time()) + timedelta(hours=10)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2030, 11, 12, 12, 0, tzinfo=tz)

    monkeypatch.setattr(main, "datetime", FrozenDateTime)
    monkeypatch.setattr(settings, "claude_api_key", SecretStr(""))
    monkeypatch.setattr(settings, "planning_sanity_enabled", False)
    monkeypatch.setattr(settings, "planning_variant_search_enabled", False)
    monkeypatch.setattr(
        DayPlanner, "capture_simulation_inputs",
        lambda self, day: SimulationInputs(24.0, tuple([1 / 24] * 24), tuple([0.0] * 24)),
    )
    monkeypatch.setattr(DayPlanner, "plan", lambda self, data: [
        PlanAction(
            date_key=day.isoformat(), start_time=start, end_time=start + timedelta(hours=1),
            action="hold", reason="past period", is_manual_override=False,
        )
    ])

    def unexpected_simulation(*args, **kwargs):
        pytest.fail("An empty remaining plan must not generate a synthetic simulation point")

    monkeypatch.setattr(DayPlanner, "simulate", unexpected_simulation)
    price = PricePoint(
        timestamp=start, area="DK2", price_ore_per_kwh=20.0,
        currency="DKK", source="elprisenligenu",
    )

    async def get_prices(day):
        return [price], False

    async def start_soc(day):
        return 50.0

    async def weather(day):
        return None

    async def tariff():
        return [0.0] * 24

    monkeypatch.setattr(main, "_get_day_prices_with_provisional_fallback", get_prices)
    monkeypatch.setattr(main, "_resolve_start_soc_for_day", start_soc)
    monkeypatch.setattr(main.weather_forecast_service, "get_hourly_pv_factor_24h", weather)
    monkeypatch.setattr(main.tariff_service, "get_network_tariff_24h", tariff)
    monkeypatch.setattr(main.tariff_service, "total_tariff_ore_24h", lambda network: network)

    asyncio.run(main._materialize_day_plan_if_missing(day))

    assert repositories.SimulationRepository.get_points(day.isoformat()) == []
