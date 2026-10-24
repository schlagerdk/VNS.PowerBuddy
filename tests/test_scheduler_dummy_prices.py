from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from powerbuddy.database import Base
from powerbuddy.models import PlanAction, PricePoint
from powerbuddy.repositories import PriceRepository, SessionLocal
from powerbuddy.services.scheduler import PowerBuddyScheduler


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 9, 1, 14, 30)
        if tz is not None:
            return value.astimezone(tz)
        return value


class MorningDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 9, 1, 8, 30)
        if tz is not None:
            return value.astimezone(tz)
        return value


def test_refresh_prices_and_replan_creates_dummy_prices_after_14(monkeypatch):
    monkeypatch.setattr("powerbuddy.services.scheduler.datetime", FixedDateTime)
    monkeypatch.setattr("powerbuddy.services.scheduler.settings.allow_dummy_prices", True)

    captured = {}

    class DummyProvider:
        async def get_day_prices(self, day, area):
          return []

        async def get_latest_available_day(self, area):
            return datetime(2026, 9, 1).date()

    async def fake_resolve_start_soc_for_day(day):
        return 50.0

    async def fake_plan_and_simulate(day, prices, soc, lock_hours=0):
        captured["day"] = day
        captured["prices"] = prices
        captured["soc"] = soc
        captured["lock_hours"] = lock_hours

    def fake_get_by_day(day, area):
      return []

    def fake_upsert_prices(points):
        captured["upserted"] = points

    def fake_get_plan(day_key):
      return []

    monkeypatch.setattr("powerbuddy.services.scheduler.get_price_provider", lambda: DummyProvider())
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.get_by_day", fake_get_by_day)
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.get_latest_day", lambda area: datetime(2026, 9, 1).date())
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.upsert_prices", fake_upsert_prices)
    monkeypatch.setattr("powerbuddy.services.scheduler.PriceRepository.clean_stale_dummy_prices", lambda now=None: 0)
    monkeypatch.setattr("powerbuddy.services.scheduler.PlanRepository.get_plan", fake_get_plan)
    monkeypatch.setattr("powerbuddy.services.scheduler.PlanRepository.replace_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr("powerbuddy.services.scheduler.PowerBuddyScheduler._resolve_start_soc_for_day", fake_resolve_start_soc_for_day)
    monkeypatch.setattr("powerbuddy.services.scheduler.PowerBuddyScheduler._plan_and_simulate", fake_plan_and_simulate)

    scheduler = object.__new__(PowerBuddyScheduler)
    scheduler.price_provider = DummyProvider()
    scheduler.planner = None
    scheduler._execution_enabled = True
    scheduler._last_solar_replan_at = None
    scheduler._last_intraday_replan_at = None
    scheduler._last_executed_signature = None
    scheduler._last_executed_at = None

    asyncio.run(scheduler.refresh_prices_and_replan())

    assert "upserted" in captured
    prices = captured["upserted"]
    assert prices
    assert len(prices) == 24
    assert all(point.source.startswith("dummy-") for point in prices)
    assert all(point.timestamp.date() == datetime(2026, 9, 2).date() for point in prices)


def test_should_fetch_day_timing():
    scheduler = object.__new__(PowerBuddyScheduler)
    now_morning = datetime(2026, 9, 1, 8, 0)
    now_after_13 = datetime(2026, 9, 1, 13, 30)
    today = date(2026, 9, 1)
    tomorrow = date(2026, 9, 2)
    day_after = date(2026, 9, 3)

    # Today is always fetchable
    assert scheduler._should_fetch_day(today, now_morning, []) is True
    assert scheduler._should_fetch_day(today, now_after_13, []) is True

    # Tomorrow is NOT fetchable before 13:00
    assert scheduler._should_fetch_day(tomorrow, now_morning, []) is False

    # Tomorrow IS fetchable at/after 13:00
    assert scheduler._should_fetch_day(tomorrow, now_after_13, []) is True

    # Days beyond tomorrow are NEVER fetchable
    assert scheduler._should_fetch_day(day_after, now_morning, []) is False
    assert scheduler._should_fetch_day(day_after, now_after_13, []) is False


def test_full_battery_conversion_converts_active_manual_charge(monkeypatch):
    now = datetime(2026, 9, 1, 14, 30)
    manual_charge = PlanAction(
        id=1,
        date_key="2026-09-01",
        start_time=datetime(2026, 9, 1, 14),
        end_time=datetime(2026, 9, 1, 15),
        action="charge",
        target_soc=90.0,
        reason="manual override",
        is_manual_override=True,
    )
    generated_charge = PlanAction(
        id=2,
        date_key="2026-09-01",
        start_time=datetime(2026, 9, 1, 15),
        end_time=datetime(2026, 9, 1, 16),
        action="charge",
        target_soc=90.0,
        reason="price optimization",
        is_manual_override=False,
    )
    actions = [manual_charge, generated_charge]
    updates = []

    monkeypatch.setattr(
        "powerbuddy.services.scheduler.PlanRepository.get_plan",
        lambda day_key: actions,
    )

    def update_action(action_id, **kwargs):
        updates.append((action_id, kwargs))
        return generated_charge

    monkeypatch.setattr(
        "powerbuddy.services.scheduler.PlanRepository.update_action",
        update_action,
    )

    scheduler = object.__new__(PowerBuddyScheduler)
    converted = scheduler._convert_active_charge_block_to_hold("2026-09-01", now)

    assert converted == 1
    assert [u[0] for u in updates] == [1]


def test_full_battery_converts_only_active_half_hour(monkeypatch):
    now = datetime(2026, 9, 1, 11, 45)
    first_half = PlanAction(
        id=1,
        date_key="2026-09-01",
        start_time=datetime(2026, 9, 1, 11),
        end_time=datetime(2026, 9, 1, 11, 30),
        action="auto",
        reason="manual override",
        is_manual_override=True,
    )
    second_half = PlanAction(
        id=2,
        date_key="2026-09-01",
        start_time=datetime(2026, 9, 1, 11, 30),
        end_time=datetime(2026, 9, 1, 12),
        action="charge",
        reason="manual override",
        is_manual_override=True,
    )
    updates = []
    monkeypatch.setattr(
        "powerbuddy.services.scheduler.PlanRepository.get_plan",
        lambda day_key: [first_half, second_half],
    )
    monkeypatch.setattr(
        "powerbuddy.services.scheduler.PlanRepository.update_action",
        lambda action_id, **kwargs: updates.append((action_id, kwargs)) or second_half,
    )

    scheduler = object.__new__(PowerBuddyScheduler)
    converted = scheduler._convert_active_charge_block_to_hold("2026-09-01", now)

    assert converted == 1
    assert [u[0] for u in updates] == [2]


def test_clean_stale_dummy_prices_removes_morning_dummy_for_tomorrow(monkeypatch):
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, future=True)
    monkeypatch.setattr("powerbuddy.repositories.SessionLocal", test_session)
    monkeypatch.setattr("powerbuddy.repositories.settings.allow_dummy_prices", True)

    today = date(2026, 9, 1)
    tomorrow = date(2026, 9, 2)
    day_after = date(2026, 9, 3)

    # Populate dummy prices for today, tomorrow, and day-after-tomorrow
    points = [
        PricePoint(
            timestamp=datetime(today.year, today.month, today.day, hour),
            area="DK2",
            price_ore_per_kwh=0.0,
            currency="DKK",
            source="dummy-today",
        )
        for hour in range(24)
    ] + [
        PricePoint(
            timestamp=datetime(tomorrow.year, tomorrow.month, tomorrow.day, hour),
            area="DK2",
            price_ore_per_kwh=0.0,
            currency="DKK",
            source="dummy-tomorrow",
        )
        for hour in range(24)
    ] + [
        PricePoint(
            timestamp=datetime(day_after.year, day_after.month, day_after.day, hour),
            area="DK2",
            price_ore_per_kwh=0.0,
            currency="DKK",
            source="dummy-future",
        )
        for hour in range(24)
    ]

    with test_session() as session:
        for p in points:
            session.add(p)
        session.commit()

    # In the morning at 08:30:
    # - today dummy is allowed as fallback
    # - tomorrow dummy is NOT allowed (now.hour < 14)
    # - day-after dummy is NOT allowed
    now_morning = datetime(2026, 9, 1, 8, 30)
    deleted = PriceRepository.clean_stale_dummy_prices(now_morning)
    assert deleted == 48  # tomorrow (24) + day_after (24)

    # Verify only today dummy remains
    with test_session() as session:
        remaining = list(session.execute(sqlalchemy.select(PricePoint)).scalars())
        assert len(remaining) == 24
        assert all(p.timestamp.date() == today for p in remaining)


def test_upsert_prices_handles_dst_duplicate_hour(monkeypatch):
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr("powerbuddy.repositories.SessionLocal", sessionmaker(bind=engine, future=True, autoflush=False))

    def make_points():
        return [
            PricePoint(timestamp=datetime.fromisoformat(ts), area="DK2", price_ore_per_kwh=100.0, currency="DKK", source="elprisenligenu")
            for ts in (
                "2026-10-25T01:00:00+02:00",
                "2026-10-25T02:00:00+02:00",
                "2026-10-25T02:00:00+01:00",
                "2026-10-25T03:00:00+01:00",
            )
        ]

    PriceRepository.upsert_prices(make_points())
    PriceRepository.upsert_prices(make_points())

    with sessionmaker(bind=engine, future=True)() as session:
        assert session.query(PricePoint).count() == 3
