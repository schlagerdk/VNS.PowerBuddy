from __future__ import annotations

import asyncio
from datetime import date, datetime

from powerbuddy.main import _dashboard_apply_plan_state, _discover_latest_released_day_from, get_prices
from powerbuddy.models import PricePoint
from powerbuddy.schemas import PriceOut


class FixedDashboardDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 9, 1, 10, 0, tzinfo=tz)


def test_get_prices_keeps_dummy_source_in_api_output(monkeypatch):
    async def fake_get_network_tariff_24h():
        return {hour: 0.0 for hour in range(24)}

    monkeypatch.setattr("powerbuddy.main.tariff_service.get_network_tariff_24h", fake_get_network_tariff_24h)
    monkeypatch.setattr("powerbuddy.main.settings.price_area", "DK1")
    monkeypatch.setattr("powerbuddy.main.settings.timezone", "Europe/Copenhagen")
    monkeypatch.setattr("powerbuddy.main.settings.tariff_vat_factor", "1.25")
    monkeypatch.setattr("powerbuddy.main.settings.price_supplier_markup_ore", "0")
    monkeypatch.setattr("powerbuddy.main.settings.price_transport_fixed_ore", "0")
    monkeypatch.setattr(
        "powerbuddy.main.PriceRepository.get_by_day",
        lambda day, area: [
            PricePoint(
                timestamp=datetime(2026, 9, 1, 12, 0, tzinfo=None),
                area=area,
                price_ore_per_kwh=123.0,
                currency="DKK",
                source="dummy-afternoon-fallback",
            )
        ],
    )

    result = asyncio.run(get_prices(target_date=date(2026, 9, 1)))

    assert len(result) == 1
    assert result[0].source == "dummy-afternoon-fallback"


def test_price_discovery_does_not_fetch_tomorrow_before_publication(monkeypatch):
    today = date(2026, 9, 1)
    today_prices = [
        PricePoint(
            timestamp=datetime(2026, 9, 1, hour),
            area="DK2",
            price_ore_per_kwh=100.0,
            currency="DKK",
            source="elprisenligenu",
        )
        for hour in range(24)
    ]
    requested_days = []

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 1, 10, 0, tzinfo=tz)

    class Provider:
        async def get_day_prices(self, day, area):
            requested_days.append(day)
            return []

    monkeypatch.setattr("powerbuddy.main.datetime", FixedDateTime)
    monkeypatch.setattr("powerbuddy.main.get_price_provider", lambda: Provider())
    monkeypatch.setattr("powerbuddy.main.settings.timezone", "Europe/Copenhagen")
    monkeypatch.setattr("powerbuddy.main.settings.day_ahead_publish_hour_local", 13)
    monkeypatch.setattr(
        "powerbuddy.main.PriceRepository.get_by_day",
        lambda day, area: today_prices if day == today else [],
    )

    latest_day = asyncio.run(_discover_latest_released_day_from(today))

    assert latest_day == today
    assert requested_days == []


def test_dashboard_dummy_prices_are_colored_and_skip_badges(monkeypatch):
    monkeypatch.setattr("powerbuddy.main.datetime", FixedDashboardDateTime)
    now = datetime(2026, 9, 1, 12, 0, 0)
    html = '<div class="chart-bars"></div>'

    prices = [
        PriceOut(
            timestamp=datetime(2026, 9, 1, 11, 0, 0),
            area="DK1",
            price_ore_per_kwh=0.0,
            spot_price_ore_per_kwh=0.0,
            price_without_fees_ore_per_kwh=0.0,
            price_with_fees_ore_per_kwh=0.0,
            currency="DKK",
            source="dummy-afternoon-fallback",
        ),
        PriceOut(
            timestamp=datetime(2026, 9, 1, 12, 0, 0),
            area="DK1",
            price_ore_per_kwh=150.0,
            spot_price_ore_per_kwh=150.0,
            price_without_fees_ore_per_kwh=150.0,
            price_with_fees_ore_per_kwh=150.0,
            currency="DKK",
            source="energidataservice",
        ),
        PriceOut(
            timestamp=datetime(2026, 9, 1, 13, 0, 0),
            area="DK1",
            price_ore_per_kwh=200.0,
            spot_price_ore_per_kwh=200.0,
            price_without_fees_ore_per_kwh=200.0,
            price_with_fees_ore_per_kwh=200.0,
            currency="DKK",
            source="energidataservice",
        ),
    ]

    rendered = _dashboard_apply_plan_state(html, [], prices)

    # Verify dummy bar is marked
    assert 'is-dummy' in rendered
    assert 'data-category="dummy"' in rendered
    assert 'data-source="dummy-afternoon-fallback"' in rendered

    # Verify badges ARE shown on real prices
    assert 'Billigste' in rendered
    assert 'Dyreste' in rendered


def test_dashboard_dummy_prices_have_fixed_height_and_no_price_text(monkeypatch):
    """Verify dummy prices display at fixed 50% height with no price text."""
    monkeypatch.setattr("powerbuddy.main.datetime", FixedDashboardDateTime)
    html = '<div class="chart-bars"></div>'

    prices = [
        PriceOut(
            timestamp=datetime(2026, 9, 1, 11, 0, 0),
            area="DK1",
            price_ore_per_kwh=0.0,
            spot_price_ore_per_kwh=0.0,
            price_without_fees_ore_per_kwh=0.0,
            price_with_fees_ore_per_kwh=0.0,
            currency="DKK",
            source="dummy-afternoon-fallback",
        ),
        PriceOut(
            timestamp=datetime(2026, 9, 1, 12, 0, 0),
            area="DK1",
            price_ore_per_kwh=500.0,
            spot_price_ore_per_kwh=500.0,
            price_without_fees_ore_per_kwh=500.0,
            price_with_fees_ore_per_kwh=500.0,
            currency="DKK",
            source="energidataservice",
        ),
    ]

    rendered = _dashboard_apply_plan_state(html, [], prices)

    # Dummy bar should have fixed 50% height
    assert 'height: 50.0%' in rendered

    # Dummy bars must not render an empty price-value element.
    assert 'data-source="dummy-afternoon-fallback"' in rendered
    assert '<span class="bar-value"></span>' not in rendered

    # Real price should be displayed
    assert '5,00' in rendered


def test_get_prices_morning_does_not_create_dummy_for_tomorrow(monkeypatch):
    class MorningTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 1, 9, 0, tzinfo=tz)

    today = date(2026, 9, 1)
    today_prices = [
        PricePoint(
            timestamp=datetime(2026, 9, 1, hour),
            area="DK2",
            price_ore_per_kwh=100.0,
            currency="DKK",
            source="elprisenligenu",
        )
        for hour in range(24)
    ]

    monkeypatch.setattr("powerbuddy.main.datetime", MorningTime)
    monkeypatch.setattr("powerbuddy.main.settings.timezone", "Europe/Copenhagen")
    monkeypatch.setattr("powerbuddy.main.settings.allow_dummy_prices", True)
    monkeypatch.setattr("powerbuddy.main.settings.price_area", "DK2")
    monkeypatch.setattr(
        "powerbuddy.main.PriceRepository.get_by_day",
        lambda day, area: today_prices if day == today else [],
    )
    monkeypatch.setattr(
        "powerbuddy.main.PriceRepository.get_by_time_window",
        lambda start, end, area: [p for p in today_prices if p.timestamp >= start.replace(tzinfo=None) and p.timestamp < end.replace(tzinfo=None)],
    )
    monkeypatch.setattr("powerbuddy.main.PriceRepository.clean_stale_dummy_prices", lambda now=None: 0)

    dummy_created_days = []
    async def fake_ensure_dummy(day, now):
        dummy_created_days.append(day)
        return False

    monkeypatch.setattr("powerbuddy.main.scheduler._ensure_dummy_future_prices_if_needed", fake_ensure_dummy)

    result = asyncio.run(get_prices())

    # In the morning at 09:00, no dummy prices should be created for tomorrow
    assert dummy_created_days == []
    assert all(p.timestamp.date() == today for p in result)
