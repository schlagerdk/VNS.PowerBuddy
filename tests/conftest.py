import pytest

from powerbuddy.services.planner import DayPlanner


@pytest.fixture
def isolated_forecasts(monkeypatch):
    monkeypatch.setattr(DayPlanner, "resolve_expected_daily_consumption", lambda self, day: (60.0, "test"))
    monkeypatch.setattr(DayPlanner, "resolve_hourly_consumption_profile", lambda self, day: ([1 / 24] * 24, "test"))
    monkeypatch.setattr(DayPlanner, "resolve_hourly_pv_profile", lambda self, day: ([0.0] * 24, "test"))
