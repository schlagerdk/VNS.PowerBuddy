from __future__ import annotations

from datetime import date

import httpx

from powerbuddy.config import settings


class WeatherForecastService:
    def __init__(self) -> None:
        self._cache_day: date | None = None
        self._cache_timezone: str | None = None
        self._cache_factors: list[float] | None = None

    async def get_hourly_pv_factor_24h(self, target_day: date) -> list[float] | None:
        """
        Returns 24 local-hour scaling factors for PV profile based on weather forecast.
        1.0 means neutral versus historical PV profile, <1.0 cloudy/weak sun, >1.0 strong sun.
        """
        if not settings.weather_forecast_enabled:
            return None

        if (
            self._cache_day == target_day
            and self._cache_timezone == settings.timezone
            and self._cache_factors is not None
        ):
            return list(self._cache_factors)

        params = {
            "latitude": settings.weather_latitude,
            "longitude": settings.weather_longitude,
            "hourly": "cloud_cover,shortwave_radiation",
            "timezone": settings.timezone,
            "forecast_days": 7,
        }

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
                resp.raise_for_status()
                payload = resp.json()
        except Exception:
            return None

        hourly = payload.get("hourly", {})
        times = hourly.get("time") or []
        cloud = hourly.get("cloud_cover") or []
        radiation = hourly.get("shortwave_radiation") or []
        if not times:
            return None

        factors = [1.0] * 24
        found = False
        day_key = target_day.isoformat()

        for idx, ts in enumerate(times):
            if not str(ts).startswith(day_key):
                continue
            hour_str = str(ts)[11:13]
            if len(hour_str) != 2 or not hour_str.isdigit():
                continue
            hour = int(hour_str)
            if not (0 <= hour <= 23):
                continue

            c = float(cloud[idx]) if idx < len(cloud) and cloud[idx] is not None else 50.0
            r = float(radiation[idx]) if idx < len(radiation) and radiation[idx] is not None else 0.0
            cloud_frac = max(0.0, min(1.0, c / 100.0))

            # Radiation gives daylight magnitude (season + weather). Cloud cover damps it.
            rad_component = max(0.0, min(1.8, r / 450.0))
            cloud_component = max(0.25, 1.0 - (0.7 * cloud_frac))
            factor = max(0.05, min(1.8, rad_component * cloud_component))

            # Keep dark hours near zero to prevent false PV in the night.
            if r < 5.0:
                factor = 0.0

            factors[hour] = round(factor, 3)
            found = True

        if not found:
            return None

        self._cache_day = target_day
        self._cache_timezone = settings.timezone
        self._cache_factors = list(factors)
        return factors


weather_forecast_service = WeatherForecastService()
