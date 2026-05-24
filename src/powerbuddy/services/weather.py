from __future__ import annotations

from datetime import date
import json
import math

import httpx

from powerbuddy.config import settings


class WeatherForecastService:
    def __init__(self) -> None:
        self._cache_day: date | None = None
        self._cache_timezone: str | None = None
        self._cache_factors: list[float] | None = None

    @staticmethod
    def _parse_solar_arrays() -> list[tuple[float, float, float]]:
        if not settings.solar_site_declared_enabled:
            return []

        raw = (settings.solar_arrays_json or "").strip()
        if not raw:
            return []

        try:
            payload = json.loads(raw)
        except Exception:
            return []

        if not isinstance(payload, list):
            return []

        arrays: list[tuple[float, float, float]] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            try:
                kwp = float(row.get("kwp", 0.0))
                azimuth = float(row.get("azimuth_deg", 0.0))
                tilt = float(row.get("tilt_deg", 30.0))
            except Exception:
                continue
            if kwp <= 0.0:
                continue
            # Azimuth: degrees from north clockwise (0..360). Tilt: from horizontal.
            azimuth = azimuth % 360.0
            tilt = max(0.0, min(90.0, tilt))
            arrays.append((kwp, azimuth, tilt))
        return arrays

    @staticmethod
    def _solar_position(lat_deg: float, day_of_year: int, hour_local: float) -> tuple[float, float]:
        lat = math.radians(lat_deg)
        # Approximate declination (Cooper equation).
        decl = math.radians(23.45 * math.sin(math.radians((360.0 / 365.0) * (284 + day_of_year))))
        # Hour angle: negative morning, positive afternoon.
        hra = math.radians(15.0 * (hour_local - 12.0))

        sin_elev = (math.sin(lat) * math.sin(decl)) + (math.cos(lat) * math.cos(decl) * math.cos(hra))
        sin_elev = max(-1.0, min(1.0, sin_elev))
        elev = math.asin(sin_elev)

        # Solar azimuth from north clockwise.
        if elev <= 0.0:
            return elev, 180.0

        cos_az = (math.sin(decl) - (math.sin(elev) * math.sin(lat))) / max(1e-9, (math.cos(elev) * math.cos(lat)))
        cos_az = max(-1.0, min(1.0, cos_az))
        az = math.degrees(math.acos(cos_az))
        if hra > 0:
            az = 360.0 - az
        return elev, az

    @classmethod
    def _array_model_power_kw(
        cls,
        arrays: list[tuple[float, float, float]],
        day_of_year: int,
        hour: int,
        radiation_w_m2: float,
    ) -> float:
        if not arrays:
            return 0.0

        # Mid-hour approximation for solar geometry.
        hour_local = float(hour) + 0.5
        elev, sun_az = cls._solar_position(float(settings.weather_latitude), day_of_year, hour_local)
        if elev <= 0.0 or radiation_w_m2 <= 1.0:
            return 0.0

        cos_elev = max(1e-6, math.cos(elev))
        sin_elev = max(0.0, math.sin(elev))
        irradiance_factor = max(0.0, min(1.6, float(radiation_w_m2) / 1000.0))

        total_kw = 0.0
        for kwp, panel_az_deg, tilt_deg in arrays:
            tilt = math.radians(tilt_deg)
            az_diff = math.radians(abs((sun_az - panel_az_deg + 180.0) % 360.0 - 180.0))
            # Plane-of-array incidence approximation.
            cos_inc = (sin_elev * math.cos(tilt)) + (cos_elev * math.sin(tilt) * math.cos(az_diff))
            poa_factor = max(0.0, cos_inc)
            total_kw += kwp * irradiance_factor * poa_factor
        return max(0.0, total_kw)

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
        arrays = self._parse_solar_arrays()
        array_power = [0.0] * 24

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

            if arrays:
                day_of_year = int(target_day.strftime("%j"))
                array_power[hour] = self._array_model_power_kw(arrays, day_of_year, hour, r)

            factors[hour] = round(factor, 3)
            found = True

        if not found:
            return None

        if arrays:
            peak = max(array_power) if array_power else 0.0
            if peak > 1e-6:
                for hour in range(24):
                    shape = max(0.0, min(1.5, array_power[hour] / peak))
                    # Blend baseline weather factor with array orientation shape.
                    blended = (0.5 * factors[hour]) + (0.5 * shape)
                    factors[hour] = round(max(0.0, min(1.8, blended)), 3)

        self._cache_day = target_day
        self._cache_timezone = settings.timezone
        self._cache_factors = list(factors)
        return factors


weather_forecast_service = WeatherForecastService()
