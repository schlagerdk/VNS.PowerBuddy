from __future__ import annotations

from datetime import date, datetime, timedelta

import httpx

from powerbuddy.config import settings
from powerbuddy.models import PricePoint


def _normalize_area(area: str) -> str:
    normalized = area.strip().upper()
    return normalized if normalized in {"DK1", "DK2"} else "DK1"


class PriceProvider:
    async def get_day_prices(self, day: date, area: str) -> list[PricePoint]:
        raise NotImplementedError

    async def get_latest_available_day(self, area: str) -> date | None:
        raise NotImplementedError


class EnergiDataServiceProvider(PriceProvider):
    base_url = "https://api.energidataservice.dk/dataset/Elspotprices"

    async def get_day_prices(self, day: date, area: str) -> list[PricePoint]:
        area = _normalize_area(area)
        start_dt = datetime.combine(day, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)

        # Energi Data Service expects minute precision timestamps.
        start = start_dt.strftime("%Y-%m-%dT%H:%M")
        end = end_dt.strftime("%Y-%m-%dT%H:%M")
        params = {
            "start": start,
            "end": end,
            "filter": '{"PriceArea":"%s"}' % area,
            "limit": 48,
            "sort": "HourUTC ASC",
        }
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()
            records = response.json().get("records", [])

        prices: list[PricePoint] = []
        for row in records:
            spot_price_dkk_mwh = float(row.get("SpotPriceDKK", 0))
            ore_per_kwh = (spot_price_dkk_mwh / 1000.0) * 100.0
            timestamp = datetime.fromisoformat(row["HourUTC"].replace("Z", "+00:00"))
            prices.append(
                PricePoint(
                    timestamp=timestamp,
                    area=area,
                    price_ore_per_kwh=ore_per_kwh,
                    currency="DKK",
                    source="energidataservice",
                )
            )
        return prices

    async def get_latest_available_day(self, area: str) -> date | None:
        area = _normalize_area(area)
        params = {
            "sort": "HourUTC DESC",
            "limit": 1,
            "filter": '{"PriceArea":"%s"}' % area,
        }
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()
            records = response.json().get("records", [])

        if not records:
            return None

        ts = datetime.fromisoformat(records[0]["HourUTC"].replace("Z", "+00:00"))
        return ts.date()


class ElprisenLigeNuProvider(PriceProvider):
    base_url = "https://www.elprisenligenu.dk/api/v1/prices"

    async def get_day_prices(self, day: date, area: str) -> list[PricePoint]:
        area = _normalize_area(area)
        date_part = day.strftime("%m-%d")
        url = f"{self.base_url}/{day.year}/{date_part}_{area}.json"

        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(url)
            if response.status_code == 404:
                return []
            response.raise_for_status()
            records = response.json()

        prices: list[PricePoint] = []
        for row in records:
            ore_per_kwh = float(row.get("DKK_per_kWh", 0.0)) * 100.0
            timestamp = datetime.fromisoformat(row["time_start"])
            prices.append(
                PricePoint(
                    timestamp=timestamp,
                    area=area,
                    price_ore_per_kwh=ore_per_kwh,
                    currency="DKK",
                    source="elprisenligenu",
                )
            )
        return prices

    async def get_latest_available_day(self, area: str) -> date | None:
        area = _normalize_area(area)
        today = date.today()
        for offset in range(0, 14):
            probe_day = today - timedelta(days=offset)
            points = await self.get_day_prices(probe_day, area)
            if points:
                return probe_day
        return None


def get_price_provider() -> PriceProvider:
    provider = settings.price_provider.lower()
    if provider == "energidataservice":
        return EnergiDataServiceProvider()
    if provider in {"elprisenligenu", "elprisen"}:
        return ElprisenLigeNuProvider()
    raise ValueError(f"Unsupported price provider: {settings.price_provider}")
