"""
TariffService — fetches the Danish DSO network tariff (Nettarif C, DT_C_01)
from Energi Data Service and combines it with fixed state fees so the planner
can optimise on total cost (spot + network + state taxes).

Tariff layers (all in øre/kWh, excluding VAT):
  1. Network tariff      — time-varying, fetched fresh from DatahubPricelist
  2. Energinet systemtarif — flat, configured via POWERBUDDY_TARIFF_ENERGINET_ORE
  3. Elafgift (state tax) — flat, configured via POWERBUDDY_TARIFF_ELAFGIFT_ORE

VAT (25 %) is applied on top for display purposes only; the optimizer works with
pre-VAT values because VAT is proportional and does not change hour ranking.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import httpx

from powerbuddy.config import settings

logger = logging.getLogger(__name__)

# ── Fallback Nettarif C values ────────────────────────────────────────────────
# Captured from DatahubPricelist for Radius Elnet A/S / DT_C_01 on 2026-Q1.
# Used when the live API call fails.
_FALLBACK_NETWORK_ORE: list[float] = [
    10.617, 10.617, 10.617, 10.617, 10.617, 10.617,   # h00-05  (lav)
    15.926, 15.926, 15.926, 15.926, 15.926, 15.926,   # h06-11  (dag)
    15.926, 15.926, 15.926, 15.926, 15.926,            # h12-16  (dag)
    41.408, 41.408, 41.408, 41.408,                    # h17-20  (peak)
    15.926, 15.926, 15.926,                            # h21-23  (dag)
]

_DATAHUB_URL = "https://api.energidataservice.dk/dataset/DatahubPricelist"


class TariffService:
    """
    Provides a 24-element hourly tariff profile (øre/kWh, excl. VAT) for use
    in the battery day planner.  Data is refreshed at most once per calendar day.
    """

    def __init__(self) -> None:
        self._cached_network: list[float] | None = None
        self._cache_date: date | None = None
        self._manual_network_override: list[float] | None = None
        self._state_path = Path("./data/tariff_overrides.json")
        self._load_persisted_overrides()

    # ── Public interface ──────────────────────────────────────────────────────

    async def get_network_tariff_24h(self) -> list[float]:
        """
        Returns 24 hourly Nettarif C values in øre/kWh (excl. VAT).
        Refreshes from the API once per day; falls back to hardcoded values on error.
        """
        today = date.today()
        if self._manual_network_override is not None:
            return list(self._manual_network_override)
        if self._cached_network is not None and self._cache_date == today:
            return self._cached_network

        try:
            fetched = await self._fetch_nettarif_c(today)
            if fetched and len(fetched) == 24:
                self._cached_network = fetched
                self._cache_date = today
                logger.info(
                    "Network tariff refreshed from DatahubPricelist: h17-20 peak = %.3f øre/kWh",
                    fetched[17],
                )
                return fetched
        except Exception as exc:
            logger.warning("DatahubPricelist fetch failed (%s) — using fallback tariff", exc)

        # Return fallback (but don't cache it against today so next run retries)
        return list(_FALLBACK_NETWORK_ORE)

    def update_runtime_config(
        self,
        network_owner: str | None = None,
        network_code: str | None = None,
        energinet_ore_flat: float | None = None,
        elafgift_ore_flat: float | None = None,
        vat_factor: float | None = None,
    ) -> None:
        if network_owner is not None:
            settings.tariff_network_owner = network_owner
        if network_code is not None:
            settings.tariff_network_code = network_code
        if energinet_ore_flat is not None:
            settings.tariff_energinet_ore = float(energinet_ore_flat)
        if elafgift_ore_flat is not None:
            settings.tariff_elafgift_ore = float(elafgift_ore_flat)
        if vat_factor is not None:
            settings.tariff_vat_factor = float(vat_factor)

        # Invalidate cache so next read pulls new data if owner/code changed.
        self._cached_network = None
        self._cache_date = None
        self._save_persisted_overrides()

    def set_manual_network_override(self, hourly_ore: list[float]) -> None:
        if len(hourly_ore) != 24:
            raise ValueError("network override must contain 24 hourly values")
        self._manual_network_override = [float(v) for v in hourly_ore]
        self._save_persisted_overrides()

    def clear_manual_network_override(self) -> None:
        self._manual_network_override = None
        self._save_persisted_overrides()

    def total_tariff_ore_24h(self, network_tariff: list[float]) -> list[float]:
        """
        Adds flat Energinet systemtarif + elafgift (both excl. VAT) to each
        hourly network tariff to get the full 24h fixed-fee profile.

        Spot price is NOT included here; add it separately in the planner.
        """
        flat = settings.tariff_energinet_ore + settings.tariff_elafgift_ore
        return [n + flat for n in network_tariff]

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _fetch_nettarif_c(self, day: date) -> list[float] | None:
        """
        Fetch the currently valid DT_C_01 record from DatahubPricelist.
        Price{1}–Price{24} correspond to hours 0–23 in DKK/kWh; we convert to øre.
        """
        owner = settings.tariff_network_owner
        code = settings.tariff_network_code

        # Filter for records with ValidFrom ≤ today so we get the *currently active*
        # tariff and not a future one that has not taken effect yet.
        tomorrow = day.strftime("%Y-%m-%dT00:00")
        params = {
            "limit": 5,
            "filter": f'{{"ChargeOwner":"{owner}","ChargeTypeCode":"{code}"}}',
            "end": tomorrow,          # ValidFrom must be < tomorrow (i.e. ≤ today)
            "sort": "ValidFrom DESC", # most recently started record first
        }
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(_DATAHUB_URL, params=params)
            resp.raise_for_status()
            records = resp.json().get("records", [])

        if not records:
            return None

        rec = records[0]
        # Price fields are 1-indexed (Price1 = h00, Price24 = h23) in DKK/kWh.
        return [(rec.get(f"Price{i + 1}") or 0.0) * 100.0 for i in range(24)]

    def _load_persisted_overrides(self) -> None:
        if not self._state_path.exists():
            return
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed reading tariff override file: %s", exc)
            return

        runtime = payload.get("runtime", {})
        self.update_runtime_config(
            network_owner=runtime.get("network_owner"),
            network_code=runtime.get("network_code"),
            energinet_ore_flat=runtime.get("energinet_ore_flat"),
            elafgift_ore_flat=runtime.get("elafgift_ore_flat"),
            vat_factor=runtime.get("vat_factor"),
        )

        manual = payload.get("manual_network_tariff_ore_per_hour")
        if isinstance(manual, list) and len(manual) == 24:
            self._manual_network_override = [float(v) for v in manual]

    def _save_persisted_overrides(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "runtime": {
                "network_owner": settings.tariff_network_owner,
                "network_code": settings.tariff_network_code,
                "energinet_ore_flat": settings.tariff_energinet_ore,
                "elafgift_ore_flat": settings.tariff_elafgift_ore,
                "vat_factor": settings.tariff_vat_factor,
            },
            "manual_network_tariff_ore_per_hour": self._manual_network_override,
        }
        self._state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# Module-level singleton — import this everywhere.
tariff_service = TariffService()
