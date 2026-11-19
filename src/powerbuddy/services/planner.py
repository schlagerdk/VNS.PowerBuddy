from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
from zoneinfo import ZoneInfo

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.repositories import PowerRepository


WINTER_CHEAP_CHARGE_REASON = "winter cheap-hour charging floor"


def is_winter_charge_floor(action: PlanAction) -> bool:
    return action.action == "charge" and any(
        reason.removeprefix("provisional fallback: ") == WINTER_CHEAP_CHARGE_REASON
        for reason in (action.reason or "").split("; ")
    )


def is_protected_action(action: PlanAction, protected_starts: set[datetime]) -> bool:
    start = action.start_time.replace(tzinfo=None) if action.start_time.tzinfo else action.start_time
    return bool(action.is_manual_override) or start in protected_starts


@dataclass(frozen=True, slots=True)
class SimulationInputs:
    expected_daily_consumption_kwh: float
    hourly_consumption_profile: tuple[float, ...]
    hourly_pv_profile_kwh: tuple[float, ...]


@dataclass(slots=True)
class PlannerInput:
    day: date
    price_points: list[PricePoint]
    start_soc: float
    planning_start_time: datetime | None = None
    # Optional 24-element array of total non-spot tariff (ore/kWh, excl. VAT).
    tariff_ore_per_hour: list[float] | None = None
    # Optional weather scaling for PV forecast (24 local-hour factors).
    pv_weather_factor_24h: list[float] | None = None
    locally_protected_actions: list[PlanAction] | None = None
    simulation_inputs: SimulationInputs | None = None
    simulation_start_time: datetime | None = None
    # Released prices for the following day; used to value energy left in the battery at day end.
    next_day_price_points: list[PricePoint] | None = None


class DayPlanner:
    def __init__(self) -> None:
        self.min_soc = float(settings.battery_min_soc)
        self.max_soc = float(settings.battery_max_soc)
        self.capacity_kwh = float(settings.battery_capacity_kwh)
        self.max_charge_kwh = float(settings.max_charge_kw)
        self.charge_setpoint_w = round(self.max_charge_kwh * 1000.0, 1)
        self.max_discharge_kwh = float(settings.max_discharge_kw)
        self.charge_efficiency = max(0.5, min(1.0, float(settings.charge_efficiency)))
        self.discharge_efficiency = max(0.5, min(1.0, float(settings.discharge_efficiency)))
        self.degradation_cost_ore_per_kwh = max(0.0, float(settings.cycle_degradation_cost_ore_per_kwh))
        self.timezone = ZoneInfo(settings.timezone)

    @staticmethod
    def _seasonal_anchor_daily_kwh(reference_day: date) -> float | None:
        raw = (settings.seasonal_anchor_monthly_daily_kwh_json or "").strip()
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except Exception:
            return None
        month_key = str(reference_day.month)
        if month_key not in payload:
            return None
        try:
            value = float(payload[month_key])
        except Exception:
            return None
        return value if value > 0 else None

    def resolve_expected_daily_consumption(self, reference_day: date) -> tuple[float, str]:
        fallback = float(settings.expected_daily_consumption_kwh)
        source = "config"
        if not settings.dynamic_consumption_enabled:
            dynamic = fallback
        else:
            dynamic_value = PowerRepository.rolling_average_daily_consumption_kwh(
                reference_day=reference_day,
                lookback_days=settings.dynamic_consumption_lookback_days,
                min_samples_per_day=settings.dynamic_consumption_min_samples_per_day,
            )
            if dynamic_value is None:
                dynamic = fallback
            else:
                dynamic = dynamic_value
                source = "historical-average"

        if not settings.seasonal_anchor_enabled:
            return dynamic, source

        anchor = self._seasonal_anchor_daily_kwh(reference_day)
        if anchor is None:
            return dynamic, source

        weight = max(0.0, min(1.0, float(settings.seasonal_anchor_weight)))
        blended = ((1.0 - weight) * dynamic) + (weight * anchor)

        max_dev = max(0.0, float(settings.seasonal_anchor_max_deviation_ratio))
        floor = anchor * (1.0 - max_dev)
        ceil = anchor * (1.0 + max_dev)
        bounded = max(floor, min(ceil, blended))
        return bounded, f"{source}+seasonal-anchor"

    def resolve_hourly_consumption_profile(self, reference_day: date) -> tuple[list[float], str]:
        if settings.dynamic_consumption_enabled and settings.consumption_profile_weekpart_enabled:
            weekpart_profile = PowerRepository.rolling_average_hourly_consumption_profile_weekpart(
                reference_day=reference_day,
                lookback_days=settings.dynamic_consumption_lookback_days,
                min_samples_per_day=settings.dynamic_consumption_min_samples_per_day,
            )
            if weekpart_profile is not None:
                return weekpart_profile, "historical-hourly-shape-weekpart"

        if settings.dynamic_consumption_enabled:
            profile = PowerRepository.rolling_average_hourly_consumption_profile(
                reference_day=reference_day,
                lookback_days=settings.dynamic_consumption_lookback_days,
                min_samples_per_day=settings.dynamic_consumption_min_samples_per_day,
            )
            if profile is not None:
                return profile, "historical-hourly-shape"

        return ([1.0 / 24.0] * 24), "flat"

    def resolve_hourly_pv_profile(self, reference_day: date) -> tuple[list[float], str]:
        if not settings.pv_forecast_enabled:
            return ([0.0] * 24), "disabled"

        profile = PowerRepository.rolling_average_hourly_pv_profile(
            reference_day=reference_day,
            lookback_days=settings.pv_forecast_lookback_days,
            min_samples_per_day=settings.pv_forecast_min_samples_per_day,
        )
        if profile is None:
            return ([0.0] * 24), "none"
        return profile, "historical-hourly-pv"

    def capture_simulation_inputs(self, reference_day: date) -> SimulationInputs:
        daily_consumption, _ = self.resolve_expected_daily_consumption(reference_day)
        consumption_profile, _ = self.resolve_hourly_consumption_profile(reference_day)
        pv_profile, _ = self.resolve_hourly_pv_profile(reference_day)
        return SimulationInputs(
            expected_daily_consumption_kwh=daily_consumption,
            hourly_consumption_profile=tuple(consumption_profile),
            hourly_pv_profile_kwh=tuple(pv_profile),
        )

    @staticmethod
    def _cost_ore(grid_kwh: float, price_ore_per_kwh: float, tariff_ore: float = 0.0) -> float:
        if grid_kwh >= 0:
            vat_factor = float(settings.tariff_vat_factor) if settings.objective_include_vat else 1.0
            return grid_kwh * (price_ore_per_kwh + tariff_ore) * vat_factor
        return grid_kwh * float(settings.feed_in_tariff_ore)

    def _next_soc(self, soc: int, delta_battery_kwh: float) -> int:
        next_soc = int(round(soc + (delta_battery_kwh / self.capacity_kwh) * 100.0))
        return max(int(self.min_soc), min(int(self.max_soc), next_soc))

    def _local_hour(self, dt: datetime) -> int:
        if dt.tzinfo is None:
            return int(dt.hour)
        return int(dt.astimezone(self.timezone).hour)

    def _is_reserve_hour(self, dt: datetime) -> bool:
        if not settings.reserve_soc_enabled:
            return False
        start = max(0, min(23, int(settings.reserve_soc_start_hour_local)))
        end = max(1, min(24, int(settings.reserve_soc_end_hour_local)))
        hour = self._local_hour(dt)
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    @staticmethod
    def _is_hour_in_window(hour: int, start: int, end: int) -> bool:
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def _hourly_consumption_for_points(
        self,
        points: list[PricePoint],
        expected_daily_consumption_kwh: float,
        hourly_profile: Sequence[float],
        pv_profile_kwh: Sequence[float],
        pv_weather_factor_24h: list[float] | None = None,
        reference_day: date | None = None,
    ) -> list[float]:
        if not points:
            return []

        day = reference_day or (
            points[0].timestamp.astimezone(self.timezone).date()
            if points[0].timestamp.tzinfo else points[0].timestamp.date()
        )
        weights = [max(0.0, float(hourly_profile[hour])) for hour in range(24)]
        day_start = datetime.combine(day, time.min, tzinfo=self.timezone).astimezone(timezone.utc)
        day_end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=self.timezone).astimezone(timezone.utc)
        local_hours: list[int] = []
        cursor = day_start
        while cursor < day_end:
            local_hours.append(cursor.astimezone(self.timezone).hour)
            cursor += timedelta(hours=1)
        total_weight = sum(weights[hour] for hour in local_hours)
        if total_weight <= 0:
            weights = [1.0] * 24
            total_weight = float(len(local_hours))
        scale = expected_daily_consumption_kwh / total_weight

        net: list[float] = []
        for point in points:
            hour = self._local_hour(point.timestamp)
            weather_factor = 1.0
            if pv_weather_factor_24h is not None and len(pv_weather_factor_24h) == 24:
                weather_factor = max(0.0, float(pv_weather_factor_24h[hour]))
            pv = max(0.0, pv_profile_kwh[hour] * weather_factor)
            # Keep sign so negative values represent PV surplus available for battery charging.
            net.append(weights[hour] * scale - pv)
        return net

    def _transition(
        self,
        action: str,
        soc: float,
        hourly_net_consumption_kwh: float,
        duration_hours: float = 1.0,
    ) -> tuple[float, float, float]:
        """
        Returns (delta_battery_kwh, grid_kwh, battery_throughput_kwh) for one hour.
        """
        duration = max(0.0, min(1.0, float(duration_hours)))
        net_consumption_kwh = float(hourly_net_consumption_kwh) * duration
        soc_f = float(soc)
        available_room_kwh = ((self.max_soc - soc_f) / 100.0) * self.capacity_kwh
        available_energy_kwh = ((soc_f - self.min_soc) / 100.0) * self.capacity_kwh

        if action == "charge":
            max_grid_charge_kwh = min(self.max_charge_kwh * duration, available_room_kwh / max(self.charge_efficiency, 1e-6))
            grid_charge_kwh = max(0.0, max_grid_charge_kwh)
            battery_delta = grid_charge_kwh * self.charge_efficiency
            return battery_delta, net_consumption_kwh + grid_charge_kwh, abs(battery_delta)

        if action in {"auto", "discharge"}:
            # With PV surplus (negative net), auto can absorb solar into battery.
            if action == "auto" and net_consumption_kwh < 0.0:
                solar_surplus_kwh = abs(net_consumption_kwh)
                max_solar_charge_kwh = min(
                    solar_surplus_kwh,
                    self.max_charge_kwh * duration,
                    available_room_kwh / max(self.charge_efficiency, 1e-6),
                )
                battery_delta = max_solar_charge_kwh * self.charge_efficiency
                return battery_delta, net_consumption_kwh + max_solar_charge_kwh, abs(battery_delta)

            max_delivery_kwh = min(self.max_discharge_kwh * duration, net_consumption_kwh)
            max_delivery_from_soc = available_energy_kwh * self.discharge_efficiency
            delivered_kwh = max(0.0, min(max_delivery_kwh, max_delivery_from_soc))
            battery_delta = -(delivered_kwh / max(self.discharge_efficiency, 1e-6))
            grid_kwh = net_consumption_kwh - delivered_kwh
            return battery_delta, grid_kwh, abs(battery_delta)

        return 0.0, net_consumption_kwh, 0.0

    def step(
        self,
        action: str,
        soc: float,
        hourly_net_consumption_kwh: float,
        duration_hours: float,
        charge_power_w: float | None = None,
    ) -> tuple[float, float]:
        """Advance one interval; returns (soc_after, grid_kwh). Shared by simulation and optimizer."""
        duration = max(0.0, min(1.0, float(duration_hours)))
        consumption_kwh = float(hourly_net_consumption_kwh) * duration
        soc_before = soc

        if duration > 0.0 and action == "charge":
            if charge_power_w is not None:
                grid_charge_kwh = min(self.max_charge_kwh, max(0.0, float(charge_power_w) / 1000.0)) * duration
                delta_soc = (grid_charge_kwh * self.charge_efficiency / self.capacity_kwh) * 100.0
                soc = min(self.max_soc, soc + delta_soc)
            else:
                delta_battery, _, _ = self._transition(
                    "charge", soc_before, hourly_net_consumption_kwh, duration_hours=duration
                )
                soc = max(self.min_soc, min(self.max_soc, soc + (delta_battery / self.capacity_kwh) * 100.0))
        elif duration > 0.0 and action in {"auto", "discharge", "discharge_force"}:
            transition_action = "discharge" if action == "discharge_force" else action
            delta_battery, _, _ = self._transition(
                transition_action, soc_before, hourly_net_consumption_kwh, duration_hours=duration
            )
            soc = max(self.min_soc, min(self.max_soc, soc + (delta_battery / self.capacity_kwh) * 100.0))

        actual_battery_delta_kwh = ((soc - soc_before) / 100.0) * self.capacity_kwh
        if action == "hold":
            grid_kwh = consumption_kwh
        elif action == "charge":
            grid_charge = max(0.0, actual_battery_delta_kwh) / max(self.charge_efficiency, 1e-6)
            grid_kwh = consumption_kwh + grid_charge
        elif consumption_kwh < 0:
            solar_charge = max(0.0, actual_battery_delta_kwh) / max(self.charge_efficiency, 1e-6)
            grid_kwh = min(0.0, consumption_kwh + solar_charge)
        else:
            delivered = max(0.0, -actual_battery_delta_kwh * self.discharge_efficiency)
            grid_kwh = max(0.0, consumption_kwh - delivered)
        return soc, grid_kwh

    def plan(self, data: PlannerInput) -> list[PlanAction]:
        """Cost-optimal plan; see powerbuddy.services.optimizer."""
        from powerbuddy.services.optimizer import optimize_day

        return optimize_day(self, data)

    def simulate(
        self,
        day: date,
        actions: list[PlanAction],
        start_soc: float,
        pv_weather_factor_24h: list[float] | None = None,
        simulation_inputs: SimulationInputs | None = None,
        simulation_start_time: datetime | None = None,
    ) -> list[SimulationPoint]:
        soc = max(self.min_soc, min(self.max_soc, start_soc))
        points: list[SimulationPoint] = []
        simulation_inputs = simulation_inputs or self.capture_simulation_inputs(day)
        sorted_actions = sorted(actions, key=lambda x: x.start_time)

        hourly_net_consumption = self._hourly_consumption_for_points(
            points=[
                PricePoint(
                    timestamp=action.start_time,
                    area=settings.price_area,
                    price_ore_per_kwh=0.0,
                    currency="DKK",
                    source="simulate",
                )
                for action in sorted_actions
            ],
            expected_daily_consumption_kwh=simulation_inputs.expected_daily_consumption_kwh,
            hourly_profile=simulation_inputs.hourly_consumption_profile,
            pv_profile_kwh=simulation_inputs.hourly_pv_profile_kwh,
            pv_weather_factor_24h=pv_weather_factor_24h,
            reference_day=day,
        )

        for idx, action in enumerate(sorted_actions):
            start = action.start_time
            end = action.end_time
            if start.tzinfo and end.tzinfo:
                start = start.astimezone(timezone.utc)
                end = end.astimezone(timezone.utc)
                if simulation_start_time is not None:
                    as_of = simulation_start_time
                    if as_of.tzinfo is None:
                        as_of = as_of.replace(tzinfo=self.timezone)
                    start = max(start, as_of.astimezone(timezone.utc))
            elif simulation_start_time is not None:
                as_of = simulation_start_time
                if as_of.tzinfo:
                    as_of = as_of.astimezone(self.timezone).replace(tzinfo=None)
                start = max(start, as_of)
            duration = max(0.0, min(1.0, (end - start).total_seconds() / 3600.0))
            soc, projected_grid = self.step(
                action.action, soc, hourly_net_consumption[idx], duration, action.charge_power_w
            )

            points.append(
                SimulationPoint(
                    date_key=day.isoformat(),
                    timestamp=action.start_time,
                    action=action.action,
                    projected_soc=round(soc, 2),
                    projected_grid_kwh=round(projected_grid, 3),
                )
            )

        if not points:
            midnight = datetime.combine(day, time.min, tzinfo=timezone.utc)
            points.append(
                SimulationPoint(
                    date_key=day.isoformat(),
                    timestamp=midnight,
                    action="hold",
                    projected_soc=soc,
                    projected_grid_kwh=0.0,
                )
            )

        return points
