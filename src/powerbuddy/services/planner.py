from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
from zoneinfo import ZoneInfo

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.repositories import PowerRepository


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


class DayPlanner:
    def __init__(self) -> None:
        self.min_soc = float(settings.battery_min_soc)
        self.max_soc = float(settings.battery_max_soc)
        self.capacity_kwh = float(settings.battery_capacity_kwh)
        self.max_charge_kwh = min(float(settings.max_charge_kw), float(settings.planned_charge_kw))
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

    def _hourly_consumption_for_points(
        self,
        points: list[PricePoint],
        expected_daily_consumption_kwh: float,
        hourly_profile: list[float],
        pv_profile_kwh: list[float],
        pv_weather_factor_24h: list[float] | None = None,
    ) -> list[float]:
        if not points:
            return []

        weights = [max(0.0, hourly_profile[self._local_hour(point.timestamp)]) for point in points]
        total_weight = sum(weights)
        if total_weight <= 0:
            gross = [expected_daily_consumption_kwh / max(len(points), 1)] * len(points)
        else:
            scale = expected_daily_consumption_kwh / total_weight
            gross = [weight * scale for weight in weights]

        net: list[float] = []
        for idx, point in enumerate(points):
            hour = self._local_hour(point.timestamp)
            weather_factor = 1.0
            if pv_weather_factor_24h is not None and len(pv_weather_factor_24h) == 24:
                weather_factor = max(0.0, float(pv_weather_factor_24h[hour]))
            pv = max(0.0, pv_profile_kwh[hour] * weather_factor)
            net.append(max(0.0, gross[idx] - pv))
        return net

    def _transition(self, action: str, soc: int, hourly_net_consumption_kwh: float) -> tuple[float, float, float]:
        """
        Returns (delta_battery_kwh, grid_kwh, battery_throughput_kwh) for one hour.
        """
        soc_f = float(soc)
        available_room_kwh = ((self.max_soc - soc_f) / 100.0) * self.capacity_kwh
        available_energy_kwh = ((soc_f - self.min_soc) / 100.0) * self.capacity_kwh

        if action == "charge":
            max_grid_charge_kwh = min(self.max_charge_kwh, available_room_kwh / max(self.charge_efficiency, 1e-6))
            grid_charge_kwh = max(0.0, max_grid_charge_kwh)
            battery_delta = grid_charge_kwh * self.charge_efficiency
            return battery_delta, hourly_net_consumption_kwh + grid_charge_kwh, abs(battery_delta)

        if action == "discharge":
            max_delivery_kwh = min(self.max_discharge_kwh, hourly_net_consumption_kwh)
            max_delivery_from_soc = available_energy_kwh * self.discharge_efficiency
            delivered_kwh = max(0.0, min(max_delivery_kwh, max_delivery_from_soc))
            battery_delta = -(delivered_kwh / max(self.discharge_efficiency, 1e-6))
            grid_kwh = max(0.0, hourly_net_consumption_kwh - delivered_kwh)
            return battery_delta, grid_kwh, abs(battery_delta)

        return 0.0, hourly_net_consumption_kwh, 0.0

    def plan(self, data: PlannerInput) -> list[PlanAction]:
        if not data.price_points:
            return []

        points = sorted(data.price_points, key=lambda p: p.timestamp)
        if data.planning_start_time is not None:
            planning_start = (
                data.planning_start_time.replace(tzinfo=None)
                if data.planning_start_time.tzinfo
                else data.planning_start_time
            )
            points = [
                p
                for p in points
                if ((p.timestamp.replace(tzinfo=None) if p.timestamp.tzinfo else p.timestamp) >= planning_start)
            ]
        if not points:
            return []

        expected_daily_consumption_kwh, _ = self.resolve_expected_daily_consumption(data.day)
        hourly_profile, _ = self.resolve_hourly_consumption_profile(data.day)
        pv_profile_kwh, _ = self.resolve_hourly_pv_profile(data.day)
        net_base_kwh = self._hourly_consumption_for_points(
            points=points,
            expected_daily_consumption_kwh=expected_daily_consumption_kwh,
            hourly_profile=hourly_profile,
            pv_profile_kwh=pv_profile_kwh,
            pv_weather_factor_24h=data.pv_weather_factor_24h,
        )

        low_factor = max(0.4, float(settings.scenario_low_factor))
        base_factor = max(0.4, float(settings.scenario_base_factor))
        high_factor = max(base_factor, float(settings.scenario_high_factor))

        scenario_low = [value * low_factor for value in net_base_kwh]
        scenario_base = [value * base_factor for value in net_base_kwh]
        scenario_high = [value * high_factor for value in net_base_kwh]

        soc_min = int(self.min_soc)
        soc_max = int(self.max_soc)
        reserve_soc = max(soc_min, min(soc_max, int(round(float(settings.reserve_soc_min_percent)))))
        start_soc = max(soc_min, min(soc_max, int(round(data.start_soc))))

        actions_space = ("charge", "hold", "discharge")
        inf = float("inf")
        eps = 1e-9

        dp: list[dict[int, float]] = [{soc: inf for soc in range(soc_min, soc_max + 1)} for _ in range(len(points) + 1)]
        choice: list[dict[int, str]] = [{soc: "hold" for soc in range(soc_min, soc_max + 1)} for _ in range(len(points))]

        # Terminal cost: soft preference to maintain battery state close to start_soc.
        for soc in range(soc_min, soc_max + 1):
            soc_deviation = abs(soc - start_soc)
            dp[len(points)][soc] = soc_deviation * 50.0

        robust_risk_weight = max(0.0, float(settings.scenario_high_penalty_weight))
        reserve_start = max(0, min(23, int(settings.reserve_soc_start_hour_local)))

        for t in range(len(points) - 1, -1, -1):
            price = points[t].price_ore_per_kwh
            tariff = data.tariff_ore_per_hour[t] if data.tariff_ore_per_hour else 0.0
            for soc in range(soc_min, soc_max + 1):
                best_cost = inf
                best_action = "hold"

                for action in actions_space:
                    delta_base, grid_base, throughput_base = self._transition(action, soc, scenario_base[t])
                    if action != "hold" and abs(delta_base) < eps:
                        continue

                    next_soc = self._next_soc(soc, delta_base)

                    if self._is_reserve_hour(points[t].timestamp) and next_soc < reserve_soc:
                        continue

                    _, grid_low, _ = self._transition(action, soc, scenario_low[t])
                    _, grid_high, _ = self._transition(action, soc, scenario_high[t])

                    cost_low = self._cost_ore(grid_low, price, tariff)
                    cost_base = self._cost_ore(grid_base, price, tariff)
                    cost_high = self._cost_ore(grid_high, price, tariff)

                    expected_cost = (0.2 * cost_low) + (0.6 * cost_base) + (0.2 * cost_high)
                    risk_penalty = robust_risk_weight * max(0.0, cost_high - cost_base)
                    degradation_cost = throughput_base * self.degradation_cost_ore_per_kwh

                    reserve_precharge_penalty = 0.0
                    local_hour = self._local_hour(points[t].timestamp)
                    if settings.reserve_soc_enabled and local_hour == ((reserve_start - 1) % 24) and next_soc < reserve_soc:
                        reserve_precharge_penalty = float(reserve_soc - next_soc) * 200.0

                    total_cost = expected_cost + risk_penalty + degradation_cost + reserve_precharge_penalty + dp[t + 1][next_soc]

                    if total_cost < best_cost - eps or (abs(total_cost - best_cost) <= eps and action == "hold"):
                        best_cost = total_cost
                        best_action = action

                dp[t][soc] = best_cost
                choice[t][soc] = best_action

        actions: list[PlanAction] = []
        soc = start_soc
        for idx, point in enumerate(points):
            action = choice[idx][soc]
            delta_battery_kwh, _grid_kwh, _throughput = self._transition(action, soc, scenario_base[idx])
            next_soc = self._next_soc(soc, delta_battery_kwh)

            target_soc = None
            charge_power_w = None
            reason = "normal operation window"

            if action == "discharge":
                target_soc = float(next_soc)
                reason = "robust optimization: discharge at expensive hour"
            elif action == "charge":
                target_soc = float(next_soc)
                grid_charge_kwh = max(0.0, delta_battery_kwh / max(self.charge_efficiency, 1e-6))
                charge_power_w = round(grid_charge_kwh * 1000.0, 1)
                reason = "robust optimization: charge at cheap hour"

            start = point.timestamp
            end = start + timedelta(hours=1)
            actions.append(
                PlanAction(
                    date_key=data.day.isoformat(),
                    start_time=start,
                    end_time=end,
                    action=action,
                    charge_power_w=charge_power_w,
                    target_soc=target_soc,
                    reason=reason,
                    is_manual_override=False,
                )
            )
            soc = next_soc

        return actions


    def simulate(
        self,
        day: date,
        actions: list[PlanAction],
        start_soc: float,
        pv_weather_factor_24h: list[float] | None = None,
    ) -> list[SimulationPoint]:
        soc = max(self.min_soc, min(self.max_soc, start_soc))
        points: list[SimulationPoint] = []
        expected_daily_consumption_kwh, _ = self.resolve_expected_daily_consumption(day)
        hourly_profile, _ = self.resolve_hourly_consumption_profile(day)
        pv_profile_kwh, _ = self.resolve_hourly_pv_profile(day)
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
            expected_daily_consumption_kwh=expected_daily_consumption_kwh,
            hourly_profile=hourly_profile,
            pv_profile_kwh=pv_profile_kwh,
            pv_weather_factor_24h=pv_weather_factor_24h,
        )

        for idx, action in enumerate(sorted_actions):
            consumption_kwh = hourly_net_consumption[idx] if idx < len(hourly_net_consumption) else 0.0
            soc_before = soc
            soc_int = int(round(soc_before))

            if action.action == "charge":
                if action.charge_power_w is not None:
                    grid_charge_kwh = min(self.max_charge_kwh, max(0.0, float(action.charge_power_w) / 1000.0))
                    delta_soc = (grid_charge_kwh * self.charge_efficiency / self.capacity_kwh) * 100.0
                    soc = min(self.max_soc, soc + delta_soc)
                    projected_grid_kwh = consumption_kwh + grid_charge_kwh
                else:
                    delta_battery, projected_grid_kwh, _ = self._transition("charge", soc_int, consumption_kwh)
                    soc = max(self.min_soc, min(self.max_soc, soc + (delta_battery / self.capacity_kwh) * 100.0))
            elif action.action == "discharge":
                delta_battery, projected_grid_kwh, _ = self._transition("discharge", soc_int, consumption_kwh)
                soc = max(self.min_soc, min(self.max_soc, soc + (delta_battery / self.capacity_kwh) * 100.0))
            else:
                projected_grid_kwh = consumption_kwh

            if action.target_soc is not None:
                soc = max(self.min_soc, min(self.max_soc, float(action.target_soc), soc))

            actual_battery_delta_kwh = ((soc - soc_before) / 100.0) * self.capacity_kwh
            if action.action == "hold":
                projected_grid = consumption_kwh
            elif action.action == "charge":
                projected_grid = max(0.0, consumption_kwh + (actual_battery_delta_kwh / max(self.charge_efficiency, 1e-6)))
            else:
                delivered = max(0.0, -actual_battery_delta_kwh * self.discharge_efficiency)
                projected_grid = max(0.0, consumption_kwh - delivered)

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
