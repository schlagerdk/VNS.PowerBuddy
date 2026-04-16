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
        default_charge_kw = max(0.0, float(settings.default_charge_power_w) / 1000.0)
        effective_charge_kw = default_charge_kw if default_charge_kw > 0.0 else float(settings.planned_charge_kw)
        self.max_charge_kwh = min(float(settings.max_charge_kw), effective_charge_kw)
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

    def _transition(
        self,
        action: str,
        soc: int,
        hourly_net_consumption_kwh: float,
        duration_hours: float = 1.0,
    ) -> tuple[float, float, float]:
        """
        Returns (delta_battery_kwh, grid_kwh, battery_throughput_kwh) for one hour.
        """
        duration = max(0.0, min(1.0, float(duration_hours)))
        net_consumption_kwh = max(0.0, float(hourly_net_consumption_kwh)) * duration
        soc_f = float(soc)
        available_room_kwh = ((self.max_soc - soc_f) / 100.0) * self.capacity_kwh
        available_energy_kwh = ((soc_f - self.min_soc) / 100.0) * self.capacity_kwh

        if action == "charge":
            max_grid_charge_kwh = min(self.max_charge_kwh * duration, available_room_kwh / max(self.charge_efficiency, 1e-6))
            grid_charge_kwh = max(0.0, max_grid_charge_kwh)
            battery_delta = grid_charge_kwh * self.charge_efficiency
            return battery_delta, net_consumption_kwh + grid_charge_kwh, abs(battery_delta)

        if action in {"auto", "discharge"}:
            max_delivery_kwh = min(self.max_discharge_kwh * duration, net_consumption_kwh)
            max_delivery_from_soc = available_energy_kwh * self.discharge_efficiency
            delivered_kwh = max(0.0, min(max_delivery_kwh, max_delivery_from_soc))
            battery_delta = -(delivered_kwh / max(self.discharge_efficiency, 1e-6))
            grid_kwh = max(0.0, net_consumption_kwh - delivered_kwh)
            return battery_delta, grid_kwh, abs(battery_delta)

        return 0.0, net_consumption_kwh, 0.0

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

        now_local_naive_raw = datetime.now(self.timezone).replace(tzinfo=None)
        hour_durations: list[float] = []
        for point in points:
            point_ts = point.timestamp.replace(tzinfo=None) if point.timestamp.tzinfo else point.timestamp
            duration = 1.0
            if point_ts.date() == now_local_naive_raw.date() and point_ts.hour == now_local_naive_raw.hour:
                hour_end = point_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                duration = max(0.0, min(1.0, (hour_end - now_local_naive_raw).total_seconds() / 3600.0))
            hour_durations.append(duration)

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
        must_charge_soc = max(
            soc_min,
            min(soc_max, int(round(float(settings.must_charge_window_min_soc_percent)))),
        )
        low_soc_force_threshold = max(
            soc_min,
            min(
                soc_max,
                int(round(float(settings.battery_min_soc) + float(settings.low_soc_force_charge_margin_percent))),
            ),
        )
        start_soc = max(soc_min, min(soc_max, int(round(data.start_soc))))

        must_charge_enabled = bool(settings.must_charge_window_enabled)
        low_soc_force_charge_enabled = bool(settings.low_soc_force_charge_enabled)
        low_soc_force_near_now_hours = max(0, int(settings.low_soc_force_planning_near_now_hours))
        must_start_hour = max(0, min(23, int(settings.must_charge_window_start_hour_local)))
        must_end_hour = max(1, min(24, int(settings.must_charge_window_end_hour_local)))
        now_local_naive = now_local_naive_raw.replace(minute=0, second=0, microsecond=0, tzinfo=None)

        enforce_low_soc_by_index: list[bool] = []
        for point in points:
            point_ts = point.timestamp.replace(tzinfo=None) if point.timestamp.tzinfo else point.timestamp
            enforce_low_soc_by_index.append(point_ts <= (now_local_naive + timedelta(hours=low_soc_force_near_now_hours)))

        auto_expensive_guard_enabled = bool(settings.auto_only_expensive_hours_enabled)
        auto_expensive_quantile = max(0.0, min(1.0, float(settings.auto_only_expensive_quantile)))
        auto_expensive_min_spread = max(0.0, float(settings.auto_only_expensive_min_spread_ore))
        cheap_slot_capture_enabled = bool(settings.cheap_slot_capture_enabled)
        cheap_slot_quantile = max(0.0, min(1.0, float(settings.cheap_slot_quantile)))
        cheap_slot_min_spread = max(0.0, float(settings.cheap_slot_min_spread_ore))
        cheap_slot_miss_penalty = max(0.0, float(settings.cheap_slot_miss_penalty_ore))
        cheap_equal_tolerance = max(0.0, float(settings.cheap_slot_equal_price_tolerance_ore))
        cheap_slot_min_target_soc = max(
            soc_min,
            min(soc_max, int(round(float(settings.cheap_slot_min_target_soc_percent)))),
        )
        expensive_precharge_enabled = bool(settings.expensive_window_precharge_enabled)
        expensive_coverage_ratio = max(0.0, min(1.0, float(settings.expensive_window_coverage_ratio)))
        expensive_min_span_hours = max(1, int(settings.expensive_window_min_span_hours))
        auto_block_extension_enabled = bool(settings.auto_block_extension_enabled)
        auto_block_drop_limit = max(0.0, float(settings.auto_block_extension_drop_limit_ore))
        auto_block_floor_above_cheap = max(0.0, float(settings.auto_block_extension_floor_above_cheap_ore))
        total_cost_levels = [
            float(points[idx].price_ore_per_kwh) + (float(data.tariff_ore_per_hour[idx]) if data.tariff_ore_per_hour else 0.0)
            for idx in range(len(points))
        ]
        auto_expensive_threshold = 0.0
        cheap_slot_threshold = 0.0
        cost_spread = 0.0
        if total_cost_levels:
            sorted_levels = sorted(total_cost_levels)
            cost_spread = sorted_levels[-1] - sorted_levels[0]
            if cost_spread >= auto_expensive_min_spread:
                q_index = int(round((len(sorted_levels) - 1) * auto_expensive_quantile))
                q_index = max(0, min(len(sorted_levels) - 1, q_index))
                auto_expensive_threshold = sorted_levels[q_index]
            else:
                auto_expensive_guard_enabled = False

            if cost_spread >= cheap_slot_min_spread:
                cheap_index = int(round((len(sorted_levels) - 1) * cheap_slot_quantile))
                cheap_index = max(0, min(len(sorted_levels) - 1, cheap_index))
                cheap_slot_threshold = sorted_levels[cheap_index]
            else:
                cheap_slot_capture_enabled = False
        else:
            auto_expensive_guard_enabled = False
            cheap_slot_capture_enabled = False

        expensive_entry_soc_requirements: dict[int, int] = {}
        if expensive_precharge_enabled and total_cost_levels and auto_expensive_threshold > 0.0:
            expensive_mask = [level >= auto_expensive_threshold for level in total_cost_levels]

            segments: list[tuple[int, int]] = []
            seg_start: int | None = None
            for idx, is_expensive in enumerate(expensive_mask):
                if is_expensive and seg_start is None:
                    seg_start = idx
                elif (not is_expensive) and seg_start is not None:
                    segments.append((seg_start, idx))
                    seg_start = None
            if seg_start is not None:
                segments.append((seg_start, len(expensive_mask)))

            for seg_start, seg_end in segments:
                if (seg_end - seg_start) < expensive_min_span_hours:
                    continue

                pre_idx = seg_start - 1
                if pre_idx < 0:
                    continue

                covered_discharge_kwh = 0.0
                for idx in range(seg_start, seg_end):
                    covered_discharge_kwh += min(self.max_discharge_kwh, max(0.0, scenario_base[idx]))

                needed_battery_kwh = (covered_discharge_kwh * expensive_coverage_ratio) / max(self.discharge_efficiency, 1e-6)
                needed_soc_points = int(round((needed_battery_kwh / max(self.capacity_kwh, 1e-6)) * 100.0))
                required_soc = max(reserve_soc, min(soc_max, reserve_soc + needed_soc_points))

                if required_soc > reserve_soc:
                    existing_required = expensive_entry_soc_requirements.get(pre_idx, reserve_soc)
                    expensive_entry_soc_requirements[pre_idx] = max(existing_required, required_soc)

        must_charge_exit_indices: set[int] = set()
        if must_charge_enabled:
            for idx, point in enumerate(points):
                hour = self._local_hour(point.timestamp)
                next_hour = self._local_hour(point.timestamp + timedelta(hours=1))
                in_window_now = self._is_hour_in_window(hour, must_start_hour, must_end_hour)
                in_window_next = self._is_hour_in_window(next_hour, must_start_hour, must_end_hour)
                if in_window_now and not in_window_next:
                    must_charge_exit_indices.add(idx)

        # Planner can only produce auto/charge/hold. Explicit discharge is manual-only.
        actions_space = ("charge", "hold", "auto")
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
        reserve_precharge_hours = max(1, int(settings.reserve_precharge_hours))
        reserve_precharge_penalty_per_soc = max(0.0, float(settings.reserve_precharge_penalty_per_soc_ore))
        reserve_precharge_enforce_enabled = bool(settings.reserve_precharge_enforce_enabled)
        reserve_hour_charge_penalty = max(0.0, float(settings.reserve_hour_charge_penalty_ore))
        reserve_entry_index = next((idx for idx, point in enumerate(points) if self._is_reserve_hour(point.timestamp)), None)
        cheap_slot_indices: set[int] = set(
            idx for idx, level in enumerate(total_cost_levels)
            if cheap_slot_capture_enabled and level <= cheap_slot_threshold
        )
        future_soc_targets: list[int] = [reserve_soc for _ in range(len(points))]
        rolling_required_soc = reserve_soc
        for idx in range(len(points) - 1, -1, -1):
            required_here = expensive_entry_soc_requirements.get(idx)
            if required_here is not None:
                rolling_required_soc = max(rolling_required_soc, required_here)
            future_soc_targets[idx] = rolling_required_soc

        for t in range(len(points) - 1, -1, -1):
            price = points[t].price_ore_per_kwh
            tariff = data.tariff_ore_per_hour[t] if data.tariff_ore_per_hour else 0.0
            for soc in range(soc_min, soc_max + 1):
                best_cost = inf
                best_action = "hold"

                for action in actions_space:
                    if (
                        low_soc_force_charge_enabled
                        and enforce_low_soc_by_index[t]
                        and soc <= low_soc_force_threshold
                        and action != "charge"
                    ):
                        continue

                    if self._is_reserve_hour(points[t].timestamp) and soc < reserve_soc and action != "charge":
                        continue

                    if (
                        action == "auto"
                        and auto_expensive_guard_enabled
                        and total_cost_levels[t] < auto_expensive_threshold
                        and not self._is_reserve_hour(points[t].timestamp)
                    ):
                        continue

                    delta_base, grid_base, throughput_base = self._transition(
                        action,
                        soc,
                        scenario_base[t],
                        duration_hours=hour_durations[t],
                    )
                    if action != "hold" and abs(delta_base) < eps:
                        continue

                    next_soc = self._next_soc(soc, delta_base)

                    if self._is_reserve_hour(points[t].timestamp) and next_soc < reserve_soc:
                        continue

                    if t in must_charge_exit_indices and next_soc < must_charge_soc:
                        continue

                    entry_required_soc = expensive_entry_soc_requirements.get(t)
                    if entry_required_soc is not None and next_soc < entry_required_soc:
                        continue

                    _, grid_low, _ = self._transition(action, soc, scenario_low[t], duration_hours=hour_durations[t])
                    _, grid_high, _ = self._transition(action, soc, scenario_high[t], duration_hours=hour_durations[t])

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

                    if (
                        reserve_precharge_enforce_enabled
                        and reserve_entry_index is not None
                        and t < reserve_entry_index
                        and (reserve_entry_index - t) <= reserve_precharge_hours
                        and next_soc < reserve_soc
                    ):
                        distance = max(1, reserve_entry_index - t)
                        urgency = 1.0 + ((reserve_precharge_hours - distance) / max(1.0, float(reserve_precharge_hours)))
                        reserve_precharge_penalty += float(reserve_soc - next_soc) * reserve_precharge_penalty_per_soc * urgency

                    cheap_slot_penalty = 0.0
                    cheap_slot_target_soc = max(future_soc_targets[t], cheap_slot_min_target_soc)
                    if (
                        cheap_slot_capture_enabled
                        and t in cheap_slot_indices
                        and action != "charge"
                        and next_soc < cheap_slot_target_soc
                        and cost_spread >= cheap_slot_min_spread
                    ):
                        soc_gap = max(1, cheap_slot_target_soc - next_soc)
                        cheap_slot_penalty = cheap_slot_miss_penalty * (1.0 + (float(soc_gap) / 40.0))

                    reserve_charge_penalty = 0.0
                    if action == "charge" and self._is_reserve_hour(points[t].timestamp):
                        reserve_charge_penalty = reserve_hour_charge_penalty

                    total_cost = (
                        expected_cost
                        + risk_penalty
                        + degradation_cost
                        + reserve_precharge_penalty
                        + cheap_slot_penalty
                        + reserve_charge_penalty
                        + dp[t + 1][next_soc]
                    )

                    if total_cost < best_cost - eps or (abs(total_cost - best_cost) <= eps and action == "hold"):
                        best_cost = total_cost
                        best_action = action

                dp[t][soc] = best_cost
                choice[t][soc] = best_action

        actions: list[PlanAction] = []
        soc = start_soc
        for idx, point in enumerate(points):
            action = choice[idx][soc]
            delta_battery_kwh, _grid_kwh, _throughput = self._transition(
                action,
                soc,
                scenario_base[idx],
                duration_hours=hour_durations[idx],
            )
            next_soc = self._next_soc(soc, delta_battery_kwh)

            target_soc = None
            charge_power_w = None
            reason = "normal operation window"

            if action == "auto":
                target_soc = float(next_soc)
                reason = "robust optimization: auto at expensive hour"
            elif action == "charge":
                target_soc = float(next_soc)
                charge_power_w = self.charge_setpoint_w
                if low_soc_force_charge_enabled and enforce_low_soc_by_index[idx] and soc <= low_soc_force_threshold:
                    reason = "low-soc safety charge"
                else:
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

        def _rebuild_actions_with_forced_charge(forced_indices: set[int], reason_prefix: str) -> list[PlanAction]:
            rebuilt: list[PlanAction] = []
            soc_local = start_soc
            for idx, point in enumerate(points):
                action_name = "charge" if idx in forced_indices else actions[idx].action
                delta_battery_kwh, _grid_kwh, _throughput = self._transition(
                    action_name,
                    soc_local,
                    scenario_base[idx],
                    duration_hours=hour_durations[idx],
                )
                next_soc_local = self._next_soc(soc_local, delta_battery_kwh)

                target_soc_local = None
                charge_power_w_local = None
                reason_local = "normal operation window"
                if action_name == "auto":
                    target_soc_local = float(next_soc_local)
                    reason_local = "robust optimization: auto at expensive hour"
                elif action_name == "charge":
                    target_soc_local = float(next_soc_local)
                    charge_power_w_local = self.charge_setpoint_w
                    if idx in forced_indices:
                        reason_local = reason_prefix
                    elif low_soc_force_charge_enabled and enforce_low_soc_by_index[idx] and soc_local <= low_soc_force_threshold:
                        reason_local = "low-soc safety charge"
                    else:
                        reason_local = "robust optimization: charge at cheap hour"

                start = point.timestamp
                end = start + timedelta(hours=1)
                rebuilt.append(
                    PlanAction(
                        date_key=data.day.isoformat(),
                        start_time=start,
                        end_time=end,
                        action=action_name,
                        charge_power_w=charge_power_w_local,
                        target_soc=target_soc_local,
                        reason=reason_local,
                        is_manual_override=False,
                    )
                )
                soc_local = next_soc_local
            return rebuilt

        def _soc_before_index(actions_input: list[PlanAction], index: int) -> int:
            soc_local = start_soc
            for idx in range(min(index, len(actions_input))):
                delta_battery_kwh, _grid_kwh, _throughput = self._transition(
                    actions_input[idx].action,
                    soc_local,
                    scenario_base[idx],
                    duration_hours=hour_durations[idx],
                )
                soc_local = self._next_soc(soc_local, delta_battery_kwh)
            return soc_local

        def _is_feasible_action_set(actions_input: list[PlanAction]) -> bool:
            soc_local = start_soc
            for idx, point in enumerate(points):
                action_name = actions_input[idx].action

                if (
                    low_soc_force_charge_enabled
                    and enforce_low_soc_by_index[idx]
                    and soc_local <= low_soc_force_threshold
                    and action_name != "charge"
                ):
                    return False

                if self._is_reserve_hour(point.timestamp) and soc_local < reserve_soc and action_name != "charge":
                    return False

                delta_battery_kwh, _grid_kwh, _throughput = self._transition(
                    action_name,
                    soc_local,
                    scenario_base[idx],
                    duration_hours=hour_durations[idx],
                )
                next_soc_local = self._next_soc(soc_local, delta_battery_kwh)

                if self._is_reserve_hour(point.timestamp) and next_soc_local < reserve_soc:
                    return False

                if idx in must_charge_exit_indices and next_soc_local < must_charge_soc:
                    return False

                entry_required_soc = expensive_entry_soc_requirements.get(idx)
                if entry_required_soc is not None and next_soc_local < entry_required_soc:
                    return False

                soc_local = next_soc_local

            return True

        def _rebuild_actions_with_overrides(overrides: dict[int, str], reason_prefix: str) -> list[PlanAction]:
            rebuilt: list[PlanAction] = []
            soc_local = start_soc
            for idx, point in enumerate(points):
                action_name = overrides.get(idx, actions[idx].action)
                delta_battery_kwh, _grid_kwh, _throughput = self._transition(
                    action_name,
                    soc_local,
                    scenario_base[idx],
                    duration_hours=hour_durations[idx],
                )
                next_soc_local = self._next_soc(soc_local, delta_battery_kwh)

                target_soc_local = None
                charge_power_w_local = None
                reason_local = "normal operation window"
                if action_name == "auto":
                    target_soc_local = float(next_soc_local)
                    reason_local = "robust optimization: auto at expensive hour"
                elif action_name == "charge":
                    target_soc_local = float(next_soc_local)
                    charge_power_w_local = self.charge_setpoint_w
                    if idx in overrides:
                        reason_local = reason_prefix
                    elif low_soc_force_charge_enabled and enforce_low_soc_by_index[idx] and soc_local <= low_soc_force_threshold:
                        reason_local = "low-soc safety charge"
                    else:
                        reason_local = "robust optimization: charge at cheap hour"

                start = point.timestamp
                end = start + timedelta(hours=1)
                rebuilt.append(
                    PlanAction(
                        date_key=data.day.isoformat(),
                        start_time=start,
                        end_time=end,
                        action=action_name,
                        charge_power_w=charge_power_w_local,
                        target_soc=target_soc_local,
                        reason=reason_local,
                        is_manual_override=False,
                    )
                )
                soc_local = next_soc_local
            return rebuilt

        reserve_enforce_enabled = bool(settings.reserve_precharge_enforce_enabled)
        reserve_precharge_min_spread = max(0.0, float(settings.reserve_precharge_min_spread_ore))
        reserve_entry_index = next((idx for idx, point in enumerate(points) if self._is_reserve_hour(point.timestamp)), None)
        if (
            reserve_enforce_enabled
            and reserve_entry_index is not None
            and reserve_entry_index > 0
            and total_cost_levels
        ):
            spread = max(total_cost_levels) - min(total_cost_levels)
            soc_before_reserve = _soc_before_index(actions, reserve_entry_index)
            if soc_before_reserve < reserve_soc and spread >= reserve_precharge_min_spread:
                candidates = sorted(
                    [idx for idx in range(reserve_entry_index) if actions[idx].action != "charge"],
                    key=lambda idx: total_cost_levels[idx],
                )
                forced: set[int] = set()
                updated_actions: list[PlanAction] | None = None
                for idx in candidates:
                    forced.add(idx)
                    trial_actions = _rebuild_actions_with_forced_charge(
                        forced,
                        "reserve precharge at cheap hour",
                    )
                    if _soc_before_index(trial_actions, reserve_entry_index) >= reserve_soc:
                        updated_actions = trial_actions
                        break
                if updated_actions is not None:
                    actions = updated_actions

        # Anti-idiotic normalization: if a pricier hour charges while a clearly cheaper hour holds,
        # swap the actions when constraints remain feasible.
        if total_cost_levels:
            swap_threshold = max(4.0, cheap_slot_min_spread * 0.25)
            max_passes = 4
            for _ in range(max_passes):
                changed = False
                charge_indices = [idx for idx, action in enumerate(actions) if action.action == "charge"]
                hold_indices = [idx for idx, action in enumerate(actions) if action.action == "hold"]
                for charge_idx in charge_indices:
                    if self._is_reserve_hour(points[charge_idx].timestamp):
                        continue
                    pricier = total_cost_levels[charge_idx]

                    better_hold = None
                    better_price = pricier
                    for hold_idx in hold_indices:
                        if self._is_reserve_hour(points[hold_idx].timestamp):
                            continue
                        cheaper = total_cost_levels[hold_idx]
                        if cheaper + swap_threshold < better_price:
                            better_hold = hold_idx
                            better_price = cheaper

                    if better_hold is None:
                        continue

                    trial = _rebuild_actions_with_overrides(
                        {charge_idx: "hold", better_hold: "charge"},
                        "price-order normalization: moved charge to cheaper hour",
                    )
                    if _is_feasible_action_set(trial):
                        actions = trial
                        changed = True
                        break

                if not changed:
                    break

            # Equal-price smoothing: if a later hour charges while an immediately earlier
            # hour with (near) identical price holds, shift charge left when feasible.
            max_equal_shift_passes = max(1, len(actions))
            for _ in range(max_equal_shift_passes):
                shifted = False
                for idx in range(1, len(actions)):
                    if actions[idx].action != "charge" or actions[idx - 1].action != "hold":
                        continue
                    if self._is_reserve_hour(points[idx].timestamp) or self._is_reserve_hour(points[idx - 1].timestamp):
                        continue

                    current_price = total_cost_levels[idx]
                    previous_price = total_cost_levels[idx - 1]
                    same_ui_price = round(current_price / 100.0, 2) == round(previous_price / 100.0, 2)
                    if (not same_ui_price) and abs(current_price - previous_price) > cheap_equal_tolerance:
                        continue

                    trial = _rebuild_actions_with_overrides(
                        {idx - 1: "charge", idx: "hold"},
                        "price-order normalization: shifted charge to equal earlier hour",
                    )
                    if _is_feasible_action_set(trial):
                        actions = trial
                        shifted = True
                        break

                if not shifted:
                    break

            # De-zigzag: avoid charge-hold-charge patterns inside cheap segments when
            # we can make the charging block contiguous without breaking constraints.
            max_zigzag_passes = max(1, len(actions))
            for _ in range(max_zigzag_passes):
                normalized = False
                for idx in range(1, len(actions) - 1):
                    if not (
                        actions[idx - 1].action == "charge"
                        and actions[idx].action == "hold"
                        and actions[idx + 1].action == "charge"
                    ):
                        continue

                    if any(
                        self._is_reserve_hour(points[j].timestamp)
                        for j in (idx - 1, idx, idx + 1)
                    ):
                        continue

                    p_prev = total_cost_levels[idx - 1]
                    p_mid = total_cost_levels[idx]
                    p_next = total_cost_levels[idx + 1]

                    same_ui_mid_next = round(p_mid / 100.0, 2) == round(p_next / 100.0, 2)
                    mid_not_more_expensive_than_next = same_ui_mid_next or (p_mid <= (p_next + cheap_equal_tolerance))

                    # Prefer moving the right charge into the middle if the middle is
                    # not more expensive and constraints allow it.
                    if mid_not_more_expensive_than_next:
                        trial_right_to_mid = _rebuild_actions_with_overrides(
                            {idx: "charge", idx + 1: "hold"},
                            "price-order normalization: removed charge zigzag",
                        )
                        if _is_feasible_action_set(trial_right_to_mid):
                            actions = trial_right_to_mid
                            normalized = True
                            break

                    same_ui_prev_mid = round(p_prev / 100.0, 2) == round(p_mid / 100.0, 2)
                    prev_not_more_expensive_than_mid = same_ui_prev_mid or (p_prev <= (p_mid + cheap_equal_tolerance))

                    # Otherwise try moving the left charge into the middle.
                    if prev_not_more_expensive_than_mid:
                        trial_left_to_mid = _rebuild_actions_with_overrides(
                            {idx - 1: "hold", idx: "charge"},
                            "price-order normalization: removed charge zigzag",
                        )
                        if _is_feasible_action_set(trial_left_to_mid):
                            actions = trial_left_to_mid
                            normalized = True
                            break

                if not normalized:
                    break

            # Expensive-hour smoothing: avoid auto-hold-auto with a costly middle hour.
            # If battery energy is limited, keep auto count the same but move discharge
            # from the cheaper flank into the pricier middle slot.
            max_auto_shift_passes = max(1, len(actions))
            for _ in range(max_auto_shift_passes):
                shifted_auto = False
                for idx in range(1, len(actions) - 1):
                    if not (
                        actions[idx - 1].action == "auto"
                        and actions[idx].action == "hold"
                        and actions[idx + 1].action == "auto"
                    ):
                        continue

                    p_prev = total_cost_levels[idx - 1]
                    p_mid = total_cost_levels[idx]
                    p_next = total_cost_levels[idx + 1]

                    # Middle should be materially expensive to justify shift.
                    if p_mid < max(p_prev, p_next) - cheap_equal_tolerance:
                        continue

                    # Move auto from cheaper side to middle to keep SOC usage stable.
                    if p_prev <= p_next:
                        overrides = {idx - 1: "hold", idx: "auto"}
                    else:
                        overrides = {idx + 1: "hold", idx: "auto"}

                    trial_auto_shift = _rebuild_actions_with_overrides(
                        overrides,
                        "price-order normalization: shifted auto to expensive middle hour",
                    )
                    if _is_feasible_action_set(trial_auto_shift):
                        actions = trial_auto_shift
                        shifted_auto = True
                        break

                if not shifted_auto:
                    break

            # Auto block extension: continue auto from an expensive block into
            # nearby shoulder hours while price remains sufficiently elevated.
            if auto_block_extension_enabled and total_cost_levels:
                extension_passes = max(1, len(actions))
                cheap_floor = cheap_slot_threshold + auto_block_floor_above_cheap
                for _ in range(extension_passes):
                    extended = False
                    for idx in range(1, len(actions)):
                        if actions[idx - 1].action != "auto" or actions[idx].action != "hold":
                            continue

                        prev_price = total_cost_levels[idx - 1]
                        cur_price = total_cost_levels[idx]
                        if cur_price < (prev_price - auto_block_drop_limit):
                            continue
                        if cur_price < cheap_floor:
                            continue

                        trial_extend = _rebuild_actions_with_overrides(
                            {idx: "auto"},
                            "price-order normalization: extended auto block",
                        )
                        if _is_feasible_action_set(trial_extend):
                            actions = trial_extend
                            extended = True
                            break

                    if not extended:
                        break

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
            elif action.action in {"auto", "discharge"}:
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
