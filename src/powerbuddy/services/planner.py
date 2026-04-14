from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint, SimulationPoint
from powerbuddy.repositories import PowerRepository


@dataclass(slots=True)
class PlannerInput:
    day: date
    price_points: list[PricePoint]
    start_soc: float
    planning_start_time: datetime | None = None
    # Optional 24-element array of total non-spot tariff (øre/kWh, excl. VAT).
    # When provided the planner minimises (spot + tariff) cost, making it avoid
    # peak network-tariff windows (e.g. 17-20) for grid imports.
    tariff_ore_per_hour: list[float] | None = None


class DayPlanner:
    def __init__(self) -> None:
        self.min_soc = float(settings.battery_min_soc)
        self.max_soc = float(settings.battery_max_soc)
        self.capacity_kwh = float(settings.battery_capacity_kwh)
        self.max_charge_kwh = min(float(settings.max_charge_kw), float(settings.planned_charge_kw))
        self.max_discharge_kwh = float(settings.max_discharge_kw)

    def resolve_expected_daily_consumption(self, reference_day: date) -> tuple[float, str]:
        fallback = float(settings.expected_daily_consumption_kwh)
        if not settings.dynamic_consumption_enabled:
            return fallback, "config"

        dynamic_value = PowerRepository.rolling_average_daily_consumption_kwh(
            reference_day=reference_day,
            lookback_days=settings.dynamic_consumption_lookback_days,
            min_samples_per_day=settings.dynamic_consumption_min_samples_per_day,
        )
        if dynamic_value is None:
            return fallback, "config"
        return dynamic_value, "historical-average"

    @staticmethod
    def _cost_ore(grid_kwh: float, price_ore_per_kwh: float, tariff_ore: float = 0.0) -> float:
        """
        Calculate grid cost (øre) for one hour.
        - Import (grid_kwh ≥ 0): pay spot price + tariff (network + state fees, excl. VAT).
        - Export (grid_kwh < 0): receive feed-in tariff only (no cost recovery on state fees).
        """
        if grid_kwh >= 0:
            return grid_kwh * (price_ore_per_kwh + tariff_ore)
        return grid_kwh * float(settings.feed_in_tariff_ore)

    def _action_delta_kwh(self, action: str, soc: int, base_hourly_consumption_kwh: float) -> float:
        """Battery energy delta for one hour. Positive = charging battery, negative = discharging."""
        soc_f = float(soc)
        available_room_kwh = ((self.max_soc - soc_f) / 100.0) * self.capacity_kwh
        available_energy_kwh = ((soc_f - self.min_soc) / 100.0) * self.capacity_kwh

        if action == "charge":
            return max(0.0, min(self.max_charge_kwh, available_room_kwh))
        if action == "discharge":
            # Only offset household demand; avoid forced export arbitrage.
            return -max(0.0, min(self.max_discharge_kwh, available_energy_kwh, base_hourly_consumption_kwh))
        return 0.0

    def _next_soc(self, soc: int, delta_battery_kwh: float) -> int:
        next_soc = int(round(soc + (delta_battery_kwh / self.capacity_kwh) * 100.0))
        return max(int(self.min_soc), min(int(self.max_soc), next_soc))

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
        base_hourly_consumption_kwh = expected_daily_consumption_kwh / max(len(points), 1)

        soc_min = int(self.min_soc)
        soc_max = int(self.max_soc)
        start_soc = max(soc_min, min(soc_max, int(round(data.start_soc))))

        actions_space = ("charge", "hold", "discharge")
        inf = float("inf")
        eps = 1e-9

        # dp[t][soc] = minimum total cost from hour t..end when entering hour t with soc.
        dp: list[dict[int, float]] = [{soc: inf for soc in range(soc_min, soc_max + 1)} for _ in range(len(points) + 1)]
        choice: list[dict[int, str]] = [{soc: "hold" for soc in range(soc_min, soc_max + 1)} for _ in range(len(points))]

        for soc in range(soc_min, soc_max + 1):
            dp[len(points)][soc] = 0.0

        for t in range(len(points) - 1, -1, -1):
            price = points[t].price_ore_per_kwh
            tariff = data.tariff_ore_per_hour[t] if data.tariff_ore_per_hour else 0.0
            for soc in range(soc_min, soc_max + 1):
                best_cost = inf
                best_action = "hold"

                for action in actions_space:
                    delta_battery_kwh = self._action_delta_kwh(action, soc, base_hourly_consumption_kwh)
                    # Prevent no-op labels like "charge" at max SOC or "discharge" at min SOC.
                    if action != "hold" and abs(delta_battery_kwh) < eps:
                        continue
                    next_soc = self._next_soc(soc, delta_battery_kwh)
                    grid_kwh = base_hourly_consumption_kwh + delta_battery_kwh
                    hour_cost = self._cost_ore(grid_kwh, price, tariff)
                    total_cost = hour_cost + dp[t + 1][next_soc]

                    if total_cost < best_cost - eps or (abs(total_cost - best_cost) <= eps and action == "hold"):
                        best_cost = total_cost
                        best_action = action

                dp[t][soc] = best_cost
                choice[t][soc] = best_action

        actions: list[PlanAction] = []
        soc = start_soc
        for point in points:
            idx = len(actions)
            action = choice[idx][soc]
            delta_battery_kwh = self._action_delta_kwh(action, soc, base_hourly_consumption_kwh)
            next_soc = self._next_soc(soc, delta_battery_kwh)

            target_soc = None
            charge_power_w = None
            reason = "normal operation window"

            if action == "discharge":
                target_soc = float(next_soc)
                reason = "global day-cost optimization: discharge at expensive hour"
            elif action == "charge":
                target_soc = float(next_soc)
                charge_power_w = round(max(0.0, delta_battery_kwh) * 1000.0, 1)
                reason = "global day-cost optimization: charge at cheap hour"

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

    def simulate(self, day: date, actions: list[PlanAction], start_soc: float) -> list[SimulationPoint]:
        soc = max(self.min_soc, min(self.max_soc, start_soc))
        points: list[SimulationPoint] = []
        expected_daily_consumption_kwh, _ = self.resolve_expected_daily_consumption(day)
        base_hourly_consumption_kwh = expected_daily_consumption_kwh / 24.0

        for action in sorted(actions, key=lambda x: x.start_time):
            delta_soc = 0.0
            projected_grid_kwh = base_hourly_consumption_kwh
            soc_before = soc

            if action.action == "charge":
                charge_kwh = self.max_charge_kwh
                if action.charge_power_w is not None:
                    charge_kwh = max(0.0, float(action.charge_power_w) / 1000.0)
                delta_soc = (charge_kwh / self.capacity_kwh) * 100.0
            elif action.action == "discharge":
                delta_soc = -(self.max_discharge_kwh / self.capacity_kwh) * 100.0

            target = action.target_soc
            if target is not None:
                if delta_soc > 0:
                    soc = min(soc + delta_soc, target, self.max_soc)
                elif delta_soc < 0:
                    soc = max(soc + delta_soc, target, self.min_soc)
            else:
                soc = max(self.min_soc, min(self.max_soc, soc + delta_soc))

            actual_battery_delta_kwh = ((soc - soc_before) / 100.0) * self.capacity_kwh
            projected_grid_kwh = base_hourly_consumption_kwh + actual_battery_delta_kwh

            points.append(
                SimulationPoint(
                    date_key=day.isoformat(),
                    timestamp=action.start_time,
                    action=action.action,
                    projected_soc=round(soc, 2),
                    projected_grid_kwh=round(projected_grid_kwh, 3),
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
