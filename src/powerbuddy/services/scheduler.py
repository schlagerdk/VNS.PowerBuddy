from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from powerbuddy.config import settings
from powerbuddy.database import SessionLocal
from powerbuddy.models import PlanAction, PlannerKPI, PowerSnapshot
from powerbuddy.repositories import KPIRepository, PlanRepository, PowerRepository, PriceRepository, SimulationRepository
from powerbuddy.services.inverter import get_inverter_client
from powerbuddy.services.planner import DayPlanner, PlannerInput
from powerbuddy.services.pricing import PricePoint, get_price_provider
from powerbuddy.services.tariff import tariff_service
from powerbuddy.services.weather import weather_forecast_service

logger = logging.getLogger(__name__)


class PowerBuddyScheduler:
    def __init__(self) -> None:
        self.scheduler = AsyncIOScheduler(timezone=settings.timezone)
        self.price_provider = get_price_provider()
        self.inverter_client = get_inverter_client()
        self.planner = DayPlanner()
        self._execution_enabled = bool(settings.execution_enabled)
        self._last_solar_replan_at: datetime | None = None
        self._last_intraday_replan_at: datetime | None = None
        self._last_executed_signature: tuple[str, str, str] | None = None
        self._last_executed_at: datetime | None = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fetch_soc(self) -> float:
        try:
            realtime = await self.inverter_client.get_realtime()
            return realtime.battery_soc
        except Exception:
            return PowerRepository.get_latest_battery_soc() or 85.0

    async def _resolve_start_soc_for_day(self, day: date) -> float:
        """Return the expected battery SOC at the start of 'day' (local midnight 00:00).

        For today or past: returns live SOC.
        For future days: simulates today's remaining plan from current SOC and returns
        the projected end-of-day SOC so the plan starts with an accurate state.
        """
        today = date.today()
        live_soc = await self._fetch_soc()

        if day <= today:
            return live_soc

        today_actions = PlanRepository.get_plan(today.isoformat())
        if not today_actions:
            return live_soc

        tz = ZoneInfo(settings.timezone)
        now_local_hour = datetime.now(tz).replace(minute=0, second=0, microsecond=0).replace(tzinfo=None)
        remaining_actions = [
            a for a in today_actions
            if (a.start_time.replace(tzinfo=None) if a.start_time.tzinfo else a.start_time) >= now_local_hour
        ]
        if not remaining_actions:
            return live_soc

        try:
            weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(today)
            simulation = self.planner.simulate(today, remaining_actions, start_soc=live_soc, pv_weather_factor_24h=weather_factors)
            if simulation:
                projected_soc = simulation[-1].projected_soc
                return max(float(settings.battery_min_soc), min(100.0, projected_soc))
        except Exception:
            pass

        return live_soc

    def is_execution_enabled(self) -> bool:
        return bool(self._execution_enabled)

    def execution_mode(self) -> str:
        return "active" if self._execution_enabled else "paused"

    async def pause_execution(self) -> bool:
        """Pause inverter dispatch only and immediately relax control to auto mode."""
        self._execution_enabled = False
        self._last_executed_signature = None
        self._last_executed_at = None
        try:
            return await self.inverter_client.apply_action("auto", charge_power_w=None)
        except Exception:
            return False

    async def start_execution(self) -> None:
        """Resume inverter dispatch and immediately apply the currently active plan action."""
        self._execution_enabled = True
        self._last_executed_signature = None
        self._last_executed_at = None
        await self.execute_current_plan_action()

    async def _plan_and_simulate(self, day: date, prices: list[PricePoint], soc: float, lock_hours: int = 0) -> None:
        tz = ZoneInfo(settings.timezone)
        now_local_hour = datetime.now(tz).replace(minute=0, second=0, microsecond=0).replace(tzinfo=None)
        planning_start = now_local_hour if day == now_local_hour.date() else None
        lock_end = None
        if planning_start is not None and lock_hours > 0:
            lock_end = planning_start + timedelta(hours=max(0, int(lock_hours)))

        network_tariff = await tariff_service.get_network_tariff_24h()
        tariff_24h = tariff_service.total_tariff_ore_24h(network_tariff)
        weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(day)
        day_key = day.isoformat()
        existing = PlanRepository.get_plan(day_key)
        manual_overrides = [action for action in existing if action.is_manual_override]

        def _overlaps(a: PlanAction, b: PlanAction) -> bool:
            a_start = a.start_time.replace(tzinfo=None) if a.start_time.tzinfo else a.start_time
            a_end = a.end_time.replace(tzinfo=None) if a.end_time.tzinfo else a.end_time
            b_start = b.start_time.replace(tzinfo=None) if b.start_time.tzinfo else b.start_time
            b_end = b.end_time.replace(tzinfo=None) if b.end_time.tzinfo else b.end_time
            return a_start < b_end and a_end > b_start

        frozen_actions: list[PlanAction] = []
        if planning_start is not None:
            for action in existing:
                if action.is_manual_override:
                    continue
                end = action.end_time.replace(tzinfo=None) if action.end_time.tzinfo else action.end_time
                freeze_boundary = lock_end if lock_end is not None else planning_start
                if end <= freeze_boundary:
                    frozen_actions.append(
                        PlanAction(
                            date_key=action.date_key,
                            start_time=action.start_time,
                            end_time=action.end_time,
                            action=action.action,
                            charge_power_w=action.charge_power_w,
                            target_soc=action.target_soc,
                            reason=action.reason,
                            is_manual_override=False,
                        )
                    )

        actions_future = self.planner.plan(
            PlannerInput(
                day=day,
                price_points=prices,
                start_soc=soc,
                planning_start_time=planning_start,
                tariff_ore_per_hour=tariff_24h,
                pv_weather_factor_24h=weather_factors,
            )
        )
        if manual_overrides:
            actions_future = [
                action
                for action in actions_future
                if not any(_overlaps(action, manual_action) for manual_action in manual_overrides)
            ]
        actions = frozen_actions + actions_future
        PlanRepository.replace_plan(day_key, actions)
        actions_for_simulation = PlanRepository.get_plan(day_key)
        if planning_start is not None:
            simulation_boundary = lock_end if lock_end is not None else planning_start
            actions_for_simulation = [
                action
                for action in actions_for_simulation
                if (action.start_time.replace(tzinfo=None) if action.start_time.tzinfo else action.start_time) >= simulation_boundary
            ]
        simulation = (
            self.planner.simulate(day, actions_for_simulation, soc, pv_weather_factor_24h=weather_factors)
            if actions_for_simulation
            else []
        )
        SimulationRepository.replace_points(day_key, simulation)

    def _window_expected_consumption_kwh(self, start: datetime, end: datetime) -> float:
        if end <= start:
            return 0.0

        expected_daily, _ = self.planner.resolve_expected_daily_consumption(start.date())
        profile, _ = self.planner.resolve_hourly_consumption_profile(start.date())

        current = start
        total = 0.0
        while current < end:
            next_hour = (current + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            segment_end = min(end, next_hour)
            segment_hours = max(0.0, (segment_end - current).total_seconds() / 3600.0)
            total += expected_daily * profile[current.hour] * segment_hours
            current = segment_end

        return max(0.0, total)

    def _horizon_days_from_now(self) -> list[date]:
        """
        Return calendar days that must be covered by plans from current time
        and at least 48h forward.
        """
        now = datetime.now()
        horizon_hours = max(48, int(settings.planning_horizon_hours))
        horizon_end = now + timedelta(hours=horizon_hours)

        # Keep compatibility with explicit day-ahead prefetch setting.
        configured_end = now + timedelta(days=max(0, int(settings.price_fetch_days_ahead)))
        end_date = max(horizon_end.date(), configured_end.date())

        days: list[date] = []
        current = now.date()
        while current <= end_date:
            days.append(current)
            current += timedelta(days=1)
        return days

    def _should_fetch_day(self, target_day: date, now: datetime, existing_prices: list[PricePoint]) -> bool:
        """
        Price cadence:
        - Today's spot prices are relevant all day; keep refreshing.
        - Future day prices are typically published around 13:00 local time.
          Before that, only refresh if we already have prices stored for that day.
        """
        if target_day <= now.date():
            return True

        if existing_prices:
            return True

        publish_hour = min(23, max(0, int(settings.day_ahead_publish_hour_local)))
        return now.hour >= publish_hour

    # ------------------------------------------------------------------
    # Core jobs
    # ------------------------------------------------------------------

    async def refresh_prices_and_replan(self) -> None:
        """
        Fetch prices for forward horizon and re-plan only when new/changed
        price data arrives for a day.

        Once a plan is generated for a given set of prices, it remains locked
        until prices for that day change.
        """
        now = datetime.now()
        days_to_fetch = self._horizon_days_from_now()

        for day in days_to_fetch:
            existing_prices = PriceRepository.get_by_day(day, settings.price_area)

            if not self._should_fetch_day(day, now, existing_prices):
                logger.debug(
                    "Skipping fetch for %s before day-ahead publication hour (%02d:00)",
                    day,
                    settings.day_ahead_publish_hour_local,
                )
                continue

            existing_price_map = {
                (
                    p.timestamp.replace(tzinfo=None) if p.timestamp.tzinfo else p.timestamp,
                    p.area,
                ): round(float(p.price_ore_per_kwh), 6)
                for p in existing_prices
            }

            try:
                fetched_prices = await self.price_provider.get_day_prices(day, settings.price_area)
            except Exception as exc:
                logger.warning("Price fetch failed for %s: %s", day, exc)
                continue

            if not fetched_prices:
                logger.debug("No prices returned for %s (may not be published yet)", day)
                continue

            fetched_price_map = {
                (
                    p.timestamp.replace(tzinfo=None) if p.timestamp.tzinfo else p.timestamp,
                    p.area,
                ): round(float(p.price_ore_per_kwh), 6)
                for p in fetched_prices
            }
            prices_changed = fetched_price_map != existing_price_map

            if not prices_changed:
                logger.debug("Prices unchanged for %s — plan remains locked", day)
                continue

            PriceRepository.upsert_prices(fetched_prices)
            prices = PriceRepository.get_by_day(day, settings.price_area)
            if not prices:
                continue

            day_key = day.isoformat()
            existing_plan = PlanRepository.get_plan(day_key)
            if existing_plan:
                logger.info("Prices changed for %s — regenerating locked plan for new price set", day)

            logger.info("Generating daily plan for %s", day)
            soc = await self._resolve_start_soc_for_day(day)
            await self._plan_and_simulate(day, prices, soc)

    async def midnight_replan_forward_horizon(self) -> None:
        """
        Force re-plan for the full forward horizon at local midnight.

        Unlike interval refresh, this updates days even when plans already exist,
        so the visible horizon stays fresh after date rollover.
        """
        now = datetime.now()
        today = now.date()
        replanned_days: list[str] = []

        for day in self._horizon_days_from_now():
            prices = PriceRepository.get_by_day(day, settings.price_area)

            if not self._should_fetch_day(day, now, prices):
                continue

            if not prices:
                try:
                    fetched = await self.price_provider.get_day_prices(day, settings.price_area)
                except Exception as exc:
                    logger.warning("Midnight replan failed to fetch prices for %s: %s", day, exc)
                    continue
                if fetched:
                    PriceRepository.upsert_prices(fetched)
                    prices = PriceRepository.get_by_day(day, settings.price_area)

            if not prices:
                continue

            soc = await self._resolve_start_soc_for_day(day)
            lock_hours = 0
            if day == today:
                lock_hours = max(0, int(settings.midnight_replan_lock_hours))
            await self._plan_and_simulate(day, prices, soc, lock_hours=lock_hours)
            replanned_days.append(day.isoformat())

        if replanned_days:
            logger.info(
                "Midnight forward replan completed for %d day(s): %s",
                len(replanned_days),
                ", ".join(replanned_days),
            )
        else:
            logger.info("Midnight forward replan completed with no replanned days")

    async def snapshot_power(self) -> None:
        data = await self.inverter_client.get_realtime()
        snapshot = PowerSnapshot(
            timestamp=data.timestamp,
            grid_power_w=data.grid_power_w,
            load_power_w=data.load_power_w,
            pv_power_w=data.pv_power_w,
            battery_power_w=data.battery_power_w,
            battery_soc=data.battery_soc,
        )
        PowerRepository.add_snapshot(snapshot)

    async def adaptive_solar_replan(self) -> None:
        """
        Re-plan during the day when PV output suddenly ramps up.
        Trigger: PV above threshold and either exporting to grid or at least
        supplying most of current household load.
        """
        if not settings.solar_replan_enabled:
            return

        try:
            realtime = await self.inverter_client.get_realtime()
        except Exception as exc:
            logger.debug("Adaptive solar replan skipped (inverter unavailable): %s", exc)
            return

        pv_w = float(realtime.pv_power_w)
        grid_w = float(realtime.grid_power_w)
        load_w = float(realtime.load_power_w)

        pv_trigger = pv_w >= settings.solar_replan_trigger_w
        surplus_trigger = grid_w < -300 or pv_w >= max(0.0, load_w * 0.9)
        if not (pv_trigger and surplus_trigger):
            return

        now = datetime.now()
        cooldown = timedelta(minutes=max(1, settings.solar_replan_cooldown_minutes))
        if self._last_solar_replan_at and now - self._last_solar_replan_at < cooldown:
            return

        for day in self._horizon_days_from_now():
            prices = PriceRepository.get_by_day(day, settings.price_area)
            if not self._should_fetch_day(day, now, prices):
                continue
            if not prices:
                try:
                    fetched = await self.price_provider.get_day_prices(day, settings.price_area)
                except Exception as exc:
                    logger.warning("Adaptive solar replan failed to fetch prices for %s: %s", day, exc)
                    continue
                if fetched:
                    PriceRepository.upsert_prices(fetched)
                    prices = PriceRepository.get_by_day(day, settings.price_area)
            if not prices:
                continue

            soc_for_day = await self._resolve_start_soc_for_day(day)
            await self._plan_and_simulate(day, prices, soc_for_day)

        self._last_solar_replan_at = now
        logger.info(
            "Adaptive solar replan executed (pv=%.0fW, grid=%.0fW, load=%.0fW)",
            pv_w,
            grid_w,
            load_w,
        )

    async def intraday_guarded_replan(self) -> None:
        """
        Intraday re-plan with guardrails:
        - runs on interval
        - triggers only when recent consumption deviates enough from expectation
        - keeps the next N hours frozen to avoid churn
        """
        if not settings.intraday_replan_enabled:
            return

        now = datetime.now(ZoneInfo(settings.timezone)).replace(tzinfo=None)
        lock_hours = max(0, int(settings.intraday_replan_lock_hours))
        live_soc = await self._fetch_soc()
        low_soc_threshold = float(settings.battery_min_soc) + float(settings.low_soc_force_charge_margin_percent)
        force_low_soc_replan = bool(settings.low_soc_force_charge_enabled) and (live_soc <= low_soc_threshold)

        if force_low_soc_replan:
            day = now.date()
            prices = PriceRepository.get_by_day(day, settings.price_area)
            if not prices:
                try:
                    fetched = await self.price_provider.get_day_prices(day, settings.price_area)
                    if fetched:
                        PriceRepository.upsert_prices(fetched)
                        prices = PriceRepository.get_by_day(day, settings.price_area)
                except Exception as exc:
                    logger.warning("Low-SOC replan price fetch failed for %s: %s", day, exc)
                    return
            if not prices:
                return

            await self._plan_and_simulate(day, prices, live_soc, lock_hours=lock_hours)
            self._last_intraday_replan_at = datetime.now()
            logger.warning(
                "Low-SOC guarded replan executed (soc=%.1f%% threshold=%.1f%%)",
                live_soc,
                low_soc_threshold,
            )
            return

        lookback_hours = max(1, lock_hours)
        start = now - timedelta(hours=lookback_hours)

        actual_kwh, samples = PowerRepository.estimate_consumption_kwh_in_window(start=start, end=now)
        if samples == 0:
            return

        expected_kwh = self._window_expected_consumption_kwh(start=start, end=now)
        if expected_kwh <= 0.1:
            return

        deviation = abs(actual_kwh - expected_kwh) / expected_kwh
        threshold = max(0.05, float(settings.intraday_replan_consumption_deviation_trigger_ratio))
        if deviation < threshold:
            return

        day = now.date()
        prices = PriceRepository.get_by_day(day, settings.price_area)
        if not prices:
            try:
                fetched = await self.price_provider.get_day_prices(day, settings.price_area)
                if fetched:
                    PriceRepository.upsert_prices(fetched)
                    prices = PriceRepository.get_by_day(day, settings.price_area)
            except Exception as exc:
                logger.warning("Intraday replan price fetch failed for %s: %s", day, exc)
                return
        if not prices:
            return

        await self._plan_and_simulate(day, prices, live_soc, lock_hours=lock_hours)
        self._last_intraday_replan_at = datetime.now()
        logger.info(
            "Intraday guarded replan executed (actual=%.2f kWh expected=%.2f kWh deviation=%.1f%%)",
            actual_kwh,
            expected_kwh,
            deviation * 100.0,
        )

    async def update_planner_kpis_and_autotune(self) -> None:
        if not settings.kpi_tracking_enabled:
            return

        tz = ZoneInfo(settings.timezone)
        target_day = (datetime.now(tz) - timedelta(days=1)).date()
        day_key = target_day.isoformat()

        simulation = SimulationRepository.get_points(day_key)
        planned_grid_kwh = sum(max(0.0, float(point.projected_grid_kwh)) for point in simulation)

        reserve_start = max(0, min(23, int(settings.reserve_soc_start_hour_local)))
        reserve_end = max(1, min(24, int(settings.reserve_soc_end_hour_local)))

        def _in_peak(hour: int) -> bool:
            if reserve_start < reserve_end:
                return reserve_start <= hour < reserve_end
            return hour >= reserve_start or hour < reserve_end

        planned_peak_import_kwh = sum(
            max(0.0, float(point.projected_grid_kwh))
            for point in simulation
            if _in_peak(point.timestamp.astimezone(tz).hour if point.timestamp.tzinfo else point.timestamp.hour)
        )

        day_start = datetime.combine(target_day, datetime.min.time())
        day_end = day_start + timedelta(days=1)
        with_peak_start = day_start + timedelta(hours=reserve_start)
        with_peak_end = day_start + timedelta(hours=reserve_end if reserve_end > reserve_start else reserve_end + 24)

        actual_grid_kwh = 0.0
        actual_peak_import_kwh = 0.0
        with SessionLocal() as session:
            snapshots = list(
                session.execute(
                    select(PowerSnapshot)
                    .where(
                        PowerSnapshot.timestamp >= day_start,
                        PowerSnapshot.timestamp < day_end,
                    )
                    .order_by(PowerSnapshot.timestamp.asc())
                ).scalars()
            )

        for idx, current in enumerate(snapshots):
            if idx + 1 < len(snapshots):
                next_ts = snapshots[idx + 1].timestamp
                delta_hours = max(0.0, (next_ts - current.timestamp).total_seconds() / 3600.0)
            else:
                delta_hours = 5.0 / 60.0
            delta_hours = min(delta_hours, 0.25)

            import_kwh = max(0.0, float(current.grid_power_w) / 1000.0) * delta_hours
            actual_grid_kwh += import_kwh

            ts = current.timestamp.replace(tzinfo=None) if current.timestamp.tzinfo else current.timestamp
            peak_match = False
            if reserve_start < reserve_end:
                peak_match = reserve_start <= ts.hour < reserve_end
            else:
                peak_match = ts.hour >= reserve_start or ts.hour < reserve_end
            if peak_match:
                actual_peak_import_kwh += import_kwh

        expected_daily_consumption_kwh, _ = self.planner.resolve_expected_daily_consumption(target_day)
        realized_daily_consumption_kwh, _ = PowerRepository.estimate_daily_consumption_kwh(target_day)
        baseline = max(1.0, actual_grid_kwh)
        plan_error_ratio = abs(actual_grid_kwh - planned_grid_kwh) / baseline

        soc_at_peak_start = 0.0
        peak_candidates = [s for s in snapshots if (s.timestamp.hour == reserve_start)]
        if peak_candidates:
            soc_at_peak_start = float(peak_candidates[0].battery_soc)
        elif snapshots:
            soc_at_peak_start = float(snapshots[-1].battery_soc)

        KPIRepository.upsert_daily_kpi(
            PlannerKPI(
                date_key=day_key,
                planned_grid_kwh=round(planned_grid_kwh, 3),
                actual_grid_kwh=round(actual_grid_kwh, 3),
                planned_peak_import_kwh=round(planned_peak_import_kwh, 3),
                actual_peak_import_kwh=round(actual_peak_import_kwh, 3),
                plan_error_ratio=round(plan_error_ratio, 4),
                soc_at_peak_start=round(soc_at_peak_start, 2),
                expected_daily_consumption_kwh=round(expected_daily_consumption_kwh, 3),
                realized_daily_consumption_kwh=round(realized_daily_consumption_kwh, 3),
                updated_at=datetime.now(tz),
            )
        )

        if not settings.auto_tuning_enabled:
            return

        recent = KPIRepository.get_recent(limit=5)
        if not recent:
            return

        ratios = [k.realized_daily_consumption_kwh / max(1.0, k.expected_daily_consumption_kwh) for k in recent]
        target_ratio = sum(ratios) / len(ratios)
        step_limit = max(0.01, float(settings.auto_tuning_step_max_ratio))
        bounded_ratio = max(1.0 - step_limit, min(1.0 + step_limit, target_ratio))
        settings.expected_daily_consumption_kwh = max(
            5.0,
            float(settings.expected_daily_consumption_kwh) * bounded_ratio,
        )
        logger.info(
            "Auto-tuned expected_daily_consumption_kwh to %.2f (ratio %.3f)",
            settings.expected_daily_consumption_kwh,
            bounded_ratio,
        )

    async def execute_current_plan_action(self) -> None:
        """
        Execution layer: applies the currently active plan action to inverter.
        """
        if not self._execution_enabled:
            return

        now_local = datetime.now(ZoneInfo(settings.timezone)).replace(tzinfo=None)
        day_key = now_local.date().isoformat()
        actions = PlanRepository.get_plan(day_key)

        current_action = "hold"
        current_is_manual_override = False
        current_charge_power_w: float | None = None
        current_start = now_local.replace(minute=0, second=0, microsecond=0)
        current_end = current_start + timedelta(hours=1)
        for action in actions:
            start = action.start_time.replace(tzinfo=None) if action.start_time.tzinfo else action.start_time
            end = action.end_time.replace(tzinfo=None) if action.end_time.tzinfo else action.end_time
            if start <= now_local < end:
                current_action = action.action
                current_is_manual_override = bool(action.is_manual_override)
                current_charge_power_w = action.charge_power_w
                current_start = start
                current_end = end
                break

        runtime_action = current_action
        realtime_for_hold: object | None = None

        # Backward compatibility: old non-manual plans may still contain "discharge".
        # New planner uses "auto" instead.
        if runtime_action == "discharge" and not current_is_manual_override:
            runtime_action = "auto"

        # If plan says hold but there is clear PV surplus export and battery is not full,
        # force charge to capture excess solar instead of curtailing/exporting.
        if runtime_action == "hold" and settings.hold_solar_capture_enabled:
            try:
                realtime_for_hold = await self.inverter_client.get_realtime()
                pv_w = max(0.0, float(realtime_for_hold.pv_power_w))
                grid_w = float(realtime_for_hold.grid_power_w)
                soc = float(realtime_for_hold.battery_soc)
                if (
                    pv_w >= float(settings.hold_solar_capture_pv_w_threshold)
                    and grid_w <= float(settings.hold_solar_capture_export_w_threshold)
                    and soc < float(settings.battery_max_soc)
                ):
                    runtime_action = "charge"
                    default_charge_kw = max(0.0, float(settings.default_charge_power_w) / 1000.0)
                    effective_charge_kw = default_charge_kw if default_charge_kw > 0.0 else float(settings.planned_charge_kw)
                    current_charge_power_w = round(min(float(settings.max_charge_kw), effective_charge_kw) * 1000.0, 1)
                    logger.info(
                        "Hold overridden to charge due to solar surplus (pv=%.1fW, grid=%.1fW, soc=%.1f%%)",
                        pv_w,
                        grid_w,
                        soc,
                    )
            except Exception:
                pass

        # If plan says hold but we have high PV and SOC isn't full, switch to auto mode
        # so the inverter can absorb free solar energy instead of staying rigidly locked.
        if runtime_action == "hold" and settings.hold_high_solar_auto_enabled:
            try:
                realtime_for_hold = realtime_for_hold or await self.inverter_client.get_realtime()
                pv_w = max(0.0, float(realtime_for_hold.pv_power_w))
                soc = float(realtime_for_hold.battery_soc)
                if (
                    pv_w >= float(settings.hold_high_solar_auto_pv_w_threshold)
                    and soc < float(settings.hold_high_solar_auto_soc_below_percent)
                ):
                    runtime_action = "auto"
                    logger.info(
                        "Hold overridden to auto due to high solar (pv=%.1fW, soc=%.1f%%)",
                        pv_w,
                        soc,
                    )
            except Exception:
                pass

        # Emergency guard: when SOC is too close to minimum, do not keep holding.
        if runtime_action == "hold" and settings.low_soc_force_charge_enabled:
            try:
                realtime_low_soc = realtime_for_hold or await self.inverter_client.get_realtime()
                low_soc_threshold = float(settings.battery_min_soc) + float(settings.low_soc_force_charge_margin_percent)
                if float(realtime_low_soc.battery_soc) <= low_soc_threshold:
                    runtime_action = "charge"
                    default_charge_kw = max(0.0, float(settings.default_charge_power_w) / 1000.0)
                    effective_charge_kw = default_charge_kw if default_charge_kw > 0.0 else float(settings.planned_charge_kw)
                    current_charge_power_w = round(min(float(settings.max_charge_kw), effective_charge_kw) * 1000.0, 1)
                    logger.warning(
                        "Hold overridden to charge due to low SOC (soc=%.1f%% threshold=%.1f%%)",
                        float(realtime_low_soc.battery_soc),
                        low_soc_threshold,
                    )
            except Exception:
                pass

        power_signature = "" if current_charge_power_w is None else f"{current_charge_power_w:.1f}"
        signature = (runtime_action, current_start.isoformat(), f"{current_end.isoformat()}|{power_signature}")
        if runtime_action != "charge" and signature == self._last_executed_signature:
            if runtime_action == "hold":
                try:
                    realtime = realtime_for_hold or await self.inverter_client.get_realtime()
                    if abs(float(realtime.battery_power_w)) > float(settings.hold_reassert_threshold_w):
                        logger.info(
                            "Hold drift detected (battery_power_w=%.1fW), forcing immediate re-apply",
                            float(realtime.battery_power_w),
                        )
                    else:
                        refresh_sec = max(60, int(settings.execution_non_charge_refresh_seconds))
                        if self._last_executed_at and (datetime.now() - self._last_executed_at).total_seconds() < refresh_sec:
                            return
                except Exception:
                    refresh_sec = max(60, int(settings.execution_non_charge_refresh_seconds))
                    if self._last_executed_at and (datetime.now() - self._last_executed_at).total_seconds() < refresh_sec:
                        return
            else:
                refresh_sec = max(60, int(settings.execution_non_charge_refresh_seconds))
                if self._last_executed_at and (datetime.now() - self._last_executed_at).total_seconds() < refresh_sec:
                    return

        ok = await self.inverter_client.apply_action(runtime_action, charge_power_w=current_charge_power_w)
        if ok:
            self._last_executed_signature = signature
            self._last_executed_at = datetime.now()

    async def force_reconcile_current_action(self) -> None:
        """
        Force immediate execution re-check after API mutations (plan overrides/updates).
        """
        self._last_executed_signature = None
        await self.execute_current_plan_action()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        interval_min = max(1, settings.price_recheck_interval_minutes)

        # Runs every N minutes: fetch prices for forward horizon, re-plan on changes
        self.scheduler.add_job(
            self.refresh_prices_and_replan,
            "interval",
            minutes=interval_min,
            id="price-refresh-replan",
        )

        # Deterministic day-ahead safety net around publication window.
        self.scheduler.add_job(
            self.refresh_prices_and_replan,
            "cron",
            hour=14,
            minute=5,
            id="day-ahead-refresh-1405",
        )
        self.scheduler.add_job(
            self.refresh_prices_and_replan,
            "cron",
            hour=14,
            minute=20,
            id="day-ahead-refresh-1420",
        )

        # Power snapshot every 5 minutes
        self.scheduler.add_job(self.snapshot_power, "interval", minutes=5, id="power-snapshot")

        # KPI and auto-tuning once per day.
        self.scheduler.add_job(
            self.update_planner_kpis_and_autotune,
            "cron",
            hour=0,
            minute=25,
            id="planner-kpi-autotune",
        )

        # Apply active plan action to inverter.
        self.scheduler.add_job(
            self.execute_current_plan_action,
            "interval",
            seconds=max(10, settings.execution_interval_seconds),
            id="plan-execution",
        )

        # Intraday auto-replan is intentionally disabled.

        self.scheduler.start()

        # Run an immediate price fetch + plan on startup (don't block start())
        import asyncio
        loop = asyncio.get_event_loop()
        loop.create_task(self.refresh_prices_and_replan())
        loop.create_task(self.update_planner_kpis_and_autotune())
        loop.create_task(self.execute_current_plan_action())

    def shutdown(self) -> None:
        self.scheduler.shutdown(wait=False)

