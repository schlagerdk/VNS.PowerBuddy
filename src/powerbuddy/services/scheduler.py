from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PowerSnapshot
from powerbuddy.repositories import PlanRepository, PowerRepository, PriceRepository, SimulationRepository
from powerbuddy.services.inverter import get_inverter_client
from powerbuddy.services.planner import DayPlanner, PlannerInput
from powerbuddy.services.pricing import PricePoint, get_price_provider
from powerbuddy.services.tariff import tariff_service

logger = logging.getLogger(__name__)


class PowerBuddyScheduler:
    def __init__(self) -> None:
        self.scheduler = AsyncIOScheduler(timezone=settings.timezone)
        self.price_provider = get_price_provider()
        self.inverter_client = get_inverter_client()
        self.planner = DayPlanner()
        self._last_solar_replan_at: datetime | None = None
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
            return PowerRepository.get_latest_battery_soc() or 50.0

    async def _plan_and_simulate(self, day: date, prices: list[PricePoint], soc: float) -> None:
        tz = ZoneInfo(settings.timezone)
        now_local_hour = datetime.now(tz).replace(minute=0, second=0, microsecond=0).replace(tzinfo=None)
        planning_start = now_local_hour if day == now_local_hour.date() else None

        network_tariff = await tariff_service.get_network_tariff_24h()
        tariff_24h = tariff_service.total_tariff_ore_24h(network_tariff)
        day_key = day.isoformat()

        frozen_actions: list[PlanAction] = []
        if planning_start is not None:
            existing = PlanRepository.get_plan(day_key)
            for action in existing:
                if action.is_manual_override:
                    continue
                end = action.end_time.replace(tzinfo=None) if action.end_time.tzinfo else action.end_time
                if end <= planning_start:
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
            )
        )
        actions = frozen_actions + actions_future
        PlanRepository.replace_plan(day_key, actions)
        actions_for_simulation = PlanRepository.get_plan(day_key)
        if planning_start is not None:
            actions_for_simulation = [
                action
                for action in actions_for_simulation
                if (action.start_time.replace(tzinfo=None) if action.start_time.tzinfo else action.start_time) >= planning_start
            ]
        simulation = self.planner.simulate(day, actions_for_simulation, soc) if actions_for_simulation else []
        SimulationRepository.replace_points(day_key, simulation)

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
        Fetch prices for forward horizon and only generate plans for days
        that do not already have a plan.

        Existing plans are never auto-overwritten during the day.
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

            try:
                new_prices = await self.price_provider.get_day_prices(day, settings.price_area)
            except Exception as exc:
                logger.warning("Price fetch failed for %s: %s", day, exc)
                continue

            if not new_prices:
                logger.debug("No prices returned for %s (may not be published yet)", day)
                continue

            PriceRepository.upsert_prices(new_prices)
            prices = PriceRepository.get_by_day(day, settings.price_area)
            if not prices:
                continue

            day_key = day.isoformat()
            existing_plan = PlanRepository.get_plan(day_key)
            if existing_plan:
                logger.debug("Plan already exists for %s — auto re-plan disabled", day)
                continue

            logger.info("No existing plan for %s — generating daily plan", day)
            soc = await self._fetch_soc()
            await self._plan_and_simulate(day, prices, soc)

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

            await self._plan_and_simulate(day, prices, realtime.battery_soc)

        self._last_solar_replan_at = now
        logger.info(
            "Adaptive solar replan executed (pv=%.0fW, grid=%.0fW, load=%.0fW)",
            pv_w,
            grid_w,
            load_w,
        )

    async def execute_current_plan_action(self) -> None:
        """
        Execution layer: applies the currently active plan action to inverter.
        """
        if not settings.execution_enabled:
            return

        now_local = datetime.now(ZoneInfo(settings.timezone)).replace(tzinfo=None)
        day_key = now_local.date().isoformat()
        actions = PlanRepository.get_plan(day_key)

        current_action = "hold"
        current_charge_power_w: float | None = None
        current_start = now_local.replace(minute=0, second=0, microsecond=0)
        current_end = current_start + timedelta(hours=1)
        for action in actions:
            start = action.start_time.replace(tzinfo=None) if action.start_time.tzinfo else action.start_time
            end = action.end_time.replace(tzinfo=None) if action.end_time.tzinfo else action.end_time
            if start <= now_local < end:
                current_action = action.action
                current_charge_power_w = action.charge_power_w
                current_start = start
                current_end = end
                break

        power_signature = "" if current_charge_power_w is None else f"{current_charge_power_w:.1f}"
        signature = (current_action, current_start.isoformat(), f"{current_end.isoformat()}|{power_signature}")
        if current_action != "charge" and signature == self._last_executed_signature:
            if current_action == "hold":
                try:
                    realtime = await self.inverter_client.get_realtime()
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

        ok = await self.inverter_client.apply_action(current_action, charge_power_w=current_charge_power_w)
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

        # Power snapshot every 5 minutes
        self.scheduler.add_job(self.snapshot_power, "interval", minutes=5, id="power-snapshot")

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
        loop.create_task(self.execute_current_plan_action())

    def shutdown(self) -> None:
        self.scheduler.shutdown(wait=False)

