from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from powerbuddy.config import settings
from powerbuddy.database import init_db
from powerbuddy.models import PlanAction, PricePoint
from powerbuddy.repositories import KPIRepository, PlanRepository, PowerRepository, PriceRepository, SimulationRepository
from powerbuddy.schemas import (
    InverterRealtime,
    ManualOverrideIn,
    PlanActionOut,
    PlanNowStatusOut,
    PlanActionUpdateIn,
    PlanReplaceIn,
    PlannerKPIOut,
    PlanningChartOut,
    PriceOut,
    SimulationPointOut,
    TariffConfigUpdateIn,
    TariffManualHoursIn,
    TariffOut,
    TariffHourOut,
)
from powerbuddy.services.inverter import get_inverter_client
from powerbuddy.services.planner import DayPlanner, PlannerInput
from powerbuddy.services.planning_sanity import apply_planning_sanity
from powerbuddy.services.planning_variants import choose_best_plan_variant
from powerbuddy.services.pricing import get_price_provider
from powerbuddy.services.scheduler import PowerBuddyScheduler
from powerbuddy.services.tariff import tariff_service
from powerbuddy.services.weather import weather_forecast_service


scheduler = PowerBuddyScheduler()


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    scheduler.start()
    yield
    scheduler.shutdown()


openapi_tags = [
    {"name": "system", "description": "Health, runtime config and service metadata."},
    {"name": "prices", "description": "Spot price fetch/read endpoints."},
    {"name": "tariff", "description": "Network tariffs and fee configuration/overrides."},
    {"name": "planning", "description": "Battery charge/discharge planning and simulation."},
    {"name": "kpi", "description": "Planning quality metrics and backtesting signals."},
    {"name": "inverter", "description": "Live inverter telemetry."},
]

app = FastAPI(
    title="VNS PowerBuddy API",
    version="1.0.1",
    description=(
        "API for spot prices, Danish tariffs and battery planning. "
        "Designed to be consumed directly from external applications (for example Umbraco)."
    ),
    lifespan=lifespan,
    docs_url="/swagger",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    openapi_tags=openapi_tags,
)


def _cors_origins() -> list[str]:
    return [o.strip() for o in settings.cors_allowed_origins.split(",") if o.strip()]


_origins = _cors_origins()
if _origins:
    allow_credentials = settings.cors_allow_credentials and "*" not in _origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/", tags=["system"], summary="API root")
def index() -> dict[str, object]:
    return {
        "service": "VNS PowerBuddy API",
        "status": "ok",
        "swagger": "/swagger",
        "openapi": "/openapi.json",
        "redoc": "/redoc",
        "key_endpoints": [
            "/planning",
            "/planning/simulate",
            "/execution/status",
            "/execution/pause",
            "/execution/start",
            "/tariff",
            "/tariff/config",
            "/inverter/realtime",
        ],
    }


async def _resolve_current_soc() -> float:
    """Fetch live SOC from inverter; fall back to latest DB snapshot, then assume battery was charged overnight to 85%."""
    try:
        client = get_inverter_client()
        data = await client.get_realtime()
        return data.battery_soc
    except Exception:
        pass
    snapshot_soc = PowerRepository.get_latest_battery_soc()
    # If no realtime data and no recent snapshot, assume battery was charged overnight (85% default makes plans realistic).
    return snapshot_soc if snapshot_soc is not None else 85.0


async def _resolve_start_soc_for_day(day: date) -> float:
    """Return the expected battery SOC at the start of 'day' (local midnight 00:00).

    - day == today or past: return live SOC directly (plan starts from current state).
    - day > today: simulate today's remaining plan actions from current SOC and return
      the projected end-of-day SOC so the plan for tomorrow starts with accurate state.
    - Falls back to live SOC if today has no plan or simulation fails.
    """
    today = date.today()
    live_soc = await _resolve_current_soc()

    if day <= today:
        return live_soc

    # Future day — project through end of today to estimate SOC at midnight.
    today_actions = PlanRepository.get_plan(today.isoformat())
    if not today_actions:
        return live_soc

    tz = ZoneInfo(settings.timezone)
    now_local_hour = datetime.now(tz).replace(minute=0, second=0, microsecond=0).replace(tzinfo=None)
    remaining_actions = [a for a in today_actions if _naive_ts(a.start_time) >= now_local_hour]
    if not remaining_actions:
        return live_soc

    try:
        planner = DayPlanner()
        weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(today)
        simulation = planner.simulate(today, remaining_actions, start_soc=live_soc, pv_weather_factor_24h=weather_factors)
        if simulation:
            projected_soc = simulation[-1].projected_soc
            return max(float(settings.battery_min_soc), min(100.0, projected_soc))
    except Exception:
        pass

    return live_soc


async def _ensure_prices_with_fallback(requested_day: date) -> tuple[date, list, bool]:
    prices = PriceRepository.get_by_day(requested_day, settings.price_area)
    provider = get_price_provider()

    if not prices:
        fetched = await provider.get_day_prices(requested_day, settings.price_area)
        if fetched:
            PriceRepository.upsert_prices(fetched)
            prices = PriceRepository.get_by_day(requested_day, settings.price_area)

    if prices:
        return requested_day, prices, False

    fallback_day = await provider.get_latest_available_day(settings.price_area)
    if fallback_day is None:
        return requested_day, [], False

    fallback_prices = PriceRepository.get_by_day(fallback_day, settings.price_area)
    if not fallback_prices:
        fetched = await provider.get_day_prices(fallback_day, settings.price_area)
        if fetched:
            PriceRepository.upsert_prices(fetched)
            fallback_prices = PriceRepository.get_by_day(fallback_day, settings.price_area)

    return fallback_day, fallback_prices, fallback_day != requested_day


async def _ensure_prices_for_window(start: datetime, end: datetime) -> None:
    """
    Ensure we have stored prices for every calendar day touched by [start, end).
    """
    provider = get_price_provider()
    current_day = start.date()
    end_day = (end - timedelta(seconds=1)).date()

    while current_day <= end_day:
        existing = PriceRepository.get_by_day(current_day, settings.price_area)
        if not existing:
            fetched = await provider.get_day_prices(current_day, settings.price_area)
            if fetched:
                PriceRepository.upsert_prices(fetched)
        current_day += timedelta(days=1)


async def _reconcile_after_plan_change() -> None:
    """Apply changed plan actions immediately instead of waiting for scheduler interval."""
    try:
        await scheduler.force_reconcile_current_action()
    except Exception:
        # Best-effort only; periodic scheduler run will retry shortly.
        pass


def _naive_ts(dt: datetime) -> datetime:
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def _overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and a_end > b_start


def _resolve_default_charge_power_w(action: str, charge_power_w: float | None) -> float | None:
    if action == "charge" and charge_power_w is None:
        return float(settings.default_charge_power_w)
    return charge_power_w


def _effective_charge_power_w(charge_power_w: float | None) -> float:
    requested = float(charge_power_w) if charge_power_w is not None else float(settings.default_charge_power_w)
    return max(0.0, min(requested, float(settings.max_charge_kw) * 1000.0))


def _has_full_hourly_coverage(day: date, actions: list[PlanAction]) -> bool:
    starts: set[datetime] = set()
    for action in actions:
        ts = _naive_ts(action.start_time)
        if ts.date() == day:
            starts.add(ts.replace(minute=0, second=0, microsecond=0))
    return len(starts) >= 24


def _remap_fallback_prices_to_day(target_day: date, fallback_prices: list[PricePoint]) -> list[PricePoint]:
    if not fallback_prices:
        return []

    source = fallback_prices[0].source
    area = fallback_prices[0].area
    by_hour: dict[int, float] = {}
    for point in fallback_prices:
        hour = int((_naive_ts(point.timestamp)).hour)
        by_hour[hour] = float(point.price_ore_per_kwh)

    if not by_hour:
        return []

    fallback_avg = sum(by_hour.values()) / float(len(by_hour))
    remapped: list[PricePoint] = []
    for hour in range(24):
        remapped.append(
            PricePoint(
                timestamp=datetime.combine(target_day, datetime.min.time()) + timedelta(hours=hour),
                area=area,
                price_ore_per_kwh=float(by_hour.get(hour, fallback_avg)),
                currency="DKK",
                source=f"{source}-fallback",
            )
        )
    return remapped


def _has_full_24h_price_shape(points: list[PricePoint]) -> bool:
    if not points:
        return False
    covered_hours = {int(_naive_ts(point.timestamp).hour) for point in points}
    return len(covered_hours) >= 24


async def _load_best_fallback_profile(day: date) -> tuple[list[PricePoint], bool]:
    """
    Return a remapped fallback profile and whether it is provisional.

    If the requested day has no prices, avoid using a partially published day profile
    (for example only 00-07). Instead, walk backwards to find the newest day with a
    full 24-hour shape so provisional planning remains realistic.
    """
    provider = get_price_provider()
    latest_day = await provider.get_latest_available_day(settings.price_area)
    if latest_day is None:
        return [], False

    for offset in range(0, 7):
        probe_day = latest_day - timedelta(days=offset)
        probe_prices = PriceRepository.get_by_day(probe_day, settings.price_area)
        if not probe_prices:
            fetched = await provider.get_day_prices(probe_day, settings.price_area)
            if fetched:
                PriceRepository.upsert_prices(fetched)
                probe_prices = PriceRepository.get_by_day(probe_day, settings.price_area)
        if _has_full_24h_price_shape(probe_prices):
            return _remap_fallback_prices_to_day(day, probe_prices), True

    # Last resort: use latest partial profile if no full day is available.
    latest_prices = PriceRepository.get_by_day(latest_day, settings.price_area)
    if not latest_prices:
        fetched = await provider.get_day_prices(latest_day, settings.price_area)
        if fetched:
            PriceRepository.upsert_prices(fetched)
            latest_prices = PriceRepository.get_by_day(latest_day, settings.price_area)
    if not latest_prices:
        return [], False
    return _remap_fallback_prices_to_day(day, latest_prices), True


async def _get_day_prices_with_provisional_fallback(day: date) -> tuple[list[PricePoint], bool]:
    prices = PriceRepository.get_by_day(day, settings.price_area)
    provider = get_price_provider()

    if not prices:
        fetched = await provider.get_day_prices(day, settings.price_area)
        if fetched:
            PriceRepository.upsert_prices(fetched)
            prices = PriceRepository.get_by_day(day, settings.price_area)

    if prices:
        return prices, False

    if not settings.allow_provisional_prices:
        return [], False

    return await _load_best_fallback_profile(day)


async def _materialize_day_plan_if_missing(day: date) -> None:
    day_key = day.isoformat()
    existing = PlanRepository.get_plan(day_key)
    prices, provisional = await _get_day_prices_with_provisional_fallback(day)

    if existing and _has_full_hourly_coverage(day, existing):
        # Strict mode: do not keep stale future plans when real prices are unavailable.
        if (not prices) and (day > date.today()) and (not settings.allow_provisional_plans):
            manual_only = [action for action in existing if action.is_manual_override]
            PlanRepository.replace_plan(day_key, manual_only)
            return

        is_degenerate_provisional = all(
            action.action == "hold" and (action.reason or "").startswith("provisional fallback:")
            for action in existing
        )
        if not is_degenerate_provisional:
            return

    if not prices:
        # In strict mode, if prices are unavailable, do not keep stale provisional plans.
        if existing:
            manual_only = [action for action in existing if action.is_manual_override]
            PlanRepository.replace_plan(day_key, manual_only)
        return

    planner = DayPlanner()
    start_soc = await _resolve_start_soc_for_day(day)
    weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(day)
    network_tariff = await tariff_service.get_network_tariff_24h()
    tariff_24h = tariff_service.total_tariff_ore_24h(network_tariff)

    generated = planner.plan(
        PlannerInput(
            day=day,
            price_points=prices,
            start_soc=start_soc,
            tariff_ore_per_hour=tariff_24h,
            pv_weather_factor_24h=weather_factors,
        )
    )

    manual_overrides = [action for action in existing if action.is_manual_override]
    if manual_overrides:
        filtered: list[PlanAction] = []
        for action in generated:
            start = _naive_ts(action.start_time)
            end = _naive_ts(action.end_time)
            if any(
                _overlaps(start, end, _naive_ts(manual.start_time), _naive_ts(manual.end_time))
                for manual in manual_overrides
            ):
                continue
            filtered.append(action)
        generated = filtered

    if provisional:
        if generated and all(action.action == "hold" for action in generated):
            # If we only have provisional prices and DP collapses to all-hold,
            # enforce a sensible baseline policy: precharge at night and keep
            # reserve window in auto mode.
            target_soc = min(
                float(settings.battery_max_soc),
                max(
                    float(settings.must_charge_min_soc_percent),
                    float(settings.reserve_soc_min_percent) + 20.0,
                    90.0,
                ),
            )
            estimated_soc = max(float(settings.battery_min_soc), min(float(settings.battery_max_soc), float(start_soc)))
            charge_soc_step = (
                (planner.max_charge_kwh * planner.charge_efficiency / max(planner.capacity_kwh, 1e-6)) * 100.0
            )

            for action in generated:
                hour = int(_naive_ts(action.start_time).hour)
                if 0 <= hour < 7 and estimated_soc + 0.1 < target_soc:
                    estimated_soc = min(float(settings.battery_max_soc), estimated_soc + charge_soc_step)
                    action.action = "charge"
                    action.charge_power_w = round(planner.max_charge_kwh * 1000.0, 1)
                    action.target_soc = round(estimated_soc, 1)
                    action.reason = "provisional fallback: night precharge policy"
                elif planner._is_reserve_hour(action.start_time):
                    action.action = "auto"
                    action.charge_power_w = None
                    action.target_soc = round(estimated_soc, 1)
                    action.reason = "provisional fallback: reserve discharge readiness"
                else:
                    action.action = "hold"
                    action.charge_power_w = None
                    action.target_soc = None
                    action.reason = "provisional fallback: normal operation window"

        for action in generated:
            if not action.reason.startswith("provisional fallback:"):
                action.reason = f"provisional fallback: {action.reason}"

    generated, sanity_report = apply_planning_sanity(
        planner=planner,
        day=day,
        actions=generated,
        prices=prices,
        start_soc=start_soc,
        tariff_ore_per_hour=tariff_24h,
        pv_weather_factor_24h=weather_factors,
        auto_fix=bool(settings.planning_sanity_autofix_enabled),
    )
    if bool(sanity_report.get("auto_fix_applied")):
        generated = sorted(generated, key=lambda action: action.start_time)

    generated, variant_report = choose_best_plan_variant(
        planner=planner,
        day=day,
        actions=generated,
        prices=prices,
        start_soc=start_soc,
        tariff_ore_per_hour=tariff_24h,
        pv_weather_factor_24h=weather_factors,
    )
    if variant_report.get("best_changes"):
        generated = sorted(generated, key=lambda action: action.start_time)

    PlanRepository.replace_plan(day_key, generated)

    simulation = planner.simulate(day, PlanRepository.get_plan(day_key), start_soc, pv_weather_factor_24h=weather_factors)
    SimulationRepository.replace_points(day_key, simulation)


async def _ensure_plan_for_window(start: datetime, end: datetime) -> None:
    current_day = start.date()
    end_day = (end - timedelta(seconds=1)).date()

    while current_day <= end_day:
        await _materialize_day_plan_if_missing(current_day)
        current_day += timedelta(days=1)


async def _refresh_simulation_for_day(day: date, actions: list[PlanAction] | None = None) -> None:
    """Recompute simulation and persist it; for today, project only from current hour using live SOC."""
    planner = DayPlanner()
    day_key = day.isoformat()
    day_actions = actions if actions is not None else PlanRepository.get_plan(day_key)

    current_soc = await _resolve_current_soc()
    weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(day)
    if day == date.today():
        now_local_hour = datetime.now(ZoneInfo(settings.timezone)).replace(minute=0, second=0, microsecond=0).replace(tzinfo=None)
        day_actions = [a for a in day_actions if _naive_ts(a.start_time) >= now_local_hour]

    simulation = (
        planner.simulate(day, day_actions, start_soc=current_soc, pv_weather_factor_24h=weather_factors)
        if day_actions
        else []
    )
    SimulationRepository.replace_points(day_key, simulation)


@app.get("/health", tags=["system"], summary="Health check")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/config", tags=["system"], summary="Read effective runtime settings")
def get_config() -> dict[str, object]:
    execution_mode = scheduler.execution_mode()
    return {
        "db_path": settings.db_path,
        "timezone": settings.timezone,
        "price_provider": settings.price_provider,
        "price_area": settings.price_area,
        "inverter_type": settings.inverter_type,
        "inverter_url": settings.fronius_url,
        "feed_in_tariff_ore": settings.feed_in_tariff_ore,
        "charge_efficiency": settings.charge_efficiency,
        "discharge_efficiency": settings.discharge_efficiency,
        "cycle_degradation_cost_ore_per_kwh": settings.cycle_degradation_cost_ore_per_kwh,
        "reserve_soc_enabled": settings.reserve_soc_enabled,
        "reserve_soc_window": [settings.reserve_soc_start_hour_local, settings.reserve_soc_end_hour_local],
        "reserve_soc_min_percent": settings.reserve_soc_min_percent,
        "pv_forecast_enabled": settings.pv_forecast_enabled,
        "intraday_replan_enabled": settings.intraday_replan_enabled,
        "kpi_tracking_enabled": settings.kpi_tracking_enabled,
        "auto_tuning_enabled": settings.auto_tuning_enabled,
        "plan_execution_mode": execution_mode,
        "config_source": ".env",
        "runtime_mutable": True,
    }


@app.get("/execution/status", tags=["system"], summary="Read execution-layer runtime status")
def get_execution_status() -> dict[str, object]:
    return {
        "execution_enabled": scheduler.is_execution_enabled(),
        "execution_mode": scheduler.execution_mode(),
        "inverter_dispatch": "enabled" if scheduler.is_execution_enabled() else "paused",
        "planning_jobs_running": True,
    }


@app.post("/execution/pause", tags=["system"], summary="Pause inverter dispatch and switch inverter to auto")
async def pause_execution() -> dict[str, object]:
    auto_ok = await scheduler.pause_execution()
    return {
        "execution_enabled": scheduler.is_execution_enabled(),
        "execution_mode": scheduler.execution_mode(),
        "inverter_set_to_auto": bool(auto_ok),
        "planning_jobs_running": True,
    }


@app.post("/execution/start", tags=["system"], summary="Resume inverter dispatch and apply active plan action")
async def start_execution() -> dict[str, object]:
    await scheduler.start_execution()
    return {
        "execution_enabled": scheduler.is_execution_enabled(),
        "execution_mode": scheduler.execution_mode(),
        "planning_jobs_running": True,
    }


@app.get("/kpi/planning", tags=["kpi"], summary="Read recent planning KPIs")
def get_planner_kpis(limit: int = 14) -> list[PlannerKPIOut]:
    items = KPIRepository.get_recent(limit=max(1, min(limit, 90)))
    return [
        PlannerKPIOut(
            date_key=item.date_key,
            planned_grid_kwh=item.planned_grid_kwh,
            actual_grid_kwh=item.actual_grid_kwh,
            planned_peak_import_kwh=item.planned_peak_import_kwh,
            actual_peak_import_kwh=item.actual_peak_import_kwh,
            plan_error_ratio=item.plan_error_ratio,
            soc_at_peak_start=item.soc_at_peak_start,
            expected_daily_consumption_kwh=item.expected_daily_consumption_kwh,
            realized_daily_consumption_kwh=item.realized_daily_consumption_kwh,
            updated_at=item.updated_at,
        )
        for item in items
    ]


@app.post("/prices/fetch", tags=["prices"], summary="Fetch spot prices")
async def fetch_prices(target_date: date | None = None) -> dict[str, int]:
    day = target_date or date.today()
    provider = get_price_provider()
    points = await provider.get_day_prices(day, settings.price_area)
    PriceRepository.upsert_prices(points)
    return {"stored": len(points)}


@app.get("/tariff", tags=["tariff"], summary="Read active tariff breakdown")
async def get_tariff() -> TariffOut:
    """Returns the 24-hour tariff breakdown (network + state fees) for today."""
    network = await tariff_service.get_network_tariff_24h()
    energinet = settings.tariff_energinet_ore
    elafgift = settings.tariff_elafgift_ore
    vat = settings.tariff_vat_factor
    flat = energinet + elafgift

    hours = [
        TariffHourOut(
            hour=h,
            network_tariff_ore=round(network[h], 3),
            total_tariff_ore_excl_vat=round(network[h] + flat, 3),
            total_tariff_ore_incl_vat=round((network[h] + flat) * vat, 3),
        )
        for h in range(24)
    ]
    return TariffOut(
        network_owner=settings.tariff_network_owner,
        network_code=settings.tariff_network_code,
        energinet_ore_flat=energinet,
        elafgift_ore_flat=elafgift,
        vat_factor=vat,
        hours=hours,
    )


@app.put("/tariff/config", tags=["tariff"], summary="Update tariff config")
async def update_tariff_config(payload: TariffConfigUpdateIn) -> TariffOut:
    tariff_service.update_runtime_config(
        network_owner=payload.network_owner,
        network_code=payload.network_code,
        energinet_ore_flat=payload.energinet_ore_flat,
        elafgift_ore_flat=payload.elafgift_ore_flat,
        vat_factor=payload.vat_factor,
    )
    return await get_tariff()


@app.put("/tariff/manual-hours", tags=["tariff"], summary="Set manual hourly network tariff")
async def set_tariff_manual_hours(payload: TariffManualHoursIn) -> TariffOut:
    tariff_service.set_manual_network_override(payload.network_tariff_ore_per_hour)
    return await get_tariff()


@app.delete("/tariff/manual-hours", tags=["tariff"], summary="Clear manual hourly network tariff")
async def clear_tariff_manual_hours() -> TariffOut:
    tariff_service.clear_manual_network_override()
    return await get_tariff()


@app.get("/planning/chart-data", tags=["planning"], summary="Get plan and chart data")
async def planning_chart_data(target_date: date | None = None) -> PlanningChartOut:
    requested_day = target_date or date.today()
    used_day, prices, used_fallback = await _ensure_prices_with_fallback(requested_day)
    if not prices:
        raise HTTPException(status_code=404, detail="No prices found for requested or fallback day")

    planner = DayPlanner()
    day_key = used_day.isoformat()
    expected_daily_consumption_kwh, consumption_source = planner.resolve_expected_daily_consumption(used_day)
    current_soc = await _resolve_start_soc_for_day(used_day)
    weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(used_day)
    network_tariff = await tariff_service.get_network_tariff_24h()

    all_actions = PlanRepository.get_plan(day_key)
    if not all_actions:
        # Bootstrap when no schedule exists yet.
        network_tariff_bootstrap = await tariff_service.get_network_tariff_24h()
        tariff_24h_bootstrap = tariff_service.total_tariff_ore_24h(network_tariff_bootstrap)
        actions = planner.plan(
            PlannerInput(
                day=used_day,
                price_points=prices,
                start_soc=current_soc,
                tariff_ore_per_hour=tariff_24h_bootstrap,
                pv_weather_factor_24h=weather_factors,
            )
        )
        PlanRepository.replace_plan(day_key, actions)
        all_actions = PlanRepository.get_plan(day_key)

    if used_day == date.today():
        await _refresh_simulation_for_day(used_day, all_actions)
        simulation = SimulationRepository.get_points(day_key)
    else:
        simulation = SimulationRepository.get_points(day_key)
        if not simulation:
            simulation = planner.simulate(
                used_day,
                all_actions,
                start_soc=current_soc,
                pv_weather_factor_24h=weather_factors,
            )
            SimulationRepository.replace_points(day_key, simulation)

    price_by_ts = {p.timestamp: p for p in prices}
    action_by_ts = {a.start_time: a for a in all_actions}
    sim_by_ts = {s.timestamp: s for s in simulation}

    timeline = sorted(price_by_ts.keys())
    labels = [ts.isoformat() for ts in timeline]
    prices_series = [price_by_ts[ts].price_ore_per_kwh if ts in price_by_ts else 0.0 for ts in timeline]
    actions_series = [action_by_ts[ts].action if ts in action_by_ts else "hold" for ts in timeline]
    target_soc_series = [action_by_ts[ts].target_soc if ts in action_by_ts else None for ts in timeline]
    projected_soc_series = [sim_by_ts[ts].projected_soc if ts in sim_by_ts else None for ts in timeline]
    projected_grid_series = [sim_by_ts[ts].projected_grid_kwh if ts in sim_by_ts else None for ts in timeline]

    # Tariff series aligned with timeline (network tariff indexed by hour; prices sorted by UTC, same order)
    sorted_prices = sorted(prices, key=lambda p: p.timestamp)
    network_series: list[float] = []
    total_cost_series: list[float | None] = []
    vat = settings.tariff_vat_factor
    flat_tariff = settings.tariff_energinet_ore + settings.tariff_elafgift_ore
    for i, ts in enumerate(timeline):
        net = network_tariff[i] if i < len(network_tariff) else network_tariff[-1]
        network_series.append(round(net, 3))
        spot = price_by_ts[ts].price_ore_per_kwh if ts in price_by_ts else None
        if spot is not None:
            total_cost_series.append(round((spot + net + flat_tariff) * vat, 2))
        else:
            total_cost_series.append(None)

    action_hours = {"charge": 0, "hold": 0, "discharge": 0}
    for item in actions_series:
        action_hours[item] = action_hours.get(item, 0) + 1

    # Cheapest/most expensive hours by total cost (incl. all tariffs + VAT)
    priced = [(ts, c) for ts, c in zip(timeline, total_cost_series) if c is not None]
    cheapest_hours = [ts.isoformat() for ts, _ in sorted(priced, key=lambda x: x[1])[:3]]
    most_expensive_hours = [ts.isoformat() for ts, _ in sorted(priced, key=lambda x: x[1])[-3:]]

    return PlanningChartOut(
        requested_date=requested_day,
        used_date=used_day,
        area=settings.price_area,
        used_fallback=used_fallback,
        expected_daily_consumption_kwh=expected_daily_consumption_kwh,
        consumption_source=consumption_source,
        labels=labels,
        prices_ore_per_kwh=prices_series,
        network_tariff_ore_per_hour=network_series,
        total_cost_ore_incl_vat=total_cost_series,
        actions=actions_series,
        target_soc=target_soc_series,
        projected_soc=projected_soc_series,
        projected_grid_kwh=projected_grid_series,
        action_hours=action_hours,
        cheapest_hours=cheapest_hours,
        most_expensive_hours=most_expensive_hours,
    )


@app.get("/prices", tags=["prices"], summary="Read stored spot prices")
async def get_prices(
    target_date: date | None = None,
    from_timestamp: datetime | None = None,
    hours: int = 24,
) -> list[PriceOut]:
    """
    Default behavior (no query params): return prices from current whole hour and
    24 hours forward.

    Backward-compatible behavior: if target_date is provided, return that day's
    prices (00:00..24:00).
    """
    if target_date is not None:
        prices = PriceRepository.get_by_day(target_date, settings.price_area)
        if not prices:
            provider = get_price_provider()
            fetched = await provider.get_day_prices(target_date, settings.price_area)
            if fetched:
                PriceRepository.upsert_prices(fetched)
                prices = PriceRepository.get_by_day(target_date, settings.price_area)
    else:
        horizon_hours = max(1, min(hours, 72))
        start = from_timestamp.astimezone(timezone.utc) if from_timestamp else datetime.now(timezone.utc)
        start = start.replace(minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=horizon_hours)
        await _ensure_prices_for_window(start, end)

        raw_prices = PriceRepository.get_by_time_window(start, end, settings.price_area)

        if not settings.allow_provisional_prices:
            prices = raw_prices
        else:
            def _slot_key(dt: datetime) -> datetime:
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc)
                return dt.replace(minute=0, second=0, microsecond=0)

            price_by_slot: dict[datetime, PricePoint] = {
                _slot_key(point.timestamp): point
                for point in raw_prices
            }

            provisional_cache: dict[date, list[PricePoint]] = {}
            filled: list[PricePoint] = []
            for offset in range(horizon_hours):
                slot = start + timedelta(hours=offset)
                key = _slot_key(slot)
                point = price_by_slot.get(key)

                if point is None:
                    slot_day = key.date()
                    if slot_day not in provisional_cache:
                        provisional_prices, _is_provisional = await _get_day_prices_with_provisional_fallback(slot_day)
                        provisional_cache[slot_day] = provisional_prices

                    hour = int(key.hour)
                    provisional_point = next(
                        (candidate for candidate in provisional_cache[slot_day] if int(_naive_ts(candidate.timestamp).hour) == hour),
                        None,
                    )
                    if provisional_point is not None:
                        point = PricePoint(
                            timestamp=slot,
                            area=settings.price_area,
                            price_ore_per_kwh=float(provisional_point.price_ore_per_kwh),
                            currency="DKK",
                            source="provisional-fallback",
                        )

                if point is None:
                    # Ultimate fallback: keep API contract with a neutral placeholder point.
                    point = PricePoint(
                        timestamp=slot,
                        area=settings.price_area,
                        price_ore_per_kwh=0.0,
                        currency="DKK",
                        source="missing",
                    )

                filled.append(point)

            prices = filled

    network_tariff_24h = await tariff_service.get_network_tariff_24h()
    vat = float(settings.tariff_vat_factor)
    supplier_markup = max(0.0, float(settings.price_supplier_markup_ore))
    transport_fixed = max(0.0, float(settings.price_transport_fixed_ore))
    local_tz = ZoneInfo(settings.timezone)

    out: list[PriceOut] = []
    for p in prices:
        ts = p.timestamp
        if ts.tzinfo is not None:
            local_hour = int(ts.astimezone(local_tz).hour)
        else:
            local_hour = int(ts.hour)

        network_component = float(network_tariff_24h[local_hour]) if 0 <= local_hour <= 23 else 0.0
        spot = float(p.price_ore_per_kwh)
        without_fees = (spot + supplier_markup) * vat
        with_fees = (spot + supplier_markup + network_component + transport_fixed) * vat

        out.append(
            PriceOut(
                timestamp=p.timestamp,
                area=p.area,
                # Keep primary field aligned with DB/planner input price.
                price_ore_per_kwh=spot,
                spot_price_ore_per_kwh=spot,
                price_without_fees_ore_per_kwh=without_fees,
                price_with_fees_ore_per_kwh=with_fees,
                currency=p.currency,
            )
        )

    return out


@app.get("/planning/now", tags=["planning"], summary="Read current planned action vs realtime")
async def get_current_plan_status() -> PlanNowStatusOut:
    now_utc = datetime.now(timezone.utc)
    local_tz = ZoneInfo(settings.timezone)
    now_local = now_utc.astimezone(local_tz)
    now_naive = now_local.replace(tzinfo=None)
    day_key = now_local.date().isoformat()
    actions = PlanRepository.get_plan(day_key)

    current_action = "hold"
    current_start: datetime | None = None
    current_end: datetime | None = None
    for action in actions:
        start = action.start_time.replace(tzinfo=None) if action.start_time.tzinfo else action.start_time
        end = action.end_time.replace(tzinfo=None) if action.end_time.tzinfo else action.end_time
        if start <= now_naive < end:
            current_action = action.action
            current_start = action.start_time
            current_end = action.end_time
            break

    realtime = await get_inverter_client().get_realtime()
    battery_power_w = float(realtime.battery_power_w)
    # On this Fronius setup, negative battery power indicates charging and positive indicates discharging.
    is_charging = battery_power_w < -50.0
    is_discharging = battery_power_w > 50.0
    soc = float(realtime.battery_soc)
    at_min_soc = soc <= float(settings.battery_min_soc) + 0.2
    at_max_soc = soc >= float(settings.battery_max_soc) - 0.2

    # PowerBuddy currently provides planning only (no direct inverter dispatch).
    # This makes it explicit when realtime behavior differs from the plan.
    matches = (
        (current_action == "charge" and (is_charging or at_max_soc))
        or (current_action == "discharge" and (is_discharging or at_min_soc))
        or (current_action == "auto" and (is_charging or is_discharging or at_min_soc or at_max_soc))
        or (current_action == "hold" and not is_charging and not is_discharging)
    )

    execution_mode = scheduler.execution_mode()
    return PlanNowStatusOut(
        timestamp=now_utc,
        execution_mode=execution_mode,
        planned_action=current_action,
        planned_start_time=current_start,
        planned_end_time=current_end,
        battery_power_w=battery_power_w,
        is_battery_charging=is_charging,
        is_battery_discharging=is_discharging,
        matches_plan=matches,
    )


@app.get("/inverter/realtime", tags=["inverter"], summary="Read inverter realtime values")
async def inverter_realtime() -> InverterRealtime:
    client = get_inverter_client()
    data = await client.get_realtime()
    return InverterRealtime(
        timestamp=data.timestamp,
        grid_power_w=data.grid_power_w,
        load_power_w=data.load_power_w,
        pv_power_w=data.pv_power_w,
        battery_power_w=data.battery_power_w,
        battery_soc=data.battery_soc,
    )


@app.get("/planning", tags=["planning"], summary="Read battery plan")
async def get_plan(
    target_date: date | None = None,
    from_timestamp: datetime | None = None,
    hours: int | None = None,
) -> list[PlanActionOut]:
    """
    If target_date is provided: return that calendar day's plan.
    Otherwise: return a rolling plan window from current whole UTC hour
    (or from_timestamp) and forward; default window is at least 48 hours.
    """
    if target_date is not None:
        await _materialize_day_plan_if_missing(target_date)
        actions = [
            action
            for action in PlanRepository.get_plan(target_date.isoformat())
            if _naive_ts(action.start_time).date() == target_date
        ]
    else:
        default_hours = max(24, int(settings.planning_horizon_hours))
        horizon_hours = max(1, min(int(hours) if hours is not None else default_hours, 72))

        if from_timestamp is None:
            start = datetime.now(timezone.utc)
        elif from_timestamp.tzinfo is None:
            start = from_timestamp.replace(tzinfo=timezone.utc)
        else:
            start = from_timestamp.astimezone(timezone.utc)

        start = start.replace(minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=horizon_hours)
        await _ensure_plan_for_window(start, end)
        actions = PlanRepository.get_plan_window(start, end)

    # Keep API output stable: one action per hour (manual overrides are already sorted first).
    deduped: list[PlanAction] = []
    seen_slots: set[datetime] = set()
    for action in actions:
        slot = _naive_ts(action.start_time).replace(minute=0, second=0, microsecond=0)
        if slot in seen_slots:
            continue
        seen_slots.add(slot)
        deduped.append(action)
    actions = deduped

    now_local = datetime.now(ZoneInfo(settings.timezone)).replace(tzinfo=None)
    current_soc: float | None = None
    adjusted: list[PlanActionOut] = []
    for a in actions:
        target_soc = a.target_soc
        start = _naive_ts(a.start_time)
        end = _naive_ts(a.end_time)
        if (
            a.action == "charge"
            and target_soc is not None
            and start <= now_local < end
        ):
            if current_soc is None:
                current_soc = await _resolve_current_soc()
            remaining_hours = max(0.0, (end - now_local).total_seconds() / 3600.0)
            charge_power_w = _effective_charge_power_w(a.charge_power_w)
            delta_soc = (
                (charge_power_w / 1000.0)
                * remaining_hours
                * float(settings.charge_efficiency)
                / max(float(settings.battery_capacity_kwh), 1e-6)
            ) * 100.0
            achievable_soc = min(float(settings.battery_max_soc), current_soc + delta_soc)
            target_soc = round(min(max(float(target_soc), current_soc), achievable_soc), 1)

        adjusted.append(
            PlanActionOut(
                id=a.id,
                date_key=a.date_key,
                start_time=a.start_time,
                end_time=a.end_time,
                action=a.action,
                charge_power_w=a.charge_power_w,
                target_soc=target_soc,
                reason=a.reason,
                is_manual_override=a.is_manual_override,
            )
        )

    return adjusted


@app.get("/planning/sanity", tags=["planning"], summary="Validate/auto-fix plan sanity")
async def planning_sanity(target_date: date, auto_fix: bool = False) -> dict[str, object]:
    await _materialize_day_plan_if_missing(target_date)

    planner = DayPlanner()
    day_key = target_date.isoformat()
    actions = PlanRepository.get_plan(day_key)
    prices, provisional = await _get_day_prices_with_provisional_fallback(target_date)
    if not prices:
        raise HTTPException(status_code=404, detail="No prices available for sanity check")

    start_soc = await _resolve_start_soc_for_day(target_date)
    weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(target_date)
    network_tariff = await tariff_service.get_network_tariff_24h()
    tariff_24h = tariff_service.total_tariff_ore_24h(network_tariff)

    should_fix = bool(auto_fix) and bool(settings.planning_sanity_autofix_enabled)
    updated_actions, report = apply_planning_sanity(
        planner=planner,
        day=target_date,
        actions=actions,
        prices=prices,
        start_soc=start_soc,
        tariff_ore_per_hour=tariff_24h,
        pv_weather_factor_24h=weather_factors,
        auto_fix=should_fix,
    )

    changed = bool(report.get("auto_fix_applied"))
    if changed:
        persisted_actions = [
            PlanAction(
                date_key=action.date_key,
                start_time=action.start_time,
                end_time=action.end_time,
                action=action.action,
                charge_power_w=action.charge_power_w,
                target_soc=action.target_soc,
                reason=action.reason,
                is_manual_override=action.is_manual_override,
            )
            for action in updated_actions
        ]
        PlanRepository.replace_plan(day_key, persisted_actions)
        simulation = planner.simulate(
            target_date,
            PlanRepository.get_plan(day_key),
            start_soc,
            pv_weather_factor_24h=weather_factors,
        )
        SimulationRepository.replace_points(day_key, simulation)

    report["auto_fix_requested"] = bool(auto_fix)
    report["auto_fix_effective"] = bool(should_fix)
    report["used_provisional_prices"] = bool(provisional)
    report["action_count"] = len(updated_actions)
    return report


@app.post("/planning/override", tags=["planning"], summary="Add manual override action")
async def add_override(payload: ManualOverrideIn) -> PlanActionOut:
    charge_power_w = _resolve_default_charge_power_w(payload.action, payload.charge_power_w)
    action = PlanAction(
        date_key=payload.date.isoformat(),
        start_time=payload.start_time,
        end_time=payload.end_time,
        action=payload.action,
        charge_power_w=charge_power_w,
        target_soc=payload.target_soc,
        reason=payload.reason,
        is_manual_override=True,
    )
    stored = PlanRepository.add_manual_override(action)
    await _refresh_simulation_for_day(payload.date)
    await _reconcile_after_plan_change()
    return PlanActionOut(
        id=stored.id,
        date_key=stored.date_key,
        start_time=stored.start_time,
        end_time=stored.end_time,
        action=stored.action,
        charge_power_w=stored.charge_power_w,
        target_soc=stored.target_soc,
        reason=stored.reason,
        is_manual_override=stored.is_manual_override,
    )

@app.put("/planning/action/{action_id}", tags=["planning"], summary="Update a plan action")
async def update_plan_action(action_id: int, payload: PlanActionUpdateIn) -> PlanActionOut:
    existing = PlanRepository.get_action(action_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Plan action not found")

    updates = payload.model_dump(exclude_unset=True)
    effective_action = str(updates.get("action", existing.action))
    updates["charge_power_w"] = _resolve_default_charge_power_w(
        effective_action,
        updates.get("charge_power_w", existing.charge_power_w),
    )
    # Any direct action edit is considered a manual override and must survive auto re-plans.
    updates["is_manual_override"] = True
    if not updates.get("reason"):
        updates["reason"] = "manual override"
    updated = PlanRepository.update_action(action_id, **updates)
    if updated is None:
        raise HTTPException(status_code=404, detail="Plan action not found")
    try:
        updated_day = date.fromisoformat(updated.date_key)
    except Exception:
        updated_day = date.today()
    await _refresh_simulation_for_day(updated_day)
    await _reconcile_after_plan_change()
    return PlanActionOut(
        id=updated.id,
        date_key=updated.date_key,
        start_time=updated.start_time,
        end_time=updated.end_time,
        action=updated.action,
        charge_power_w=updated.charge_power_w,
        target_soc=updated.target_soc,
        reason=updated.reason,
        is_manual_override=updated.is_manual_override,
    )


@app.delete("/planning/action/{action_id}", status_code=204, tags=["planning"], summary="Delete a plan action")
async def delete_plan_action(action_id: int) -> None:
    target_day = date.today()
    target_action = PlanRepository.get_action(action_id)
    if target_action is not None:
        try:
            target_day = date.fromisoformat(target_action.date_key)
        except Exception:
            target_day = date.today()

    if not PlanRepository.delete_action(action_id):
        raise HTTPException(status_code=404, detail="Plan action not found")
    await _refresh_simulation_for_day(target_day)
    await _reconcile_after_plan_change()


@app.put("/planning", tags=["planning"], summary="Replace full day battery plan")
async def replace_plan(payload: PlanReplaceIn) -> dict[str, int]:
    day_key = payload.date.isoformat()
    actions = [
        PlanAction(
            date_key=day_key,
            start_time=a.start_time,
            end_time=a.end_time,
            action=a.action,
            charge_power_w=_resolve_default_charge_power_w(a.action, a.charge_power_w),
            target_soc=a.target_soc,
            reason=a.reason,
            is_manual_override=a.is_manual_override,
        )
        for a in payload.actions
    ]
    PlanRepository.replace_full_plan(day_key, actions)
    await _refresh_simulation_for_day(payload.date, actions)
    await _reconcile_after_plan_change()
    return {"actions": len(actions)}


@app.post("/planning/simulate", tags=["planning"], summary="Simulate existing plan")
async def simulate_plan(target_date: date | None = None) -> list[SimulationPointOut]:
    day = target_date or date.today()
    planner = DayPlanner()
    day_key = day.isoformat()
    actions = PlanRepository.get_plan(day_key)
    if not actions:
        raise HTTPException(status_code=400, detail="No plan found for requested date")

    current_soc = await _resolve_current_soc()
    weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(day)
    simulation = planner.simulate(day, actions, start_soc=current_soc, pv_weather_factor_24h=weather_factors)
    SimulationRepository.replace_points(day_key, simulation)

    points = SimulationRepository.get_points(day_key)
    return [
        SimulationPointOut(
            timestamp=p.timestamp,
            action=p.action,
            projected_soc=p.projected_soc,
            projected_grid_kwh=p.projected_grid_kwh,
        )
        for p in points
    ]
