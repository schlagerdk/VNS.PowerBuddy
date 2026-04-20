from typing import Final

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BYD_HVM_CAPACITY_POWER_KW: Final[dict[float, float]] = {
    8.3: 4.51,
    11.0: 5.63,
    13.8: 6.76,
    16.6: 7.88,
    19.3: 9.01,
    22.1: 9.01,
}

FIXED_BATTERY_MIN_SOC: Final[int] = 5
FIXED_BATTERY_MAX_SOC: Final[int] = 100
DEFAULT_BATTERY_CAPACITY_KWH: Final[float] = 13.8

_detected_battery_capacity_kwh: float | None = None


def set_detected_battery_capacity_kwh(capacity_kwh: float | None) -> None:
    global _detected_battery_capacity_kwh
    if capacity_kwh is None:
        return
    value = float(capacity_kwh)
    if value <= 0.0:
        return
    _detected_battery_capacity_kwh = value


def get_detected_battery_capacity_kwh() -> float | None:
    return _detected_battery_capacity_kwh


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

    db_path: str = Field(default="./data/powerbuddy.db", alias="POWERBUDDY_DB_PATH")
    timezone: str = Field(default="Europe/Copenhagen", alias="POWERBUDDY_TIMEZONE")

    price_provider: str = Field(default="energidataservice", alias="POWERBUDDY_PRICE_PROVIDER")
    price_area: str = Field(default="DK2", alias="POWERBUDDY_PRICE_AREA")

    inverter_type: str = Field(default="fronius", alias="POWERBUDDY_INVERTER_TYPE")
    # Keep default empty to satisfy static type-checkers; must be set via env in real runs.
    fronius_url: str = Field(default="", alias="POWERBUDDY_FRONIUS_URL")
    # Execution layer: when enabled, scheduler applies current plan action to inverter.
    execution_enabled: bool = Field(default=True, alias="POWERBUDDY_EXECUTION_ENABLED")
    execution_interval_seconds: int = Field(default=60, alias="POWERBUDDY_EXECUTION_INTERVAL_SECONDS")
    execution_non_charge_refresh_seconds: int = Field(
        default=60,
        alias="POWERBUDDY_EXECUTION_NON_CHARGE_REFRESH_SECONDS",
    )
    inverter_realtime_cache_seconds: float = Field(
        default=2.0,
        alias="POWERBUDDY_INVERTER_REALTIME_CACHE_SECONDS",
    )
    hold_reassert_threshold_w: float = Field(
        default=120.0,
        alias="POWERBUDDY_HOLD_REASSERT_THRESHOLD_W",
    )
    hold_discharge_reassert_threshold_w: float = Field(
        default=5.0,
        alias="POWERBUDDY_HOLD_DISCHARGE_REASSERT_THRESHOLD_W",
    )

    # Fronius control endpoints (optional). If unset, execution logs warnings and skips writes.
    fronius_action_method: str = Field(default="POST", alias="POWERBUDDY_FRONIUS_ACTION_METHOD")
    fronius_charge_url: str = Field(default="", alias="POWERBUDDY_FRONIUS_CHARGE_URL")
    fronius_hold_url: str = Field(default="", alias="POWERBUDDY_FRONIUS_HOLD_URL")
    fronius_discharge_url: str = Field(default="", alias="POWERBUDDY_FRONIUS_DISCHARGE_URL")
    fronius_action_auth_user: str = Field(default="", alias="POWERBUDDY_FRONIUS_ACTION_AUTH_USER")
    fronius_action_auth_pass: str = Field(default="", alias="POWERBUDDY_FRONIUS_ACTION_AUTH_PASS")
    fronius_action_timeout_seconds: int = Field(default=10, alias="POWERBUDDY_FRONIUS_ACTION_TIMEOUT_SECONDS")

    # Modbus fallback execution (used when Fronius HTTP action URLs are unavailable).
    modbus_host: str = Field(default="", alias="POWERBUDDY_MODBUS_HOST")
    modbus_port: int = Field(default=502, alias="POWERBUDDY_MODBUS_PORT")
    modbus_unit_id: int = Field(default=1, alias="POWERBUDDY_MODBUS_UNIT_ID")
    modbus_charge_power_setpoint_address: int = Field(
        default=40352,
        alias="POWERBUDDY_MODBUS_CHARGE_POWER_SETPOINT_ADDRESS",
    )
    modbus_charge_power_setpoint_scale_w: float = Field(
        default=10.0,
        alias="POWERBUDDY_MODBUS_CHARGE_POWER_SETPOINT_SCALE_W",
    )
    # JSON array format: [{"address": 40348, "value": 1}, ...]
    modbus_charge_writes_json: str = Field(default="", alias="POWERBUDDY_MODBUS_CHARGE_WRITES_JSON")
    modbus_hold_writes_json: str = Field(default="", alias="POWERBUDDY_MODBUS_HOLD_WRITES_JSON")
    modbus_discharge_writes_json: str = Field(default="", alias="POWERBUDDY_MODBUS_DISCHARGE_WRITES_JSON")

    # Override with numeric kW values, or use "auto" to derive limits from BYD HVM table.
    max_charge_kw_override: str = Field(default="auto", alias="POWERBUDDY_MAX_CHARGE_KW")
    max_discharge_kw_override: str = Field(default="auto", alias="POWERBUDDY_MAX_DISCHARGE_KW")
    # Backward compatibility: keep accepting deprecated keys present in existing .env files.
    legacy_battery_capacity_kwh: float | None = Field(default=None, alias="POWERBUDDY_BATTERY_CAPACITY_KWH")
    legacy_battery_min_soc: int | None = Field(default=None, alias="POWERBUDDY_BATTERY_MIN_SOC")
    legacy_battery_max_soc: int | None = Field(default=None, alias="POWERBUDDY_BATTERY_MAX_SOC")
    legacy_planned_charge_kw: float | None = Field(default=None, alias="POWERBUDDY_PLANNED_CHARGE_KW")
    hold_charge_power_w: float = Field(default=5.0, alias="POWERBUDDY_HOLD_CHARGE_POWER_W")
    hold_solar_capture_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_HOLD_SOLAR_CAPTURE_ENABLED",
    )
    hold_solar_capture_pv_w_threshold: float = Field(
        default=2500.0,
        alias="POWERBUDDY_HOLD_SOLAR_CAPTURE_PV_W_THRESHOLD",
    )
    hold_solar_capture_export_w_threshold: float = Field(
        default=-800.0,
        alias="POWERBUDDY_HOLD_SOLAR_CAPTURE_EXPORT_W_THRESHOLD",
    )
    hold_override_min_surplus_w: float = Field(
        default=400.0,
        alias="POWERBUDDY_HOLD_OVERRIDE_MIN_SURPLUS_W",
    )
    force_load_solar_aware_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_FORCE_LOAD_SOLAR_AWARE_ENABLED",
    )
    force_load_high_solar_pv_w_threshold: float = Field(
        default=2500.0,
        alias="POWERBUDDY_FORCE_LOAD_HIGH_SOLAR_PV_W_THRESHOLD",
    )
    force_load_grid_import_limit_w: float = Field(
        default=200.0,
        alias="POWERBUDDY_FORCE_LOAD_GRID_IMPORT_LIMIT_W",
    )
    hold_high_solar_auto_enabled: bool = Field(
        default=False,
        alias="POWERBUDDY_HOLD_HIGH_SOLAR_AUTO_ENABLED",
    )
    hold_high_solar_auto_pv_w_threshold: float = Field(
        default=2500.0,
        alias="POWERBUDDY_HOLD_HIGH_SOLAR_AUTO_PV_W_THRESHOLD",
    )
    hold_high_solar_auto_soc_below_percent: float = Field(
        default=100.0,
        alias="POWERBUDDY_HOLD_HIGH_SOLAR_AUTO_SOC_BELOW_PERCENT",
    )
    force_discharge_power_w: int = Field(
        default=6000,
        alias="POWERBUDDY_FORCE_DISCHARGE_POWER_W",
    )

    expected_daily_consumption_kwh: float = Field(
        default=60.0,
        alias="POWERBUDDY_EXPECTED_DAILY_CONSUMPTION_KWH",
    )
    dynamic_consumption_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_DYNAMIC_CONSUMPTION_ENABLED",
    )
    dynamic_consumption_lookback_days: int = Field(
        default=7,
        alias="POWERBUDDY_DYNAMIC_CONSUMPTION_LOOKBACK_DAYS",
    )
    dynamic_consumption_min_samples_per_day: int = Field(
        default=24,
        alias="POWERBUDDY_DYNAMIC_CONSUMPTION_MIN_SAMPLES_PER_DAY",
    )
    consumption_profile_weekpart_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_CONSUMPTION_PROFILE_WEEKPART_ENABLED",
    )
    seasonal_anchor_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_SEASONAL_ANCHOR_ENABLED",
    )
    # JSON object keyed by month number (1-12) with daily kWh anchor values.
    # Example: {"1": 75.0, "2": 64.0, "3": 41.0, "4": 28.0, ...}
    seasonal_anchor_monthly_daily_kwh_json: str = Field(
        default='{"1":75.2,"2":64.1,"3":41.3,"4":27.9,"5":30.0,"6":28.0,"7":27.0,"8":29.0,"9":32.0,"10":40.0,"11":55.0,"12":69.4}',
        alias="POWERBUDDY_SEASONAL_ANCHOR_MONTHLY_DAILY_KWH_JSON",
    )
    seasonal_anchor_weight: float = Field(
        default=0.35,
        alias="POWERBUDDY_SEASONAL_ANCHOR_WEIGHT",
    )
    seasonal_anchor_max_deviation_ratio: float = Field(
        default=0.45,
        alias="POWERBUDDY_SEASONAL_ANCHOR_MAX_DEVIATION_RATIO",
    )
    feed_in_tariff_ore: float = Field(default=25.0, alias="POWERBUDDY_FEED_IN_TARIFF_ORE")

    # Robust scenario planning (multipliers on expected hourly consumption)
    scenario_low_factor: float = Field(default=0.85, alias="POWERBUDDY_SCENARIO_LOW_FACTOR")
    scenario_base_factor: float = Field(default=1.0, alias="POWERBUDDY_SCENARIO_BASE_FACTOR")
    scenario_high_factor: float = Field(default=1.25, alias="POWERBUDDY_SCENARIO_HIGH_FACTOR")
    scenario_high_penalty_weight: float = Field(default=0.35, alias="POWERBUDDY_SCENARIO_HIGH_PENALTY_WEIGHT")

    # Reserve SOC window to protect expensive hours.
    reserve_soc_enabled: bool = Field(default=True, alias="POWERBUDDY_RESERVE_SOC_ENABLED")
    reserve_soc_start_hour_local: int = Field(default=17, alias="POWERBUDDY_RESERVE_SOC_START_HOUR_LOCAL")
    reserve_soc_end_hour_local: int = Field(default=21, alias="POWERBUDDY_RESERVE_SOC_END_HOUR_LOCAL")
    reserve_soc_min_percent: float = Field(default=45.0, alias="POWERBUDDY_RESERVE_SOC_MIN_PERCENT")

    # Battery efficiency and wear cost.
    charge_efficiency: float = Field(default=0.93, alias="POWERBUDDY_CHARGE_EFFICIENCY")
    discharge_efficiency: float = Field(default=0.93, alias="POWERBUDDY_DISCHARGE_EFFICIENCY")
    cycle_degradation_cost_ore_per_kwh: float = Field(
        default=8.0,
        alias="POWERBUDDY_CYCLE_DEGRADATION_COST_ORE_PER_KWH",
    )
    objective_include_vat: bool = Field(default=True, alias="POWERBUDDY_OBJECTIVE_INCLUDE_VAT")

    # PV forecast from historical snapshots.
    pv_forecast_enabled: bool = Field(default=True, alias="POWERBUDDY_PV_FORECAST_ENABLED")
    pv_forecast_lookback_days: int = Field(default=14, alias="POWERBUDDY_PV_FORECAST_LOOKBACK_DAYS")
    pv_forecast_min_samples_per_day: int = Field(default=24, alias="POWERBUDDY_PV_FORECAST_MIN_SAMPLES_PER_DAY")
    weather_forecast_enabled: bool = Field(default=True, alias="POWERBUDDY_WEATHER_FORECAST_ENABLED")
    weather_latitude: float = Field(default=55.6761, alias="POWERBUDDY_WEATHER_LATITUDE")
    weather_longitude: float = Field(default=12.5683, alias="POWERBUDDY_WEATHER_LONGITUDE")

    # How often (minutes) to re-fetch prices and possibly re-plan
    price_recheck_interval_minutes: int = Field(
        default=5,
        alias="POWERBUDDY_PRICE_RECHECK_INTERVAL_MINUTES",
    )
    # Planning horizon measured from "now".
    # Minimum practical value is 48h to keep a forward plan for the next 2 days.
    planning_horizon_hours: int = Field(
        default=48,
        alias="POWERBUDDY_PLANNING_HORIZON_HOURS",
    )
    # How many days ahead to try to pre-fetch prices (0 = today only, 1 = also tomorrow)
    # This is an additional floor on top of planning_horizon_hours-derived days.
    price_fetch_days_ahead: int = Field(
        default=1,
        alias="POWERBUDDY_PRICE_FETCH_DAYS_AHEAD",
    )
    # Nord Pool day-ahead publication is typically around 13:00 local time.
    # We use this to avoid noisy pre-publication fetches for future days.
    day_ahead_publish_hour_local: int = Field(
        default=13,
        alias="POWERBUDDY_DAY_AHEAD_PUBLISH_HOUR_LOCAL",
    )
    # Minimum price change (øre/kWh) that triggers a full re-plan
    price_replan_threshold_ore: float = Field(
        default=2.0,
        alias="POWERBUDDY_PRICE_REPLAN_THRESHOLD_ORE",
    )

    # Adaptive solar re-plan: if PV suddenly produces significant power, re-plan.
    solar_replan_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_SOLAR_REPLAN_ENABLED",
    )
    solar_replan_check_minutes: int = Field(
        default=5,
        alias="POWERBUDDY_SOLAR_REPLAN_CHECK_MINUTES",
    )
    solar_replan_trigger_w: float = Field(
        default=1500.0,
        alias="POWERBUDDY_SOLAR_REPLAN_TRIGGER_W",
    )
    solar_replan_cooldown_minutes: int = Field(
        default=15,
        alias="POWERBUDDY_SOLAR_REPLAN_COOLDOWN_MINUTES",
    )

    intraday_replan_enabled: bool = Field(default=True, alias="POWERBUDDY_INTRADAY_REPLAN_ENABLED")
    intraday_replan_interval_minutes: int = Field(default=30, alias="POWERBUDDY_INTRADAY_REPLAN_INTERVAL_MINUTES")
    intraday_replan_lock_hours: int = Field(default=6, alias="POWERBUDDY_INTRADAY_REPLAN_LOCK_HOURS")
    intraday_replan_consumption_deviation_trigger_ratio: float = Field(
        default=0.22,
        alias="POWERBUDDY_INTRADAY_REPLAN_CONSUMPTION_DEVIATION_TRIGGER_RATIO",
    )

    # Keep early-night decisions stable across date rollover.
    midnight_replan_lock_hours: int = Field(default=6, alias="POWERBUDDY_MIDNIGHT_REPLAN_LOCK_HOURS")

    # Hard planning guardrail: ensure battery reaches a minimum SOC before morning.
    must_charge_window_enabled: bool = Field(default=True, alias="POWERBUDDY_MUST_CHARGE_WINDOW_ENABLED")
    must_charge_window_start_hour_local: int = Field(
        default=0,
        alias="POWERBUDDY_MUST_CHARGE_WINDOW_START_HOUR_LOCAL",
    )
    must_charge_window_end_hour_local: int = Field(
        default=6,
        alias="POWERBUDDY_MUST_CHARGE_WINDOW_END_HOUR_LOCAL",
    )
    must_charge_window_min_soc_percent: float = Field(
        default=55.0,
        alias="POWERBUDDY_MUST_CHARGE_WINDOW_MIN_SOC_PERCENT",
    )

    # Prevent unusable plan states near battery minimum by forcing charging.
    low_soc_force_charge_enabled: bool = Field(default=True, alias="POWERBUDDY_LOW_SOC_FORCE_CHARGE_ENABLED")
    low_soc_force_charge_margin_percent: float = Field(
        default=3.0,
        alias="POWERBUDDY_LOW_SOC_FORCE_CHARGE_MARGIN_PERCENT",
    )
    low_soc_force_planning_near_now_hours: int = Field(
        default=2,
        alias="POWERBUDDY_LOW_SOC_FORCE_PLANNING_NEAR_NOW_HOURS",
    )

    # Strategy guardrail: use auto/discharge primarily in expensive hours.
    auto_only_expensive_hours_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_AUTO_ONLY_EXPENSIVE_HOURS_ENABLED",
    )
    auto_only_expensive_quantile: float = Field(
        default=0.70,
        alias="POWERBUDDY_AUTO_ONLY_EXPENSIVE_QUANTILE",
    )
    auto_only_expensive_min_spread_ore: float = Field(
        default=20.0,
        alias="POWERBUDDY_AUTO_ONLY_EXPENSIVE_MIN_SPREAD_ORE",
    )

    # If spread is meaningful, precharge before reserve window instead of staying idle.
    reserve_precharge_enforce_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_RESERVE_PRECHARGE_ENFORCE_ENABLED",
    )
    reserve_precharge_min_spread_ore: float = Field(
        default=15.0,
        alias="POWERBUDDY_RESERVE_PRECHARGE_MIN_SPREAD_ORE",
    )
    reserve_precharge_hours: int = Field(
        default=8,
        alias="POWERBUDDY_RESERVE_PRECHARGE_HOURS",
    )
    reserve_precharge_penalty_per_soc_ore: float = Field(
        default=350.0,
        alias="POWERBUDDY_RESERVE_PRECHARGE_PENALTY_PER_SOC_ORE",
    )

    # Aggressive cheap-slot capture to avoid missing low-price hours.
    cheap_slot_capture_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_CHEAP_SLOT_CAPTURE_ENABLED",
    )
    cheap_slot_quantile: float = Field(
        default=0.35,
        alias="POWERBUDDY_CHEAP_SLOT_QUANTILE",
    )
    cheap_slot_min_spread_ore: float = Field(
        default=18.0,
        alias="POWERBUDDY_CHEAP_SLOT_MIN_SPREAD_ORE",
    )
    cheap_slot_miss_penalty_ore: float = Field(
        default=500.0,
        alias="POWERBUDDY_CHEAP_SLOT_MISS_PENALTY_ORE",
    )
    cheap_slot_equal_price_tolerance_ore: float = Field(
        default=1.0,
        alias="POWERBUDDY_CHEAP_SLOT_EQUAL_PRICE_TOLERANCE_ORE",
    )
    price_order_swap_min_delta_ore: float = Field(
        default=0.25,
        alias="POWERBUDDY_PRICE_ORDER_SWAP_MIN_DELTA_ORE",
    )
    cheap_slot_min_target_soc_percent: float = Field(
        default=80.0,
        alias="POWERBUDDY_CHEAP_SLOT_MIN_TARGET_SOC_PERCENT",
    )
    reserve_hour_charge_penalty_ore: float = Field(
        default=900.0,
        alias="POWERBUDDY_RESERVE_HOUR_CHARGE_PENALTY_ORE",
    )

    # Ensure sufficient SOC before expensive windows so discharge can cover costly hours.
    expensive_window_precharge_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_EXPENSIVE_WINDOW_PRECHARGE_ENABLED",
    )
    expensive_window_coverage_ratio: float = Field(
        default=1.0,
        alias="POWERBUDDY_EXPENSIVE_WINDOW_COVERAGE_RATIO",
    )
    expensive_window_min_span_hours: int = Field(
        default=2,
        alias="POWERBUDDY_EXPENSIVE_WINDOW_MIN_SPAN_HOURS",
    )

    # Extend auto/discharge blocks into nearby expensive shoulder hours.
    auto_block_extension_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_AUTO_BLOCK_EXTENSION_ENABLED",
    )
    auto_block_extension_drop_limit_ore: float = Field(
        default=30.0,
        alias="POWERBUDDY_AUTO_BLOCK_EXTENSION_DROP_LIMIT_ORE",
    )
    auto_block_extension_floor_above_cheap_ore: float = Field(
        default=0.0,
        alias="POWERBUDDY_AUTO_BLOCK_EXTENSION_FLOOR_ABOVE_CHEAP_ORE",
    )

    kpi_tracking_enabled: bool = Field(default=True, alias="POWERBUDDY_KPI_TRACKING_ENABLED")
    auto_tuning_enabled: bool = Field(default=True, alias="POWERBUDDY_AUTO_TUNING_ENABLED")
    auto_tuning_step_max_ratio: float = Field(default=0.12, alias="POWERBUDDY_AUTO_TUNING_STEP_MAX_RATIO")

    # Planning sanity guardrail: validate/fix SOC readiness and expensive-hour behavior.
    planning_sanity_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_PLANNING_SANITY_ENABLED",
    )
    planning_sanity_autofix_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_PLANNING_SANITY_AUTOFIX_ENABLED",
    )
    planning_sanity_target_soc_percent: float = Field(
        default=100.0,
        alias="POWERBUDDY_PLANNING_SANITY_TARGET_SOC_PERCENT",
    )
    planning_sanity_expensive_quantile: float = Field(
        default=0.7,
        alias="POWERBUDDY_PLANNING_SANITY_EXPENSIVE_QUANTILE",
    )
    planning_sanity_expensive_window_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_PLANNING_SANITY_EXPENSIVE_WINDOW_ENABLED",
    )
    planning_sanity_expensive_window_start_hour_local: int = Field(
        default=17,
        alias="POWERBUDDY_PLANNING_SANITY_EXPENSIVE_WINDOW_START_HOUR_LOCAL",
    )
    planning_sanity_expensive_window_end_hour_local: int = Field(
        default=22,
        alias="POWERBUDDY_PLANNING_SANITY_EXPENSIVE_WINDOW_END_HOUR_LOCAL",
    )
    planning_sanity_min_expensive_auto_share: float = Field(
        default=0.6,
        alias="POWERBUDDY_PLANNING_SANITY_MIN_EXPENSIVE_AUTO_SHARE",
    )
    planning_sanity_charge_candidate_quantile: float = Field(
        default=0.4,
        alias="POWERBUDDY_PLANNING_SANITY_CHARGE_CANDIDATE_QUANTILE",
    )
    planning_sanity_max_added_charge_hours: int = Field(
        default=6,
        alias="POWERBUDDY_PLANNING_SANITY_MAX_ADDED_CHARGE_HOURS",
    )
    planning_sanity_pv_credit_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_PLANNING_SANITY_PV_CREDIT_ENABLED",
    )
    planning_sanity_pv_credit_capture_ratio: float = Field(
        default=0.7,
        alias="POWERBUDDY_PLANNING_SANITY_PV_CREDIT_CAPTURE_RATIO",
    )
    planning_sanity_pv_credit_max_soc_percent: float = Field(
        default=20.0,
        alias="POWERBUDDY_PLANNING_SANITY_PV_CREDIT_MAX_SOC_PERCENT",
    )
    planning_sanity_pv_credit_min_kwh: float = Field(
        default=0.5,
        alias="POWERBUDDY_PLANNING_SANITY_PV_CREDIT_MIN_KWH",
    )
    planning_sanity_precheap_arbitrage_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_PLANNING_SANITY_PRECHEAP_ARBITRAGE_ENABLED",
    )
    planning_sanity_precheap_auto_soc_buffer_percent: float = Field(
        default=3.0,
        alias="POWERBUDDY_PLANNING_SANITY_PRECHEAP_AUTO_SOC_BUFFER_PERCENT",
    )
    planning_sanity_precheap_auto_min_delta_ore: float = Field(
        default=2.0,
        alias="POWERBUDDY_PLANNING_SANITY_PRECHEAP_AUTO_MIN_DELTA_ORE",
    )
    planning_sanity_precheap_auto_start_hour_local: int = Field(
        default=5,
        alias="POWERBUDDY_PLANNING_SANITY_PRECHEAP_AUTO_START_HOUR_LOCAL",
    )
    planning_variant_search_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_PLANNING_VARIANT_SEARCH_ENABLED",
    )
    planning_variant_search_candidate_count: int = Field(
        default=9,
        alias="POWERBUDDY_PLANNING_VARIANT_SEARCH_CANDIDATE_COUNT",
    )
    planning_variant_search_max_precheap_auto_hours: int = Field(
        default=8,
        alias="POWERBUDDY_PLANNING_VARIANT_SEARCH_MAX_PRECHEAP_AUTO_HOURS",
    )
    planning_variant_search_max_cheap_charge_hours: int = Field(
        default=4,
        alias="POWERBUDDY_PLANNING_VARIANT_SEARCH_MAX_CHEAP_CHARGE_HOURS",
    )
    planning_variant_search_base_solar_weight: float = Field(
        default=0.35,
        alias="POWERBUDDY_PLANNING_VARIANT_SEARCH_BASE_SOLAR_WEIGHT",
    )
    planning_variant_search_low_solar_weight: float = Field(
        default=0.50,
        alias="POWERBUDDY_PLANNING_VARIANT_SEARCH_LOW_SOLAR_WEIGHT",
    )
    planning_variant_search_high_solar_weight: float = Field(
        default=0.15,
        alias="POWERBUDDY_PLANNING_VARIANT_SEARCH_HIGH_SOLAR_WEIGHT",
    )
    planning_quality_gate_enabled: bool = Field(
        default=True,
        alias="POWERBUDDY_PLANNING_QUALITY_GATE_ENABLED",
    )
    planning_quality_gate_minute_local: int = Field(
        default=5,
        alias="POWERBUDDY_PLANNING_QUALITY_GATE_MINUTE_LOCAL",
    )
    planning_quality_gate_retry_minute_local: int = Field(
        default=25,
        alias="POWERBUDDY_PLANNING_QUALITY_GATE_RETRY_MINUTE_LOCAL",
    )

    # ── Tariff / fees ──────────────────────────────────────────────────────────
    # DSO network tariff fetched from Energi Data Service (DatahubPricelist).
    tariff_network_owner: str = Field(
        default="Radius Elnet A/S",
        alias="POWERBUDDY_TARIFF_NETWORK_OWNER",
    )
    tariff_network_code: str = Field(
        default="DT_C_01",
        alias="POWERBUDDY_TARIFF_NETWORK_CODE",
    )
    # Energinet system tariff — flat per kWh, approximate 2026 value (øre, excl. VAT)
    tariff_energinet_ore: float = Field(
        default=6.0,
        alias="POWERBUDDY_TARIFF_ENERGINET_ORE",
    )
    # State electricity tax (elafgift) — flat per kWh, 2026 rate (øre, excl. VAT)
    tariff_elafgift_ore: float = Field(
        default=76.10,
        alias="POWERBUDDY_TARIFF_ELAFGIFT_ORE",
    )
    # VAT factor applied on top of all components for display purposes (not optimisation)
    tariff_vat_factor: float = Field(
        default=1.25,
        alias="POWERBUDDY_TARIFF_VAT_FACTOR",
    )

    # Strict data policy: do not synthesize tomorrow prices/plans before real day-ahead data exists.
    allow_provisional_prices: bool = Field(
        default=False,
        alias="POWERBUDDY_ALLOW_PROVISIONAL_PRICES",
    )
    allow_provisional_plans: bool = Field(
        default=False,
        alias="POWERBUDDY_ALLOW_PROVISIONAL_PLANS",
    )

    # Retail price-model calibration (excl. VAT):
    # - supplier markup added on top of spot ("without transport/afgifter" in UI)
    # - transport fixed component added with network tariff for "with transport/afgifter"
    price_supplier_markup_ore: float = Field(
        default=12.0,
        alias="POWERBUDDY_PRICE_SUPPLIER_MARKUP_ORE",
    )
    price_transport_fixed_ore: float = Field(
        default=12.0,
        alias="POWERBUDDY_PRICE_TRANSPORT_FIXED_ORE",
    )

    # CORS: comma-separated origins allowed to call API from browser.
    # Example: https://umbraco.example.dk,https://staging.example.dk
    cors_allowed_origins: str = Field(
        default="",
        alias="POWERBUDDY_CORS_ALLOWED_ORIGINS",
    )
    cors_allow_credentials: bool = Field(
        default=False,
        alias="POWERBUDDY_CORS_ALLOW_CREDENTIALS",
    )

    @staticmethod
    def _parse_kw_override(raw_value: str | float | int | None) -> float | None:
        if raw_value is None:
            return None
        text = str(raw_value).strip().lower()
        if text in {"", "auto", "none", "null"}:
            return None
        try:
            value = float(text)
        except Exception:
            return None
        if value <= 0.0:
            return None
        return value

    @staticmethod
    def _nearest_hvm_capacity_kwh(capacity_kwh: float) -> float:
        return min(
            BYD_HVM_CAPACITY_POWER_KW.keys(),
            key=lambda candidate: abs(candidate - float(capacity_kwh)),
        )

    @property
    def battery_capacity_kwh(self) -> float:
        if _detected_battery_capacity_kwh is not None:
            return float(self._nearest_hvm_capacity_kwh(_detected_battery_capacity_kwh))
        return float(DEFAULT_BATTERY_CAPACITY_KWH)

    @property
    def battery_capacity_source(self) -> str:
        return "detected" if _detected_battery_capacity_kwh is not None else "fallback-default"

    @property
    def battery_auto_power_limit_kw(self) -> float:
        capacity_key = self._nearest_hvm_capacity_kwh(self.battery_capacity_kwh)
        return float(BYD_HVM_CAPACITY_POWER_KW[capacity_key])

    @property
    def battery_min_soc(self) -> int:
        return int(FIXED_BATTERY_MIN_SOC)

    @property
    def battery_max_soc(self) -> int:
        return int(FIXED_BATTERY_MAX_SOC)

    @property
    def max_charge_kw(self) -> float:
        override = self._parse_kw_override(self.max_charge_kw_override)
        return float(override if override is not None else self.battery_auto_power_limit_kw)

    @property
    def max_discharge_kw(self) -> float:
        override = self._parse_kw_override(self.max_discharge_kw_override)
        return float(override if override is not None else self.battery_auto_power_limit_kw)

    @property
    def default_charge_power_w(self) -> int:
        return int(round(self.max_charge_kw * 1000.0))


settings = Settings()
