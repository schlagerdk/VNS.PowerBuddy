from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class PriceOut(BaseModel):
    timestamp: datetime
    area: str
    price_ore_per_kwh: float
    currency: str


class InverterRealtime(BaseModel):
    timestamp: datetime
    grid_power_w: float
    load_power_w: float
    pv_power_w: float
    battery_power_w: float
    battery_soc: float


class PlanActionOut(BaseModel):
    id: int
    date_key: str
    start_time: datetime
    end_time: datetime
    action: str
    charge_power_w: float | None
    target_soc: float | None
    reason: str
    is_manual_override: bool


class PlanNowStatusOut(BaseModel):
    timestamp: datetime
    execution_mode: str
    planned_action: str
    planned_start_time: datetime | None
    planned_end_time: datetime | None
    battery_power_w: float
    is_battery_charging: bool
    is_battery_discharging: bool
    matches_plan: bool


class ManualOverrideIn(BaseModel):
    date: date
    start_time: datetime
    end_time: datetime
    action: Literal["charge", "discharge", "hold"]
    charge_power_w: float | None = Field(default=None, ge=0)
    target_soc: float | None = None
    reason: str = "manual override"


class SimulationPointOut(BaseModel):
    timestamp: datetime
    action: str
    projected_soc: float
    projected_grid_kwh: float


class TariffHourOut(BaseModel):
    hour: int
    network_tariff_ore: float
    total_tariff_ore_excl_vat: float
    total_tariff_ore_incl_vat: float


class TariffOut(BaseModel):
    network_owner: str
    network_code: str
    energinet_ore_flat: float
    elafgift_ore_flat: float
    vat_factor: float
    hours: list[TariffHourOut]


class TariffConfigUpdateIn(BaseModel):
    network_owner: str | None = None
    network_code: str | None = None
    energinet_ore_flat: float | None = Field(default=None, ge=0)
    elafgift_ore_flat: float | None = Field(default=None, ge=0)
    vat_factor: float | None = Field(default=None, ge=1.0)


class TariffManualHoursIn(BaseModel):
    network_tariff_ore_per_hour: list[float] = Field(min_length=24, max_length=24)


class PlanActionUpdateIn(BaseModel):
    action: Literal["charge", "discharge", "hold"] | None = None
    charge_power_w: float | None = Field(default=None, ge=0)
    target_soc: float | None = None
    reason: str | None = None
    is_manual_override: bool | None = None


class PlanActionIn(BaseModel):
    start_time: datetime
    end_time: datetime
    action: Literal["charge", "discharge", "hold"]
    charge_power_w: float | None = Field(default=None, ge=0)
    target_soc: float | None = None
    reason: str = "manual plan"
    is_manual_override: bool = True


class PlanReplaceIn(BaseModel):
    date: date
    actions: list[PlanActionIn]


class PlanningChartOut(BaseModel):
    requested_date: date
    used_date: date
    area: str
    used_fallback: bool
    expected_daily_consumption_kwh: float
    consumption_source: str
    labels: list[str]
    prices_ore_per_kwh: list[float | None]
    # Tariff breakdown — non-spot components (excl. VAT) aligned with labels
    network_tariff_ore_per_hour: list[float]
    total_cost_ore_incl_vat: list[float | None]
    actions: list[str]
    target_soc: list[float | None]
    projected_soc: list[float | None]
    projected_grid_kwh: list[float | None]
    action_hours: dict[str, int]
    cheapest_hours: list[str]
    most_expensive_hours: list[str]


class PlannerKPIOut(BaseModel):
    date_key: str
    planned_grid_kwh: float
    actual_grid_kwh: float
    planned_peak_import_kwh: float
    actual_peak_import_kwh: float
    plan_error_ratio: float
    soc_at_peak_start: float
    expected_daily_consumption_kwh: float
    realized_daily_consumption_kwh: float
    updated_at: datetime
