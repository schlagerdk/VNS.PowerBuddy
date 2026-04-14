from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from powerbuddy.database import Base


class PricePoint(Base):
    __tablename__ = "price_points"
    __table_args__ = (UniqueConstraint("timestamp", "area", name="uq_price_time_area"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    area: Mapped[str] = mapped_column(String(16), index=True)
    price_ore_per_kwh: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(8), default="DKK")
    source: Mapped[str] = mapped_column(String(64), default="energidataservice")


class PowerSnapshot(Base):
    __tablename__ = "power_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    grid_power_w: Mapped[float] = mapped_column(Float, default=0)
    load_power_w: Mapped[float] = mapped_column(Float, default=0)
    pv_power_w: Mapped[float] = mapped_column(Float, default=0)
    battery_power_w: Mapped[float] = mapped_column(Float, default=0)
    battery_soc: Mapped[float] = mapped_column(Float, default=0)


class PlanAction(Base):
    __tablename__ = "plan_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date_key: Mapped[str] = mapped_column(String(10), index=True)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    action: Mapped[str] = mapped_column(String(16), default="hold")
    charge_power_w: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_soc: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason: Mapped[str] = mapped_column(String(256), default="")
    is_manual_override: Mapped[bool] = mapped_column(Boolean, default=False)


class SimulationPoint(Base):
    __tablename__ = "simulation_points"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date_key: Mapped[str] = mapped_column(String(10), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    action: Mapped[str] = mapped_column(String(16), default="hold")
    projected_soc: Mapped[float] = mapped_column(Float, default=0)
    projected_grid_kwh: Mapped[float] = mapped_column(Float, default=0)


class PlannerKPI(Base):
    __tablename__ = "planner_kpis"
    __table_args__ = (UniqueConstraint("date_key", name="uq_planner_kpi_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date_key: Mapped[str] = mapped_column(String(10), index=True)
    planned_grid_kwh: Mapped[float] = mapped_column(Float, default=0)
    actual_grid_kwh: Mapped[float] = mapped_column(Float, default=0)
    planned_peak_import_kwh: Mapped[float] = mapped_column(Float, default=0)
    actual_peak_import_kwh: Mapped[float] = mapped_column(Float, default=0)
    plan_error_ratio: Mapped[float] = mapped_column(Float, default=0)
    soc_at_peak_start: Mapped[float] = mapped_column(Float, default=0)
    expected_daily_consumption_kwh: Mapped[float] = mapped_column(Float, default=0)
    realized_daily_consumption_kwh: Mapped[float] = mapped_column(Float, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
