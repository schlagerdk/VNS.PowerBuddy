from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import delete, func, select

from powerbuddy.database import SessionLocal
from powerbuddy.models import PlanAction, PowerSnapshot, PricePoint, SimulationPoint


class PriceRepository:
    @staticmethod
    def upsert_prices(points: list[PricePoint]) -> None:
        with SessionLocal() as session:
            for point in points:
                existing = session.execute(
                    select(PricePoint).where(
                        PricePoint.timestamp == point.timestamp,
                        PricePoint.area == point.area,
                    )
                ).scalar_one_or_none()
                if existing:
                    existing.price_ore_per_kwh = point.price_ore_per_kwh
                    existing.currency = point.currency
                    existing.source = point.source
                else:
                    session.add(point)
            session.commit()

    @staticmethod
    def get_by_day(day: date, area: str) -> list[PricePoint]:
        day_start = datetime.combine(day, datetime.min.time())
        day_end = day_start + timedelta(days=1)
        with SessionLocal() as session:
            return list(
                session.execute(
                    select(PricePoint)
                    .where(
                        PricePoint.timestamp >= day_start,
                        PricePoint.timestamp < day_end,
                        PricePoint.area == area,
                    )
                    .order_by(PricePoint.timestamp.asc())
                ).scalars()
            )

    @staticmethod
    def get_by_time_window(start: datetime, end: datetime, area: str) -> list[PricePoint]:
        with SessionLocal() as session:
            return list(
                session.execute(
                    select(PricePoint)
                    .where(
                        PricePoint.timestamp >= start,
                        PricePoint.timestamp < end,
                        PricePoint.area == area,
                    )
                    .order_by(PricePoint.timestamp.asc())
                ).scalars()
            )

    @staticmethod
    def get_latest_day(area: str) -> date | None:
        with SessionLocal() as session:
            latest_ts = session.execute(
                select(func.max(PricePoint.timestamp)).where(PricePoint.area == area)
            ).scalar_one_or_none()
            if latest_ts is None:
                return None
            return latest_ts.date()


class PowerRepository:
    @staticmethod
    def add_snapshot(snapshot: PowerSnapshot) -> None:
        with SessionLocal() as session:
            session.add(snapshot)
            session.commit()

    @staticmethod
    def get_latest_battery_soc() -> float | None:
        with SessionLocal() as session:
            latest = session.execute(
                select(PowerSnapshot.battery_soc)
                .order_by(PowerSnapshot.timestamp.desc())
                .limit(1)
            ).scalar_one_or_none()
            return float(latest) if latest is not None else None

    @staticmethod
    def estimate_daily_consumption_kwh(day: date) -> tuple[float, int]:
        day_start = datetime.combine(day, datetime.min.time())
        day_end = day_start + timedelta(days=1)

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

        if not snapshots:
            return 0.0, 0

        total_kwh = 0.0
        for idx, current in enumerate(snapshots):
            if idx + 1 < len(snapshots):
                next_ts = snapshots[idx + 1].timestamp
                delta_hours = max(0.0, (next_ts - current.timestamp).total_seconds() / 3600.0)
            else:
                # Default to scheduler interval for final sample.
                delta_hours = 5.0 / 60.0

            # Cap large gaps to avoid overweighting sparse data.
            delta_hours = min(delta_hours, 0.25)
            total_kwh += (current.load_power_w / 1000.0) * delta_hours

        return round(total_kwh, 3), len(snapshots)

    @staticmethod
    def rolling_average_daily_consumption_kwh(
        reference_day: date,
        lookback_days: int,
        min_samples_per_day: int,
    ) -> float | None:
        values: list[float] = []
        for offset in range(1, max(lookback_days, 1) + 1):
            day = reference_day - timedelta(days=offset)
            kwh, samples = PowerRepository.estimate_daily_consumption_kwh(day)
            if samples >= min_samples_per_day and kwh > 0:
                values.append(kwh)

        if not values:
            return None

        return round(sum(values) / len(values), 3)


class PlanRepository:
    @staticmethod
    def get_action(action_id: int) -> PlanAction | None:
        with SessionLocal() as session:
            return session.get(PlanAction, action_id)

    @staticmethod
    def replace_plan(day_key: str, actions: list[PlanAction]) -> None:
        with SessionLocal() as session:
            session.execute(
                delete(PlanAction).where(
                    PlanAction.date_key == day_key,
                    PlanAction.is_manual_override.is_(False),
                )
            )
            for action in actions:
                session.add(action)
            session.commit()

    @staticmethod
    def add_manual_override(action: PlanAction) -> PlanAction:
        with SessionLocal() as session:
            session.add(action)
            session.commit()
            session.refresh(action)
            return action

    @staticmethod
    def get_plan(day_key: str) -> list[PlanAction]:
        with SessionLocal() as session:
            return list(
                session.execute(
                    select(PlanAction)
                    .where(PlanAction.date_key == day_key)
                    .order_by(PlanAction.start_time.asc())
                ).scalars()
            )

    @staticmethod
    def update_action(action_id: int, **kwargs) -> PlanAction | None:
        with SessionLocal() as session:
            action = session.get(PlanAction, action_id)
            if action is None:
                return None
            for key, value in kwargs.items():
                setattr(action, key, value)
            session.commit()
            session.refresh(action)
            return action

    @staticmethod
    def delete_action(action_id: int) -> bool:
        with SessionLocal() as session:
            action = session.get(PlanAction, action_id)
            if action is None:
                return False
            session.delete(action)
            session.commit()
            return True

    @staticmethod
    def replace_full_plan(day_key: str, actions: list[PlanAction]) -> None:
        with SessionLocal() as session:
            session.execute(delete(PlanAction).where(PlanAction.date_key == day_key))
            for action in actions:
                session.add(action)
            session.commit()


class SimulationRepository:
    @staticmethod
    def replace_points(day_key: str, points: list[SimulationPoint]) -> None:
        with SessionLocal() as session:
            session.execute(delete(SimulationPoint).where(SimulationPoint.date_key == day_key))
            for point in points:
                session.add(point)
            session.commit()

    @staticmethod
    def get_points(day_key: str) -> list[SimulationPoint]:
        with SessionLocal() as session:
            return list(
                session.execute(
                    select(SimulationPoint)
                    .where(SimulationPoint.date_key == day_key)
                    .order_by(SimulationPoint.timestamp.asc())
                ).scalars()
            )
