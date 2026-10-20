"""Helpers for editing the two half-hours of a plan hour independently."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from powerbuddy.models import PlanAction

HALF = timedelta(minutes=30)
HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class HalfRecord:
    action: str
    charge_power_w: float | None
    target_soc: float | None
    reason: str | None
    manual: bool


def _naive(ts: datetime) -> datetime:
    return ts.replace(tzinfo=None) if ts.tzinfo else ts


def hour_start(ts: datetime) -> datetime:
    return _naive(ts).replace(minute=0, second=0, microsecond=0)


def split_halves(rows: list[PlanAction], slot_start: datetime) -> list[HalfRecord | None]:
    """Effective record for [first half, second half]; manual rows win over planned ones."""
    result: list[HalfRecord | None] = []
    ordered = sorted(rows, key=lambda r: (0 if r.is_manual_override else 1, -(r.id or 0)))
    for half_start in (slot_start, slot_start + HALF):
        half_end = half_start + HALF
        found: HalfRecord | None = None
        for row in ordered:
            if _naive(row.start_time) < half_end and _naive(row.end_time) > half_start:
                found = HalfRecord(
                    action=row.action,
                    charge_power_w=row.charge_power_w,
                    target_soc=row.target_soc,
                    reason=row.reason,
                    manual=bool(row.is_manual_override),
                )
                break
        result.append(found)
    return result


def apply_edit(
    halves: list[HalfRecord | None], duration: str, new_record: HalfRecord
) -> list[HalfRecord | None]:
    first, second = halves
    if duration in {"full", "first_half"}:
        first = new_record
    if duration in {"full", "second_half"}:
        second = new_record
    return [first, second]


def build_rows(day_key: str, slot_start: datetime, halves: list[HalfRecord | None]) -> list[PlanAction]:
    """One full-hour row when both halves are identical, otherwise one row per covered half."""

    def make(rec: HalfRecord, start: datetime, end: datetime) -> PlanAction:
        return PlanAction(
            date_key=day_key,
            start_time=start,
            end_time=end,
            action=rec.action,
            charge_power_w=rec.charge_power_w,
            target_soc=rec.target_soc,
            reason=rec.reason,
            is_manual_override=rec.manual,
        )

    first, second = halves
    if first is not None and first == second:
        return [make(first, slot_start, slot_start + HOUR)]
    rows: list[PlanAction] = []
    if first is not None:
        rows.append(make(first, slot_start, slot_start + HALF))
    if second is not None:
        rows.append(make(second, slot_start + HALF, slot_start + HOUR))
    return rows


def planned_remainders(
    planned: list[PlanAction], manual: list[PlanAction]
) -> list[PlanAction]:
    """For hours where a manual row covers only one half, return planned rows for the other half."""
    result: list[PlanAction] = []
    for man in manual:
        start, end = _naive(man.start_time), _naive(man.end_time)
        if end - start != HALF:
            continue
        slot = hour_start(start)
        other_start = slot + HALF if start == slot else slot
        other_end = other_start + HALF
        if any(
            _naive(m.start_time) < other_end and _naive(m.end_time) > other_start for m in manual
        ):
            continue
        for plan in planned:
            if hour_start(plan.start_time) == slot:
                result.append(
                    PlanAction(
                        date_key=plan.date_key,
                        start_time=other_start,
                        end_time=other_end,
                        action=plan.action,
                        charge_power_w=plan.charge_power_w,
                        target_soc=plan.target_soc,
                        reason=plan.reason,
                        is_manual_override=False,
                    )
                )
                break
    return result


__all__ = ["HalfRecord", "split_halves", "apply_edit", "build_rows", "planned_remainders", "hour_start"]
