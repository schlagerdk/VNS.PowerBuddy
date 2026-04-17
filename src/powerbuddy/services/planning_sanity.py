from __future__ import annotations

from datetime import date

from powerbuddy.config import settings
from powerbuddy.models import PlanAction, PricePoint
from powerbuddy.services.planner import DayPlanner


def _quantile(values: list[float], q: float) -> float:
	if not values:
		return 0.0
	qq = max(0.0, min(1.0, float(q)))
	sorted_values = sorted(values)
	idx = int(round((len(sorted_values) - 1) * qq))
	idx = max(0, min(len(sorted_values) - 1, idx))
	return float(sorted_values[idx])


def _is_hour_in_window(hour: int, start: int, end: int) -> bool:
	if start < end:
		return start <= hour < end
	return hour >= start or hour < end


def _simulate_soc_by_index(
	planner: DayPlanner,
	day: date,
	actions: list[PlanAction],
	start_soc: float,
	pv_weather_factor_24h: list[float] | None,
) -> list[float]:
	if not actions:
		return []
	simulation = planner.simulate(
		day,
		actions,
		start_soc=start_soc,
		pv_weather_factor_24h=pv_weather_factor_24h,
	)
	return [float(point.projected_soc) for point in simulation]


def apply_planning_sanity(
	planner: DayPlanner,
	day: date,
	actions: list[PlanAction],
	prices: list[PricePoint],
	start_soc: float,
	tariff_ore_per_hour: list[float] | None = None,
	pv_weather_factor_24h: list[float] | None = None,
	auto_fix: bool = True,
) -> tuple[list[PlanAction], dict[str, object]]:
	report: dict[str, object] = {
		"day": day.isoformat(),
		"enabled": bool(settings.planning_sanity_enabled),
		"auto_fix_applied": False,
		"changes": 0,
		"findings": [],
		"ok": True,
	}

	if not settings.planning_sanity_enabled:
		return actions, report

	if not actions or not prices:
		report["findings"] = ["No actions or prices available for sanity check"]
		report["ok"] = False
		return actions, report

	price_by_hour = {int(point.timestamp.hour): float(point.price_ore_per_kwh) for point in prices}
	total_levels: list[float] = []
	valid_indices: list[int] = []
	for idx, action in enumerate(actions):
		hour = int(action.start_time.hour)
		if hour not in price_by_hour:
			continue
		tariff = float(tariff_ore_per_hour[hour]) if tariff_ore_per_hour and hour < len(tariff_ore_per_hour) else 0.0
		total_levels.append(price_by_hour[hour] + tariff)
		valid_indices.append(idx)

	if not total_levels:
		report["findings"] = ["No aligned hourly prices found for actions"]
		report["ok"] = False
		return actions, report

	expensive_threshold = _quantile(total_levels, settings.planning_sanity_expensive_quantile)

	window_enabled = bool(settings.planning_sanity_expensive_window_enabled)
	window_start = max(0, min(23, int(settings.planning_sanity_expensive_window_start_hour_local)))
	window_end = max(1, min(24, int(settings.planning_sanity_expensive_window_end_hour_local)))

	window_indices = [
		idx for idx in valid_indices
		if _is_hour_in_window(int(actions[idx].start_time.hour), window_start, window_end)
	]

	quantile_indices = [
		idx for idx in valid_indices
		if (
			(price_by_hour.get(int(actions[idx].start_time.hour), 0.0)
			 + (float(tariff_ore_per_hour[int(actions[idx].start_time.hour)]) if tariff_ore_per_hour else 0.0))
			>= expensive_threshold
		)
	]

	if window_enabled and window_indices:
		expensive_indices = window_indices
		expensive_source = "fixed-window"
	else:
		expensive_indices = quantile_indices
		expensive_source = "quantile"

	if not expensive_indices:
		report["findings"] = ["No expensive-hour window found"]
		report["ok"] = True
		return actions, report

	first_expensive_idx = min(expensive_indices)
	sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)
	target_soc_base = max(
		float(settings.reserve_soc_min_percent),
		float(settings.must_charge_window_min_soc_percent),
		float(settings.planning_sanity_target_soc_percent),
	)
	target_soc_base = max(float(settings.battery_min_soc), min(float(settings.battery_max_soc), target_soc_base))

	# Solar-aware target: reduce required pre-expensive SOC when historical+weather
	# indicate likely PV surplus before expensive hours.
	pv_credit_soc = 0.0
	pv_surplus_kwh = 0.0
	if bool(settings.planning_sanity_pv_credit_enabled) and first_expensive_idx > 0:
		expected_daily_consumption_kwh, _ = planner.resolve_expected_daily_consumption(day)
		hourly_profile, _ = planner.resolve_hourly_consumption_profile(day)
		pv_profile_kwh, _ = planner.resolve_hourly_pv_profile(day)
		for idx in range(first_expensive_idx):
			hour = int(actions[idx].start_time.hour)
			if hour < 0 or hour > 23:
				continue
			weather_factor = 1.0
			if pv_weather_factor_24h is not None and len(pv_weather_factor_24h) == 24:
				weather_factor = max(0.0, float(pv_weather_factor_24h[hour]))
			pv_kwh = max(0.0, float(pv_profile_kwh[hour]) * weather_factor)
			gross_kwh = max(0.0, float(expected_daily_consumption_kwh) * float(hourly_profile[hour]))
			pv_surplus_kwh += max(0.0, pv_kwh - gross_kwh)

		if pv_surplus_kwh >= max(0.0, float(settings.planning_sanity_pv_credit_min_kwh)):
			capture_ratio = max(0.0, min(1.0, float(settings.planning_sanity_pv_credit_capture_ratio)))
			pv_credit_soc = (
				pv_surplus_kwh
				* capture_ratio
				* float(settings.charge_efficiency)
				/ max(float(settings.battery_capacity_kwh), 1e-6)
			) * 100.0
			pv_credit_soc = min(
				pv_credit_soc,
				max(0.0, float(settings.planning_sanity_pv_credit_max_soc_percent)),
			)

	target_soc = max(
		float(settings.reserve_soc_min_percent),
		target_soc_base - pv_credit_soc,
	)
	target_soc = max(float(settings.battery_min_soc), min(float(settings.battery_max_soc), target_soc))

	def _soc_before(index: int, current_start_soc: float, soc_series: list[float]) -> float:
		if index <= 0:
			return float(current_start_soc)
		if index - 1 < len(soc_series):
			return float(soc_series[index - 1])
		return float(soc_series[-1]) if soc_series else float(current_start_soc)

	soc_before_expensive = _soc_before(first_expensive_idx, start_soc, sim_soc)
	reserve_soc = float(settings.reserve_soc_min_percent)
	precheap_cheap_cutoff: float | None = None
	precheap_forced_charge_indices: list[int] = []
	precheap_forced_auto_indices: list[int] = []

	findings: list[str] = []
	if soc_before_expensive + 1e-6 < target_soc:
		findings.append(
			f"SOC before expensive window is too low ({soc_before_expensive:.1f}% < target {target_soc:.1f}%)"
		)

	expensive_auto_count = sum(1 for idx in expensive_indices if actions[idx].action == "auto")
	expensive_share = (expensive_auto_count / max(1, len(expensive_indices)))
	min_auto_share = max(0.0, min(1.0, float(settings.planning_sanity_min_expensive_auto_share)))
	if expensive_share + 1e-6 < min_auto_share:
		findings.append(
			f"Auto coverage in expensive window is low ({expensive_share:.2f} < target {min_auto_share:.2f})"
		)

	changes = 0
	if auto_fix and bool(settings.planning_sanity_precheap_arbitrage_enabled) and first_expensive_idx > 0:
		max_added_charge_hours = max(1, int(settings.planning_sanity_max_added_charge_hours))
		precheap_auto_start = max(0, min(23, int(settings.planning_sanity_precheap_auto_start_hour_local)))
		pre_exp_indices = [
			idx
			for idx in valid_indices
			if idx < first_expensive_idx
			and not actions[idx].is_manual_override
			and (not planner._is_reserve_hour(actions[idx].start_time))
		]

		if pre_exp_indices:
			def _slot_total(idx: int) -> float:
				hour = int(actions[idx].start_time.hour)
				return price_by_hour.get(hour, 0.0) + (float(tariff_ore_per_hour[hour]) if tariff_ore_per_hour else 0.0)

			pre_costs = [_slot_total(idx) for idx in pre_exp_indices]
			precheap_cheap_cutoff = _quantile(pre_costs, settings.planning_sanity_charge_candidate_quantile)

			cheap_indices = [
				idx for idx in pre_exp_indices
				if _slot_total(idx) <= (precheap_cheap_cutoff + 1e-6)
			]
			cheap_indices = sorted(cheap_indices, key=lambda idx: (int(actions[idx].start_time.hour), idx))

			if cheap_indices:
				first_cheap_idx = min(cheap_indices)
				min_delta = max(0.0, float(settings.planning_sanity_precheap_auto_min_delta_ore))
				soc_buffer = max(0.0, float(settings.planning_sanity_precheap_auto_soc_buffer_percent))
				sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)

				for idx in pre_exp_indices:
					if idx >= first_cheap_idx:
						continue
					if actions[idx].action != "hold":
						continue
					hour = int(actions[idx].start_time.hour)
					if hour < precheap_auto_start:
						continue
					if precheap_cheap_cutoff is None:
						continue
					if _slot_total(idx) < (precheap_cheap_cutoff + min_delta):
						continue

					soc_before_here = _soc_before(idx, start_soc, sim_soc)
					if soc_before_here <= (reserve_soc + soc_buffer):
						continue

					actions[idx].action = "auto"
					actions[idx].charge_power_w = None
					actions[idx].reason = "sanity autofix: pre-cheap arbitrage auto"
					precheap_forced_auto_indices.append(idx)
					changes += 1
					sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)

				# Re-fill only as needed in cheap slots so we hit target SOC before expensive window.
				cheap_by_price = sorted(cheap_indices, key=lambda idx: _slot_total(idx))
				added_charge = 0
				for idx in cheap_by_price:
					if added_charge >= max_added_charge_hours:
						break
					if _soc_before(first_expensive_idx, start_soc, sim_soc) + 1e-6 >= target_soc:
						break
					if actions[idx].action == "charge":
						continue
					actions[idx].action = "charge"
					actions[idx].charge_power_w = planner.charge_setpoint_w
					actions[idx].reason = "sanity autofix: force charge in cheap window"
					precheap_forced_charge_indices.append(idx)
					added_charge += 1
					changes += 1
					sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)

	if auto_fix and findings:
		# Priority 1: ensure enough SOC before expensive window by charging cheapest pre-expensive hours.
		if soc_before_expensive + 1e-6 < target_soc:
			max_added_charge_hours = max(1, int(settings.planning_sanity_max_added_charge_hours))
			pre_candidates = [
				idx
				for idx in valid_indices
				if idx < first_expensive_idx
				and actions[idx].action != "charge"
				and not actions[idx].is_manual_override
				and (not planner._is_reserve_hour(actions[idx].start_time))
			]
			pre_candidates = sorted(pre_candidates, key=lambda i: price_by_hour.get(int(actions[i].start_time.hour), 0.0))

			if pre_candidates:
				pre_candidate_costs = [price_by_hour.get(int(actions[i].start_time.hour), 0.0) for i in pre_candidates]
				cheap_cutoff = _quantile(pre_candidate_costs, settings.planning_sanity_charge_candidate_quantile)
				filtered = [
					i for i in pre_candidates
					if price_by_hour.get(int(actions[i].start_time.hour), 0.0) <= (cheap_cutoff + 1e-6)
				]
				if filtered:
					pre_candidates = filtered

			for idx in pre_candidates:
				if changes >= max_added_charge_hours:
					break
				actions[idx].action = "charge"
				actions[idx].charge_power_w = planner.charge_setpoint_w
				actions[idx].reason = "sanity autofix: ensure target SOC before expensive window"
				changes += 1
				sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)
				soc_before_expensive = _soc_before(first_expensive_idx, start_soc, sim_soc)
				if soc_before_expensive + 1e-6 >= target_soc:
					break

		# Priority 2: improve expensive-window auto coverage when battery is comfortably above reserve.
		sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)
		for idx in expensive_indices:
			if actions[idx].action != "hold" or actions[idx].is_manual_override:
				continue
			soc_before_here = _soc_before(idx, start_soc, sim_soc)
			if soc_before_here <= (reserve_soc + 2.0):
				continue
			actions[idx].action = "auto"
			actions[idx].charge_power_w = None
			actions[idx].reason = "sanity autofix: use auto in expensive window"
			changes += 1
			sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)

	# Refresh charge target SOC hints from simulation after all changes.
	sim_soc = _simulate_soc_by_index(planner, day, actions, start_soc, pv_weather_factor_24h)
	for idx, action in enumerate(actions):
		if idx >= len(sim_soc):
			continue
		if action.action == "charge":
			action.target_soc = round(float(sim_soc[idx]), 1)
		elif action.action == "auto":
			action.target_soc = round(float(sim_soc[idx]), 1)

	soc_before_expensive_final = _soc_before(first_expensive_idx, start_soc, sim_soc)
	expensive_auto_count_final = sum(1 for idx in expensive_indices if actions[idx].action == "auto")
	expensive_share_final = (expensive_auto_count_final / max(1, len(expensive_indices)))

	report["first_expensive_hour_index"] = first_expensive_idx
	report["expensive_hours_count"] = len(expensive_indices)
	report["expensive_source"] = expensive_source
	report["expensive_window"] = [window_start, window_end]
	report["precheap_cheap_cutoff_ore"] = round(precheap_cheap_cutoff, 3) if precheap_cheap_cutoff is not None else None
	report["precheap_forced_charge_hours"] = [int(actions[idx].start_time.hour) for idx in precheap_forced_charge_indices]
	report["precheap_forced_auto_hours"] = [int(actions[idx].start_time.hour) for idx in precheap_forced_auto_indices]
	report["target_soc_base_percent"] = round(target_soc_base, 2)
	report["pv_surplus_kwh_before_expensive"] = round(pv_surplus_kwh, 3)
	report["pv_credit_soc_percent"] = round(pv_credit_soc, 2)
	report["target_soc_percent"] = round(target_soc, 2)
	report["soc_before_expensive_percent"] = round(soc_before_expensive_final, 2)
	report["expensive_auto_share"] = round(expensive_share_final, 3)
	report["changes"] = int(changes)
	report["auto_fix_applied"] = bool(changes > 0)
	report["findings"] = findings

	ok = (soc_before_expensive_final + 1e-6 >= target_soc) and (expensive_share_final + 1e-6 >= min_auto_share)
	report["ok"] = bool(ok)
	return actions, report
