from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
import hmac
import ipaddress
import logging
import re
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal, TypedDict, cast
from urllib.parse import parse_qs
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from powerbuddy.config import set_detected_battery_capacity_kwh, settings
from powerbuddy.dashboard_ui import render_dashboard_html
from powerbuddy.database import init_db
from powerbuddy.models import PlanAction, PricePoint
from powerbuddy.repositories import AppSettingsRepository, KPIRepository, PlanRepository, PowerRepository, PriceRepository, SimulationRepository
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
from powerbuddy.services.easee import get_easee_status
from powerbuddy.services.scheduler import PowerBuddyScheduler
from powerbuddy.services.tariff import tariff_service
from powerbuddy.services.weather import weather_forecast_service


logger = logging.getLogger(__name__)


def _is_redundant_httpx_error_log(line: str) -> bool:
    return bool(re.search(r"\sINFO httpx: HTTP Request: .*\"HTTP/\d\.\d [45]\d\d", line))


def resolve_log_file_path(base_dir: Path | str | None = None) -> Path:
    if base_dir is None:
        candidates = [Path("/var/log"), Path.cwd() / "data"]
    elif isinstance(base_dir, Path):
        candidates = [base_dir]
    else:
        candidates = [Path(base_dir)]

    for candidate in candidates:
        if candidate.name == "powerbuddy.log":
            return candidate

        log_path = candidate / "powerbuddy.log"
        if candidate.exists():
            try:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.touch(exist_ok=True)
                return log_path
            except OSError:
                continue

    fallback = Path.cwd() / "data" / "powerbuddy.log"
    fallback.parent.mkdir(parents=True, exist_ok=True)
    return fallback


def configure_logging() -> Path:
    root_logger = logging.getLogger()
    log_path = resolve_log_file_path()

    has_file_handler = any(isinstance(handler, (logging.FileHandler, RotatingFileHandler)) for handler in root_logger.handlers)
    if not has_file_handler:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        file_handler = RotatingFileHandler(log_path, maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)

    if not any(isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler) for handler in root_logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        stream_handler.setLevel(logging.INFO)
        root_logger.addHandler(stream_handler)

    root_logger.setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return log_path


configure_logging()

scheduler = PowerBuddyScheduler()
_battery_capacity_discovery_lock = asyncio.Lock()
_battery_capacity_last_discovery_monotonic = 0.0
_battery_capacity_discovery_interval_seconds = 1800.0


class DashboardCookieOptions(TypedDict):
    httponly: bool
    samesite: Literal["lax", "strict", "none"]
    secure: bool
    max_age: int
    path: str


def _request_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        first = forwarded.split(",")[0].strip()
        if first.startswith("::ffff:"):
            first = first[7:]
        return first
    client_host = request.client.host if request.client else ""
    if client_host.startswith("::ffff:"):
        client_host = client_host[7:]
    return client_host


def _request_ip_candidates(request: Request) -> list[str]:
    candidates: list[str] = []

    forwarded = request.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        for part in forwarded.split(","):
            ip_text = part.strip()
            if ip_text.startswith("::ffff:"):
                ip_text = ip_text[7:]
            if ip_text:
                candidates.append(ip_text)

    for header_name in ("x-real-ip", "x-client-ip"):
        value = (request.headers.get(header_name) or "").strip()
        if value.startswith("::ffff:"):
            value = value[7:]
        if value:
            candidates.append(value)

    direct_ip = _request_ip(request)
    if direct_ip:
        candidates.append(direct_ip)

    unique: list[str] = []
    for ip_text in candidates:
        if ip_text not in unique:
            unique.append(ip_text)
    return unique


def _dashboard_signing_secret() -> bytes:
    secret = (settings.dashboard_secret or "").strip()
    return secret.encode("utf-8")


def _dashboard_is_trusted_ip(ip: str) -> bool:
    if not ip:
        return False
    trusted = settings.dashboard_trusted_ip_list
    return ip in trusted


def _dashboard_is_password_valid(password: str) -> bool:
    if not password:
        return False
    for expected in settings.dashboard_password_list:
        if hmac.compare_digest(password, expected):
            return True
    return False


def _dashboard_encode_token(ip: str) -> str:
    now_epoch = int(time.time())
    payload = f"{now_epoch}:{ip}"
    secret = _dashboard_signing_secret()
    signature = hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
    raw = f"{payload}|{signature}".encode("utf-8")
    token = base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")
    return token


def _dashboard_decode_token(token: str, ip: str) -> bool:
    if not token:
        return False

    secret = _dashboard_signing_secret()
    if not secret:
        return False

    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        payload, signature = decoded.rsplit("|", 1)
        issued_text, token_ip = payload.split(":", 1)
        expected_sig = hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            return False

        if token_ip != ip:
            return False

        issued = int(issued_text)
        age = int(time.time()) - issued
        if age < 0:
            return False
        if age > max(60, int(settings.dashboard_session_ttl_seconds)):
            return False
        return True
    except Exception:
        return False


def _dashboard_cookie_options(request: Request) -> DashboardCookieOptions:
    max_age = max(60, int(settings.dashboard_session_ttl_seconds))
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").lower()
    secure = request.url.scheme == "https" or "https" in forwarded_proto
    return {
        "httponly": True,
        "samesite": "lax",
        "secure": secure,
        "max_age": max_age,
        "path": "/",
    }


def _dashboard_resolve_icon_url(raw_url: str, fallback_url: str) -> str:
    url = (raw_url or "").strip()
    legacy_icon_urls = {
        "/powerbuddy/favicon-32x32.png",
        "/powerbuddy/favicon.ico",
        "/powerbuddy/apple-touch-icon.png",
    }
    if url and url not in legacy_icon_urls and not url.startswith("/media/"):
        return url
    return fallback_url


def _dashboard_apply_runtime_state(html: str) -> str:
    is_paused = not scheduler.is_execution_enabled()

    if is_paused:
        html = html.replace('data-is-paused="0"', 'data-is-paused="1"')
        html = re.sub(
            r'(id="priceChart"\s+class="[^"]*?)(?<!\bis-paused)(")',
            r'\1 is-paused\2',
            html,
            count=1,
        )
    else:
        html = html.replace('data-is-paused="1"', 'data-is-paused="0"')
        html = re.sub(r'\s+is-paused(?=[^"]*"\s+id="priceChart")', '', html, count=1)

    return html


def _dashboard_apply_auth_state(html: str, *, authorized: bool, request: Request) -> str:
    battery_link = _dashboard_resolve_battery_link(authorized=authorized, request=request)
    return re.sub(
        r'(<div[^>]*id="pbBatteryStat"[^>]*\sdata-battery-link=")[^"]*(")',
        rf'\1{battery_link}\2',
        html,
        count=1,
    )


def _dashboard_is_same_network(ip: str, host: str) -> bool:
    ip_text = (ip or "").strip()
    host_text = (host or "").strip()
    if not ip_text or not host_text:
        return False

    try:
        client_ip = ipaddress.ip_address(ip_text)
        target_ip = ipaddress.ip_address(host_text)
    except ValueError:
        return False

    if client_ip.version != target_ip.version:
        return False

    if client_ip == target_ip or client_ip.is_loopback:
        return True

    if client_ip.version == 4 and client_ip.is_private and target_ip.is_private:
        network = ipaddress.ip_network(f"{target_ip}/24", strict=False)
        return client_ip in network

    return False


def _dashboard_resolve_battery_link(*, authorized: bool, request: Request | None = None) -> str:
    if not authorized:
        return ""

    host = (settings.modbus_host or "").strip()
    if not host:
        return "https://solarweb.com"

    ip_candidates = _request_ip_candidates(request) if request is not None else []
    if any(_dashboard_is_same_network(ip, host) for ip in ip_candidates):
        return f"https://{host}"
    return "https://solarweb.com"


def _dashboard_apply_mode_state(html: str, pb_mode: str) -> str:
    mode = (pb_mode or "").strip().lower()
    is_overview = mode == "overview"

    html = re.sub(
        r'(id="priceChart"[^>]*\sdata-overview-mode=")[^"]*(")',
        rf'\g<1>{"1" if is_overview else "0"}\g<2>',
        html,
        count=1,
    )

    def _normalize_chart_classes(classes_text: str) -> str:
        classes = [c for c in (classes_text or "").split() if c and c != "overview-mode"]
        if is_overview:
            classes.append("overview-mode")
        return " ".join(classes)

    def _patch_price_chart_class_id_first(match: re.Match[str]) -> str:
        classes_text = match.group(1) or ""
        classes = _normalize_chart_classes(classes_text)
        return f'id="priceChart" class="{classes}"'

    def _patch_price_chart_class_class_first(match: re.Match[str]) -> str:
        classes = [c for c in (match.group(1) or "").split() if c and c != "overview-mode"]
        if is_overview:
            classes.append("overview-mode")
        return f'class="{" ".join(classes)}" id="priceChart"'

    html = re.sub(
        r'id="priceChart"\s+class="([^"]*)"',
        _patch_price_chart_class_id_first,
        html,
        count=1,
    )
    html = re.sub(
        r'class="([^"]*)"\s+id="priceChart"',
        _patch_price_chart_class_class_first,
        html,
        count=1,
    )

    if is_overview:
        # ORG behavior: overview mode does not only hide header/hero with CSS,
        # it removes that markup from the rendered HTML before the chart block.
        html = re.sub(
            r'(<div class="page">\s*)(<div class="header">[\s\S]*?)(?=<div class="chart-container)',
            r'\1',
            html,
            count=1,
        )
        html = re.sub(r'(<body\b[^>]*class=")([^"]*)(")', r'\1\2 overview-mode\3', html, count=1)
    else:
        html = re.sub(r'(<body\b[^>]*class=")([^"]*)\boverview-mode\b([^\"]*)(")', r'\1\2\3\4', html, count=1)
    html = re.sub(r'(<body\b[^>]*class=")\s+', r'\1', html, count=1)
    html = re.sub(r'\s{2,}', ' ', html)
    return html


def _dashboard_slot_key(dt: datetime) -> str:
    return _naive_ts(dt).strftime("%Y-%m-%dT%H.%M.%S")


def _dashboard_action_icon_svg(action_name: str) -> str:
    action = (action_name or "").lower()
    if action == "charge":
        return '<svg viewBox="0 0 16 16"><path d="M8 13V3"></path><path d="M4.5 6.5L8 3l3.5 3.5"></path></svg>'
    if action == "discharge":
        return '<svg viewBox="0 0 16 16"><path d="M8 3v10"></path><path d="M4.5 9.5L8 13l3.5-3.5"></path></svg>'
    if action == "hold":
        return '<svg viewBox="0 0 16 16"><path d="M6 3v10"></path><path d="M10 3v10"></path></svg>'
    return '<svg viewBox="0 0 16 16"><path d="M12.8 5.2A5 5 0 0 0 4.2 3.8"></path><path d="M4.1 1.8V4h2.2"></path><path d="M3.2 10.8A5 5 0 0 0 11.8 12.2"></path><path d="M11.9 14.2V12h-2.2"></path></svg>'


def _dashboard_action_label(action_name: str) -> str:
    action = (action_name or "").lower()
    if action == "charge":
        return "Charge"
    if action == "discharge":
        return "Discharge"
    if action == "hold":
        return "Hold"
    return "Auto"


def _dashboard_apply_plan_state(
    html: str,
    actions: list[PlanAction | PlanActionOut],
    prices: list[PriceOut],
) -> str:
    action_map = {
        _dashboard_slot_key(action.start_time): (str(action.action), str(action.id))
        for action in actions
    }
    now_local = datetime.now(ZoneInfo(settings.timezone)).replace(tzinfo=None)
    current_hour = now_local.replace(minute=0, second=0, microsecond=0)
    tomorrow_start = current_hour.replace(hour=0) + timedelta(days=1)
    window_end = tomorrow_start + timedelta(days=1)
    local_tz = ZoneInfo(settings.timezone)
    current_action_name = "auto"
    current_slot_key = now_local.strftime("%Y-%m-%dT%H.00.00")
    for action in actions:
        start = _naive_ts(action.start_time)
        end = _naive_ts(action.end_time)
        if start <= now_local < end:
            current_action_name = str(action.action or "auto").lower()
            current_slot_key = _dashboard_slot_key(action.start_time)
            break

    current_category = "medium"
    current_price_kr: float | None = None

    normalized_prices: dict[datetime, float] = {}
    source_by_slot: dict[datetime, str] = {}
    for point in prices:
        ts = point.timestamp
        if ts.tzinfo is not None:
            ts_local = ts.astimezone(local_tz).replace(tzinfo=None)
        else:
            ts_local = ts
        slot_local = ts_local.replace(minute=0, second=0, microsecond=0)
        if slot_local < current_hour or slot_local >= window_end:
            continue

        raw_price_ore = point.price_without_fees_ore_per_kwh
        if raw_price_ore is None:
            raw_price_ore = point.price_ore_per_kwh
        normalized_prices[slot_local] = float(raw_price_ore)
        source_by_slot[slot_local] = (point.source or "").strip()

    slot_meta_by_key: dict[str, tuple[str, float]] = {}
    sorted_slots = sorted(normalized_prices.keys())
    if sorted_slots:
        values = [normalized_prices[slot] for slot in sorted_slots]
        min_value = min(values)
        max_value = max(values)
        value_range = max(0.0, max_value - min_value)
        threshold1 = min_value + (value_range * 0.25)
        threshold2 = min_value + (value_range * 0.50)
        threshold3 = min_value + (value_range * 0.75)

        def _price_category(value: float, source_name: str = "") -> str:
            if "dummy" in source_name.lower():
                return "dummy"
            if value <= threshold1:
                return "low"
            if value <= threshold2:
                return "medium"
            if value <= threshold3:
                return "high"
            return "peak"

        non_dummy_slots = [slot for slot in sorted_slots if "dummy" not in source_by_slot.get(slot, "").lower()]
        if non_dummy_slots:
            cheapest_slot = min(non_dummy_slots, key=lambda slot: normalized_prices[slot])
            priciest_slot = max(non_dummy_slots, key=lambda slot: normalized_prices[slot])
        else:
            cheapest_slot = None
            priciest_slot = None
        bars: list[str] = []
        previous_day: date | None = None
        for idx, slot in enumerate(sorted_slots):
            price_ore = normalized_prices[slot]
            source_name = source_by_slot.get(slot, "")
            is_dummy = "dummy" in source_name.lower()
            category = _price_category(price_ore, source_name)
            slot_key = slot.strftime("%Y-%m-%dT%H.%M.%S")
            action_name, action_id = action_map.get(slot_key, ("", ""))
            action_label = _dashboard_action_label(action_name)
            action_icon = _dashboard_action_icon_svg(action_name)
            price_kr = price_ore / 100.0
            price_text = f"{price_kr:.2f}".replace(".", ",")
            hour_text = slot.strftime("%H")
            hour_label = slot.strftime("%H.%M")
            day_is_today = slot < tomorrow_start
            day_class = "today" if day_is_today else "tomorrow"

            is_day_boundary = previous_day is not None and slot.date() != previous_day
            boundary_class = "day-boundary" if is_day_boundary else ""
            boundary_badge = ""
            if is_day_boundary:
                if slot >= tomorrow_start:
                    boundary_text = "I MORGEN"
                else:
                    boundary_text = slot.strftime("%d/%m")
                boundary_badge = f'\n\t\t\t\t\t\t\t\t\t\t<span class="day-boundary-badge">{boundary_text}</span>'

            if is_dummy:
                height_percent = 50.0
            elif value_range <= 0.0:
                height_percent = 55.0
            else:
                height_percent = 10.0 + (((price_ore - min_value) / value_range) * 90.0)

            flag_html = ""
            if not is_dummy and slot == cheapest_slot:
                flag_html += '\n\t\t\t\t\t\t\t\t\t\t<span class="bar-flag low">Billigste</span>'
            if not is_dummy and slot == priciest_slot:
                flag_html += '\n\t\t\t\t\t\t\t\t\t\t<span class="bar-flag peak">Dyreste</span>'

            action_icon_html = ""
            editable_class = ""
            if action_name:
                editable_class = "is-action-editable"
                action_icon_html = "\n".join(
                    [
                        f'\t\t\t\t\t\t\t\t<span class="bar-action-icon {action_name}" title="PowerBuddy: {action_label}" aria-hidden="true">',
                        f"\t\t\t\t\t\t\t\t\t{action_icon}",
                        "\t\t\t\t\t\t\t\t</span>",
                    ]
                )

            wrapper_classes = f"bar-wrapper {day_class} {boundary_class} {editable_class} {'is-dummy' if is_dummy else ''}".strip()
            price_value_html = "" if is_dummy else f'\t\t\t\t\t\t\t\t\t<span class="bar-value">{price_text}</span>'
            bars.append(
                "\n".join(
                    [
                        f'\t\t\t\t\t\t\t<div class="{wrapper_classes}" data-day="{day_class}" data-index="{idx}" data-category="{category}" data-price="{price_ore:.4f}" data-action="{action_name}" data-action-id="{action_id}" data-start-time="{slot_key}" data-hour-label="{hour_label}" data-source="{source_name}">',
                        f"{boundary_badge}" if boundary_badge else "",
                        f'\t\t\t\t\t\t\t\t<div class="bar {category}" style="height: {height_percent:.1f}%;" aria-label="{hour_label} - {price_text} kr">',
                        f"{flag_html}" if flag_html else "",
                        price_value_html,
                        "\t\t\t\t\t\t\t\t</div>",
                        f"{action_icon_html}" if action_icon_html else "",
                        f'\t\t\t\t\t\t\t\t<div class="hour-label">{hour_text}<span class="hour-zero">:00</span></div>',
                        "\t\t\t\t\t\t\t</div>",
                    ]
                )
            )

            slot_meta_by_key[slot_key] = (category, price_kr)
            previous_day = slot.date()

        bars_html = "\n".join(part for part in bars if part)
        replacement = re.sub(
            r'(<div class="chart-bars">\s*)([\s\S]*?)(\s*</div>\s*</div>\s*</div>)',
            rf'\1\n{bars_html}\n\t\t\t\t\t\3',
            html,
            count=1,
        )
        if replacement != html:
            html = replacement
        elif '<div class="chart-bars"></div>' in html:
            html = html.replace(
                '<div class="chart-bars"></div>',
                f'<div class="chart-bars">\n{bars_html}\n</div>',
                1,
            )

    current_meta = slot_meta_by_key.get(current_slot_key)
    if current_meta is not None:
        current_category = current_meta[0]
        current_price_kr = current_meta[1]
    elif sorted_slots:
        fallback_key = sorted_slots[0].strftime("%Y-%m-%dT%H.%M.%S")
        fallback_meta = slot_meta_by_key.get(fallback_key)
        if fallback_meta is not None:
            current_category = fallback_meta[0]
            current_price_kr = fallback_meta[1]

    if current_price_kr is not None:
        current_price_text = f"{current_price_kr:.2f}".replace(".", ",")
        html = re.sub(
            r'(<div class="hero-price">)[^<]*(</div>)',
            rf'\g<1>{current_price_text} kr\g<2>',
            html,
            count=1,
        )
        html = re.sub(
            r'(id="priceChart"[^>]*\sdata-current-price=")[^"]*(")',
            rf'\g<1>{current_price_kr:.4f}\g<2>',
            html,
            count=1,
        )

    alert_by_category = {
        "low": ("alert-success", "Lav pris - god tid at bruge strøm"),
        "medium": ("alert-info", "Fornuftig pris - helt ok"),
        "high": ("alert-warning", "Høj pris - spar hvis muligt"),
        "peak": ("alert-danger", "Meget høj pris - vent hvis muligt"),
    }
    alert_class, alert_text = alert_by_category.get(current_category, alert_by_category["medium"])
    alert_icon_by_category = {
        "low": ("check-circle-fill", "Success:"),
        "medium": ("info-fill", "Info:"),
        "high": ("exclamation-triangle-fill", "Warning:"),
        "peak": ("stop-fill", "Danger:"),
    }
    alert_icon_id, alert_icon_label = alert_icon_by_category.get(current_category, alert_icon_by_category["medium"])

    def _replace_now_alert_class(match: re.Match[str]) -> str:
        classes = (match.group(1) or "").split()
        classes = [cls for cls in classes if not cls.startswith("alert-") or cls == "alert"]
        if "alert" not in classes:
            classes.insert(0, "alert")
        if "pb-now-alert" not in classes:
            classes.append("pb-now-alert")
        classes.append(alert_class)
        return f'<div class="{" ".join(classes)}" role="alert">'

    html = re.sub(
        r'<div class="([^"]*\bpb-now-alert\b[^"]*)" role="alert">',
        _replace_now_alert_class,
        html,
        count=1,
    )
    html = re.sub(
        r'(<div class="pb-now-alert-text">)[^<]*(</div>)',
        rf'\1{alert_text}\2',
        html,
        count=1,
    )
    html = re.sub(
        r'(<svg class="pb-now-alert-icon[^"]*"[\s\S]*?aria-label=")[^"]*(")',
        rf'\1{alert_icon_label}\2',
        html,
        count=1,
    )
    html = re.sub(
        r'(<use href=")[^"]*("\s+xlink:href=")[^"]*("></use>)',
        rf'\1#{alert_icon_id}\2#{alert_icon_id}\3',
        html,
        count=1,
    )

    current_label = _dashboard_action_label(current_action_name)
    html = re.sub(
        r'(id="priceChart"[^>]*\sdata-current-action=")[^"]*(")',
        rf'\1{current_action_name}\2',
        html,
        count=1,
    )
    html = re.sub(
        r'(id="priceChart"[^>]*\sdata-current-start-time=")[^"]*(")',
        rf'\1{current_slot_key}\2',
        html,
        count=1,
    )
    html = re.sub(
        r'(id="priceChart"[^>]*\sdata-current-category=")[^"]*(")',
        rf'\1{current_category}\2',
        html,
        count=1,
    )
    if 'data-current-start-time="' not in html:
        html = re.sub(
            r'(<div class="chart-container[^>]*\sid="priceChart")',
            rf'\1 data-current-start-time="{current_slot_key}"',
            html,
            count=1,
        )
    html = re.sub(
        r'(<div class="hero-action-ribbon chart-action-ribbon\s+)[^"]+("\s+data-action=")[^"]+("\s+data-action-label=")[^"]+("[^>]*>\s*<span>)[^<]*(</span>)',
        rf'\1{current_action_name}\2{current_action_name}\3{current_label}\4{current_label}\5',
        html,
        count=1,
        flags=re.S,
    )
    return html


def _dashboard_is_authorized(request: Request) -> bool:
    ip = _request_ip(request)
    if _dashboard_is_trusted_ip(ip):
        return True

    cookie_name = settings.dashboard_session_cookie
    token = request.cookies.get(cookie_name, "")
    return any(_dashboard_decode_token(token, candidate) for candidate in _request_ip_candidates(request))


def _dashboard_has_login_session(request: Request) -> bool:
    token = request.cookies.get(settings.dashboard_session_cookie, "")
    return any(_dashboard_decode_token(token, candidate) for candidate in _request_ip_candidates(request))


async def _discover_battery_capacity() -> None:
    global _battery_capacity_last_discovery_monotonic
    now_monotonic = time.monotonic()
    if (
        _battery_capacity_last_discovery_monotonic > 0.0
        and (now_monotonic - _battery_capacity_last_discovery_monotonic) < _battery_capacity_discovery_interval_seconds
    ):
        return

    async with _battery_capacity_discovery_lock:
        now_monotonic = time.monotonic()
        if (
            _battery_capacity_last_discovery_monotonic > 0.0
            and (now_monotonic - _battery_capacity_last_discovery_monotonic) < _battery_capacity_discovery_interval_seconds
        ):
            return
        _battery_capacity_last_discovery_monotonic = now_monotonic

    try:
        client = get_inverter_client()
        capacity = await client.get_battery_capacity_kwh()
    except Exception as exc:
        logger.warning("Battery capacity discovery failed: %s", exc)
        return

    if capacity is None:
        logger.warning("Battery capacity discovery returned no value; using fallback default")
        return

    previous_capacity = float(settings.battery_capacity_kwh)
    set_detected_battery_capacity_kwh(capacity)
    current_capacity = float(settings.battery_capacity_kwh)
    if abs(current_capacity - previous_capacity) > 1e-6:
        # Scheduler owns a long-lived planner instance created at import time.
        # Recreate it to use freshly detected battery capacity.
        scheduler.planner = DayPlanner()
        logger.info(
            "Battery capacity updated %.2f -> %.2f kWh; planner reloaded",
            previous_capacity,
            current_capacity,
        )

    logger.info(
        "Detected battery capacity %.2f kWh (effective capacity %.2f, auto power limit %.2f kW)",
        float(capacity),
        float(settings.battery_capacity_kwh),
        float(settings.battery_auto_power_limit_kw),
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    saved_dummy_prices = AppSettingsRepository.get_bool("allow_dummy_prices")
    if saved_dummy_prices is not None:
        settings.allow_dummy_prices = saved_dummy_prices
    PriceRepository.clean_stale_dummy_prices()
    await _discover_battery_capacity()
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
    version="1.0.12",
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


static_dir = Path(__file__).resolve().parent / "static"
if static_dir.exists():
    app.mount("/dashboard/static", StaticFiles(directory=static_dir), name="dashboard-static")
    app.mount("/powerbuddy/static", StaticFiles(directory=static_dir), name="powerbuddy-static")
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    css_dir = static_dir / "css"
    if css_dir.exists():
        app.mount("/css", StaticFiles(directory=css_dir), name="css")

    scripts_dir = static_dir / "scripts"
    if scripts_dir.exists():
        app.mount("/scripts", StaticFiles(directory=scripts_dir), name="scripts")


def _static_icon_response(file_name: str, media_type: str) -> FileResponse:
    path = static_dir / file_name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Icon not found")
    return FileResponse(path, media_type=media_type)


@app.get("/favicon.ico", include_in_schema=False)
@app.head("/favicon.ico", include_in_schema=False)
def favicon_ico() -> FileResponse:
    return _static_icon_response("favicon.ico", "image/x-icon")


@app.get("/favicon-16x16.png", include_in_schema=False)
@app.head("/favicon-16x16.png", include_in_schema=False)
def favicon_16_png() -> FileResponse:
    return _static_icon_response("favicon-16x16.png", "image/png")


@app.get("/favicon-32x32.png", include_in_schema=False)
@app.head("/favicon-32x32.png", include_in_schema=False)
def favicon_32_png() -> FileResponse:
    return _static_icon_response("favicon-32x32.png", "image/png")


@app.get("/apple-touch-icon.png", include_in_schema=False)
@app.head("/apple-touch-icon.png", include_in_schema=False)
def apple_touch_icon_png() -> FileResponse:
    return _static_icon_response("apple-touch-icon.png", "image/png")


@app.get("/android-chrome-192x192.png", include_in_schema=False)
@app.head("/android-chrome-192x192.png", include_in_schema=False)
def android_chrome_192() -> FileResponse:
    return _static_icon_response("android-chrome-192x192.png", "image/png")


@app.get("/android-chrome-512x512.png", include_in_schema=False)
@app.head("/android-chrome-512x512.png", include_in_schema=False)
def android_chrome_512() -> FileResponse:
    return _static_icon_response("android-chrome-512x512.png", "image/png")


@app.get("/favicon-64x64.png", include_in_schema=False)
@app.head("/favicon-64x64.png", include_in_schema=False)
def favicon_64_png() -> FileResponse:
    return _static_icon_response("favicon-64x64.png", "image/png")


@app.get("/favicon-96x96.png", include_in_schema=False)
@app.head("/favicon-96x96.png", include_in_schema=False)
def favicon_96_png() -> FileResponse:
    return _static_icon_response("favicon-96x96.png", "image/png")


@app.get("/mstile-150x150.png", include_in_schema=False)
@app.head("/mstile-150x150.png", include_in_schema=False)
def mstile_150() -> FileResponse:
    return _static_icon_response("mstile-150x150.png", "image/png")


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


@app.get("/powerbuddy", tags=["system"], summary="PowerBuddy dashboard alias for proxied /powerbuddy route", response_class=HTMLResponse)
async def powerbuddy_dashboard_alias(request: Request, secret: str | None = None) -> Response:
    return await dashboard(request, secret=secret)


@app.get("/powerbuddy/dashboard", tags=["system"], summary="PowerBuddy dashboard nested alias for proxied /powerbuddy/dashboard route", response_class=HTMLResponse)
async def powerbuddy_dashboard_nested_alias(request: Request, secret: str | None = None) -> Response:
    return await dashboard(request, secret=secret)


@app.get("/dashboard", tags=["system"], summary="PowerBuddy dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, secret: str | None = None) -> Response:
    if not settings.dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")

    compat_action = (request.query_params.get("action") or "").strip().lower()
    if compat_action == "planning-auth-status":
        return JSONResponse(await dashboard_auth_status(request))
    if compat_action == "inverter-realtime":
        realtime = await inverter_realtime()
        payload = realtime.model_dump()
        payload["battery_capacity_kwh"] = float(settings.battery_capacity_kwh)
        payload["battery_min_soc"] = float(settings.battery_min_soc)
        payload["reserve_min_soc"] = float(settings.battery_min_soc)
        payload.update(await get_easee_status())
        return JSONResponse(jsonable_encoder(payload))
    if compat_action:
        return JSONResponse({"ok": False, "error": "unknown_action"}, status_code=400)

    icon_url = _dashboard_resolve_icon_url(settings.dashboard_icon_url, "/powerbuddy/static/favicon-32x32.png")
    favicon_url = _dashboard_resolve_icon_url(settings.dashboard_favicon_url, "/powerbuddy/static/favicon.ico")
    apple_touch_icon_url = _dashboard_resolve_icon_url(
        settings.dashboard_apple_touch_icon_url,
        "/powerbuddy/static/apple-touch-icon.png",
    )

    secret_value = (secret or "").strip()
    secret_is_valid = bool(
        secret_value
        and settings.dashboard_secret
        and hmac.compare_digest(secret_value, settings.dashboard_secret)
    )
    is_authorized = _dashboard_is_authorized(request) or secret_is_valid

    html = render_dashboard_html(
        title="PowerBuddy",
        icon_url=icon_url,
        favicon_url=favicon_url,
        apple_touch_icon_url=apple_touch_icon_url,
    )
    today = datetime.now(ZoneInfo(settings.timezone)).date()
    actions_today = await get_plan(target_date=today)
    actions_tomorrow = await get_plan(target_date=today + timedelta(days=1))
    prices = await get_prices()
    html = _dashboard_apply_plan_state(html, [*actions_today, *actions_tomorrow], prices)
    html = _dashboard_apply_mode_state(html, request.query_params.get("pbMode", ""))
    html = _dashboard_apply_runtime_state(html)
    html = _dashboard_apply_auth_state(html, authorized=is_authorized, request=request)

    response = HTMLResponse(content=html)
    ip = _request_ip(request)
    if secret_is_valid:
        token = _dashboard_encode_token(ip)
        response.set_cookie(
            key=settings.dashboard_session_cookie,
            value=token,
            **_dashboard_cookie_options(request),
        )

    return response


async def _dashboard_read_payload(request: Request) -> dict[str, object]:
    payload: dict[str, object] = {}
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
    else:
        try:
            raw = (await request.body()).decode("utf-8", "ignore")
        except Exception:
            raw = ""

        if raw:
            parsed = parse_qs(raw, keep_blank_values=True)
            payload = {key: values[-1] if values else "" for key, values in parsed.items()}
        else:
            try:
                form_data = await request.form()
                payload = dict(form_data)
            except Exception:
                payload = {}
    return payload


def _dashboard_find_action_id_by_start_time(start_time_text: str) -> int | None:
    raw = (start_time_text or "").strip()
    if not raw:
        return None

    raw_norm = re.sub(r'T(\d{2})\.(\d{2})\.(\d{2})$', r'T\1:\2:\3', raw)
    try:
        parsed = datetime.fromisoformat(raw_norm.replace("Z", "+00:00"))
    except Exception:
        return None

    if parsed.tzinfo is not None:
        local_time = parsed.astimezone(ZoneInfo(settings.timezone)).replace(tzinfo=None)
    else:
        local_time = parsed

    slot = local_time.replace(minute=0, second=0, microsecond=0)
    for action in PlanRepository.get_plan(slot.date().isoformat()):
        if _naive_ts(action.start_time) == slot:
            return int(action.id)
    return None


@app.post("/powerbuddy", tags=["system"], summary="PowerBuddy dashboard compat actions via proxy")
@app.post("/dashboard", tags=["system"], summary="PowerBuddy dashboard compat actions")
async def dashboard_compat_action(request: Request) -> JSONResponse:
    if not settings.dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")

    compat_action = (request.query_params.get("action") or "").strip().lower()
    if not compat_action:
        return JSONResponse({"ok": False, "error": "missing_action"}, status_code=400)

    if compat_action == "planning-auth-login":
        try:
            return await dashboard_auth_login(request)
        except HTTPException as exc:
            if exc.status_code == 401:
                return JSONResponse({"ok": False, "authorized": False, "error": "unauthorized"}, status_code=401)
            raise

    if compat_action == "planning-control":
        if not _dashboard_is_authorized(request):
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

        payload = await _dashboard_read_payload(request)
        command = str(payload.get("command") or "").strip().lower()
        try:
            if command == "pause":
                result = await pause_execution()
            elif command == "start":
                result = await start_execution()
            else:
                return JSONResponse({"ok": False, "error": "invalid_command"}, status_code=400)
        except Exception:
            return JSONResponse({"ok": False, "error": "control_failed"}, status_code=500)

        return JSONResponse(
            {
                "ok": True,
                "isPaused": not bool(result.get("execution_enabled", True)),
                "execution_mode": result.get("execution_mode"),
            }
        )

    if compat_action == "planning-update":
        if not _dashboard_is_authorized(request):
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

        payload = await _dashboard_read_payload(request)
        next_action = str(payload.get("actionName") or payload.get("action") or "").strip().lower()
        if next_action not in {"auto", "charge", "hold", "discharge"}:
            return JSONResponse({"ok": False, "error": "invalid_action"}, status_code=400)

        action_id_raw = str(payload.get("actionId") or "").strip()
        action_id: int | None = None
        if action_id_raw:
            try:
                action_id = int(action_id_raw)
            except ValueError:
                action_id = None
        if action_id is None:
            action_id = _dashboard_find_action_id_by_start_time(str(payload.get("startTime") or ""))
        if action_id is None:
            return JSONResponse({"ok": False, "error": "action_not_found"}, status_code=404)

        plan_action = cast(Literal["auto", "charge", "discharge", "hold", "discharge_force"], next_action)
        try:
            updated = await update_plan_action(
                action_id,
                PlanActionUpdateIn(action=plan_action, reason="manual override (dashboard compat)"),
            )
        except HTTPException as exc:
            if exc.status_code == 404:
                fallback_id = _dashboard_find_action_id_by_start_time(str(payload.get("startTime") or ""))
                if fallback_id is None:
                    return JSONResponse({"ok": False, "error": "action_not_found"}, status_code=404)
                updated = await update_plan_action(
                    fallback_id,
                    PlanActionUpdateIn(action=plan_action, reason="manual override (dashboard compat)"),
                )
            else:
                detail = exc.detail if isinstance(exc.detail, str) else "update_failed"
                return JSONResponse({"ok": False, "error": detail}, status_code=exc.status_code)

        return JSONResponse({"ok": True, "id": updated.id, "action": updated.action})

    return JSONResponse({"ok": False, "error": "unknown_action"}, status_code=400)


@app.get("/dashboard/auth/status", tags=["system"], summary="Dashboard auth status")
async def dashboard_auth_status(request: Request) -> dict[str, object]:
    authorized = _dashboard_is_authorized(request)
    return {
        "ok": True,
        "authorized": authorized,
        "battery_link": _dashboard_resolve_battery_link(authorized=authorized, request=request),
        "ip": _request_ip(request),
    }


@app.get("/powerbuddy/dashboard/logs", tags=["system"], summary="Read latest dashboard logs via proxied /powerbuddy route")
@app.get("/dashboard/logs", tags=["system"], summary="Read latest dashboard logs")
async def dashboard_logs(request: Request, lines: int = 200) -> JSONResponse:
    if not _dashboard_is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    limit = max(1, min(5000, int(lines or 200)))
    log_path = resolve_log_file_path()
    file_lines: list[str] = []

    if log_path.exists():
        try:
            with log_path.open("r", encoding="utf-8", errors="replace") as handle:
                file_lines = [line for line in handle if not _is_redundant_httpx_error_log(line)]
        except OSError:
            file_lines = []

    tail = file_lines[-limit:]
    return JSONResponse(
        {
            "ok": True,
            "path": str(log_path),
            "count": len(tail),
            "total": len(file_lines),
            "lines": tail,
        }
    )


@app.post(
    "/dashboard/auth/login",
    tags=["system"],
    summary="Dashboard login",
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/x-www-form-urlencoded": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "password": {"type": "string", "description": "Dashboard password"},
                            "txtLogin": {"type": "string", "description": "Legacy login field"},
                        },
                        "anyOf": [{"required": ["password"]}, {"required": ["txtLogin"]}],
                    }
                },
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "password": {"type": "string", "description": "Dashboard password"},
                            "txtLogin": {"type": "string", "description": "Legacy login field"},
                        },
                    }
                },
            },
        }
    },
)
async def dashboard_auth_login(request: Request) -> JSONResponse:
    if not settings.dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")

    payload = await _dashboard_read_payload(request)
    submitted_password = str(payload.get("password") or payload.get("txtLogin") or "").strip()

    if not _dashboard_is_password_valid(submitted_password):
        raise HTTPException(status_code=401, detail="Invalid password")

    ip = _request_ip(request)
    token = _dashboard_encode_token(ip)
    response = JSONResponse(
        {
            "ok": True,
            "authorized": True,
            "battery_link": _dashboard_resolve_battery_link(authorized=True, request=request),
        }
    )
    response.set_cookie(
        key=settings.dashboard_session_cookie,
        value=token,
        **_dashboard_cookie_options(request),
    )
    return response


@app.post("/dashboard/auth/logout", tags=["system"], summary="Dashboard logout")
async def dashboard_auth_logout() -> JSONResponse:
    response = JSONResponse({"ok": True, "authorized": False})
    response.delete_cookie(key=settings.dashboard_session_cookie, path="/")
    return response


@app.get("/dashboard/state", tags=["system"], summary="Dashboard aggregated state")
async def dashboard_state(request: Request) -> dict[str, object]:
    if not settings.dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")

    prices = await get_prices(hours=36)
    realtime = await inverter_realtime()
    execution = get_execution_status()
    now_status = await get_current_plan_status()

    now = datetime.now(ZoneInfo(settings.timezone))
    today = now.date()
    actions_today = await get_plan(target_date=today)

    current_action = None
    for action in actions_today:
        start = _naive_ts(action.start_time)
        end = _naive_ts(action.end_time)
        if start <= now.replace(tzinfo=None) < end:
            current_action = action.action
            break

    return {
        "ok": True,
        "authorized": _dashboard_is_authorized(request),
        "realtime": realtime.model_dump(),
        "ev": await get_easee_status(),
        "execution": execution,
        "plan_now": now_status.model_dump(),
        "current_action": current_action,
        "prices": [price.model_dump() for price in prices],
    }


@app.post("/dashboard/control", tags=["system"], summary="Dashboard execution control")
async def dashboard_control(request: Request) -> dict[str, object]:
    if not _dashboard_is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()
    command = str(payload.get("command") or "").strip().lower()
    if command == "pause":
        result = await pause_execution()
    elif command == "start":
        result = await start_execution()
    else:
        raise HTTPException(status_code=400, detail="Invalid command")

    return result


@app.post("/dashboard/settings/action", tags=["system"], summary="Run a manual dashboard maintenance action")
@app.post("/powerbuddy/settings/action", tags=["system"], summary="Run a manual dashboard maintenance action via proxy")
async def dashboard_settings_action(request: Request) -> dict[str, object]:
    if not _dashboard_is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()
    action = str(payload.get("action") or "").strip().lower()
    if action == "dummy-prices-status":
        return {"ok": True, "action": action, "enabled": settings.allow_dummy_prices}
    if action == "set-dummy-prices":
        enabled = payload.get("enabled")
        if not isinstance(enabled, bool):
            raise HTTPException(status_code=400, detail="enabled must be a boolean")
        AppSettingsRepository.set_bool("allow_dummy_prices", enabled)
        settings.allow_dummy_prices = enabled
        deleted_prices = 0 if enabled else PriceRepository.delete_dummy_prices()
        logger.info(
            "Dummy price fallback %s from dashboard settings; removed %d stored dummy price(s)",
            "enabled" if enabled else "disabled",
            deleted_prices,
        )
        return {
            "ok": True,
            "action": action,
            "enabled": settings.allow_dummy_prices,
            "deleted_prices": deleted_prices,
        }
    if action == "fetch-prices":
        count = await scheduler.manual_fetch_prices()
        return {"ok": True, "action": action, "days": count}
    if action == "generate-plan":
        count = await scheduler.manual_generate_plan()
        return {"ok": True, "action": action, "days": count}
    if action == "refresh-and-plan":
        await scheduler.refresh_prices_and_replan()
        return {"ok": True, "action": action}
    raise HTTPException(status_code=400, detail="Invalid settings action")


@app.post("/dashboard/planning/action/{action_id}", tags=["planning"], summary="Dashboard update plan action")
async def dashboard_update_plan_action(action_id: int, request: Request) -> PlanActionOut:
    if not _dashboard_is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()
    action = str(payload.get("action") or "").strip().lower()
    if action not in {"auto", "charge", "hold", "discharge"}:
        raise HTTPException(status_code=400, detail="Invalid action")

    validated_action = cast(Literal["auto", "charge", "discharge", "hold", "discharge_force"], action)
    update_payload = PlanActionUpdateIn(action=validated_action, reason="manual override (dashboard)")
    return await update_plan_action(action_id, update_payload)


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
    - day > today: simulate plans day-by-day from now until target day, so each
      day starts from the prior day's projected end-of-day SOC.
    - Falls back to latest known SOC if a plan/simulation step is missing.
    """
    today = date.today()
    live_soc = await _resolve_current_soc()

    if day <= today:
        return live_soc

    planner = DayPlanner()
    tz = ZoneInfo(settings.timezone)
    now_local_hour = datetime.now(tz).replace(minute=0, second=0, microsecond=0).replace(tzinfo=None)
    projected_soc = max(float(settings.battery_min_soc), min(100.0, float(live_soc)))

    cursor_day = today
    while cursor_day < day:
        day_actions = PlanRepository.get_plan(cursor_day.isoformat())
        if not day_actions:
            break

        if cursor_day == today:
            day_actions = [a for a in day_actions if _naive_ts(a.start_time) >= now_local_hour]

        if not day_actions:
            cursor_day += timedelta(days=1)
            continue

        try:
            weather_factors = await weather_forecast_service.get_hourly_pv_factor_24h(cursor_day)
            simulation = planner.simulate(
                cursor_day,
                day_actions,
                start_soc=projected_soc,
                pv_weather_factor_24h=weather_factors,
            )
            if simulation:
                projected_soc = max(
                    float(settings.battery_min_soc),
                    min(100.0, float(simulation[-1].projected_soc)),
                )
        except Exception:
            break

        cursor_day += timedelta(days=1)

    return projected_soc


async def _ensure_prices_with_fallback(requested_day: date) -> tuple[date, list, bool]:
    prices = PriceRepository.get_by_day(requested_day, settings.price_area)
    provider = get_price_provider()
    now_local = datetime.now(ZoneInfo(settings.timezone))
    publish_hour = min(23, max(0, int(settings.day_ahead_publish_hour_local)))
    today = now_local.date()
    tomorrow = today + timedelta(days=1)

    can_fetch = (requested_day == today) or (requested_day == tomorrow and now_local.hour >= publish_hour)

    if not prices and can_fetch:
        try:
            fetched = await provider.get_day_prices(requested_day, settings.price_area)
        except Exception:
            fetched = []
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
    Network fetch is strictly limited to today and tomorrow (after publish hour).
    """
    provider = get_price_provider()
    now_local = datetime.now(ZoneInfo(settings.timezone))
    publish_hour = min(23, max(0, int(settings.day_ahead_publish_hour_local)))
    today = now_local.date()
    tomorrow = today + timedelta(days=1)

    current_day = start.date()
    end_day = (end - timedelta(seconds=1)).date()

    while current_day <= end_day:
        if current_day > tomorrow:
            current_day += timedelta(days=1)
            continue
        if current_day == tomorrow and now_local.hour < publish_hour:
            current_day += timedelta(days=1)
            continue

        existing = PriceRepository.get_by_day(current_day, settings.price_area)
        has_real_prices = any(not p.source.lower().startswith("dummy") for p in existing) and len(existing) >= 24
        if not has_real_prices:
            try:
                fetched = await provider.get_day_prices(current_day, settings.price_area)
            except Exception as exc:
                logger.warning("Price fetch for %s failed: %s", current_day, exc)
                fetched = []
            if fetched:
                PriceRepository.upsert_prices(fetched)
        current_day += timedelta(days=1)


async def _discover_latest_released_day_from(start_day: date) -> date | None:
    """
    Discover latest released day from `start_day` forward by probing providers day-by-day.
    Spot prices can only exist for today and tomorrow (from 13:00 local time).
    """
    provider = get_price_provider()
    latest_released: date | None = None
    now_local = datetime.now(ZoneInfo(settings.timezone))
    publish_hour = min(23, max(0, int(settings.day_ahead_publish_hour_local)))
    today = now_local.date()
    tomorrow = today + timedelta(days=1)

    for offset in (0, 1):
        day = start_day + timedelta(days=offset)
        if day > tomorrow:
            break
        prices = PriceRepository.get_by_day(day, settings.price_area)
        has_real_prices = any(not p.source.lower().startswith("dummy") for p in prices) and len(prices) >= 24

        if not has_real_prices:
            can_fetch = (day == today) or (day == tomorrow and now_local.hour >= publish_hour)
            if can_fetch:
                try:
                    fetched = await provider.get_day_prices(day, settings.price_area)
                except Exception:
                    fetched = []
                if fetched:
                    PriceRepository.upsert_prices(fetched)
                    prices = PriceRepository.get_by_day(day, settings.price_area)

        if prices:
            latest_released = day
            continue

        if day > start_day:
            break

    return latest_released


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
        return float(settings.max_charge_kw) * 1000.0
    return charge_power_w


def _effective_charge_power_w(charge_power_w: float | None) -> float:
    requested = float(charge_power_w) if charge_power_w is not None else (float(settings.max_charge_kw) * 1000.0)
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
    now_local = datetime.now(ZoneInfo(settings.timezone))
    publish_hour = min(23, max(0, int(settings.day_ahead_publish_hour_local)))
    today = now_local.date()
    tomorrow = today + timedelta(days=1)

    can_fetch = (day == today) or (day == tomorrow and now_local.hour >= publish_hour)

    if not prices and can_fetch:
        try:
            fetched = await provider.get_day_prices(day, settings.price_area)
        except Exception:
            fetched = []
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
                    float(settings.must_charge_window_min_soc_percent),
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
        "battery_capacity_kwh": settings.battery_capacity_kwh,
        "battery_capacity_source": settings.battery_capacity_source,
        "battery_auto_power_limit_kw": settings.battery_auto_power_limit_kw,
        "battery_min_soc_fixed": settings.battery_min_soc,
        "battery_max_soc_fixed": settings.battery_max_soc,
        "max_charge_kw_effective": settings.max_charge_kw,
        "max_discharge_kw_effective": settings.max_discharge_kw,
        "max_charge_kw_override": settings.max_charge_kw_override,
        "max_discharge_kw_override": settings.max_discharge_kw_override,
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
    hours: int | None = None,
) -> list[PriceOut]:
    """
    Default behavior (no query params): return all released prices from current
    whole hour and forward.

    If `hours` is provided, limit the horizon to that many hours (max 72).

    Backward-compatible behavior: if target_date is provided, return that day's
    prices (00:00..24:00).
    """
    now_local = datetime.now(ZoneInfo(settings.timezone))
    PriceRepository.clean_stale_dummy_prices(now_local)

    if target_date is not None:
        prices = PriceRepository.get_by_day(target_date, settings.price_area)
        publish_hour = min(23, max(0, int(settings.day_ahead_publish_hour_local)))
        today = now_local.date()
        tomorrow = today + timedelta(days=1)
        can_fetch = (target_date == today) or (target_date == tomorrow and now_local.hour >= publish_hour)

        if not prices and can_fetch:
            provider = get_price_provider()
            try:
                fetched = await provider.get_day_prices(target_date, settings.price_area)
            except Exception:
                fetched = []
            if fetched:
                PriceRepository.upsert_prices(fetched)
                prices = PriceRepository.get_by_day(target_date, settings.price_area)
    else:
        if from_timestamp is None:
            start = datetime.now(timezone.utc)
        elif from_timestamp.tzinfo is None:
            start = from_timestamp.replace(tzinfo=timezone.utc)
        else:
            start = from_timestamp.astimezone(timezone.utc)
        start = start.replace(minute=0, second=0, microsecond=0)

        if now_local.hour >= 14 and start.date() == now_local.date():
            tomorrow = start.date() + timedelta(days=1)
            await scheduler._ensure_dummy_future_prices_if_needed(tomorrow, now_local)

        if hours is not None:
            horizon_hours = max(1, min(int(hours), 72))
            end = start + timedelta(hours=horizon_hours)
        else:
            latest_available_day = await _discover_latest_released_day_from(start.date())
            if latest_available_day is None:
                end = start + timedelta(hours=1)
            else:
                latest_end = datetime.combine(
                    latest_available_day + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                )
                end = latest_end if latest_end > start else start + timedelta(hours=1)
            horizon_hours = max(1, int((end - start).total_seconds() // 3600))

        await _ensure_prices_for_window(start, end)

        raw_prices = PriceRepository.get_by_time_window(start, end, settings.price_area)

        if hours is None or not settings.allow_provisional_prices:
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
                source=(p.source or "").strip() or None,
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
    await _discover_battery_capacity()
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
        if from_timestamp is None:
            start = datetime.now(timezone.utc)
        elif from_timestamp.tzinfo is None:
            start = from_timestamp.replace(tzinfo=timezone.utc)
        else:
            start = from_timestamp.astimezone(timezone.utc)

        start = start.replace(minute=0, second=0, microsecond=0)

        if hours is not None:
            horizon_hours = max(1, min(int(hours), 72))
            end = start + timedelta(hours=horizon_hours)
        else:
            latest_available_day = await _discover_latest_released_day_from(start.date())
            if latest_available_day is None:
                end = start + timedelta(hours=1)
            else:
                latest_end = datetime.combine(
                    latest_available_day + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                )
                end = latest_end if latest_end > start else start + timedelta(hours=1)

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
