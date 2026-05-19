from __future__ import annotations

from pathlib import Path
import re


_SOURCE_TEMPLATE_PATH = Path(__file__).resolve().parent / "static" / "powerbuddy.html"
_SOURCE_TEMPLATE_HTML = _SOURCE_TEMPLATE_PATH.read_text(encoding="utf-8") if _SOURCE_TEMPLATE_PATH.exists() else ""
_ICON_CACHE_BUST = "20260519l"


def _icon_url_with_version(url: str) -> str:
    if "?" in url:
        return url
    return f"{url}?v={_ICON_CACHE_BUST}"


def render_dashboard_html(*, title: str, icon_url: str, favicon_url: str, apple_touch_icon_url: str) -> str:
    # Serve the exact source template to keep UI/UX structure 1:1.
    if _SOURCE_TEMPLATE_HTML:
        icon_url = _icon_url_with_version(icon_url)
        favicon_url = _icon_url_with_version(favicon_url)
        apple_touch_icon_url = _icon_url_with_version(apple_touch_icon_url)
        html = _SOURCE_TEMPLATE_HTML
        small_icon_url = icon_url.replace("32x32", "16x16") if "32x32" in icon_url else icon_url.replace("favicon-32x32", "favicon-16x16") if "favicon-32x32" in icon_url else icon_url
        html = re.sub(
            r'(<link rel="apple-touch-icon"[^>]*href=")[^"]*(")',
            rf'\1{apple_touch_icon_url}\2',
            html,
            count=1,
        )
        html = re.sub(
            r'(<link rel="icon" type="image/png" sizes="32x32"[^>]*href=")[^"]*(")',
            rf'\1{icon_url}\2',
            html,
            count=1,
        )
        html = re.sub(
            r'(<link rel="icon" type="image/png" sizes="16x16"[^>]*href=")[^"]*(")',
            rf'\1{small_icon_url}\2',
            html,
            count=1,
        )
        html = re.sub(
            r'(<link rel="icon" type="image/x-icon"[^>]*href=")[^"]*(")',
            rf'\1{favicon_url}\2',
            html,
            count=1,
        )
        if 'rel="shortcut icon"' not in html:
            html = re.sub(
                r'(<link rel="icon" type="image/x-icon"[^>]*>)',
                rf'\1\n    <link rel="shortcut icon" href="{favicon_url}">',
                html,
                count=1,
            )
        html = re.sub(
            r'(<title>).*?(</title>)',
            rf'\1{title}\2',
            html,
            count=1,
            flags=re.S,
        )
        return html

    # Fail-safe fallback if template file is missing.
    return """<!DOCTYPE html>
<html lang=\"da\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>PowerBuddy</title>
    <link rel=\"stylesheet\" href=\"/powerbuddy/static/css/powerbuddy.css?v=20260519m\" />
</head>
<body>
  <p>PowerBuddy template mangler.</p>
    <script src=\"/powerbuddy/static/scripts/powerbuddy.js?v=20260519m\"></script>
</body>
</html>
"""
