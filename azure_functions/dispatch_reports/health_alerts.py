"""Health-check alert helpers for dispatch timer functions."""
from __future__ import annotations

import logging
import os
import sys
import traceback
from datetime import datetime, timezone

from .config import parse_recipients
from .graph_client import send_via_graph

LOG = logging.getLogger(__name__)

_DEFAULT_HEALTHCHECK_RECIPIENTS = "bradley@qmsmedicosmetics.com"


def _resolve_environment_name() -> str:
    return (
        os.getenv("HEALTHCHECK_ENVIRONMENT")
        or os.getenv("WEBSITE_SITE_NAME")
        or "unknown-environment"
    )


def _resolve_commit_label() -> str:
    explicit = os.getenv("HEALTHCHECK_COMMIT_SHA")
    if explicit:
        return explicit
    return os.getenv("SOURCE_VERSION") or "unknown"


def _resolve_logs_url() -> str:
    return os.getenv("HEALTHCHECK_APPINSIGHTS_URL") or "https://portal.azure.com/"


def send_healthcheck_alert(function_name: str, exc: Exception, invocation_id: str | None = None) -> None:
    """Best-effort email alert when a timer function fails.

    This helper must never raise; it logs any internal issues and returns.
    """
    enabled_raw = os.getenv("HEALTHCHECK_ALERTS_ENABLED", "true").strip().lower()
    if enabled_raw in ("false", "0", "no"):
        LOG.info("Health-check alerts disabled via HEALTHCHECK_ALERTS_ENABLED=%s", enabled_raw)
        return

    recipients = parse_recipients(
        os.getenv("HEALTHCHECK_ALERT_RECIPIENTS", _DEFAULT_HEALTHCHECK_RECIPIENTS)
    )
    if not recipients:
        LOG.warning("No health-check recipients configured")
        return

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"🚨 [CRITICAL] Azure Function Failure: {function_name} (Prod)"
    tb = traceback.format_exc()
    environment_name = _resolve_environment_name()
    hostname = os.getenv("WEBSITE_HOSTNAME") or "unknown-host"
    commit_label = _resolve_commit_label()
    ai_url = _resolve_logs_url()
    effective_invocation_id = invocation_id or os.getenv("HEALTHCHECK_INVOCATION_ID") or "unknown"

    body = f"""
<html>
    <body style="font-family: Segoe UI, Arial, sans-serif; color: #111827;">
        <h2 style="margin-bottom: 0;">🚨 [CRITICAL] Azure Function Failure</h2>
        <p style="margin-top: 4px;"><strong>Function:</strong> {function_name}</p>

        <h3>Summary</h3>
        <ul>
            <li><strong>Function:</strong> {function_name}</li>
            <li><strong>Error Type:</strong> {type(exc).__name__}</li>
            <li><strong>Message:</strong> {exc}</li>
            <li><strong>Version (Commit):</strong> {commit_label}</li>
        </ul>

        <h3>Diagnostics</h3>
        <ul>
            <li><strong>Environment:</strong> {environment_name}</li>
            <li><strong>Host:</strong> {hostname}</li>
            <li><strong>Invocation ID:</strong> {effective_invocation_id}</li>
            <li><strong>Timestamp:</strong> {now_utc}</li>
            <li><strong>Python:</strong> {sys.version.split()[0]}</li>
        </ul>

        <h3>Action Required</h3>
        <p><a href="{ai_url}">View Logs in Azure Application Insights</a></p>

        <h3>Full Traceback</h3>
        <pre style="background: #F3F4F6; padding: 12px; border-radius: 6px; white-space: pre-wrap;">{tb}</pre>
    </body>
</html>
""".strip()

    try:
        send_via_graph(recipients, [], body, subject, "HTML")
        LOG.info("Health-check alert sent to %s", recipients)
    except Exception as alert_exc:  # pylint: disable=broad-except
        LOG.exception("Failed to send health-check alert: %s", alert_exc)
