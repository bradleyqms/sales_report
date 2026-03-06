"""Health-check alert helpers for dispatch timer functions."""
from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone

from .config import parse_recipients
from .graph_client import send_via_graph

LOG = logging.getLogger(__name__)

_DEFAULT_HEALTHCHECK_RECIPIENTS = "bradley@qmsmedicosmetics.com"


def send_healthcheck_alert(function_name: str, exc: Exception) -> None:
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
    subject = f"[ALERT] qms-dispatch-reports failure: {function_name}"
    tb = traceback.format_exc()
    body = (
        f"Azure Function failure detected.\n\n"
        f"Function: {function_name}\n"
        f"Timestamp: {now_utc}\n"
        f"Error: {type(exc).__name__}: {exc}\n\n"
        f"Traceback:\n{tb}"
    )

    try:
        send_via_graph(recipients, [], body, subject, "Text")
        LOG.info("Health-check alert sent to %s", recipients)
    except Exception as alert_exc:  # pylint: disable=broad-except
        LOG.exception("Failed to send health-check alert: %s", alert_exc)
