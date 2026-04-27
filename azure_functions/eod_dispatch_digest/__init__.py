"""Timer-triggered EOD digest for dispatch send-status events."""
from __future__ import annotations

import logging
import os

import azure.functions as func
from dotenv import load_dotenv

from dispatch_reports.config import parse_recipients
from dispatch_reports.eod_digest import build_digest_html, collect_digest_payload
from dispatch_reports.graph_client import send_via_graph

load_dotenv()

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

_DEFAULT_RECIPIENTS = "bradley@qmsmedicosmetics.com"


def main(mytimer: func.TimerRequest = None) -> None:
    date_token, aggregate, records = collect_digest_payload()

    recipients = parse_recipients(
        os.getenv(
            "EOD_HEALTHCHECK_RECIPIENTS",
            os.getenv("HEALTHCHECK_ALERT_RECIPIENTS", _DEFAULT_RECIPIENTS),
        )
    )
    if not recipients:
        LOG.warning("No EOD digest recipients configured")
        return

    send_when_empty_raw = os.getenv("EOD_DISPATCH_DIGEST_SEND_EMPTY", "false").strip().lower()
    send_when_empty = send_when_empty_raw in {"1", "true", "yes", "on"}
    if not records and not send_when_empty:
        LOG.info("No dispatch audit records found for %s; digest suppressed", date_token)
        return

    health = aggregate.get("health", "green").upper()
    subject = os.getenv("EOD_DISPATCH_DIGEST_SUBJECT") or f"Dispatch EOD Health Digest {date_token} [{health}]"
    body = build_digest_html(date_token, aggregate, records)

    send_via_graph(recipients, [], body, subject, "HTML")
    LOG.info("EOD dispatch digest sent to %s for %s (%d records)", recipients, date_token, len(records))
