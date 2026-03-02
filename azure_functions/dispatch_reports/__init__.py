"""Timer-triggered Azure Function -- thin entrypoint delegating to submodules."""
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import azure.functions as func
from dotenv import load_dotenv

from .config import parse_recipients, report_date_str, report_mtd_banner
from .graph_client import acquire_graph_token, send_via_graph
from .html_builder import build_html_body
from .report_collector import collect_csv_attachments, collect_html_files, refresh_reports, resolve_outputs_path, derive_report_date

load_dotenv()

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Backwards-compatible shims used by test_dispatch_local.py and tests
_resolve_outputs_path = resolve_outputs_path
_parse_recipients = parse_recipients
_collect_html_files = collect_html_files
_collect_csv_attachments = collect_csv_attachments
_build_html_body = build_html_body
_send_via_graph = send_via_graph
_acquire_graph_token = acquire_graph_token
_refresh_reports = refresh_reports


# ---- Azure Functions entry point ----------------------------------------

def main(mytimer: func.TimerRequest) -> None:
    outputs_dir = resolve_outputs_path()
    _test_recip = os.getenv("TEST_REPORT_DISPATCH_RECIPIENTS", "").strip()
    if _test_recip:
        LOG.info("TEST mode: overriding recipients with TEST_REPORT_DISPATCH_RECIPIENTS")
        recipients = parse_recipients(_test_recip)
    else:
        recipients = parse_recipients(os.getenv("REPORT_DISPATCH_RECIPIENTS"))
    if not recipients:
        LOG.warning("No recipients configured for report dispatch")
        return

    refresh_reports(outputs_dir)
    report_date = derive_report_date(outputs_dir)

    # HTML -> email body
    html_files = collect_html_files(outputs_dir)
    if not html_files:
        LOG.warning("No HTML report files found in %s", outputs_dir)
        return
    LOG.info(
        "HTML body files (%d): %s", len(html_files), [p.name for p in html_files]
    )

    plain_intro = os.getenv(
        "REPORT_DISPATCH_BODY",
        "Please find the latest QMS sales data attached.",
    )
    body_type, body_content = build_html_body(
        html_files, plain_intro,
        banner_title=(
            f"Management Report (MTD: {report_date.strftime('%B')} 1-{report_date.day}, {report_date.year})"
            if report_date else
            report_mtd_banner()
        ),
    )

    # CSVs -> attachments
    attachments = collect_csv_attachments(outputs_dir)
    if not attachments:
        LOG.warning("No CSV files found to attach from %s", outputs_dir)
    else:
        LOG.info(
            "CSV attachments (%d): %s", len(attachments), [p.name for p in attachments]
        )

    subject = os.getenv("REPORT_DISPATCH_SUBJECT") or (
        f"QMS Management Sales Report {report_date.strftime('%d.%m.%Y')}"
        if report_date else
        f"QMS Management Sales Report {report_date_str()}"
    )

    try:
        send_via_graph(recipients, attachments, body_content, subject, body_type)
    except Exception as exc:  # pylint: disable=broad-except
        LOG.exception("Graph report dispatch failed: %s", exc)
        raise
