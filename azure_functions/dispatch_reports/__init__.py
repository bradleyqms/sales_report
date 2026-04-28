"""Timer-triggered Azure Function -- thin entrypoint delegating to submodules."""
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import azure.functions as func
from dotenv import load_dotenv

from .config import (
    parse_recipients,
    report_date_str,
    dispatch_report_mode,
    report_period_banner,
    report_period_summary,
)
from .graph_client import acquire_graph_token, send_via_graph
from .health_alerts import send_healthcheck_alert
from .html_builder import build_html_body
from .report_collector import collect_csv_attachments, collect_html_files, download_outputs_from_blob, refresh_reports, resolve_outputs_path, derive_report_date

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


def _build_subject(report_date: "datetime | None") -> str:
    """Return the dispatch email subject line.

    Reads ``V2_UNIFIED_REFRESH_REPORT_TYPE`` (default ``MTD``) to decide
    whether to prefix with ``EOM``.  An explicit ``REPORT_DISPATCH_SUBJECT``
    env var always wins.
    """
    mode = dispatch_report_mode()
    date_str = report_date.strftime('%d.%m.%Y') if report_date else report_date_str()
    default = (
        f"EOM QMS Management Sales Report {date_str}"
        if mode == "EOM"
        else f"QMS Management Sales Report {date_str}"
    )
    return os.getenv("REPORT_DISPATCH_SUBJECT") or default


def _management_section_title(path: Path, _title: str, report_date: "datetime | None", mode: str | None = None) -> str:
    name = path.name.lower()
    if "core_market" in name or "core-markets" in name:
        return report_period_banner("Core Market Sales Report", report_date, mode)
    if "usa_spa" in name or "usa-spa" in name:
        return report_period_banner("USA Spa Sales Report", report_date, mode)
    return report_period_banner("Management Report", report_date, mode)


# ---- Azure Functions entry point ----------------------------------------

def main(mytimer: func.TimerRequest = None) -> None:
    try:
        outputs_dir = resolve_outputs_path()
        _test_recip = os.getenv("TEST_REPORT_DISPATCH_RECIPIENTS", "").strip()
        if _test_recip:
            LOG.info("TEST mode: overriding recipients with TEST_REPORT_DISPATCH_RECIPIENTS")
            recipients = parse_recipients(_test_recip)
        else:
            recipients = parse_recipients(os.getenv("REPORT_DISPATCH_RECIPIENTS"))
        if not recipients:
            LOG.warning("No recipients configured for report dispatch")
            return func.HttpResponse("No recipients configured", status_code=400) if req else None

        # Option B: dispatch is a pure consumer.  refresh_unified_v2_timer
        # has already run full_report_v2.py and uploaded the artefacts to
        # the reporting-outputs blob; we just hydrate the local working
        # directory from blob and send what we find.
        # Set DISPATCH_REFRESH_BEFORE_SEND=true to keep the legacy in-line
        # regeneration behaviour (slow; only intended for ad-hoc recovery).
        legacy_refresh = os.getenv("DISPATCH_REFRESH_BEFORE_SEND", "false").strip().lower() in {
            "1", "true", "yes", "on"
        }
        if legacy_refresh:
            LOG.warning("DISPATCH_REFRESH_BEFORE_SEND=true: regenerating reports inline (slow)")
            if not refresh_reports(outputs_dir):
                raise RuntimeError("Report refresh failed; aborting dispatch")
        else:
            download_outputs_from_blob(outputs_dir)
        report_date = derive_report_date(outputs_dir)

        # HTML -> email body
        html_files = collect_html_files(outputs_dir)
        if not html_files:
            LOG.warning("No HTML report files found in %s", outputs_dir)
            return func.HttpResponse("No HTML files found", status_code=400) if req else None
        LOG.info(
            "HTML body files (%d): %s", len(html_files), [p.name for p in html_files]
        )

        plain_intro = os.getenv(
            "REPORT_DISPATCH_BODY",
            "Please find the latest QMS sales data attached.",
        )
        body_type, body_content = build_html_body(
            html_files, plain_intro,
            banner_title=report_period_banner("Management Report", report_date),
            section_title_resolver=lambda path, title: _management_section_title(path, title, report_date),
            banner_subtitle=report_period_summary(report_date),
        )

        # CSVs -> attachments
        attachments = collect_csv_attachments(outputs_dir)
        if not attachments:
            LOG.warning("No CSV files found to attach from %s", outputs_dir)
        else:
            LOG.info(
                "CSV attachments (%d): %s", len(attachments), [p.name for p in attachments]
            )

        subject = _build_subject(report_date)

        try:
            send_via_graph(recipients, attachments, body_content, subject, body_type)
        except Exception as exc:  # pylint: disable=broad-except
            LOG.exception("Graph report dispatch failed: %s", exc)
            raise
    except Exception as exc:  # pylint: disable=broad-except
        send_healthcheck_alert("dispatch_reports", exc)
        raise
