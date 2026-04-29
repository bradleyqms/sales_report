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
from .send_audit import record_dispatch_status

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
    outputs_dir: Path | None = None
    recipients: list[str] = []
    subject = "QMS Management Sales Report"
    body_type = "HTML"
    html_files: list[Path] = []
    attachments: list[Path] = []
    report_date = None
    try:
        outputs_dir = resolve_outputs_path()
        LOG.info(
            "[DATA] dispatch_reports starting: is_past_due=%s outputs_dir=%s",
            getattr(mytimer, "past_due", False), outputs_dir,
        )
        _test_recip = os.getenv("TEST_REPORT_DISPATCH_RECIPIENTS", "").strip()
        if _test_recip:
            LOG.info("TEST mode: overriding recipients with TEST_REPORT_DISPATCH_RECIPIENTS")
            recipients = parse_recipients(_test_recip)
        else:
            recipients = parse_recipients(os.getenv("REPORT_DISPATCH_RECIPIENTS"))
        LOG.info(
            "[DATA] dispatch_reports recipients: mode=%s count=%d to=%s",
            "TEST" if _test_recip else "PRODUCTION",
            len(recipients),
            ";".join(recipients),
        )
        if not recipients:
            LOG.warning("No recipients configured for report dispatch")
            if outputs_dir is not None:
                record_dispatch_status(
                    outputs_dir=outputs_dir,
                    stream="management",
                    status="skipped",
                    recipients=[],
                    subject=subject,
                    body_type=body_type,
                    report_date=report_date,
                    mode=dispatch_report_mode(),
                    details={"reason": "no_recipients_configured"},
                )
            return

        # Option B: dispatch is a pure consumer.  refresh_unified_v2_timer
        # has already run full_report_v2.py and uploaded the artefacts to
        # the reporting-outputs blob; we just hydrate the local working
        # directory from blob and send what we find.
        download_outputs_from_blob(outputs_dir)
        LOG.info(
            "[DATA] dispatch_reports outputs hydrated: file_count=%d",
            sum(1 for _f in outputs_dir.glob("*") if _f.is_file()) if outputs_dir and outputs_dir.exists() else 0,
        )
        report_date = derive_report_date(outputs_dir)

        # HTML -> email body
        html_files = collect_html_files(outputs_dir)
        if not html_files:
            LOG.warning("No HTML report files found in %s", outputs_dir)
            record_dispatch_status(
                outputs_dir=outputs_dir,
                stream="management",
                status="skipped",
                recipients=recipients,
                subject=subject,
                body_type=body_type,
                report_date=report_date,
                mode=dispatch_report_mode(),
                html_files=[],
                details={"reason": "no_html_files"},
            )
            return
        LOG.info(
            "[DATA] dispatch_reports html_files: count=%d names=%s",
            len(html_files), [p.name for p in html_files]
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
                "[DATA] dispatch_reports csv_attachments: count=%d names=%s",
                len(attachments), [p.name for p in attachments]
            )

        subject = _build_subject(report_date)

        try:
            send_via_graph(recipients, attachments, body_content, subject, body_type)
            LOG.info(
                "[DATA] dispatch_reports email sent: recipients=%d subject=%r attachments=%d",
                len(recipients), subject, len(attachments),
            )
            location = record_dispatch_status(
                outputs_dir=outputs_dir,
                stream="management",
                status="sent",
                recipients=recipients,
                subject=subject,
                body_type=body_type,
                report_date=report_date,
                mode=dispatch_report_mode(),
                html_files=html_files,
                attachments=attachments,
            )
            LOG.info("Dispatch send-status stored: %s", location)
        except Exception as exc:  # pylint: disable=broad-except
            location = record_dispatch_status(
                outputs_dir=outputs_dir,
                stream="management",
                status="failed",
                recipients=recipients,
                subject=subject,
                body_type=body_type,
                report_date=report_date,
                mode=dispatch_report_mode(),
                html_files=html_files,
                attachments=attachments,
                error=str(exc),
            )
            LOG.info("Dispatch failure-status stored: %s", location)
            LOG.exception("Graph report dispatch failed: %s", exc)
            raise
    except Exception as exc:  # pylint: disable=broad-except
        send_healthcheck_alert("dispatch_reports", exc)
        raise
