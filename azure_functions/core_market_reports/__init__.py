"""Timer-triggered Azure Function — core market report dispatch.

Sends the core markets HTML report (inline) and PDF (attachment) to the
recipients configured via CORE_MARKET_DISPATCH_RECIPIENTS.

Shares all submodules with dispatch_reports but:
  - reads CORE_MARKET_DISPATCH_RECIPIENTS (not REPORT_DISPATCH_RECIPIENTS)
  - collects only management_report_core_markets_*.html for the email body
  - attaches only management_report_core_markets_*.pdf
  - subject: "QMS Core Market Report DD.MM.YYYY"
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import azure.functions as func
from dotenv import load_dotenv

# Import shared submodules from the sibling dispatch_reports package.
# On Azure: both functions live under /home/site/wwwroot/ so the relative
# import resolves correctly via the package name.
from dispatch_reports.config import (
    report_date_str,
    CORE_MARKET_HTML_PATTERNS,
    CORE_MARKET_PDF_PATTERNS,
    parse_pattern_env,
    parse_recipients,
    dispatch_report_mode,
    report_period_banner,
    report_period_summary,
)


from dispatch_reports.graph_client import send_via_graph
from dispatch_reports.health_alerts import send_healthcheck_alert
from dispatch_reports.html_builder import build_html_body
from dispatch_reports.report_collector import find_files, download_outputs_from_blob, resolve_outputs_path, derive_report_date
from dispatch_reports.send_audit import record_dispatch_status

load_dotenv()

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def _collect_core_market_html(outputs_dir: Path) -> list[Path]:
    patterns = parse_pattern_env("CORE_MARKET_HTML_PATTERNS", CORE_MARKET_HTML_PATTERNS)
    seen: set[Path] = set()
    result: list[Path] = []
    for pattern in patterns:
        for f in find_files(outputs_dir, pattern, 1):
            resolved = f.resolve()
            if resolved not in seen:
                result.append(f)
                seen.add(resolved)
    return result


def _collect_core_market_pdf(outputs_dir: Path) -> list[Path]:
    # CORE_MARKET_SEND_PDF=false|0|no disables the attachment entirely.
    # Defaults to enabled when the setting is absent.
    send_pdf_raw = os.getenv("CORE_MARKET_SEND_PDF", "true").strip().lower()
    LOG.info("CORE_MARKET_SEND_PDF raw env = %r", send_pdf_raw)
    if send_pdf_raw in ("false", "0", "no"):
        LOG.info("PDF attachment disabled via CORE_MARKET_SEND_PDF=%s", send_pdf_raw)
        return []

    patterns_raw = os.getenv("CORE_MARKET_PDF_PATTERNS")
    LOG.info("CORE_MARKET_PDF_PATTERNS raw env = %r", patterns_raw)
    patterns = parse_pattern_env("CORE_MARKET_PDF_PATTERNS", CORE_MARKET_PDF_PATTERNS)
    LOG.info("CORE_MARKET_PDF_PATTERNS resolved = %r", patterns)

    seen: set[Path] = set()
    result: list[Path] = []
    for pattern in patterns:
        for f in find_files(outputs_dir, pattern, 1):
            resolved = f.resolve()
            if resolved not in seen:
                result.append(f)
                seen.add(resolved)
    return result


# ---- Azure Functions entry point ----------------------------------------

def main(mytimer: func.TimerRequest = None) -> None:
    outputs_dir: Path | None = None
    recipients: list[str] = []
    subject = "QMS Core Market Sales Report"
    body_type = "HTML"
    html_files: list[Path] = []
    attachments: list[Path] = []
    report_date = None
    try:
        outputs_dir = resolve_outputs_path()
        LOG.info(
            "[DATA] core_market_reports starting: is_past_due=%s outputs_dir=%s",
            getattr(mytimer, "past_due", False), outputs_dir,
        )
        _test_recip = os.getenv("TEST_CORE_MARKETS_RECIPIENTS", "").strip()
        if _test_recip:
            LOG.info("TEST mode: overriding recipients with TEST_CORE_MARKETS_RECIPIENTS")
            recipients = parse_recipients(_test_recip)
        else:
            recipients = parse_recipients(os.getenv("CORE_MARKET_DISPATCH_RECIPIENTS"))
        LOG.info(
            "[DATA] core_market_reports recipients: mode=%s count=%d",
            "TEST" if _test_recip else "PRODUCTION",
            len(recipients),
        )
        if not recipients:
            LOG.warning("No recipients configured (CORE_MARKET_DISPATCH_RECIPIENTS is empty)")
            if outputs_dir is not None:
                record_dispatch_status(
                    outputs_dir=outputs_dir,
                    stream="core_market",
                    status="skipped",
                    recipients=[],
                    subject=subject,
                    body_type=body_type,
                    report_date=report_date,
                    mode=dispatch_report_mode(),
                    details={"reason": "no_recipients_configured"},
                )
            return

        # Option B: hydrate latest artefacts from the reporting-outputs blob
        # (refresh_unified_v2_timer is responsible for producing them).
        _downloaded = download_outputs_from_blob(outputs_dir)
        _local_file_count = (
            sum(1 for path in outputs_dir.rglob("*") if path.is_file())
            if outputs_dir and outputs_dir.exists()
            else 0
        )
        LOG.info(
            "[DATA] core_market_reports outputs hydrated: downloaded_count=%d local_file_count=%d",
            _downloaded,
            _local_file_count,
        )
        report_date = derive_report_date(outputs_dir)

        # HTML → email body
        html_files = _collect_core_market_html(outputs_dir)
        if not html_files:
            LOG.warning("No core market HTML files found in %s", outputs_dir)
            record_dispatch_status(
                outputs_dir=outputs_dir,
                stream="core_market",
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
            "[DATA] core_market_reports html_files: count=%d names=%s",
            len(html_files), [p.name for p in html_files]
        )

        plain_intro = os.getenv(
            "CORE_MARKET_DISPATCH_BODY",
            "Please find the latest QMS core market report attached.",
        )
        body_type, body_content = build_html_body(
            html_files, plain_intro,
            banner_title=report_period_banner("Core Market Sales Report", report_date),
            footer_note="The PDF report is attached.",
            section_title_resolver=lambda _path, _title: report_period_banner("Core Market Sales Report", report_date),
            banner_subtitle=report_period_summary(report_date),
        )

        # PDF → attachment
        attachments = _collect_core_market_pdf(outputs_dir)
        if not attachments:
            LOG.warning("No core market PDF files found in %s — sending without attachment", outputs_dir)
        else:
            LOG.info(
                "[DATA] core_market_reports pdf_attachments: count=%d names=%s",
                len(attachments), [p.name for p in attachments]
            )

        _report_mode = dispatch_report_mode()
        _date_str = report_date.strftime('%d.%m.%Y') if report_date else report_date_str()
        _default_subject = (
            f"EOM QMS Core Market Sales Report {_date_str}"
            if _report_mode == "EOM"
            else f"QMS Core Market Sales Report {_date_str}"
        )
        subject = os.getenv("CORE_MARKET_DISPATCH_SUBJECT") or _default_subject

        try:
            send_via_graph(recipients, attachments, body_content, subject, body_type)
            LOG.info(
                "[DATA] core_market_reports email sent: recipients=%d subject=%r attachments=%d",
                len(recipients), subject, len(attachments),
            )
            location = record_dispatch_status(
                outputs_dir=outputs_dir,
                stream="core_market",
                status="sent",
                recipients=recipients,
                subject=subject,
                body_type=body_type,
                report_date=report_date,
                mode=dispatch_report_mode(),
                html_files=html_files,
                attachments=attachments,
            )
            LOG.info("Core market dispatch send-status stored: %s", location)
        except Exception as exc:  # pylint: disable=broad-except
            location = record_dispatch_status(
                outputs_dir=outputs_dir,
                stream="core_market",
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
            LOG.info("Core market dispatch failure-status stored: %s", location)
            LOG.exception("Graph core market dispatch failed: %s", exc)
            raise
    except Exception as exc:  # pylint: disable=broad-except
        send_healthcheck_alert("core_market_reports", exc)
        raise
