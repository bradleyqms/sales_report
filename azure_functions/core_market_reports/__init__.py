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
)


from dispatch_reports.graph_client import send_via_graph
from dispatch_reports.html_builder import build_html_body
from dispatch_reports.report_collector import find_files, refresh_reports, resolve_outputs_path

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

def main(mytimer: func.TimerRequest) -> None:
    outputs_dir = resolve_outputs_path()
    _test_recip = os.getenv("TEST_CORE_MARKETS_RECIPIENTS", "").strip()
    if _test_recip:
        LOG.info("TEST mode: overriding recipients with TEST_CORE_MARKETS_RECIPIENTS")
        recipients = parse_recipients(_test_recip)
    else:
        recipients = parse_recipients(os.getenv("CORE_MARKET_DISPATCH_RECIPIENTS"))
    if not recipients:
        LOG.warning("No recipients configured (CORE_MARKET_DISPATCH_RECIPIENTS is empty)")
        return

    refresh_reports(outputs_dir)

    # HTML → email body
    html_files = _collect_core_market_html(outputs_dir)
    if not html_files:
        LOG.warning("No core market HTML files found in %s", outputs_dir)
        return
    LOG.info("Core market HTML files (%d): %s", len(html_files), [p.name for p in html_files])

    plain_intro = os.getenv(
        "CORE_MARKET_DISPATCH_BODY",
        "Please find the latest QMS core market report attached.",
    )
    body_type, body_content = build_html_body(
        html_files, plain_intro,
        banner_title="Core Market Sales Report",
        footer_note="The PDF report is attached.",
    )

    # PDF → attachment
    attachments = _collect_core_market_pdf(outputs_dir)
    if not attachments:
        LOG.warning("No core market PDF files found in %s — sending without attachment", outputs_dir)
    else:
        LOG.info("Core market PDF attachments (%d): %s", len(attachments), [p.name for p in attachments])

    subject = os.getenv("CORE_MARKET_DISPATCH_SUBJECT") or f"QMS Core Market Sales Report {report_date_str()}"

    try:
        send_via_graph(recipients, attachments, body_content, subject, body_type)
    except Exception as exc:  # pylint: disable=broad-except
        LOG.exception("Graph core market dispatch failed: %s", exc)
        raise
