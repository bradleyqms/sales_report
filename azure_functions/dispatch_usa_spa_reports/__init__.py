"""Timer-triggered Azure Function — USA Spa report dispatch.

Sends the USA Spa HTML report (inline) to the recipients configured via
USA_SPA_DISPATCH_RECIPIENTS.

Shares all submodules with dispatch_reports but:
  - reads USA_SPA_DISPATCH_RECIPIENTS (not REPORT_DISPATCH_RECIPIENTS)
  - collects only management_report_usa_spa_*.html for the email body
  - no PDF attachment (HTML inline only)
  - subject: "QMS USA Spa Sales Report DD.MM.YYYY"
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import azure.functions as func
from dotenv import load_dotenv

from dispatch_reports.config import (
    USA_SPA_HTML_PATTERNS,
    parse_pattern_env,
    parse_recipients,
    report_date_str,
    dispatch_report_mode,
    report_period_banner,
    report_period_summary,
)
from dispatch_reports.graph_client import send_via_graph
from dispatch_reports.health_alerts import send_healthcheck_alert
from dispatch_reports.html_builder import build_html_body
from dispatch_reports.report_collector import find_files, refresh_reports, resolve_outputs_path, derive_report_date

load_dotenv()

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def _collect_usa_spa_html(outputs_dir: Path) -> list[Path]:
    patterns = parse_pattern_env("USA_SPA_HTML_PATTERNS", USA_SPA_HTML_PATTERNS)
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
    try:
        outputs_dir = resolve_outputs_path()

        _test_recip = os.getenv("TEST_USA_SPA_RECIPIENTS", "").strip()
        if _test_recip:
            LOG.info("TEST mode: overriding recipients with TEST_USA_SPA_RECIPIENTS")
            recipients = parse_recipients(_test_recip)
        else:
            recipients = parse_recipients(os.getenv("USA_SPA_DISPATCH_RECIPIENTS"))
        if not recipients:
            LOG.warning("No recipients configured (USA_SPA_DISPATCH_RECIPIENTS is empty)")
            return

        refresh_before_send = os.getenv("USA_SPA_REFRESH_BEFORE_SEND", "false").strip().lower() in {
            "1", "true", "yes", "on"
        }
        if refresh_before_send:
            refreshed = refresh_reports(outputs_dir)
            if not refreshed:
                raise RuntimeError("Report refresh failed; aborting USA Spa dispatch")
        else:
            LOG.info("USA_SPA_REFRESH_BEFORE_SEND disabled; sending from existing outputs")
        report_date = derive_report_date(outputs_dir)

        html_files = _collect_usa_spa_html(outputs_dir)
        if not html_files:
            LOG.warning("No USA Spa HTML files found in %s — skipping", outputs_dir)
            return
        LOG.info("USA Spa HTML files (%d): %s", len(html_files), [p.name for p in html_files])

        plain_intro = os.getenv(
            "USA_SPA_DISPATCH_BODY",
            "Please find the latest QMS USA Spa sales report below.",
        )
        body_type, body_content = build_html_body(
            html_files,
            plain_intro,
            banner_title=report_period_banner("USA Spa Sales Report", report_date),
            footer_note="",
            section_title_resolver=lambda _path, _title: report_period_banner("USA Spa Sales Report", report_date),
            banner_subtitle=report_period_summary(report_date),
        )

        _report_mode = dispatch_report_mode()
        _date_str = report_date.strftime('%d.%m.%Y') if report_date else report_date_str()
        _default_subject = (
            f"EOM QMS USA Spa Sales Report {_date_str}"
            if _report_mode == "EOM"
            else f"QMS USA Spa Sales Report {_date_str}"
        )
        subject = os.getenv("USA_SPA_DISPATCH_SUBJECT") or _default_subject

        try:
            send_via_graph(recipients, [], body_content, subject, body_type)
        except Exception as exc:  # pylint: disable=broad-except
            LOG.exception("Graph USA Spa dispatch failed: %s", exc)
            raise
    except Exception as exc:  # pylint: disable=broad-except
        send_healthcheck_alert("dispatch_usa_spa_reports", exc)
        raise
