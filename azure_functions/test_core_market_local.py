"""
Standalone local test for the core_market_reports dispatch logic.
Runs outside Azure Functions runtime (no storage, no timer trigger).
Usage:
    python test_core_market_local.py [--skip-refresh] [--skip-send]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

_HERE = Path(__file__).parent
_PKG = _HERE / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

# Load local.settings.json into env
_settings_file = _HERE / "local.settings.json"
if _settings_file.exists():
    _settings = json.loads(_settings_file.read_text(encoding="utf-8"))
    for _k, _v in _settings.get("Values", {}).items():
        if _k not in os.environ:
            os.environ[_k] = str(_v)

import importlib.util

_init_path = _HERE / "core_market_reports" / "__init__.py"
_spec = importlib.util.spec_from_file_location("core_market_reports", _init_path)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)                  # type: ignore[union-attr]

_collect_html         = _mod._collect_core_market_html
_collect_pdf          = _mod._collect_core_market_pdf

from dispatch_reports.config   import parse_recipients, report_date_str
from dispatch_reports.graph_client import send_via_graph, acquire_graph_token
from dispatch_reports.html_builder import build_html_body
from dispatch_reports.report_collector import refresh_reports as _refresh, resolve_outputs_path, derive_report_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("test_core_market_local")


def section(title: str) -> None:
    LOG.info("=" * 60)
    LOG.info("  %s", title)
    LOG.info("=" * 60)


def run_test(skip_refresh: bool, skip_send: bool) -> None:
    section("1 / CONFIG")
    outputs_dir = resolve_outputs_path()
    _test_recip = os.getenv("TEST_CORE_MARKETS_RECIPIENTS", "").strip()
    if _test_recip:
        LOG.info("TEST mode — overriding recipients")
        recipients = parse_recipients(_test_recip)
    else:
        recipients = parse_recipients(os.getenv("CORE_MARKET_DISPATCH_RECIPIENTS"))
    sender  = os.getenv("REPORT_DISPATCH_GRAPH_SENDER")
    LOG.info("outputs_dir : %s", outputs_dir)
    LOG.info("recipients  : %s", recipients)
    LOG.info("sender      : %s", sender)
    if not recipients:
        LOG.error("No recipients — cannot continue")
        sys.exit(1)

    section("2 / REFRESH")
    if skip_refresh:
        LOG.info("Skipping refresh (--skip-refresh)")
    else:
        _refresh(outputs_dir)

    report_date = derive_report_date(outputs_dir)
    subject = os.getenv("CORE_MARKET_DISPATCH_SUBJECT") or (
        f"EOM QMS Core Market Sales Report {report_date.strftime('%d.%m.%Y')}"
        if report_date else f"EOM QMS Core Market Sales Report {report_date_str()}"
    )
    LOG.info("report_date : %s", report_date.strftime("%Y-%m-%d") if report_date else "N/A")
    LOG.info("subject     : %s", subject)

    section("3 / HTML BODY")
    html_files = _collect_html(outputs_dir)
    if not html_files:
        LOG.error("No core market HTML files found in %s", outputs_dir)
        sys.exit(1)
    LOG.info("Found %d HTML file(s): %s", len(html_files), [p.name for p in html_files])
    plain_intro = os.getenv(
        "CORE_MARKET_DISPATCH_BODY",
        "Please find the latest QMS core market report attached.",
    )
    body_type, body_content = build_html_body(
        html_files, plain_intro,
        banner_title="Core Market Sales Report",
        footer_note="The PDF report is attached.",
    )
    LOG.info("Body type: %s  (%d chars)", body_type, len(body_content))

    section("3b / PDF ATTACHMENT")
    attachments = _collect_pdf(outputs_dir)
    LOG.info("PDF attachments: %d", len(attachments))
    for p in attachments:
        LOG.info("  %s  (%d KB)", p.name, p.stat().st_size // 1024)

    section("4 / GRAPH TOKEN")
    token = acquire_graph_token()
    if token:
        LOG.info("Token acquired OK: %s...%s", token[:8], token[-8:])
    else:
        LOG.error("Token acquisition FAILED")
        if not skip_send:
            sys.exit(1)

    section("5 / SEND")
    if skip_send:
        LOG.info("Skipping send (--skip-send)")
    else:
        LOG.info("Sending to %s …", recipients)
        send_via_graph(recipients, attachments, body_content, subject, body_type)
        LOG.info("✓ Email sent successfully")

    section("DONE")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-refresh", action="store_true")
    parser.add_argument("--skip-send",    action="store_true")
    args = parser.parse_args()
    run_test(skip_refresh=args.skip_refresh, skip_send=args.skip_send)


if __name__ == "__main__":
    main()
