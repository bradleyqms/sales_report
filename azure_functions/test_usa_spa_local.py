"""
Standalone local test for the dispatch_usa_spa_reports logic.
Runs outside Azure Functions runtime (no storage, no timer trigger).
Usage:
    python test_usa_spa_local.py [--skip-send]
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

_init_path = _HERE / "dispatch_usa_spa_reports" / "__init__.py"
_spec = importlib.util.spec_from_file_location("dispatch_usa_spa_reports", _init_path)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)                  # type: ignore[union-attr]

_collect_usa_spa_html = _mod._collect_usa_spa_html

from dispatch_reports.config   import parse_recipients, report_date_str, dispatch_report_mode
from dispatch_reports.graph_client import send_via_graph, acquire_graph_token
from dispatch_reports.html_builder import build_html_body
from dispatch_reports.report_collector import resolve_outputs_path, derive_report_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("test_usa_spa_local")


def section(title: str) -> None:
    LOG.info("=" * 60)
    LOG.info("  %s", title)
    LOG.info("=" * 60)


def run_test(skip_send: bool) -> None:
    section("1 / CONFIG")
    outputs_dir = resolve_outputs_path()
    _test_recip = os.getenv("TEST_USA_SPA_RECIPIENTS", "").strip()
    if _test_recip:
        LOG.info("TEST mode — overriding recipients with TEST_USA_SPA_RECIPIENTS")
        recipients = parse_recipients(_test_recip)
    else:
        recipients = parse_recipients(os.getenv("USA_SPA_DISPATCH_RECIPIENTS"))
    sender = os.getenv("REPORT_DISPATCH_GRAPH_SENDER")

    LOG.info("outputs_dir : %s", outputs_dir)
    LOG.info("recipients  : %s", recipients)
    LOG.info("sender      : %s", sender)

    if not recipients:
        LOG.error("USA_SPA_DISPATCH_RECIPIENTS is not set – cannot continue")
        sys.exit(1)

    # ── 2. Derive report date ──────────────────────────────────────
    section("2 / REPORT DATE")
    report_date = derive_report_date(outputs_dir)
    LOG.info("report_date : %s (derived from Extract_Date)", report_date.strftime("%Y-%m-%d") if report_date else "N/A")

    # ── 3. Build subject ───────────────────────────────────────────
    section("3 / SUBJECT")
    report_mode = dispatch_report_mode()
    date_str = report_date.strftime('%d.%m.%Y') if report_date else report_date_str()
    subject = os.getenv("USA_SPA_DISPATCH_SUBJECT") or (
        f"EOM QMS USA Spa Sales Report {date_str}"
        if report_mode == "EOM"
        else f"QMS USA Spa Sales Report {date_str}"
    )
    LOG.info("subject     : %s", subject)

    # ── 4. Collect HTML ────────────────────────────────────────────
    section("4 / HTML BODY")
    html_files = _collect_usa_spa_html(outputs_dir)
    if not html_files:
        LOG.warning("No USA Spa HTML files found – email will be plain text")
        body_type = "Text"
        body_content = "No USA Spa sales report available."
    else:
        LOG.info("Found %d HTML file(s):", len(html_files))
        for p in html_files:
            LOG.info("  %s  (%d KB)", p.name, p.stat().st_size // 1024)

        plain_intro = os.getenv(
            "USA_SPA_DISPATCH_BODY",
            "Please find the latest QMS USA Spa sales report below.",
        )
        body_type, body_content = build_html_body(
            html_files, plain_intro,
            banner_title="USA Spa Sales Report",
        )
        LOG.info("Email body contentType: %s  (%d chars)", body_type, len(body_content))

    # ── 5. Token ───────────────────────────────────────────────────
    section("5 / ACQUIRE GRAPH TOKEN")
    token = acquire_graph_token()
    if token:
        masked = token[:8] + "..." + token[-8:]
        LOG.info("Token acquired OK: %s", masked)
    else:
        LOG.error("Token acquisition FAILED – check GRAPH_* env vars")
        sys.exit(1)

    # ── 6. Send ────────────────────────────────────────────────────
    section("6 / SEND VIA GRAPH")
    if skip_send:
        LOG.info("Skipping actual send (--skip-send)")
    else:
        LOG.info("Sending to %s …", recipients)
        try:
            send_via_graph(recipients, [], body_content, subject, body_type)
            LOG.info("✓ Email sent successfully")
        except Exception as exc:
            LOG.error("Send FAILED: %s", exc)
            sys.exit(1)

    # ── Summary ────────────────────────────────────────────────────
    section("RESULT")
    LOG.info("All checks passed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local USA Spa dispatch test runner")
    parser.add_argument("--skip-send", action="store_true",
                        help="Collect files but don't send")
    args = parser.parse_args()

    run_test(skip_send=args.skip_send)


if __name__ == "__main__":
    main()
