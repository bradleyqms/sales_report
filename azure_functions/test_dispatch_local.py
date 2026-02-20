"""
Standalone local test for the dispatch_reports logic.
Runs outside Azure Functions runtime (no storage, no timer trigger).
Usage:
    python test_dispatch_local.py [--skip-refresh] [--skip-send] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: make sure .python_packages is on sys.path so msal / requests
# resolve the same way func start does.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent
_PKG = _HERE / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

# ---------------------------------------------------------------------------
# Load local.settings.json Values into os.environ so the function module
# picks them up (func start does this automatically; plain Python does not).
# ---------------------------------------------------------------------------
_settings_file = _HERE / "local.settings.json"
if _settings_file.exists():
    _settings = json.loads(_settings_file.read_text(encoding="utf-8"))
    for _k, _v in _settings.get("Values", {}).items():
        if _k not in os.environ:          # don't override real env vars
            os.environ[_k] = str(_v)

# ---------------------------------------------------------------------------
# Pull in the real function module (same code func start runs)
# ---------------------------------------------------------------------------
import importlib.util, types

_init_path = _HERE / "dispatch_reports" / "__init__.py"
_spec = importlib.util.spec_from_file_location("dispatch_reports", _init_path)
_mod = importlib.util.module_from_spec(_spec)      # type: ignore[arg-type]
_spec.loader.exec_module(_mod)                      # type: ignore[union-attr]

# Expose everything from the module as local names for easy access
_refresh_reports      = _mod._refresh_reports
_collect_html_files   = _mod._collect_html_files
_collect_csv_attachments = _mod._collect_csv_attachments
_build_html_body      = _mod._build_html_body
_acquire_graph_token  = _mod._acquire_graph_token
_send_via_graph       = _mod._send_via_graph
_resolve_outputs_path = _mod._resolve_outputs_path
_parse_recipients     = _mod._parse_recipients

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("test_dispatch_local")


def section(title: str) -> None:
    LOG.info("=" * 60)
    LOG.info("  %s", title)
    LOG.info("=" * 60)


def run_test(skip_refresh: bool, skip_send: bool, dry_run: bool) -> None:
    # ── 1. Config ──────────────────────────────────────────────────────────
    section("1 / CONFIG")
    outputs_dir = _resolve_outputs_path()
    recipients  = _parse_recipients(os.getenv("REPORT_DISPATCH_RECIPIENTS"))
    sender      = os.getenv("REPORT_DISPATCH_GRAPH_SENDER")
    subject     = os.getenv("REPORT_DISPATCH_SUBJECT", "QMS Sales Report [local test]")

    LOG.info("outputs_dir : %s", outputs_dir)
    LOG.info("recipients  : %s", recipients)
    LOG.info("sender      : %s", sender)
    LOG.info("subject     : %s", subject)

    if not recipients:
        LOG.error("REPORT_DISPATCH_RECIPIENTS is not set – cannot continue")
        sys.exit(1)

    # ── 2. Refresh ─────────────────────────────────────────────────────────
    section("2 / REFRESH REPORTS")
    if skip_refresh:
        LOG.info("Skipping refresh (--skip-refresh)")
    else:
        ok = _refresh_reports()
        LOG.info("Refresh %s", "succeeded" if ok else "skipped / failed (non-fatal)")

    # ── 3. Attachments ────────────────────────────────────────────────────
    section("3 / COLLECT HTML BODY FILES")
    html_files = _collect_html_files(outputs_dir)
    if not html_files:
        LOG.warning("No HTML files found – email body will be plain text")
    else:
        LOG.info("Found %d HTML body file(s):", len(html_files))
        for p in html_files:
            LOG.info("  %s  (%d KB)", p.name, p.stat().st_size // 1024)

    plain_intro = os.getenv("REPORT_DISPATCH_BODY", "Please find the latest QMS sales data attached.")
    body_type, body_content = _build_html_body(html_files, plain_intro)
    LOG.info("Email body contentType: %s  (%d chars)", body_type, len(body_content))

    section("3b / COLLECT CSV ATTACHMENTS")
    attachments = _collect_csv_attachments(outputs_dir)
    if not attachments:
        LOG.warning("No CSV attachments found in %s", outputs_dir)
    else:
        LOG.info("Found %d CSV attachment(s):", len(attachments))
        for p in attachments:
            LOG.info("  %s  (%d KB)", p.name, p.stat().st_size // 1024)

    # ── 4. Token ──────────────────────────────────────────────────────────
    section("4 / ACQUIRE GRAPH TOKEN")
    token = _acquire_graph_token()
    if token:
        # Don't log the full token – just show first/last 8 chars
        masked = token[:8] + "..." + token[-8:]
        LOG.info("Token acquired OK: %s", masked)
    else:
        LOG.error("Token acquisition FAILED – check GRAPH_* env vars")
        if not dry_run:
            sys.exit(1)

    # ── 5. Send ────────────────────────────────────────────────────────────
    section("5 / SEND VIA GRAPH")
    if skip_send or dry_run:
        LOG.info("Skipping actual send (%s)", "--skip-send" if skip_send else "--dry-run")
    elif not html_files:
        LOG.warning("No HTML body – skipping send")
    else:
        LOG.info("Sending to %s …", recipients)
        try:
            _send_via_graph(recipients, attachments, body_content, subject, body_type)
            LOG.info("✓ Email sent successfully")
        except Exception as exc:
            LOG.error("Send FAILED: %s", exc)
            sys.exit(1)

    # ── Summary ────────────────────────────────────────────────────────────
    section("RESULT")
    LOG.info("All checks passed.")
    if dry_run:
        LOG.info("(dry-run mode – no email was sent)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local dispatch test runner")
    parser.add_argument("--skip-refresh", action="store_true",
                        help="Skip running full_report.py (use existing outputs)")
    parser.add_argument("--skip-send",    action="store_true",
                        help="Collect attachments and acquire token but don't send")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Full dry run: no refresh, no send")
    args = parser.parse_args()

    if args.dry_run:
        args.skip_refresh = True
        args.skip_send    = True

    run_test(
        skip_refresh=args.skip_refresh,
        skip_send=args.skip_send,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
