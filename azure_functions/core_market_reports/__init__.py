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
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import azure.functions as func
from dotenv import load_dotenv

# Import shared submodules from the sibling dispatch_reports package.
# On Azure: both functions live under /home/site/wwwroot/ so the relative
# import resolves correctly via the package name.
from dispatch_reports.config import (
    CORE_MARKET_HTML_PATTERNS,
    CORE_MARKET_PDF_PATTERNS,
    parse_int_env,
    parse_recipients,
)


def _parse_pattern_env(env_var: str, default_patterns: list[str]) -> list[str]:
    """Read a semicolon-separated pattern list from *env_var*.

    - Env var absent  → use *default_patterns*
    - Env var empty   → return [] (disables the feature)
    - Env var set     → split on ';', strip whitespace, drop blanks
    """
    raw = os.getenv(env_var)
    if raw is None:
        return default_patterns
    return [p.strip() for p in raw.split(";") if p.strip()]
from dispatch_reports.graph_client import send_via_graph
from dispatch_reports.html_builder import build_html_body
from dispatch_reports.report_collector import find_files, resolve_outputs_path

load_dotenv()

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# On Azure: __file__ = /home/site/wwwroot/core_market_reports/__init__.py
# parents[0] = core_market_reports/,  parents[1] = wwwroot/ (package root)
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REFRESH_SCRIPT = REPO_ROOT / "src" / "full_report.py"


def _build_refresh_command() -> list[str] | None:
    raw = os.getenv("REPORT_DISPATCH_REFRESH_COMMAND")
    if raw is None:
        if not DEFAULT_REFRESH_SCRIPT.exists():
            LOG.warning(
                "Default refresh script %s is missing; skipping report refresh",
                DEFAULT_REFRESH_SCRIPT,
            )
            return None
        return [sys.executable, str(DEFAULT_REFRESH_SCRIPT)]
    trimmed = raw.strip()
    if not trimmed:
        LOG.info("REPORT_DISPATCH_REFRESH_COMMAND is empty; skipping report refresh")
        return None
    return shlex.split(trimmed)


def _refresh_reports(outputs_dir: Path) -> bool:
    command = _build_refresh_command()
    if not command:
        return False
    timeout = max(30, parse_int_env("REPORT_DISPATCH_REFRESH_TIMEOUT_SECONDS", 1800))
    LOG.info("Refreshing reports with command: %s", " ".join(command))

    env = os.environ.copy()
    inherited = os.pathsep.join(p for p in sys.path if p)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{inherited}{os.pathsep}{existing}".strip(os.pathsep) if existing else inherited
    )
    env["REPORT_OUTPUT_DIR"] = str(outputs_dir)

    try:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        LOG.error("Report refresh command timed out after %s seconds", timeout)
        return False
    except subprocess.CalledProcessError as exc:
        LOG.error(
            "Report refresh failed (exit %s). stderr=%s",
            exc.returncode,
            (exc.stderr or "").strip(),
        )
        return False
    snippet = (result.stdout or "").strip().splitlines()[-5:]
    if snippet:
        LOG.info("Report refresh output:\n%s", "\n".join(snippet))
    return True


def _collect_core_market_html(outputs_dir: Path) -> list[Path]:
    patterns = _parse_pattern_env("CORE_MARKET_HTML_PATTERNS", CORE_MARKET_HTML_PATTERNS)
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
    patterns = _parse_pattern_env("CORE_MARKET_PDF_PATTERNS", CORE_MARKET_PDF_PATTERNS)
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

    _refresh_reports(outputs_dir)

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

    subject = os.getenv(
        "CORE_MARKET_DISPATCH_SUBJECT",
        f"QMS Core Market Sales Report {datetime.utcnow():%d.%m.%Y}",
    )

    try:
        send_via_graph(recipients, attachments, body_content, subject, body_type)
    except Exception as exc:  # pylint: disable=broad-except
        LOG.exception("Graph core market dispatch failed: %s", exc)
        raise
