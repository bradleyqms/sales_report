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
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import azure.functions as func
from dotenv import load_dotenv

from dispatch_reports.config import (
    USA_SPA_HTML_PATTERNS,
    parse_int_env,
    parse_recipients,
    report_date_str,
)
from dispatch_reports.graph_client import send_via_graph
from dispatch_reports.html_builder import build_html_body
from dispatch_reports.report_collector import find_files, resolve_outputs_path

load_dotenv()

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REFRESH_SCRIPT = REPO_ROOT / "src" / "full_report.py"


def _parse_pattern_env(env_var: str, default_patterns: list[str]) -> list[str]:
    raw = os.getenv(env_var)
    if raw is None:
        return default_patterns
    return [p.strip() for p in raw.split(";") if p.strip()]


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


def _collect_usa_spa_html(outputs_dir: Path) -> list[Path]:
    patterns = _parse_pattern_env("USA_SPA_HTML_PATTERNS", USA_SPA_HTML_PATTERNS)
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

    _test_recip = os.getenv("TEST_USA_SPA_RECIPIENTS", "").strip()
    if _test_recip:
        LOG.info("TEST mode: overriding recipients with TEST_USA_SPA_RECIPIENTS")
        recipients = parse_recipients(_test_recip)
    else:
        recipients = parse_recipients(os.getenv("USA_SPA_DISPATCH_RECIPIENTS"))
    if not recipients:
        LOG.warning("No recipients configured (USA_SPA_DISPATCH_RECIPIENTS is empty)")
        return

    _refresh_reports(outputs_dir)

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
        banner_title="USA Spa Sales Report",
        footer_note="",
    )

    subject = os.getenv(
        "USA_SPA_DISPATCH_SUBJECT",
        f"QMS USA Spa Sales Report {report_date_str()}",
    )

    try:
        send_via_graph(recipients, [], body_content, subject, body_type)
    except Exception as exc:  # pylint: disable=broad-except
        LOG.exception("Graph USA Spa dispatch failed: %s", exc)
        raise
