"""Timer-triggered Azure Function -- thin entrypoint delegating to submodules."""
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

from .config import parse_int_env, parse_recipients
from .graph_client import acquire_graph_token, send_via_graph
from .html_builder import build_html_body
from .report_collector import collect_csv_attachments, collect_html_files, resolve_outputs_path

load_dotenv()

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# On Azure: __file__ = /home/site/wwwroot/dispatch_reports/__init__.py
# parents[0] = dispatch_reports/,  parents[1] = wwwroot/  ← package root
# Locally:   parents[1] = azure_functions/, parents[2] = repo root
# REPORT_DISPATCH_REFRESH_COMMAND overrides this entirely if set.
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REFRESH_SCRIPT = REPO_ROOT / "src" / "full_report.py"

# Backwards-compatible shims used by test_dispatch_local.py and tests
_resolve_outputs_path = resolve_outputs_path
_parse_recipients = parse_recipients
_collect_html_files = collect_html_files
_collect_csv_attachments = collect_csv_attachments
_build_html_body = build_html_body
_send_via_graph = send_via_graph
_acquire_graph_token = acquire_graph_token


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


def _refresh_reports() -> bool:
    command = _build_refresh_command()
    if not command:
        return False
    timeout = max(30, parse_int_env("REPORT_DISPATCH_REFRESH_TIMEOUT_SECONDS", 1800))
    LOG.info("Refreshing reports with command: %s", " ".join(command))

    # Propagate the current sys.path so the subprocess sees Oryx-installed
    # packages (pandas etc.) which live outside the default site-packages.
    env = os.environ.copy()
    inherited = os.pathsep.join(p for p in sys.path if p)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{inherited}{os.pathsep}{existing}".strip(os.pathsep) if existing else inherited
    )

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


# ---- Azure Functions entry point ----------------------------------------

def main(mytimer: func.TimerRequest) -> None:
    outputs_dir = resolve_outputs_path()
    recipients = parse_recipients(os.getenv("REPORT_DISPATCH_RECIPIENTS"))
    if not recipients:
        LOG.warning("No recipients configured for report dispatch")
        return

    _refresh_reports()

    # HTML -> email body
    html_files = collect_html_files(outputs_dir)
    if not html_files:
        LOG.warning("No HTML report files found in %s", outputs_dir)
        return
    LOG.info(
        "HTML body files (%d): %s", len(html_files), [p.name for p in html_files]
    )

    plain_intro = os.getenv(
        "REPORT_DISPATCH_BODY",
        "Please find the latest QMS sales data attached.",
    )
    body_type, body_content = build_html_body(html_files, plain_intro)

    # CSVs -> attachments
    attachments = collect_csv_attachments(outputs_dir)
    if not attachments:
        LOG.warning("No CSV files found to attach from %s", outputs_dir)
    else:
        LOG.info(
            "CSV attachments (%d): %s", len(attachments), [p.name for p in attachments]
        )

    subject = os.getenv(
        "REPORT_DISPATCH_SUBJECT",
        f"QMS Sales Reports {datetime.utcnow():%Y-%m-%d %H:%M UTC}",
    )

    try:
        send_via_graph(recipients, attachments, body_content, subject, body_type)
    except Exception as exc:  # pylint: disable=broad-except
        LOG.exception("Graph report dispatch failed: %s", exc)
        raise
