from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import azure.functions as func

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def _in_refresh_window(now_utc: datetime) -> bool:
    """Belt-and-braces guard against accidental out-of-hours runs.

    The cron expression in ``V2_UNIFIED_REFRESH_SCHEDULE`` is the primary
    schedule control.  This helper just refuses to do work on weekends or
    well outside business hours, regardless of how the schedule is set.
    Set ``V2_UNIFIED_REFRESH_DISABLE_WINDOW=true`` to bypass entirely.
    """
    if os.getenv("V2_UNIFIED_REFRESH_DISABLE_WINDOW", "false").strip().lower() in {
        "1", "true", "yes", "on"
    }:
        return True

    tz_name = os.getenv("V2_UNIFIED_REFRESH_TIMEZONE", "Europe/Berlin")
    local_now = now_utc.astimezone(ZoneInfo(tz_name))

    # Monday=0 ... Sunday=6; business days and 06:00–20:00 local time.
    if local_now.weekday() > 4:
        return False
    if local_now.hour < 6 or local_now.hour > 20:
        return False
    return True


def _build_command(repo_root: Path) -> list[str]:
    configured = os.getenv("V2_UNIFIED_REFRESH_COMMAND", "").strip()
    if configured:
        return shlex.split(configured)

    script_path = repo_root / "src" / "full_report_v2.py"
    report_type = os.getenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD").strip().upper() or "MTD"

    cmd = [
        sys.executable,
        str(script_path),
        "--report-type",
        report_type,
    ]

    output_tag = os.getenv("V2_UNIFIED_REFRESH_OUTPUT_TAG", "function-timer").strip()
    if output_tag:
        cmd.extend(["--output-tag", output_tag])

    schema_mode = os.getenv("V2_UNIFIED_SCHEMA_MODE", "strict").strip()
    if schema_mode:
        cmd.extend(["--schema-mode", schema_mode])

    if os.getenv("V2_UNIFIED_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "on"}:
        cmd.append("--dry-run")

    return cmd


def main(mytimer: func.TimerRequest) -> None:
    now_utc = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

    if not _in_refresh_window(now_utc):
        LOG.info("Outside configured refresh window; skipping this tick")
        return

    repo_root = Path(__file__).resolve().parents[1]
    command = _build_command(repo_root)
    timeout_seconds = int(os.getenv("V2_UNIFIED_REFRESH_TIMEOUT_SECONDS", "1800"))

    env = os.environ.copy()
    outputs_path = os.getenv("V2_UNIFIED_REFRESH_OUTPUTS_PATH", "").strip()
    if outputs_path:
        env["REPORT_OUTPUT_DIR"] = outputs_path

    LOG.info("Running V2 refresh command: %s", " ".join(command))
    result = subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )

    if result.returncode != 0:
        LOG.error("V2 refresh failed (exit=%s)", result.returncode)
        if result.stderr:
            LOG.error("stderr: %s", result.stderr.strip())
        raise RuntimeError(f"V2 refresh failed with exit code {result.returncode}")

    if result.stdout:
        lines = [line for line in result.stdout.strip().splitlines() if line.strip()]
        if lines:
            LOG.info("V2 refresh completed. Tail output:\n%s", "\n".join(lines[-8:]))
