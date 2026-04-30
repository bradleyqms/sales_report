"""File-discovery helpers: locating HTML reports and CSV attachments."""
from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
import json
import re
from datetime import datetime, timezone
from pathlib import Path

try:
    from azure.storage.blob import BlobServiceClient as _BlobServiceClient
except ImportError:  # pragma: no cover
    _BlobServiceClient = None  # type: ignore[assignment,misc]

from .config import (
    dispatch_report_mode,
    KEY_CSV_PATTERNS,
    KEY_HTML_PATTERNS,
    parse_int_env,
    resolve_attachment_patterns,
)

LOG = logging.getLogger(__name__)

# Populated in __init__.py at import time
_REPO_ROOT: Path | None = None
_BLOB_INDEX_FILE = ".blob_index.json"


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _prefer_run_path() -> bool:
    # Prefer nested runs/report_type=.../run_id=... artifacts over stale top-level legacy files.
    return _env_flag("REPORT_DISPATCH_PREFER_RUN_PATH", True)


def _blob_index_path(outputs_dir: Path) -> Path:
    return outputs_dir / _BLOB_INDEX_FILE


def _load_blob_index(outputs_dir: Path) -> dict[str, dict]:
    index_path = _blob_index_path(outputs_dir)
    if not index_path.exists():
        return {}
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        entries = payload.get("entries")
        if isinstance(entries, dict):
            return entries
    except Exception as exc:
        LOG.warning("Could not parse blob index %s: %s", index_path, exc)
    return {}


def _store_blob_index(outputs_dir: Path, entries: dict[str, dict]) -> None:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "entries": entries,
    }
    try:
        _blob_index_path(outputs_dir).write_text(json.dumps(payload), encoding="utf-8")
    except Exception as exc:
        LOG.warning("Could not write blob index for %s: %s", outputs_dir, exc)


def _is_run_artifact(path: Path, outputs_dir: Path) -> bool:
    try:
        rel = path.relative_to(outputs_dir)
    except ValueError:
        return False
    return "runs" in rel.parts and any(part.startswith("run_id=") for part in rel.parts)


def _filename_timestamp_epoch(path: Path) -> float:
    match = re.search(r"_(\d{8}_\d{6})(?:\.[^.]+)?$", path.name)
    if not match:
        return 0.0
    try:
        return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return 0.0


def _blob_mtime_epoch(path: Path, outputs_dir: Path, blob_index: dict[str, dict]) -> float:
    try:
        rel_path = path.relative_to(outputs_dir).as_posix()
    except ValueError:
        rel_path = path.name
    meta = blob_index.get(rel_path, {})
    timestamp = meta.get("last_modified_epoch") if isinstance(meta, dict) else None
    if isinstance(timestamp, (int, float)):
        return float(timestamp)
    return path.stat().st_mtime


def _repo_root() -> Path:
    if _REPO_ROOT is not None:
        return _REPO_ROOT
    # __file__ = .../dispatch_reports/report_collector.py
    # parents[0] = dispatch_reports/, parents[1] = package root (wwwroot on Azure)
    return Path(__file__).resolve().parents[1]


def resolve_outputs_path() -> Path:
    # On Azure Consumption plan only /tmp is writable at runtime.
    # Set REPORT_DISPATCH_OUTPUTS_PATH in App Settings to override the default.
    configured = os.getenv("REPORT_DISPATCH_OUTPUTS_PATH")

    if configured:
        candidate = Path(configured) if Path(configured).is_absolute() else (_repo_root() / configured).resolve()
    else:
        # No explicit setting — prefer wwwroot/data/outputs (writable after Oryx build),
        # but fall back to /tmp/outputs if wwwroot is read-only (cold-start on Consumption plan).
        preferred = (_repo_root() / "data" / "outputs").resolve()
        candidate = preferred

    if not candidate.exists():
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            LOG.info("Created outputs directory: %s", candidate)
        except OSError:
            # wwwroot is read-only — fall back to /tmp/outputs which is always writable
            LOG.warning(
                "Cannot create outputs path %s (read-only?), falling back to /tmp/outputs",
                candidate,
            )
            candidate = Path("/tmp/outputs")
            candidate.mkdir(parents=True, exist_ok=True)

    return candidate


def _default_refresh_script() -> Path | None:
    """Resolve the built-in refresh script path across local/Azure layouts."""
    this_file = Path(__file__).resolve()
    candidates = [
        this_file.parents[1] / "src" / "full_report_v2.py",  # package-root layout
        this_file.parents[2] / "src" / "full_report_v2.py",  # repo-root layout
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def build_refresh_command() -> list[str] | None:
    """Return the shell command to regenerate reports, or None to skip.

    Reads REPORT_DISPATCH_REFRESH_COMMAND:
    - absent → run the default full_report.py (if it exists)
    - empty  → skip refresh
    - set    → shell-split and use as-is
    """
    raw = os.getenv("REPORT_DISPATCH_REFRESH_COMMAND")
    if raw is None:
        default_script = _default_refresh_script()
        if default_script is None:
            LOG.warning(
                "Default refresh script is missing; tried package-root and repo-root src/full_report_v2.py",
            )
            return None
        return [sys.executable, str(default_script)]
    trimmed = raw.strip()
    if not trimmed:
        LOG.info("REPORT_DISPATCH_REFRESH_COMMAND is empty; skipping report refresh")
        return None
    return shlex.split(trimmed)


def refresh_reports(outputs_dir: Path) -> bool:
    """Run the report generation command and return True on success."""
    command = build_refresh_command()
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

    repo_root = Path(__file__).resolve().parents[1]
    try:
        result = subprocess.run(
            command,
            cwd=repo_root,
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


def _check_outputs_freshness(outputs_dir: Path, stale_threshold_hours: float = 26.0) -> float | None:
    """Read run_summary.json from *outputs_dir* and log a [STALENESS] warning if stale.

    Returns age in hours, or None if the summary file is absent or unreadable.
    """
    summary_file = outputs_dir / "run_summary.json"
    if not summary_file.exists():
        LOG.info("[DATA] staleness_check: no run_summary.json found in %s", outputs_dir)
        return None
    try:
        data = json.loads(summary_file.read_text(encoding="utf-8"))
        generated_at = data.get("generated_at_utc") or data.get("finished_at")
        if not generated_at:
            LOG.warning("[STALENESS] run_summary.json has no generated_at_utc field")
            return None
        ts = datetime.fromisoformat(generated_at.rstrip("Z")).replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0
        if age_hours > stale_threshold_hours:
            LOG.warning(
                "[STALENESS] outputs are %.1f hours old (threshold=%.1f h) \u2014 "
                "generated_at_utc=%s \u2014 dispatch may contain stale data",
                age_hours, stale_threshold_hours, generated_at,
            )
        else:
            LOG.info(
                "[DATA] staleness_check: outputs are %.1f hours old (ok, threshold=%.1f h)",
                age_hours, stale_threshold_hours,
            )
        return age_hours
    except Exception as exc:  # pragma: no cover
        LOG.warning("[STALENESS] could not parse run_summary.json: %s", exc)
        return None


def download_outputs_from_blob(outputs_dir: Path) -> int:
    """Hydrate *outputs_dir* from the ``reporting-outputs`` blob container.

    This is the read-side of the Option B model:
      - ``refresh_unified_v2_timer`` runs ``full_report_v2.py`` once per day
        and uploads every artefact in its output directory to
        ``reporting-outputs``.
      - The dispatch functions (this caller) pull those artefacts down before
        building the email so they never have to regenerate the report
        themselves.

    Returns the number of files downloaded.  Returns ``0`` when blob storage
    is not configured (callers can decide whether to fall back to whatever is
    already on disk or abort).  Failures on individual blobs are logged but
    do not raise; a fatal connection error does raise.
    """
    if _BlobServiceClient is None:
        LOG.warning("azure-storage-blob not installed; cannot hydrate outputs from blob")
        return 0
    conn = os.getenv("AZURE_STORAGE_REPORTING_CONNECTION_STRING", "").strip()
    container = os.getenv("REPORTING_OUTPUTS_BLOB_CONTAINER", "reporting-outputs").strip()
    if not conn or not container:
        LOG.warning(
            "Blob output hydration skipped: AZURE_STORAGE_REPORTING_CONNECTION_STRING or "
            "REPORTING_OUTPUTS_BLOB_CONTAINER is not set"
        )
        return 0

    service = _BlobServiceClient.from_connection_string(conn)
    container_client = service.get_container_client(container)

    outputs_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    blob_index: dict[str, dict] = {}
    for blob in container_client.list_blobs():
        # Blob names mirror file names from full_report_v2.py (no nested
        # partition path is required. We preserve nested run paths locally and
        # persist blob last_modified metadata for deterministic file selection.
        dest = outputs_dir / blob.name
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as fh:
                container_client.download_blob(blob.name).readinto(fh)
            downloaded += 1
            blob_index[blob.name] = {
                "blob_name": blob.name,
                "last_modified_epoch": (
                    blob.last_modified.timestamp() if blob.last_modified else dest.stat().st_mtime
                ),
            }
        except Exception as exc:  # pragma: no cover
            LOG.warning("Failed to download blob %s: %s", blob.name, exc)
    _store_blob_index(outputs_dir, blob_index)
    run_blob_count = sum(1 for name in blob_index if name.startswith("runs/"))
    LOG.info(
        "[DATA] download_outputs_from_blob: container=%s downloaded=%d outputs_dir=%s "
        "(run_paths=%d top_level=%d)",
        container, downloaded, outputs_dir,
        run_blob_count,
        max(0, downloaded - run_blob_count),
    )
    _check_outputs_freshness(outputs_dir)
    return downloaded


def find_files(outputs_dir: Path, pattern: str, limit: int) -> list[Path]:
    """Return up to *limit* most-recently-modified files matching *pattern* in *outputs_dir*."""
    if not outputs_dir.exists():
        LOG.warning("Outputs directory %s is missing", outputs_dir)
        return []

    mode = dispatch_report_mode()

    def _mode_rank(path: Path) -> int:
        name = path.name.upper()
        is_eom_named = "_EOM_" in name
        if mode == "EOM":
            return 0 if is_eom_named else 1
        return 0 if not is_eom_named else 1

    candidates = [p for p in outputs_dir.rglob(pattern) if p.is_file()]
    if not candidates:
        return []

    run_candidates = [p for p in candidates if _is_run_artifact(p, outputs_dir)]
    if _prefer_run_path() and run_candidates:
        candidates = run_candidates

    blob_index = _load_blob_index(outputs_dir)
    candidates = sorted(
        candidates,
        key=lambda p: (
            _mode_rank(p),
            0 if _is_run_artifact(p, outputs_dir) else 1,
            -_blob_mtime_epoch(p, outputs_dir, blob_index),
            -_filename_timestamp_epoch(p),
            p.name,
        ),
    )
    return candidates[:limit]


def derive_report_date(outputs_dir: Path):
    """Derive the business-date anchor from the unified mapped CSV in *outputs_dir*.

    Returns the SAP ``Extract_Date`` value as-is (a ``datetime.datetime``).
    This is the date of the last completed SAP extract — already the effective
    business date. Callers must format it directly (e.g. ``report_date.strftime('%d.%m.%Y')``).
    Do NOT pass the return value into ``report_date_str()`` — that helper subtracts
    an extra working day which causes an off-by-one error.

    Priority:
    1. ``Extract_Date`` column  — set by the SAP Extract_Date_Int ingestion fix
    2. ``Load_Timestamp`` column — Azure blob write timestamp (fallback, logs WARNING)
    3. ``datetime.now()``       — last-resort fallback (logs WARNING)

    Returns a ``datetime.datetime`` in all cases.
    """
    import datetime as _dt

    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        LOG.warning("pandas not available; falling back to datetime.now() for report_date")
        return _dt.datetime.now()

    csvs = find_files(outputs_dir, "qry_unified_mapped_*.csv", 5)
    if not csvs:
        LOG.warning("No unified CSV found in %s; falling back to datetime.now()", outputs_dir)
        return _dt.datetime.now()

    for csv_path in csvs:
        try:
            df = pd.read_csv(csv_path)
            if "Extract_Date" in df.columns:
                ts = pd.to_datetime(df["Extract_Date"], errors="coerce").max()
                if pd.notna(ts):
                    LOG.info("report_date derived from Extract_Date: %s", ts)
                    return ts.to_pydatetime()
            if "Load_Timestamp" in df.columns:
                LOG.warning("Extract_Date not found; falling back to Load_Timestamp for report_date")
                ts = pd.to_datetime(df["Load_Timestamp"], errors="coerce").max()
                if pd.notna(ts):
                    return ts.to_pydatetime()
        except Exception as exc:  # pragma: no cover
            LOG.warning("Could not derive report_date from %s: %s", csv_path.name, exc)

    LOG.warning("No valid date column found in unified CSV; falling back to datetime.now()")
    return _dt.datetime.now()


def collect_html_files(outputs_dir: Path) -> list[Path]:
    """Return the newest HTML file for each KEY_HTML_PATTERNS entry."""
    seen: set[Path] = set()
    result: list[Path] = []
    for pattern in KEY_HTML_PATTERNS:
        for m in find_files(outputs_dir, pattern, 1):
            resolved = m.resolve()
            if resolved not in seen:
                result.append(m)
                seen.add(resolved)
    return result


def collect_csv_attachments(outputs_dir: Path) -> list[Path]:
    """Return CSV files to attach.

    Driven by REPORT_DISPATCH_ATTACHMENT_PATTERNS (semicolon-separated globs,
    filtered to CSV globs only) or falls back to KEY_CSV_PATTERNS.
    """
    patterns, per_limit = resolve_attachment_patterns()
    csv_patterns = [
        p for p in patterns if p.lower().endswith(".csv") or "csv" in p.lower()
    ]
    if not csv_patterns:
        csv_patterns = KEY_CSV_PATTERNS
        per_limit = 1
    seen: set[Path] = set()
    result: list[Path] = []
    for pattern in csv_patterns:
        for m in find_files(outputs_dir, pattern, per_limit):
            resolved = m.resolve()
            if resolved not in seen:
                result.append(m)
                seen.add(resolved)
    return result
