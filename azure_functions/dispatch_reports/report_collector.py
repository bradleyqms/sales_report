"""File-discovery helpers: locating HTML reports and CSV attachments."""
from __future__ import annotations

import logging
import os
from pathlib import Path

from .config import (
    KEY_CSV_PATTERNS,
    KEY_HTML_PATTERNS,
    parse_int_env,
    resolve_attachment_patterns,
)

LOG = logging.getLogger(__name__)

# Populated in __init__.py at import time
_REPO_ROOT: Path | None = None


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


def find_files(outputs_dir: Path, pattern: str, limit: int) -> list[Path]:
    """Return up to *limit* most-recently-modified files matching *pattern* in *outputs_dir*."""
    if not outputs_dir.exists():
        LOG.warning("Outputs directory %s is missing", outputs_dir)
        return []
    candidates = sorted(
        outputs_dir.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[:limit]


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
