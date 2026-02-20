"""Environment-variable helpers for the dispatch_reports function."""
from __future__ import annotations

import logging
import os

LOG = logging.getLogger(__name__)

# ── Attachment / output patterns ──────────────────────────────────────────
KEY_HTML_PATTERNS = [
    "combined_management_report_*.html",
    "management_report_core_markets_*.html",
]
KEY_REPORT_PATTERNS = KEY_HTML_PATTERNS  # backwards-compat alias

KEY_CSV_PATTERNS = [
    "management_report_core_markets_*.csv",
    "management_report_qry_*.csv",
    "germany_gap_analysis.csv",
]


def parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        LOG.warning(
            "Environment %s must be an integer, got %r; using %d", name, raw, default
        )
        return default


def parse_recipients(config: str | None) -> list[str]:
    """Split a comma- or semicolon-separated recipients string into a list."""
    if not config:
        return []
    return [
        item.strip()
        for item in config.replace(";", ",").split(",")
        if item.strip()
    ]


def resolve_attachment_patterns() -> tuple[list[str], int]:
    """Return (patterns, per_pattern_limit) from REPORT_DISPATCH_ATTACHMENT_PATTERNS."""
    spec = os.getenv("REPORT_DISPATCH_ATTACHMENT_PATTERNS")
    if not spec:
        return [], 0
    patterns = [entry.strip() for entry in spec.split(";") if entry.strip()]
    limit = max(1, parse_int_env("REPORT_DISPATCH_ATTACHMENTS_PER_PATTERN", 1))
    return patterns, limit
