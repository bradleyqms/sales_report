"""Environment-variable helpers for the dispatch_reports function."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

LOG = logging.getLogger(__name__)

# ── Attachment / output patterns ──────────────────────────────────────────
KEY_HTML_PATTERNS = [
    "combined_management_report_*.html",
    "management_report_core_markets_*.html",
]
KEY_REPORT_PATTERNS = KEY_HTML_PATTERNS  # backwards-compat alias

KEY_CSV_PATTERNS = [
    "combined_management_report_*.csv",
    "management_report_core_markets_*.csv",
]

# Core-market-only function patterns
CORE_MARKET_HTML_PATTERNS = ["management_report_core_markets_*.html"]
CORE_MARKET_PDF_PATTERNS = ["management_report_core_markets_*.pdf"]

# USA SPA dispatch function patterns
USA_SPA_HTML_PATTERNS = ["management_report_usa_spa_*.html"]


def report_date_str(reference_date: datetime | None = None) -> str:
    """Return the last working day before *reference_date* formatted as DD.MM.YYYY.

    Args:
        reference_date: Optional datetime anchor. If None, falls back to datetime.utcnow().
    """
    d = ((reference_date or datetime.utcnow()) - timedelta(days=1)).date()
    while d.weekday() >= 5:  # 5=Saturday, 6=Sunday
        d -= timedelta(days=1)
    return d.strftime("%d.%m.%Y")


def report_mtd_banner(reference_date: datetime | None = None) -> str:
    """Return e.g. 'Management Report (MTD: February 1-23, 2026)'.

    Args:
        reference_date: Optional datetime anchor. If None, falls back to datetime.utcnow().
    """
    d = ((reference_date or datetime.utcnow()) - timedelta(days=1)).date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return f"Management Report (MTD: {d.strftime('%B')} 1-{d.day}, {d.year})"


def parse_pattern_env(env_var: str, default_patterns: list[str]) -> list[str]:
    """Read a semicolon-separated glob pattern list from *env_var*.

    - Env var absent  → use *default_patterns*
    - Env var empty   → return [] (disables the feature)
    - Env var set     → split on ';', strip whitespace, drop blanks
    """
    raw = os.getenv(env_var)
    if raw is None:
        return default_patterns
    return [p.strip() for p in raw.split(";") if p.strip()]


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
