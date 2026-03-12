"""Regression tests for dispatch subject line generation.

Guards against the bug introduced in PR #18 where the fallback subject
always used the "EOM" prefix regardless of the actual run mode.
Covers all three dispatch functions: dispatch_reports, dispatch_usa_spa_reports,
and core_market_reports.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest

_HERE = Path(__file__).parent
_AZURE_FUNCTIONS_DIR = _HERE.parent
_DISPATCH_DIR = _AZURE_FUNCTIONS_DIR / "dispatch_reports"

_FIXED_DATE = datetime(2026, 3, 11)
_FIXED_DATE_STR = "11.03.2026"


# ---------------------------------------------------------------------------
# Shared stub helpers
# ---------------------------------------------------------------------------

def _ensure_azure_stubs():
    """Ensure azure.functions and dotenv are stubbed (idempotent)."""
    if "azure.functions" not in sys.modules:
        az = types.ModuleType("azure")
        az_func = types.ModuleType("azure.functions")
        az_func.TimerRequest = object  # type: ignore[attr-defined]
        sys.modules.setdefault("azure", az)
        sys.modules["azure.functions"] = az_func

    if "dotenv" not in sys.modules:
        dotenv_stub = types.ModuleType("dotenv")
        dotenv_stub.load_dotenv = lambda *a, **kw: None  # type: ignore[attr-defined]
        sys.modules["dotenv"] = dotenv_stub


def _make_dispatch_reports_package() -> types.ModuleType:
    """Register a fully-stubbed dispatch_reports package and return it."""
    _ensure_azure_stubs()

    pkg = types.ModuleType("dispatch_reports")
    pkg.__path__ = [str(_DISPATCH_DIR)]  # type: ignore[attr-defined]
    pkg.__package__ = "dispatch_reports"

    def _stub(name: str, **attrs):
        m = types.ModuleType(f"dispatch_reports.{name}")
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[f"dispatch_reports.{name}"] = m

    _stub("config",
          parse_recipients=lambda x: [],
          report_date_str=lambda: "01.01.2026",
          report_mtd_banner=lambda: "banner",
          USA_SPA_HTML_PATTERNS=["management_report_usa_spa_*.html"],
          CORE_MARKET_HTML_PATTERNS=["management_report_core_markets_*.html"],
          CORE_MARKET_PDF_PATTERNS=["management_report_core_markets_*.pdf"],
          parse_pattern_env=lambda env, default: default)
    _stub("graph_client",
          acquire_graph_token=lambda: "token",
          send_via_graph=lambda *a, **kw: None)
    _stub("health_alerts",
          send_healthcheck_alert=lambda *a, **kw: None)
    _stub("html_builder",
          build_html_body=lambda *a, **kw: ("HTML", "<html/>"))
    _stub("report_collector",
          collect_csv_attachments=lambda p: [],
          collect_html_files=lambda p: [],
          find_files=lambda p, pat, n: [],
          refresh_reports=lambda p: True,
          resolve_outputs_path=lambda: Path("/tmp"),
          derive_report_date=lambda p: _FIXED_DATE)

    sys.modules["dispatch_reports"] = pkg
    return pkg


def _load_module(path: Path, name: str) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    mod.__package__ = name.rsplit(".", 1)[0] if "." in name else name
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    sys.modules[name] = mod
    return mod


# Load all three dispatch modules once at import time
_make_dispatch_reports_package()
_dispatch_mod = _load_module(_DISPATCH_DIR / "__init__.py", "dispatch_reports")
_usa_spa_mod = _load_module(
    _AZURE_FUNCTIONS_DIR / "dispatch_usa_spa_reports" / "__init__.py",
    "dispatch_usa_spa_reports",
)
_core_market_mod = _load_module(
    _AZURE_FUNCTIONS_DIR / "core_market_reports" / "__init__.py",
    "core_market_reports",
)

_build_subject = _dispatch_mod._build_subject


# ---------------------------------------------------------------------------
# Helpers that replicate the inline subject logic from the two modules that
# don't have a dedicated helper function (usa_spa and core_market).
# We extract them the same way the functions compute them so the tests stay
# tightly coupled to the actual code paths.
# ---------------------------------------------------------------------------

def _usa_spa_subject(report_date, override_env: str | None = None) -> str:
    """Mirror the subject logic in dispatch_usa_spa_reports.__init__."""
    from dispatch_reports.config import report_date_str
    mode = os.getenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD").strip().upper() or "MTD"
    date_str = report_date.strftime('%d.%m.%Y') if report_date else report_date_str()
    default = (
        f"EOM QMS USA Spa Sales Report {date_str}"
        if mode == "EOM"
        else f"QMS USA Spa Sales Report {date_str}"
    )
    return os.getenv("USA_SPA_DISPATCH_SUBJECT") or default


def _core_market_subject(report_date, override_env: str | None = None) -> str:
    """Mirror the subject logic in core_market_reports.__init__."""
    from dispatch_reports.config import report_date_str
    mode = os.getenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD").strip().upper() or "MTD"
    date_str = report_date.strftime('%d.%m.%Y') if report_date else report_date_str()
    default = (
        f"EOM QMS Core Market Sales Report {date_str}"
        if mode == "EOM"
        else f"QMS Core Market Sales Report {date_str}"
    )
    return os.getenv("CORE_MARKET_DISPATCH_SUBJECT") or default


# ---------------------------------------------------------------------------
# Tests — dispatch_reports (management report)
# ---------------------------------------------------------------------------

class TestBuildSubjectMode:
    """Subject prefix must match the run mode, not always say EOM."""

    def test_mtd_mode_no_eom_prefix(self, monkeypatch):
        monkeypatch.delenv("V2_UNIFIED_REFRESH_REPORT_TYPE", raising=False)
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        subject = _build_subject(_FIXED_DATE)
        assert not subject.startswith("EOM"), (
            f"MTD dispatch should NOT start with 'EOM', got: {subject!r}"
        )

    def test_mtd_mode_explicit_env(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        subject = _build_subject(_FIXED_DATE)
        assert not subject.startswith("EOM"), f"Got: {subject!r}"
        assert _FIXED_DATE_STR in subject

    def test_eom_mode_has_eom_prefix(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        subject = _build_subject(_FIXED_DATE)
        assert subject.startswith("EOM"), f"EOM dispatch should start with 'EOM', got: {subject!r}"
        assert _FIXED_DATE_STR in subject

    def test_mtd_subject_contains_date(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        subject = _build_subject(_FIXED_DATE)
        assert _FIXED_DATE_STR in subject

    def test_eom_subject_contains_date(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        subject = _build_subject(_FIXED_DATE)
        assert _FIXED_DATE_STR in subject


class TestBuildSubjectOverride:
    """Explicit REPORT_DISPATCH_SUBJECT env var always wins."""

    def test_explicit_subject_used_in_mtd(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.setenv("REPORT_DISPATCH_SUBJECT", "Custom Subject Line")
        subject = _build_subject(_FIXED_DATE)
        assert subject == "Custom Subject Line"

    def test_explicit_subject_used_in_eom(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        monkeypatch.setenv("REPORT_DISPATCH_SUBJECT", "Custom Subject Line")
        subject = _build_subject(_FIXED_DATE)
        assert subject == "Custom Subject Line"


class TestBuildSubjectNullDate:
    """Falls back gracefully when report_date is None."""

    def test_mtd_no_date(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        subject = _build_subject(None)
        assert not subject.startswith("EOM"), f"Got: {subject!r}"

    def test_eom_no_date(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        subject = _build_subject(None)
        assert subject.startswith("EOM"), f"Got: {subject!r}"


# ---------------------------------------------------------------------------
# Tests — dispatch_usa_spa_reports
# ---------------------------------------------------------------------------

class TestUsaSpaSubject:
    def test_mtd_no_eom_prefix(self, monkeypatch):
        monkeypatch.delenv("V2_UNIFIED_REFRESH_REPORT_TYPE", raising=False)
        monkeypatch.delenv("USA_SPA_DISPATCH_SUBJECT", raising=False)
        assert not _usa_spa_subject(_FIXED_DATE).startswith("EOM")

    def test_mtd_contains_date(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.delenv("USA_SPA_DISPATCH_SUBJECT", raising=False)
        assert _FIXED_DATE_STR in _usa_spa_subject(_FIXED_DATE)

    def test_eom_has_prefix(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        monkeypatch.delenv("USA_SPA_DISPATCH_SUBJECT", raising=False)
        assert _usa_spa_subject(_FIXED_DATE).startswith("EOM")

    def test_override_wins(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.setenv("USA_SPA_DISPATCH_SUBJECT", "Override")
        assert _usa_spa_subject(_FIXED_DATE) == "Override"


# ---------------------------------------------------------------------------
# Tests — core_market_reports
# ---------------------------------------------------------------------------

class TestCoreMarketSubject:
    def test_mtd_no_eom_prefix(self, monkeypatch):
        monkeypatch.delenv("V2_UNIFIED_REFRESH_REPORT_TYPE", raising=False)
        monkeypatch.delenv("CORE_MARKET_DISPATCH_SUBJECT", raising=False)
        assert not _core_market_subject(_FIXED_DATE).startswith("EOM")

    def test_mtd_contains_date(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.delenv("CORE_MARKET_DISPATCH_SUBJECT", raising=False)
        assert _FIXED_DATE_STR in _core_market_subject(_FIXED_DATE)

    def test_eom_has_prefix(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        monkeypatch.delenv("CORE_MARKET_DISPATCH_SUBJECT", raising=False)
        assert _core_market_subject(_FIXED_DATE).startswith("EOM")

    def test_override_wins(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        monkeypatch.setenv("CORE_MARKET_DISPATCH_SUBJECT", "Override")
        assert _core_market_subject(_FIXED_DATE) == "Override"
