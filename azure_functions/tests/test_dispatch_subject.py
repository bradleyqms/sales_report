"""Regression tests for dispatch_reports._build_subject.

Guards against the bug introduced in PR #18 where the fallback subject
always used the "EOM" prefix regardless of the actual run mode.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load dispatch_reports.__init__ as a standalone module, stubbing out the
# heavy runtime imports (azure.functions, dotenv, and the relative submodules)
# so the test has no external dependencies.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent
_DISPATCH_DIR = _HERE.parent / "dispatch_reports"


def _load_dispatch_init():
    """Return the dispatch_reports __init__ module with all side-effect imports stubbed."""
    # Stub azure.functions
    az_func = types.ModuleType("azure.functions")
    az_func.TimerRequest = object  # type: ignore[attr-defined]
    sys.modules.setdefault("azure", types.ModuleType("azure"))
    sys.modules["azure.functions"] = az_func

    # Stub dotenv
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *a, **kw: None  # type: ignore[attr-defined]
    sys.modules.setdefault("dotenv", dotenv_stub)

    # Build a minimal stub package for the relative imports
    pkg = types.ModuleType("dispatch_reports")
    pkg.__path__ = [str(_DISPATCH_DIR)]  # type: ignore[attr-defined]
    pkg.__package__ = "dispatch_reports"

    def _make_stub(name: str, **attrs):
        m = types.ModuleType(f"dispatch_reports.{name}")
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[f"dispatch_reports.{name}"] = m
        return m

    _make_stub("config",
               parse_recipients=lambda x: [],
               report_date_str=lambda: "01.01.2026",
               report_mtd_banner=lambda: "banner")
    _make_stub("graph_client",
               acquire_graph_token=lambda: "token",
               send_via_graph=lambda *a, **kw: None)
    _make_stub("health_alerts",
               send_healthcheck_alert=lambda *a, **kw: None)
    _make_stub("html_builder",
               build_html_body=lambda *a, **kw: ("HTML", "<html/>"))
    _make_stub("report_collector",
               collect_csv_attachments=lambda p: [],
               collect_html_files=lambda p: [],
               refresh_reports=lambda p: True,
               resolve_outputs_path=lambda: Path("/tmp"),
               derive_report_date=lambda p: datetime(2026, 3, 11))

    sys.modules["dispatch_reports"] = pkg

    spec = importlib.util.spec_from_file_location(
        "dispatch_reports",
        _DISPATCH_DIR / "__init__.py",
        submodule_search_locations=[str(_DISPATCH_DIR)],
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    mod.__package__ = "dispatch_reports"
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    sys.modules["dispatch_reports"] = mod
    return mod


_dispatch = _load_dispatch_init()
_build_subject = _dispatch._build_subject

_FIXED_DATE = datetime(2026, 3, 11)
_FIXED_DATE_STR = "11.03.2026"


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
