"""Programmatic smoke checks for dispatch safety and strict SharePoint behavior.

These tests validate three release-critical behaviors:
1. All three dispatchers hydrate from blob only — no inline refresh runs on any path.
2. Core/USA/management dispatch flows send from existing blob-hydrated outputs.
3. V2 strict mode fails fast when SharePoint credentials are missing.

All dispatch sends are forced to a single safe test recipient.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest


_HERE = Path(__file__).resolve().parent
_AZURE_FUNCTIONS_DIR = _HERE.parent
_REPO_ROOT = _AZURE_FUNCTIONS_DIR.parent
_SRC_DIR = _REPO_ROOT / "src"
_DISPATCH_DIR = _AZURE_FUNCTIONS_DIR / "dispatch_reports"
_SAFE_TEST_RECIPIENT = "bradwilcock01@gmail.com"


# Ensure src imports in full_report_v2.py resolve.
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))


def _ensure_runtime_stubs() -> None:
    """Stub minimal runtime deps used by Azure Function modules."""
    if "azure.functions" not in sys.modules:
        azure_pkg = types.ModuleType("azure")
        azure_functions_pkg = types.ModuleType("azure.functions")
        azure_functions_pkg.TimerRequest = object  # type: ignore[attr-defined]
        sys.modules.setdefault("azure", azure_pkg)
        sys.modules["azure.functions"] = azure_functions_pkg

    if "dotenv" not in sys.modules:
        dotenv_pkg = types.ModuleType("dotenv")
        dotenv_pkg.load_dotenv = lambda *args, **kwargs: None  # type: ignore[attr-defined]
        sys.modules["dotenv"] = dotenv_pkg


def _load_module(name: str, path: Path, package: str | None = None) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Unable to load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    if package:
        module.__package__ = package
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _bootstrap_dispatch_modules() -> tuple[types.ModuleType, types.ModuleType, types.ModuleType]:
    """Load dispatch modules with package wiring and runtime stubs in place."""
    _ensure_runtime_stubs()

    dispatch_pkg = types.ModuleType("dispatch_reports")
    dispatch_pkg.__path__ = [str(_DISPATCH_DIR)]  # type: ignore[attr-defined]
    dispatch_pkg.__package__ = "dispatch_reports"
    sys.modules["dispatch_reports"] = dispatch_pkg

    dispatch_mod = _load_module(
        "dispatch_reports",
        _DISPATCH_DIR / "__init__.py",
        package="dispatch_reports",
    )
    core_mod = _load_module(
        "core_market_reports",
        _AZURE_FUNCTIONS_DIR / "core_market_reports" / "__init__.py",
        package="core_market_reports",
    )
    usa_mod = _load_module(
        "dispatch_usa_spa_reports",
        _AZURE_FUNCTIONS_DIR / "dispatch_usa_spa_reports" / "__init__.py",
        package="dispatch_usa_spa_reports",
    )
    return dispatch_mod, core_mod, usa_mod


def _load_full_report_v2_module() -> types.ModuleType:
    return _load_module("full_report_v2_smoke", _SRC_DIR / "full_report_v2.py")


@pytest.mark.integration
def test_smoke_dispatch_cycle_blob_hydrate_only_and_safe_recipient(monkeypatch, tmp_path):
    dispatch_mod, core_mod, usa_mod = _bootstrap_dispatch_modules()

    # Minimal files so attachment/path handling can resolve names if needed.
    mgmt_html = tmp_path / "combined_management_report_20260317_090000.html"
    core_html = tmp_path / "management_report_core_markets_20260317_090000.html"
    core_pdf = tmp_path / "management_report_core_markets_20260317_090000.pdf"
    usa_html = tmp_path / "management_report_usa_spa_20260317_090000.html"
    mgmt_csv = tmp_path / "combined_management_report_20260317_090000.csv"

    mgmt_html.write_text("<html>mgmt</html>", encoding="utf-8")
    core_html.write_text("<html>core</html>", encoding="utf-8")
    core_pdf.write_bytes(b"%PDF-1.4")
    usa_html.write_text("<html>usa</html>", encoding="utf-8")
    mgmt_csv.write_text("a,b\n1,2\n", encoding="utf-8")

    refresh_calls = {"dispatch": 0, "core": 0, "usa": 0}
    sent_payloads: list[tuple[str, list[str], str]] = []

    def _send_capture(stream: str):
        def _inner(recipients, attachments, body_content, subject, body_type):
            sent_payloads.append((stream, list(recipients), subject))
        return _inner

    # Force all flows to use safe test override recipient.
    monkeypatch.setenv("TEST_REPORT_DISPATCH_RECIPIENTS", _SAFE_TEST_RECIPIENT)
    monkeypatch.setenv("TEST_CORE_MARKETS_RECIPIENTS", _SAFE_TEST_RECIPIENT)
    monkeypatch.setenv("TEST_USA_SPA_RECIPIENTS", _SAFE_TEST_RECIPIENT)

    # If test overrides break, these would be used; keep them intentionally unsafe.
    monkeypatch.setenv("REPORT_DISPATCH_RECIPIENTS", "prod-management@example.com")
    monkeypatch.setenv("CORE_MARKET_DISPATCH_RECIPIENTS", "prod-core@example.com")
    monkeypatch.setenv("USA_SPA_DISPATCH_RECIPIENTS", "prod-usa@example.com")

    # All dispatchers now always hydrate from blob — REFRESH_BEFORE_SEND is removed.
    # Stub refresh_reports so that if it were ever called the test would catch it via counter.
    monkeypatch.setattr(dispatch_mod, "resolve_outputs_path", lambda: tmp_path)
    monkeypatch.setattr(dispatch_mod, "refresh_reports", lambda _: refresh_calls.__setitem__("dispatch", refresh_calls["dispatch"] + 1) or True)
    monkeypatch.setattr(dispatch_mod, "derive_report_date", lambda _: datetime(2026, 3, 17))
    monkeypatch.setattr(dispatch_mod, "collect_html_files", lambda _: [mgmt_html])
    monkeypatch.setattr(dispatch_mod, "collect_csv_attachments", lambda _: [mgmt_csv])
    monkeypatch.setattr(dispatch_mod, "build_html_body", lambda *a, **k: ("HTML", "<html>mgmt-body</html>"))
    monkeypatch.setattr(dispatch_mod, "send_via_graph", _send_capture("dispatch"))

    monkeypatch.setattr(core_mod, "resolve_outputs_path", lambda: tmp_path)
    monkeypatch.setattr(core_mod, "refresh_reports", lambda _: refresh_calls.__setitem__("core", refresh_calls["core"] + 1) or True)
    monkeypatch.setattr(core_mod, "derive_report_date", lambda _: datetime(2026, 3, 17))
    monkeypatch.setattr(core_mod, "_collect_core_market_html", lambda _: [core_html])
    monkeypatch.setattr(core_mod, "_collect_core_market_pdf", lambda _: [core_pdf])
    monkeypatch.setattr(core_mod, "build_html_body", lambda *a, **k: ("HTML", "<html>core-body</html>"))
    monkeypatch.setattr(core_mod, "send_via_graph", _send_capture("core"))

    monkeypatch.setattr(usa_mod, "resolve_outputs_path", lambda: tmp_path)
    monkeypatch.setattr(usa_mod, "refresh_reports", lambda _: refresh_calls.__setitem__("usa", refresh_calls["usa"] + 1) or True)
    monkeypatch.setattr(usa_mod, "derive_report_date", lambda _: datetime(2026, 3, 17))
    monkeypatch.setattr(usa_mod, "_collect_usa_spa_html", lambda _: [usa_html])
    monkeypatch.setattr(usa_mod, "build_html_body", lambda *a, **k: ("HTML", "<html>usa-body</html>"))
    monkeypatch.setattr(usa_mod, "send_via_graph", _send_capture("usa"))

    dispatch_mod.main(None)
    core_mod.main(None)
    usa_mod.main(None)

    assert refresh_calls == {"dispatch": 0, "core": 0, "usa": 0}, (
        "No dispatcher should call refresh_reports — all hydrate from blob only"
    )
    assert len(sent_payloads) == 3
    assert {stream for stream, _, _ in sent_payloads} == {"dispatch", "core", "usa"}

    for stream, recipients, _subject in sent_payloads:
        assert recipients == [_SAFE_TEST_RECIPIENT], (
            f"{stream} recipients must be forced to safe test address only"
        )


@pytest.mark.integration
def test_smoke_v2_require_sharepoint_fails_fast_when_credentials_missing(monkeypatch, tmp_path):
    module = _load_full_report_v2_module()

    monkeypatch.setenv("V2_UNIFIED_REQUIRE_SHAREPOINT", "true")
    monkeypatch.delenv("SHAREPOINT_SITE_URL", raising=False)
    monkeypatch.delenv("SHAREPOINT_CLIENT_ID", raising=False)
    monkeypatch.delenv("SHAREPOINT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("SHAREPOINT_TENANT_ID", raising=False)
    monkeypatch.delenv("SHAREPOINT_TENANT_DOMAIN", raising=False)

    with pytest.raises(RuntimeError, match="V2_UNIFIED_REQUIRE_SHAREPOINT is enabled"):
        module.resolve_unified_source_path(
            project_root=tmp_path,
            report_type="MTD",
            input_unified_csv=None,
            retries=1,
            retry_backoff_seconds=0.0,
        )
