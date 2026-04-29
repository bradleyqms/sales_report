"""Tests for dispatch_reports.health_alerts — alert email helpers.

Covers: _resolve_environment_name, _resolve_commit_label, _resolve_logs_url,
and send_healthcheck_alert (disabled, no recipients, sends email, escapes HTML,
swallows Graph failures, includes invocation_id).
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_HERE = Path(__file__).parent
_PKG = _HERE.parent / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

# -- Load dispatch_reports.config first so the relative import resolves -------
_cfg_spec = importlib.util.spec_from_file_location(
    "dispatch_reports.config",
    _HERE.parent / "dispatch_reports" / "config.py",
)
_cfg_mod = importlib.util.module_from_spec(_cfg_spec)  # type: ignore[arg-type]
# Force-assign (not setdefault) so test_dispatch_subject.py's config stub doesn't
# bleed in when test files are loaded in alphabetical order.
sys.modules["dispatch_reports.config"] = _cfg_mod
_cfg_spec.loader.exec_module(_cfg_mod)  # type: ignore[union-attr]

# -- Stub graph_client before health_alerts imports it ------------------------
_graph_stub = types.ModuleType("dispatch_reports.graph_client")
_graph_stub.send_via_graph = lambda *a, **kw: None  # type: ignore[attr-defined]
sys.modules.setdefault("dispatch_reports.graph_client", _graph_stub)

# Ensure the dispatch_reports package stub exists
if "dispatch_reports" not in sys.modules:
    _pkg_stub = types.ModuleType("dispatch_reports")
    sys.modules["dispatch_reports"] = _pkg_stub

# -- Load dispatch_reports.health_alerts --------------------------------------
_ha_spec = importlib.util.spec_from_file_location(
    "dispatch_reports.health_alerts",
    _HERE.parent / "dispatch_reports" / "health_alerts.py",
)
_mod = importlib.util.module_from_spec(_ha_spec)  # type: ignore[arg-type]
sys.modules["dispatch_reports.health_alerts"] = _mod
_ha_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

_resolve_environment_name = _mod._resolve_environment_name
_resolve_commit_label = _mod._resolve_commit_label
_resolve_logs_url = _mod._resolve_logs_url
send_healthcheck_alert = _mod.send_healthcheck_alert


# ---------------------------------------------------------------------------
# _resolve_environment_name
# ---------------------------------------------------------------------------

class TestResolveEnvironmentName:
    def test_uses_healthcheck_env_var(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ENVIRONMENT", "production")
        assert _resolve_environment_name() == "production"

    def test_falls_back_to_website_site_name(self, monkeypatch):
        monkeypatch.delenv("HEALTHCHECK_ENVIRONMENT", raising=False)
        monkeypatch.setenv("WEBSITE_SITE_NAME", "qms-dispatch-func")
        assert _resolve_environment_name() == "qms-dispatch-func"

    def test_unknown_when_no_env_vars_set(self, monkeypatch):
        monkeypatch.delenv("HEALTHCHECK_ENVIRONMENT", raising=False)
        monkeypatch.delenv("WEBSITE_SITE_NAME", raising=False)
        assert _resolve_environment_name() == "unknown-environment"


# ---------------------------------------------------------------------------
# _resolve_commit_label
# ---------------------------------------------------------------------------

class TestResolveCommitLabel:
    def test_uses_healthcheck_commit_sha(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_COMMIT_SHA", "abc1234")
        assert _resolve_commit_label() == "abc1234"

    def test_falls_back_to_source_version(self, monkeypatch):
        monkeypatch.delenv("HEALTHCHECK_COMMIT_SHA", raising=False)
        monkeypatch.setenv("SOURCE_VERSION", "d4e5f6")
        assert _resolve_commit_label() == "d4e5f6"

    def test_unknown_when_neither_env_set(self, monkeypatch):
        monkeypatch.delenv("HEALTHCHECK_COMMIT_SHA", raising=False)
        monkeypatch.delenv("SOURCE_VERSION", raising=False)
        assert _resolve_commit_label() == "unknown"


# ---------------------------------------------------------------------------
# _resolve_logs_url
# ---------------------------------------------------------------------------

class TestResolveLogsUrl:
    def test_uses_healthcheck_appinsights_url(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_APPINSIGHTS_URL", "https://example.com/logs")
        assert _resolve_logs_url() == "https://example.com/logs"

    def test_defaults_to_azure_portal(self, monkeypatch):
        monkeypatch.delenv("HEALTHCHECK_APPINSIGHTS_URL", raising=False)
        assert _resolve_logs_url() == "https://portal.azure.com/"


# ---------------------------------------------------------------------------
# send_healthcheck_alert
# ---------------------------------------------------------------------------

class TestSendHealthcheckAlert:
    def test_disabled_via_false_skips_send(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ALERTS_ENABLED", "false")
        called = []
        monkeypatch.setattr(_mod, "send_via_graph", lambda *a, **kw: called.append(1))
        send_healthcheck_alert("test_func", ValueError("boom"))
        assert called == []

    def test_disabled_via_zero_skips_send(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ALERTS_ENABLED", "0")
        called = []
        monkeypatch.setattr(_mod, "send_via_graph", lambda *a, **kw: called.append(1))
        send_healthcheck_alert("test_func", ValueError("boom"))
        assert called == []

    def test_no_recipients_configured_skips_send(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ALERTS_ENABLED", "true")
        monkeypatch.setenv("HEALTHCHECK_ALERT_RECIPIENTS", "")
        called = []
        monkeypatch.setattr(_mod, "send_via_graph", lambda *a, **kw: called.append(1))
        send_healthcheck_alert("test_func", ValueError("boom"))
        assert called == []

    def test_sends_email_with_valid_recipient(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ALERTS_ENABLED", "true")
        monkeypatch.setenv("HEALTHCHECK_ALERT_RECIPIENTS", "test@example.com")
        # Patch parse_recipients directly in case config stub bled in from another test file
        monkeypatch.setattr(_mod, "parse_recipients", lambda x: ["test@example.com"])
        captured: dict = {}

        def _fake_send(recipients, attachments, body, subject, body_type):
            captured["recipients"] = recipients
            captured["subject"] = subject
            captured["body"] = body

        monkeypatch.setattr(_mod, "send_via_graph", _fake_send)
        send_healthcheck_alert("my_function", RuntimeError("crash"))

        assert "test@example.com" in captured["recipients"]
        assert "my_function" in captured["subject"]
        assert captured.get("body")  # non-empty HTML body

    def test_html_body_escapes_dangerous_input(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ALERTS_ENABLED", "true")
        monkeypatch.setenv("HEALTHCHECK_ALERT_RECIPIENTS", "test@example.com")
        monkeypatch.setattr(_mod, "parse_recipients", lambda x: ["test@example.com"])
        captured: dict = {}

        def _fake_send(recipients, attachments, body, subject, body_type):
            captured["body"] = body

        monkeypatch.setattr(_mod, "send_via_graph", _fake_send)
        send_healthcheck_alert("safe_func", ValueError("<script>alert(1)</script>"))

        body = captured.get("body", "")
        assert "<script>" not in body
        assert "&lt;script&gt;" in body

    def test_never_raises_when_send_via_graph_fails(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ALERTS_ENABLED", "true")
        monkeypatch.setenv("HEALTHCHECK_ALERT_RECIPIENTS", "test@example.com")

        def _bad_send(*a, **kw):
            raise RuntimeError("network failure")

        monkeypatch.setattr(_mod, "send_via_graph", _bad_send)
        # Must not raise — health alerts are best-effort
        send_healthcheck_alert("func", ValueError("original error"))

    def test_custom_invocation_id_in_body(self, monkeypatch):
        monkeypatch.setenv("HEALTHCHECK_ALERTS_ENABLED", "true")
        monkeypatch.setenv("HEALTHCHECK_ALERT_RECIPIENTS", "test@example.com")
        monkeypatch.setattr(_mod, "parse_recipients", lambda x: ["test@example.com"])
        captured: dict = {}

        def _fake_send(recipients, attachments, body, subject, body_type):
            captured["body"] = body

        monkeypatch.setattr(_mod, "send_via_graph", _fake_send)
        send_healthcheck_alert("func", ValueError("crash"), invocation_id="inv-xyz-123")

        assert "inv-xyz-123" in captured.get("body", "")
