"""Tests for dispatch_reports.graph_client — token acquisition and sendMail."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import importlib.util

import pytest

_HERE = Path(__file__).parent
_PKG = _HERE.parent / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

_spec = importlib.util.spec_from_file_location(
    "graph_client",
    _HERE.parent / "dispatch_reports" / "graph_client.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)                  # type: ignore[union-attr]

acquire_graph_token = _mod.acquire_graph_token
prepare_graph_attachments = _mod.prepare_graph_attachments
send_via_graph = _mod.send_via_graph


class TestAcquireGraphToken:
    def test_missing_credentials_returns_none(self, monkeypatch):
        monkeypatch.delenv("GRAPH_TENANT_ID", raising=False)
        monkeypatch.delenv("GRAPH_CLIENT_ID", raising=False)
        monkeypatch.delenv("GRAPH_CLIENT_SECRET", raising=False)
        assert acquire_graph_token() is None

    def test_returns_token_on_success(self, monkeypatch):
        monkeypatch.setenv("GRAPH_TENANT_ID", "tenant123")
        monkeypatch.setenv("GRAPH_CLIENT_ID", "client123")
        monkeypatch.setenv("GRAPH_CLIENT_SECRET", "secret123")

        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok_abc"}

        with patch.object(_mod.msal, "ConfidentialClientApplication", return_value=mock_app):
            token = acquire_graph_token()
        assert token == "tok_abc"

    def test_returns_none_on_msal_error(self, monkeypatch):
        monkeypatch.setenv("GRAPH_TENANT_ID", "tenant123")
        monkeypatch.setenv("GRAPH_CLIENT_ID", "client123")
        monkeypatch.setenv("GRAPH_CLIENT_SECRET", "secret123")

        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"error": "invalid_client"}

        with patch.object(_mod.msal, "ConfidentialClientApplication", return_value=mock_app):
            token = acquire_graph_token()
        assert token is None


class TestPrepareGraphAttachments:
    def test_single_csv_attachment(self, tmp_path):
        f = tmp_path / "report.csv"
        f.write_bytes(b"a,b\n1,2")
        result = prepare_graph_attachments([f])
        assert len(result) == 1
        assert result[0]["name"] == "report.csv"
        assert result[0]["@odata.type"] == "#microsoft.graph.fileAttachment"
        # mimetypes is platform-specific; accept any non-empty mime type for csv
        assert result[0]["contentType"]

    def test_base64_encoded(self, tmp_path):
        import base64
        f = tmp_path / "data.csv"
        content = b"hello,world"
        f.write_bytes(content)
        result = prepare_graph_attachments([f])
        decoded = base64.b64decode(result[0]["contentBytes"])
        assert decoded == content

    def test_empty_list_returns_empty(self):
        assert prepare_graph_attachments([]) == []

    def test_unknown_extension_uses_octet_stream(self, tmp_path):
        f = tmp_path / "data.xyzabc"
        f.write_bytes(b"binary")
        result = prepare_graph_attachments([f])
        assert result[0]["contentType"] == "application/octet-stream"


class TestSendViaGraph:
    def _mock_env(self, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_GRAPH_SENDER", "sender@example.com")
        monkeypatch.setenv("GRAPH_TENANT_ID", "t")
        monkeypatch.setenv("GRAPH_CLIENT_ID", "c")
        monkeypatch.setenv("GRAPH_CLIENT_SECRET", "s")

    def test_raises_if_no_sender(self, monkeypatch):
        monkeypatch.delenv("REPORT_DISPATCH_GRAPH_SENDER", raising=False)
        with pytest.raises(ValueError, match="REPORT_DISPATCH_GRAPH_SENDER"):
            send_via_graph(["r@x.com"], [], "body", "subject")

    def test_raises_if_token_fails(self, monkeypatch):
        self._mock_env(monkeypatch)
        with patch.object(_mod, "acquire_graph_token", return_value=None):
            with pytest.raises(RuntimeError, match="access token"):
                send_via_graph(["r@x.com"], [], "body", "subject")

    def test_successful_send(self, monkeypatch):
        self._mock_env(monkeypatch)
        mock_response = MagicMock()
        mock_response.status_code = 202
        with patch.object(_mod, "acquire_graph_token", return_value="tok"), \
             patch.object(_mod.requests, "post", return_value=mock_response):
            send_via_graph(["r@x.com"], [], "body", "subject")

    def test_http_error_raises(self, monkeypatch):
        self._mock_env(monkeypatch)
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = Exception("403 Forbidden")
        with patch.object(_mod, "acquire_graph_token", return_value="tok"), \
             patch.object(_mod.requests, "post", return_value=mock_response):
            with pytest.raises(Exception, match="403"):
                send_via_graph(["r@x.com"], [], "body", "subject")
