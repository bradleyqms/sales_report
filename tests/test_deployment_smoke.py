"""Post-deployment smoke tests.

Run after every deploy to catch regressions before they surface as:
  - 503 errors (startup crashes, missing imports)
  - Broken HTML emails (html_builder / report assembler failures)
  - Unmapped entities (entity_mappings.csv missing required columns)
  - Stale or missing outputs (report_collector freshness infrastructure broken)

Usage:
    pytest -m deployment --import-mode=importlib -q
    pytest -m deployment -v --tb=short   # verbose post-deploy

These tests run WITHOUT real Azure infrastructure — all external calls are
stubbed or skipped via env-var gates.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import httpx
import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

_TESTS_DIR = Path(__file__).parent
_REPO_ROOT = _TESTS_DIR.parent
_SRC_DIR = _REPO_ROOT / "src"
_AZURE_DIR = _REPO_ROOT / "azure_functions"
_DISPATCH_DIR = _AZURE_DIR / "dispatch_reports"
_FASTAPI_DIR = _REPO_ROOT / "fastapi_web_app"
_DATA_INPUTS_MAPPINGS = _REPO_ROOT / "data" / "inputs" / "mappings"

if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if str(_AZURE_DIR) not in sys.path:
    sys.path.insert(0, str(_AZURE_DIR))


def _stub_azure_functions_runtime() -> None:
    """Stub the azure.functions runtime so dispatch modules import cleanly."""
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


def _load_dispatch_module(name: str) -> types.ModuleType:
    path = _DISPATCH_DIR / f"{name}.py"
    full_name = f"dispatch_reports.{name}"
    spec = importlib.util.spec_from_file_location(full_name, path)
    assert spec and spec.loader, f"Cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    mod.__package__ = "dispatch_reports"
    sys.modules[full_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


# ---------------------------------------------------------------------------
# Group 1: Module import health — 503 prevention
# ---------------------------------------------------------------------------

@pytest.mark.deployment
class TestModuleImportHealth:
    """Verify all critical modules import without raising.

    A failed import at startup causes an immediate 503 on Azure Functions.
    """

    def test_fastapi_app_imports_cleanly(self):
        """FastAPI app module must import without crashing."""
        import fastapi_web_app.main  # noqa: F401 — import is the test

    def test_dispatch_reports_config_imports_cleanly(self):
        _stub_azure_functions_runtime()
        spec = importlib.util.spec_from_file_location(
            "dispatch_reports.config_smoke",
            _DISPATCH_DIR / "config.py",
        )
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

    def test_dispatch_reports_graph_client_imports_cleanly(self):
        _stub_azure_functions_runtime()
        spec = importlib.util.spec_from_file_location(
            "dispatch_reports.graph_client_smoke",
            _DISPATCH_DIR / "graph_client.py",
        )
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

    def test_dispatch_reports_html_builder_imports_cleanly(self):
        _stub_azure_functions_runtime()
        # config must be loaded first for the relative import chain
        _load_dispatch_module("config") if "dispatch_reports.config" not in sys.modules else None
        spec = importlib.util.spec_from_file_location(
            "dispatch_reports.html_builder_smoke",
            _DISPATCH_DIR / "html_builder.py",
        )
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

    def test_dispatch_reports_health_alerts_imports_cleanly(self):
        """health_alerts.py must import cleanly; a crash here silences all failure emails."""
        _stub_azure_functions_runtime()
        spec = importlib.util.spec_from_file_location(
            "dispatch_reports.health_alerts_smoke",
            _DISPATCH_DIR / "health_alerts.py",
        )
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Group 2: FastAPI endpoint shape — 503 / runtime crash detection
# ---------------------------------------------------------------------------

@pytest.mark.deployment
class TestFastApiEndpoints:
    """Verify the web app's key endpoints respond correctly.

    Uses starlette TestClient (no live server required).
    """

    pass  # tests are standalone async functions below (starlette TestClient is broken with httpx>=0.28)


@pytest.mark.deployment
@pytest.mark.anyio
async def test_fastapi_status_endpoint_returns_200():
    """GET /status must respond 200 — a crash here means the app won't start."""
    import fastapi_web_app.main as app_main
    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/status")
    assert response.status_code == 200


@pytest.mark.deployment
@pytest.mark.anyio
async def test_fastapi_status_response_has_running_key():
    import fastapi_web_app.main as app_main
    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/status")
    assert "running" in response.json()


@pytest.mark.deployment
@pytest.mark.anyio
async def test_fastapi_status_response_has_auto_refresh_key():
    import fastapi_web_app.main as app_main
    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/status")
    assert "auto_refresh_enabled" in response.json()


@pytest.mark.deployment
@pytest.mark.anyio
async def test_fastapi_healthz_mappings_returns_200():
    import fastapi_web_app.main as app_main
    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/healthz/mappings")
    assert response.status_code == 200


@pytest.mark.deployment
@pytest.mark.anyio
async def test_fastapi_healthz_mappings_has_files_key():
    import fastapi_web_app.main as app_main
    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/healthz/mappings")
    assert "files" in response.json()


# ---------------------------------------------------------------------------
# Group 3: HTML email assembly — broken email prevention
# ---------------------------------------------------------------------------

@pytest.mark.deployment
class TestHtmlEmailAssembly:
    """Verify the html_builder produces well-formed HTML from realistic inputs.

    A crash or mangled output here means every recipient receives a broken email.
    """

    @pytest.fixture(scope="class")
    def html_builder(self):
        """Load html_builder module via importlib to avoid sys.modules stub contamination."""
        spec = importlib.util.spec_from_file_location(
            "dispatch_reports.html_builder_smoke",
            _DISPATCH_DIR / "html_builder.py",
        )
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = "dispatch_reports"
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    def _minimal_report_html(self, title: str = "QRY Management Report") -> str:
        return f"""
<html><body>
<h2>{title}</h2>
<table>
  <tr><th class="left">Market</th><th>YTD Sales</th><th>Budget</th><th>% vs Budget</th></tr>
  <tr class="total"><td>Germany</td><td>1,200</td><td>1,000</td><td>120%</td></tr>
  <tr class="total"><td>France</td><td>800</td><td>900</td><td>89%</td></tr>
  <tr class="grand-total"><td>Total Sales</td><td>2,000</td><td>1,900</td><td>105%</td></tr>
</table>
</body></html>
"""

    def test_build_html_body_returns_html_content_type(self, tmp_path, html_builder):
        html_file = tmp_path / "report.html"
        html_file.write_text(self._minimal_report_html(), encoding="utf-8")
        body_type, body_content = html_builder.build_html_body(
            [html_file],
            plain_intro="Sales report attached.",
            banner_title="Management Report",
        )
        assert body_type == "HTML"
        assert body_content  # non-empty

    def test_html_body_contains_opening_and_closing_html_tags(self, tmp_path, html_builder):
        html_file = tmp_path / "report.html"
        html_file.write_text(self._minimal_report_html(), encoding="utf-8")
        _, body = html_builder.build_html_body(
            [html_file],
            plain_intro="Sales report attached.",
            banner_title="Management Report",
        )
        lower = body.lower()
        assert "<html" in lower
        assert "</html>" in lower

    def test_html_body_no_unclosed_script_tags(self, tmp_path, html_builder):
        """Ensures no accidental XSS vectors leak into the email body."""
        html_file = tmp_path / "report.html"
        html_file.write_text(self._minimal_report_html(), encoding="utf-8")
        _, body = html_builder.build_html_body(
            [html_file],
            plain_intro="Sales report.",
            banner_title="Management Report",
        )
        assert "<script" not in body.lower()

    def test_process_report_table_returns_tuple(self, html_builder):
        result = html_builder.process_report_table(self._minimal_report_html())
        title, summary, tables, currency = result
        assert isinstance(title, str) and title  # non-empty title
        assert isinstance(tables, list) and tables  # at least one table
        assert currency in ("kEUR", "kUSD")

    def test_title_strips_qry_prefix(self, html_builder):
        title, _, _, _ = html_builder.process_report_table(
            self._minimal_report_html("QRY Management Report")
        )
        assert not title.startswith("QRY")
        assert "Management Report" in title

    def test_usd_report_sets_currency_kusd(self, html_builder):
        usd_html = self._minimal_report_html().replace("1,200", "1,200 kUSD")
        _, _, _, currency = html_builder.process_report_table(usd_html)
        assert currency == "kUSD"


# ---------------------------------------------------------------------------
# Group 4: Entity mapping protection — unmapped entity prevention
# ---------------------------------------------------------------------------

@pytest.mark.deployment
class TestEntityMappingStructure:
    """Guard against deploying with a corrupt or missing entity_mappings.csv.

    An absent or empty mapping file causes 100% unmapped rows and a blank
    email body — the dispatch silently sends an empty report.
    """

    # Columns that must exist for entity-to-market mapping to work correctly.
    # Derived from the actual entity_mappings.csv schema.
    REQUIRED_COLUMNS = {"Customer_Code", "Market_Group"}

    def test_entity_mappings_file_present_or_skip(self):
        csv_path = _DATA_INPUTS_MAPPINGS / "entity_mappings.csv"
        if not csv_path.exists():
            pytest.skip("entity_mappings.csv not present in this environment (blob-only deploy)")

    def test_entity_mappings_has_required_columns(self):
        csv_path = _DATA_INPUTS_MAPPINGS / "entity_mappings.csv"
        if not csv_path.exists():
            pytest.skip("entity_mappings.csv not present in this environment")

        import csv
        with open(csv_path, encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            columns = set(reader.fieldnames or [])

        missing = self.REQUIRED_COLUMNS - columns
        assert not missing, (
            f"entity_mappings.csv is missing required columns: {missing}. "
            f"Found: {sorted(columns)}"
        )

    def test_entity_mappings_is_not_empty(self):
        csv_path = _DATA_INPUTS_MAPPINGS / "entity_mappings.csv"
        if not csv_path.exists():
            pytest.skip("entity_mappings.csv not present in this environment")

        import csv
        with open(csv_path, encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        assert rows, "entity_mappings.csv exists but contains zero data rows — all entities would be unmapped"

    def test_regional_mappings_file_present_or_skip(self):
        csv_path = _DATA_INPUTS_MAPPINGS / "py25_regional_mappings.csv"
        if not csv_path.exists():
            pytest.skip("py25_regional_mappings.csv not present in this environment")


# ---------------------------------------------------------------------------
# Group 5: Dispatch wiring — subject and config checks
# ---------------------------------------------------------------------------

@pytest.mark.deployment
class TestDispatchWiring:
    """Verify that the dispatch pipeline's configuration layer is coherent."""

    def _load_dispatch_init(self) -> types.ModuleType:
        """Load dispatch_reports __init__.py via importlib to bypass sys.modules stubs."""
        _stub_azure_functions_runtime()
        # Build a fresh package module with __path__ set so relative imports resolve.
        pkg = types.ModuleType("dispatch_reports")
        pkg.__path__ = [str(_DISPATCH_DIR)]  # type: ignore[attr-defined]
        pkg.__package__ = "dispatch_reports"
        sys.modules["dispatch_reports"] = pkg
        spec = importlib.util.spec_from_file_location(
            "dispatch_reports",
            _DISPATCH_DIR / "__init__.py",
            submodule_search_locations=[str(_DISPATCH_DIR)],
        )
        assert spec and spec.loader
        spec.loader.exec_module(pkg)  # type: ignore[union-attr]
        return pkg

    def test_build_subject_mtd_returns_non_empty(self, monkeypatch):
        """_build_subject must always return a usable subject line."""
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        monkeypatch.delenv("V2_UNIFIED_REFRESH_REPORT_TYPE", raising=False)
        dispatch_init = self._load_dispatch_init()
        subject = dispatch_init._build_subject(None)
        assert subject
        assert len(subject) > 5

    def test_build_subject_eom_mode_adds_eom_prefix(self, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        monkeypatch.delenv("REPORT_DISPATCH_SUBJECT", raising=False)
        dispatch_init = self._load_dispatch_init()
        subject = dispatch_init._build_subject(None)
        assert "EOM" in subject

    def test_build_subject_explicit_override_wins(self, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_SUBJECT", "Custom Override Subject")
        dispatch_init = self._load_dispatch_init()
        subject = dispatch_init._build_subject(None)
        assert subject == "Custom Override Subject"

    def test_config_parse_recipients_handles_semicolons(self):
        from dispatch_reports.config import parse_recipients
        result = parse_recipients("a@b.com;c@d.com")
        assert "a@b.com" in result
        assert "c@d.com" in result

    def test_config_parse_recipients_empty_returns_empty_list(self):
        from dispatch_reports.config import parse_recipients
        assert parse_recipients("") == []
        assert parse_recipients(None) == []


# ---------------------------------------------------------------------------
# Group 6: Output freshness infrastructure
# ---------------------------------------------------------------------------

@pytest.mark.deployment
class TestOutputFreshnessInfrastructure:
    """Verify the freshness-check plumbing that guards against stale dispatches."""

    def test_resolve_outputs_path_does_not_raise(self, tmp_path, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_OUTPUTS_PATH", str(tmp_path))
        from dispatch_reports.report_collector import resolve_outputs_path
        result = resolve_outputs_path()
        assert result.exists()

    def test_check_outputs_freshness_returns_none_for_empty_dir(self, tmp_path):
        from dispatch_reports.report_collector import _check_outputs_freshness
        result = _check_outputs_freshness(tmp_path)
        assert result is None

    def test_check_outputs_freshness_detects_stale_outputs(self, tmp_path):
        from datetime import datetime, timezone, timedelta
        from dispatch_reports.report_collector import _check_outputs_freshness

        old_ts = datetime.now(timezone.utc) - timedelta(hours=48)
        (tmp_path / "run_summary.json").write_text(
            json.dumps({"generated_at_utc": old_ts.isoformat()}),
            encoding="utf-8",
        )
        age = _check_outputs_freshness(tmp_path, stale_threshold_hours=26.0)
        assert age is not None and age > 26.0

    def test_derive_report_date_always_returns_datetime(self, tmp_path):
        import datetime as _dt
        from dispatch_reports.report_collector import derive_report_date
        result = derive_report_date(tmp_path)
        assert isinstance(result, _dt.datetime)
