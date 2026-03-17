import io
import sys
import zipfile
import json
import base64
from pathlib import Path

import pytest
import httpx


sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi_web_app import main as app_main


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _write_file(path: Path, content: str = "sample") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_resolve_run_artifacts_prefers_same_run_directory(tmp_path):
    output_root = tmp_path / "outputs"
    run_dir = output_root / "report_type=MTD" / "date=2026-03-16" / "time=101010"
    management_dir = run_dir / "management_combined"
    core_dir = run_dir / "core_markets"
    usa_dir = run_dir / "usa_spa"
    unmapped_dir = run_dir / "unmapped_entities"
    other_dir = output_root / "report_type=MTD" / "date=2026-03-16" / "time=111111"
    timestamp = "20260316_101010"

    combined = _write_file(management_dir / f"combined_management_report_mtd_v2_{timestamp}.csv")
    core = _write_file(core_dir / f"management_report_core_markets_mtd_v2_{timestamp}.html")
    usa = _write_file(usa_dir / f"management_report_usa_spa_mtd_v2_{timestamp}.xlsx")
    matching_unmapped = _write_file(unmapped_dir / "unmapped_entities_20260316_101015.csv")
    stale_newer_unmapped = _write_file(other_dir / "unmapped_entities_20260316_111500.csv")
    stale_newer_unmapped.touch()

    report_files, unmapped_path = app_main._resolve_run_artifacts(output_root, timestamp)

    assert report_files == sorted([combined, core, usa], key=lambda path: path.name)
    assert unmapped_path == matching_unmapped


def test_resolve_run_artifacts_ignores_unmapped_from_other_run(tmp_path):
    output_root = tmp_path / "outputs"
    run_dir = output_root / "report_type=MTD" / "date=2026-03-16" / "time=101010"
    management_dir = run_dir / "management_combined"
    core_dir = run_dir / "core_markets"
    usa_dir = run_dir / "usa_spa"
    other_dir = output_root / "report_type=MTD" / "date=2026-03-16" / "time=111111"
    timestamp = "20260316_101010"

    _write_file(management_dir / f"combined_management_report_mtd_v2_{timestamp}.csv")
    _write_file(core_dir / f"management_report_core_markets_mtd_v2_{timestamp}.html")
    _write_file(usa_dir / f"management_report_usa_spa_mtd_v2_{timestamp}.xlsx")
    _write_file(other_dir / "unmapped_entities_20260316_111500.csv")

    _, unmapped_path = app_main._resolve_run_artifacts(output_root, timestamp)

    assert unmapped_path is None


class _FakeProcess:
    def __init__(self, lines: list[str], returncode: int = 0):
        self.stdout = io.StringIO("".join(lines))
        self._returncode = returncode

    def poll(self):
        if self.stdout.tell() < len(self.stdout.getvalue()):
            return None
        return self._returncode


def test_execute_report_adds_unmapped_file_to_zip(tmp_path, monkeypatch):
    output_root = tmp_path / "outputs"
    static_base = tmp_path / "fastapi_web_app"
    static_dir = static_base / "static"
    run_dir = output_root / "report_type=MTD" / "date=2026-03-16" / "time=101010"
    management_dir = run_dir / "management_combined"
    core_dir = run_dir / "core_markets"
    usa_dir = run_dir / "usa_spa"
    unmapped_dir = run_dir / "unmapped_entities"
    timestamp = "20260316_101010"

    static_base.mkdir(parents=True, exist_ok=True)

    combined = _write_file(management_dir / f"combined_management_report_mtd_v2_{timestamp}.csv")
    core = _write_file(core_dir / f"management_report_core_markets_mtd_v2_{timestamp}.html")
    usa = _write_file(usa_dir / f"management_report_usa_spa_mtd_v2_{timestamp}.xlsx")
    unmapped = _write_file(unmapped_dir / "unmapped_entities_20260316_101015.csv")

    lines = [f"[OK] combined:    {combined.stem}\n"]

    monkeypatch.setattr(app_main, "BASE_DIR", static_base)
    monkeypatch.setattr(app_main, "_resolve_outputs_dir", lambda: output_root)
    monkeypatch.setattr(app_main, "extract_latest_segment_metrics", lambda: {
        "core_markets": {"sales": 1.0, "budget_pct": 2.0},
        "usa_spa": {"sales": 3.0, "budget_pct": 4.0},
    })
    monkeypatch.setattr(app_main.subprocess, "Popen", lambda *args, **kwargs: _FakeProcess(lines))

    app_main.execute_report()

    zip_path = static_dir / f"combined_reports_{timestamp}.zip"
    assert zip_path.exists()
    assert app_main.report_status["unmapped_url"] == f"/download/{unmapped.name}"

    with zipfile.ZipFile(zip_path) as archive:
        assert sorted(archive.namelist()) == sorted([
            combined.name,
            core.name,
            usa.name,
            unmapped.name,
        ])


def _easy_auth_header(email: str) -> dict[str, str]:
    payload = {
        "claims": [
            {"typ": "preferred_username", "val": email}
        ]
    }
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return {
        "x-ms-client-principal-name": email,
        "x-ms-client-principal": encoded,
    }


@pytest.mark.parametrize(
    "path,env_var,allowed_email,blocked_email",
    [
        ("/", "GLOBAL_VIEW_EMAILS", "mgmt.allowed@qmsmedicosmetics.com", "mgmt.blocked@qmsmedicosmetics.com"),
        ("/coremarkets", "CORE_MARKETS_VIEW_EMAILS", "core.allowed@qmsmedicosmetics.com", "core.blocked@qmsmedicosmetics.com"),
        ("/usaspa", "USA_SPA_VIEW_EMAILS", "usa.allowed@qmsmedicosmetics.com", "usa.blocked@qmsmedicosmetics.com"),
    ],
)
@pytest.mark.anyio
async def test_route_authorization_allows_and_blocks_by_recipient_lists(monkeypatch, path, env_var, allowed_email, blocked_email):
    monkeypatch.setenv("GLOBAL_VIEW_EMAILS", "")
    monkeypatch.setenv("CORE_MARKETS_VIEW_EMAILS", "")
    monkeypatch.setenv("USA_SPA_VIEW_EMAILS", "")
    monkeypatch.setenv(env_var, allowed_email)

    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        allowed_resp = await client.get(path, headers=_easy_auth_header(allowed_email))
        blocked_resp = await client.get(path, headers=_easy_auth_header(blocked_email))

    assert allowed_resp.status_code == 200
    assert blocked_resp.status_code == 403


@pytest.mark.anyio
async def test_shared_api_endpoints_allow_users_from_any_audience_list(monkeypatch):
    monkeypatch.setenv("GLOBAL_VIEW_EMAILS", "mgmt.allowed@qmsmedicosmetics.com")
    monkeypatch.setenv("CORE_MARKETS_VIEW_EMAILS", "core.allowed@qmsmedicosmetics.com")
    monkeypatch.setenv("USA_SPA_VIEW_EMAILS", "usa.allowed@qmsmedicosmetics.com")

    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp_core = await client.get("/status", headers=_easy_auth_header("core.allowed@qmsmedicosmetics.com"))
        resp_usa = await client.get("/metrics", headers=_easy_auth_header("usa.allowed@qmsmedicosmetics.com"))
        resp_blocked = await client.get("/segment-metrics", headers=_easy_auth_header("not.allowed@qmsmedicosmetics.com"))

    assert resp_core.status_code == 200
    assert resp_usa.status_code == 200
    assert resp_blocked.status_code == 403


@pytest.mark.anyio
async def test_global_view_users_can_access_all_view_pages(monkeypatch):
    monkeypatch.setenv("GLOBAL_VIEW_EMAILS", "global.allowed@qmsmedicosmetics.com")
    monkeypatch.setenv("CORE_MARKETS_VIEW_EMAILS", "core.allowed@qmsmedicosmetics.com")
    monkeypatch.setenv("USA_SPA_VIEW_EMAILS", "usa.allowed@qmsmedicosmetics.com")

    transport = httpx.ASGITransport(app=app_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp_root = await client.get("/", headers=_easy_auth_header("global.allowed@qmsmedicosmetics.com"))
        resp_core = await client.get("/coremarkets", headers=_easy_auth_header("global.allowed@qmsmedicosmetics.com"))
        resp_usa = await client.get("/usaspa", headers=_easy_auth_header("global.allowed@qmsmedicosmetics.com"))

    assert resp_root.status_code == 200
    assert resp_core.status_code == 200
    assert resp_usa.status_code == 200