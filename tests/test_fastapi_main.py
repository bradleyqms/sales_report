import io
import sys
import zipfile
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi_web_app import main as app_main


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