"""Validate dispatch inputs without sending email.

Checks, per report stream (management/core/usa):
- recipients resolved (including TEST_* overrides)
- subject resolved
- body generated and contains expected title marker

Usage:
    python validate_dispatch_dry_run.py [--test-recipient you@example.com]
"""
from __future__ import annotations

import argparse
import calendar
import importlib.util
import json
import os
import re
import sys
from pathlib import Path

_HERE = Path(__file__).parent
_PKG = _HERE / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

_settings_file = _HERE / "local.settings.json"
if _settings_file.exists():
    _settings = json.loads(_settings_file.read_text(encoding="utf-8"))
    for _k, _v in _settings.get("Values", {}).items():
        if _k not in os.environ:
            os.environ[_k] = str(_v)


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Failed to load module at {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _extract_first_h2(html: str) -> str:
    m = re.search(r"<h2[^>]*>(.*?)</h2>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"<[^>]+>", "", m.group(1)).strip()


def _assert(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def _parse_force_period(force_period: str) -> tuple[int, int]:
    value = force_period.strip()
    if len(value) == 7:
        year, month = map(int, value.split("-"))
        return year, month
    if len(value) == 10:
        parts = value.split("-")
        return int(parts[0]), int(parts[1])
    raise ValueError("--force-period must be YYYY-MM or YYYY-MM-DD")


def _assert_title_month_matches(title: str, year: int, month: int, label: str) -> None:
    month_name = calendar.month_name[month]
    _assert(
        month_name in title and str(year) in title,
        f"{label} title month mismatch. Expected {month_name} {year}, got: {title}",
    )


def _fallback_find_latest(outputs_dir: Path, patterns: list[str]) -> list[Path]:
    """Find latest match per pattern recursively (supports archived layouts)."""
    found: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        matches = [p for p in outputs_dir.rglob(pattern) if p.is_file()]
        if not matches:
            continue
        latest = sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]
        rp = latest.resolve()
        if rp not in seen:
            found.append(latest)
            seen.add(rp)
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate dispatch dry-run content")
    parser.add_argument("--test-recipient", default=None, help="Override all TEST_* recipient env vars")
    parser.add_argument(
        "--outputs-dir",
        default=None,
        help="Override outputs directory used by dispatch resolver",
    )
    parser.add_argument(
        "--force-period",
        default=None,
        help="Optional period guard (YYYY-MM or YYYY-MM-DD). Fails if body title month differs.",
    )
    args = parser.parse_args()

    if args.test_recipient:
        os.environ["TEST_REPORT_DISPATCH_RECIPIENTS"] = args.test_recipient
        os.environ["TEST_CORE_MARKETS_RECIPIENTS"] = args.test_recipient
        os.environ["TEST_USA_SPA_RECIPIENTS"] = args.test_recipient

    if args.outputs_dir:
        os.environ["REPORT_DISPATCH_OUTPUTS_PATH"] = str(Path(args.outputs_dir).expanduser().resolve())
    else:
        repo_outputs = (_HERE.parent / "data" / "outputs").resolve()
        if repo_outputs.exists():
            os.environ["REPORT_DISPATCH_OUTPUTS_PATH"] = str(repo_outputs)

    dispatch_mod = _load_module("dispatch_reports", _HERE / "dispatch_reports" / "__init__.py")
    core_mod = _load_module("core_market_reports", _HERE / "core_market_reports" / "__init__.py")
    usa_mod = _load_module("dispatch_usa_spa_reports", _HERE / "dispatch_usa_spa_reports" / "__init__.py")

    outputs_dir = dispatch_mod._resolve_outputs_path()
    report_date = dispatch_mod.derive_report_date(outputs_dir)

    print(f"[INFO] outputs_dir={outputs_dir}")
    print(f"[INFO] report_date={report_date.strftime('%Y-%m-%d') if report_date else 'N/A'}")

    forced_year = None
    forced_month = None
    if args.force_period:
        forced_year, forced_month = _parse_force_period(args.force_period)
        print(f"[INFO] forced_period_guard={forced_year:04d}-{forced_month:02d}")

    # Management checks
    mgmt_recip = dispatch_mod._parse_recipients(
        os.getenv("TEST_REPORT_DISPATCH_RECIPIENTS", "").strip() or os.getenv("REPORT_DISPATCH_RECIPIENTS")
    )
    _assert(bool(mgmt_recip), "Management recipients not resolved")
    mgmt_html = dispatch_mod._collect_html_files(outputs_dir)
    if not mgmt_html:
        mgmt_html = _fallback_find_latest(outputs_dir, [
            "combined_management_report_*.html",
            "management_report_core_markets_*.html",
        ])
    _assert(bool(mgmt_html), "Management HTML files not found")
    mgmt_subject = os.getenv("REPORT_DISPATCH_SUBJECT") or (
        f"EOM QMS Management Sales Report {report_date.strftime('%d.%m.%Y')}"
        if report_date else
        f"EOM QMS Management Sales Report {dispatch_mod.report_date_str()}"
    )
    mgmt_body_type, mgmt_body = dispatch_mod._build_html_body(
        mgmt_html,
        os.getenv("REPORT_DISPATCH_BODY", "Please find the latest QMS sales data attached."),
        banner_title=(
            f"Management Report (MTD: {report_date.strftime('%B')} 1-{report_date.day}, {report_date.year})"
            if report_date else dispatch_mod.report_mtd_banner()
        ),
    )
    mgmt_h2 = _extract_first_h2(mgmt_body)
    _assert(bool(mgmt_subject.strip()), "Management subject is empty")
    _assert("Management Report" in mgmt_body, "Management body missing expected title marker")
    if forced_year and forced_month:
        _assert_title_month_matches(mgmt_h2, forced_year, forced_month, "Management")

    # Core checks
    core_recip = dispatch_mod._parse_recipients(
        os.getenv("TEST_CORE_MARKETS_RECIPIENTS", "").strip() or os.getenv("CORE_MARKET_DISPATCH_RECIPIENTS")
    )
    _assert(bool(core_recip), "Core recipients not resolved")
    core_html = core_mod._collect_core_market_html(outputs_dir)
    if not core_html:
        core_html = _fallback_find_latest(outputs_dir, ["management_report_core_markets_*.html"])
    _assert(bool(core_html), "Core market HTML files not found")
    core_subject = os.getenv("CORE_MARKET_DISPATCH_SUBJECT") or (
        f"EOM QMS Core Market Sales Report {report_date.strftime('%d.%m.%Y')}"
        if report_date else f"EOM QMS Core Market Sales Report {dispatch_mod.report_date_str()}"
    )
    core_body_type, core_body = dispatch_mod._build_html_body(
        core_html,
        os.getenv("CORE_MARKET_DISPATCH_BODY", "Please find the latest QMS core market report attached."),
        banner_title="Core Market Sales Report",
        footer_note="The PDF report is attached.",
    )
    core_h2 = _extract_first_h2(core_body)
    _assert(bool(core_subject.strip()), "Core subject is empty")
    _assert("Core Market Sales Report" in core_body, "Core body missing expected title marker")
    if forced_year and forced_month:
        _assert_title_month_matches(core_h2, forced_year, forced_month, "Core")

    # USA checks
    usa_recip = dispatch_mod._parse_recipients(
        os.getenv("TEST_USA_SPA_RECIPIENTS", "").strip() or os.getenv("USA_SPA_DISPATCH_RECIPIENTS")
    )
    _assert(bool(usa_recip), "USA recipients not resolved")
    usa_html = usa_mod._collect_usa_spa_html(outputs_dir)
    if not usa_html:
        usa_html = _fallback_find_latest(outputs_dir, ["management_report_usa_spa_*.html"])
    _assert(bool(usa_html), "USA Spa HTML files not found")
    usa_subject = os.getenv("USA_SPA_DISPATCH_SUBJECT") or (
        f"EOM QMS USA Spa Sales Report {report_date.strftime('%d.%m.%Y')}"
        if report_date else f"EOM QMS USA Spa Sales Report {dispatch_mod.report_date_str()}"
    )
    usa_body_type, usa_body = dispatch_mod._build_html_body(
        usa_html,
        os.getenv("USA_SPA_DISPATCH_BODY", "Please find the latest QMS USA Spa sales report below."),
        banner_title="USA Spa Sales Report",
        footer_note="",
    )
    usa_h2 = _extract_first_h2(usa_body)
    _assert(bool(usa_subject.strip()), "USA subject is empty")
    _assert("USA Spa Sales Report" in usa_body, "USA body missing expected title marker")
    if forced_year and forced_month:
        _assert_title_month_matches(usa_h2, forced_year, forced_month, "USA")

    print("\n[OK] Management")
    print(f"  recipients: {mgmt_recip}")
    print(f"  subject   : {mgmt_subject}")
    print(f"  body_type : {mgmt_body_type}")
    print(f"  first_h2  : {mgmt_h2}")

    print("\n[OK] Core Market")
    print(f"  recipients: {core_recip}")
    print(f"  subject   : {core_subject}")
    print(f"  body_type : {core_body_type}")
    print(f"  first_h2  : {core_h2}")

    print("\n[OK] USA Spa")
    print(f"  recipients: {usa_recip}")
    print(f"  subject   : {usa_subject}")
    print(f"  body_type : {usa_body_type}")
    print(f"  first_h2  : {usa_h2}")

    print("\n[OK] Dry-run dispatch validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
