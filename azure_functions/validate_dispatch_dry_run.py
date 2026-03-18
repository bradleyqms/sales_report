"""Validate dispatch inputs without sending email.

Checks, per report stream (management/core/usa):
- recipients resolved (including TEST_* overrides)
- subject resolved
- body generated and contains expected title marker
- attachments resolved as expected for that stream

Usage:
    python validate_dispatch_dry_run.py [--test-recipient you@example.com] [--mode both|MTD|EOM]
"""
from __future__ import annotations

import argparse
import calendar
import importlib.util
import json
import os
import re
import sys
from datetime import datetime
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


def _parse_reference_date(raw: str) -> datetime:
    value = raw.strip()
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("reference dates must be YYYY-MM-DD") from exc


def _assert_title_month_matches(title: str, year: int, month: int, label: str) -> None:
    month_name = calendar.month_name[month]
    _assert(
        month_name in title and str(year) in title,
        f"{label} title month mismatch. Expected {month_name} {year}, got: {title}",
    )


def _assert_title_day_range(title: str, expected_end_day: int, label: str) -> None:
    m = re.search(r"1-(\d{1,2}),\s+\d{4}", title)
    _assert(bool(m), f"{label} title missing expected day-range pattern: {title}")
    actual = int(m.group(1))
    _assert(actual == expected_end_day, f"{label} title day-range mismatch. Expected 1-{expected_end_day}, got: {title}")


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


def _selected_modes(mode_arg: str) -> list[str]:
    value = mode_arg.strip().upper()
    if value == "BOTH":
        return ["MTD", "EOM"]
    if value in {"MTD", "EOM"}:
        return [value]
    raise ValueError("--mode must be one of: both, MTD, EOM")


def _expected_end_day(report_date, mode: str) -> int:
    resolved_mode = mode.strip().upper()
    if resolved_mode == "EOM":
        return calendar.monthrange(report_date.year, report_date.month)[1]
    return report_date.day


def _subject_for_stream(stream_label: str, report_date, fallback_date: str, mode: str) -> str:
    date_str = report_date.strftime('%d.%m.%Y') if report_date else fallback_date
    prefix = "EOM " if mode.strip().upper() == "EOM" else ""
    return f"{prefix}QMS {stream_label} {date_str}"


def _validate_mode(
    mode: str,
    outputs_dir: Path,
    display_date,
    dispatch_mod,
    core_mod,
    usa_mod,
    forced_year: int | None,
    forced_month: int | None,
) -> None:
    os.environ["V2_UNIFIED_REFRESH_REPORT_TYPE"] = mode
    expected_end_day = _expected_end_day(display_date, mode)

    # Management checks
    mgmt_recip = dispatch_mod._parse_recipients(
        os.getenv("TEST_REPORT_DISPATCH_RECIPIENTS", "").strip() or os.getenv("REPORT_DISPATCH_RECIPIENTS")
    )
    _assert(bool(mgmt_recip), f"Management recipients not resolved ({mode})")
    mgmt_html = dispatch_mod._collect_html_files(outputs_dir)
    if not mgmt_html:
        mgmt_html = _fallback_find_latest(outputs_dir, [
            "combined_management_report_*.html",
            "management_report_core_markets_*.html",
        ])
    _assert(bool(mgmt_html), f"Management HTML files not found ({mode})")
    mgmt_attachments = dispatch_mod._collect_csv_attachments(outputs_dir)
    _assert(bool(mgmt_attachments), f"Management CSV attachments not found ({mode})")
    mgmt_subject = os.getenv("REPORT_DISPATCH_SUBJECT") or _subject_for_stream(
        "Management Sales Report",
        display_date,
        dispatch_mod.report_date_str(),
        mode,
    )
    mgmt_body_type, mgmt_body = dispatch_mod._build_html_body(
        mgmt_html,
        os.getenv("REPORT_DISPATCH_BODY", "Please find the latest QMS sales data attached."),
        banner_title=dispatch_mod.report_period_banner("Management Report", display_date, mode),
        section_title_resolver=lambda path, title: dispatch_mod._management_section_title(path, title, display_date, mode),
        banner_subtitle=dispatch_mod.report_period_summary(display_date, mode),
    )
    mgmt_h2 = _extract_first_h2(mgmt_body)
    _assert(bool(mgmt_subject.strip()), f"Management subject is empty ({mode})")
    _assert("Management Report" in mgmt_body, f"Management body missing expected title marker ({mode})")
    if forced_year and forced_month:
        _assert_title_month_matches(mgmt_h2, forced_year, forced_month, f"Management/{mode}")
        _assert_title_day_range(mgmt_h2, expected_end_day, f"Management/{mode}")

    # Core checks
    core_recip = dispatch_mod._parse_recipients(
        os.getenv("TEST_CORE_MARKETS_RECIPIENTS", "").strip() or os.getenv("CORE_MARKET_DISPATCH_RECIPIENTS")
    )
    _assert(bool(core_recip), f"Core recipients not resolved ({mode})")
    core_html = core_mod._collect_core_market_html(outputs_dir)
    if not core_html:
        core_html = _fallback_find_latest(outputs_dir, ["management_report_core_markets_*.html"])
    _assert(bool(core_html), f"Core market HTML files not found ({mode})")
    core_pdf_enabled = os.getenv("CORE_MARKET_SEND_PDF", "true").strip().lower() not in {"false", "0", "no"}
    core_attachments = core_mod._collect_core_market_pdf(outputs_dir)
    if core_pdf_enabled:
        _assert(bool(core_attachments), f"Core market PDF attachment expected but not found ({mode})")
    else:
        _assert(not core_attachments, f"Core market PDF attachments resolved despite CORE_MARKET_SEND_PDF being disabled ({mode})")
    core_subject = os.getenv("CORE_MARKET_DISPATCH_SUBJECT") or _subject_for_stream(
        "Core Market Sales Report",
        display_date,
        dispatch_mod.report_date_str(),
        mode,
    )
    core_body_type, core_body = dispatch_mod._build_html_body(
        core_html,
        os.getenv("CORE_MARKET_DISPATCH_BODY", "Please find the latest QMS core market report attached."),
        banner_title=dispatch_mod.report_period_banner("Core Market Sales Report", display_date, mode),
        footer_note="The PDF report is attached.",
        section_title_resolver=lambda _path, _title: dispatch_mod.report_period_banner("Core Market Sales Report", display_date, mode),
        banner_subtitle=dispatch_mod.report_period_summary(display_date, mode),
    )
    core_h2 = _extract_first_h2(core_body)
    _assert(bool(core_subject.strip()), f"Core subject is empty ({mode})")
    _assert("Core Market Sales Report" in core_body, f"Core body missing expected title marker ({mode})")
    if forced_year and forced_month:
        _assert_title_month_matches(core_h2, forced_year, forced_month, f"Core/{mode}")
        _assert_title_day_range(core_h2, expected_end_day, f"Core/{mode}")

    # USA checks
    usa_recip = dispatch_mod._parse_recipients(
        os.getenv("TEST_USA_SPA_RECIPIENTS", "").strip() or os.getenv("USA_SPA_DISPATCH_RECIPIENTS")
    )
    _assert(bool(usa_recip), f"USA recipients not resolved ({mode})")
    usa_html = usa_mod._collect_usa_spa_html(outputs_dir)
    if not usa_html:
        usa_html = _fallback_find_latest(outputs_dir, ["management_report_usa_spa_*.html"])
    _assert(bool(usa_html), f"USA Spa HTML files not found ({mode})")
    usa_subject = os.getenv("USA_SPA_DISPATCH_SUBJECT") or _subject_for_stream(
        "USA Spa Sales Report",
        display_date,
        dispatch_mod.report_date_str(),
        mode,
    )
    usa_body_type, usa_body = dispatch_mod._build_html_body(
        usa_html,
        os.getenv("USA_SPA_DISPATCH_BODY", "Please find the latest QMS USA Spa sales report below."),
        banner_title=dispatch_mod.report_period_banner("USA Spa Sales Report", display_date, mode),
        footer_note="",
        section_title_resolver=lambda _path, _title: dispatch_mod.report_period_banner("USA Spa Sales Report", display_date, mode),
        banner_subtitle=dispatch_mod.report_period_summary(display_date, mode),
    )
    usa_h2 = _extract_first_h2(usa_body)
    _assert(bool(usa_subject.strip()), f"USA subject is empty ({mode})")
    _assert("USA Spa Sales Report" in usa_body, f"USA body missing expected title marker ({mode})")
    if forced_year and forced_month:
        _assert_title_month_matches(usa_h2, forced_year, forced_month, f"USA/{mode}")
        _assert_title_day_range(usa_h2, expected_end_day, f"USA/{mode}")

    print(f"\n[OK] Management ({mode})")
    print(f"  reference  : {display_date.strftime('%Y-%m-%d')}")
    print(f"  recipients : {mgmt_recip}")
    print(f"  subject    : {mgmt_subject}")
    print(f"  body_type  : {mgmt_body_type}")
    print(f"  first_h2   : {mgmt_h2}")
    print(f"  attachments: {[p.name for p in mgmt_attachments]}")

    print(f"\n[OK] Core Market ({mode})")
    print(f"  reference  : {display_date.strftime('%Y-%m-%d')}")
    print(f"  recipients : {core_recip}")
    print(f"  subject    : {core_subject}")
    print(f"  body_type  : {core_body_type}")
    print(f"  first_h2   : {core_h2}")
    if core_pdf_enabled:
        print(f"  attachments: {[p.name for p in core_attachments]}")
    else:
        print("  attachments: [] (CORE_MARKET_SEND_PDF disabled)")

    print(f"\n[OK] USA Spa ({mode})")
    print(f"  reference  : {display_date.strftime('%Y-%m-%d')}")
    print(f"  recipients : {usa_recip}")
    print(f"  subject    : {usa_subject}")
    print(f"  body_type  : {usa_body_type}")
    print(f"  first_h2   : {usa_h2}")
    print("  attachments: [] (inline-only report)")


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
    parser.add_argument(
        "--mode",
        default="both",
        help="Validate dispatch formatting for one mode or both: both, MTD, EOM (default: both)",
    )
    parser.add_argument(
        "--mtd-reference-date",
        default=None,
        help="Optional MTD display/reference date for format checks (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--eom-reference-date",
        default=None,
        help="Optional EOM display/reference date for format checks (YYYY-MM-DD)",
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
    selected_modes = _selected_modes(args.mode)
    print(f"[INFO] modes={', '.join(selected_modes)}")

    mode_reference_dates = {
        "MTD": _parse_reference_date(args.mtd_reference_date) if args.mtd_reference_date else report_date,
        "EOM": _parse_reference_date(args.eom_reference_date) if args.eom_reference_date else report_date,
    }

    forced_year = None
    forced_month = None
    if args.force_period:
        forced_year, forced_month = _parse_force_period(args.force_period)
        print(f"[INFO] forced_period_guard={forced_year:04d}-{forced_month:02d}")

    for mode in selected_modes:
        _validate_mode(
            mode=mode,
            outputs_dir=outputs_dir,
            display_date=mode_reference_dates[mode],
            dispatch_mod=dispatch_mod,
            core_mod=core_mod,
            usa_mod=usa_mod,
            forced_year=forced_year,
            forced_month=forced_month,
        )

    print(f"\n[OK] Dry-run dispatch validation passed for {', '.join(selected_modes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
