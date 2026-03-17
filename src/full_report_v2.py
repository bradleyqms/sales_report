import argparse
import calendar
import datetime
import inspect
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from urllib import request

import pandas as pd
from dotenv import load_dotenv

from core_market_report import CoreMarketReportGenerator
from qry_data_mapping import apply_mappings
from receivables_report_generator import ManagementReportGenerator
from sharepoint_client import SharePointHandler
from usa_spa_report import USASpaReportGenerator
from v2_export_service import (
    V2ExportService,
    build_combined_dataframe,
    build_period_token,
    compose_output_name,
)
from v2_unified_qry_ingestion import load_unified_qry_csv
from v2_unified_qry_ingestion import get_schema_manifest_version
from v2_validation import ValidationError, run_ingestion_validations


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Unified QRY V2 report runner")
    parser.add_argument("--report-type", default="MTD", choices=["MTD", "EOM"], help="Run mode")
    parser.add_argument("--input-unified-csv", default=None, help="Optional local unified CSV override")
    parser.add_argument("--schema-mode", default="strict", choices=["strict", "flexible"], help="Unified source schema validation mode")
    parser.add_argument("--force-period", default=None, help="Period anchor: YYYY-MM or YYYY-MM-DD")
    parser.add_argument("--mapping-file", default=None, help="Optional mapping CSV override")
    parser.add_argument("--output-tag", default="v2", help="Extra output token")
    parser.add_argument("--output-dir", default=None, help="Optional output directory")
    parser.add_argument("--download-retries", type=int, default=3, help="Retry attempts for SharePoint download")
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0, help="Base backoff seconds for retries")
    parser.add_argument("--eom-completeness-policy", default="calendar-day", choices=["calendar-day", "business-day"], help="EOM completeness rule")
    parser.add_argument("--relaxed-validation", action="store_true", help="Warn instead of fail on period completeness issues")
    parser.add_argument("--dry-run", action="store_true", help="Run validation and naming without writing outputs")
    parser.add_argument("--summary-path", default=None, help="Optional JSON path for run summary artifact")
    parser.add_argument("--schema-manifest", default=None, help="Optional schema profile manifest path")
    parser.add_argument("--alert-webhook-url", default=None, help="Optional webhook URL for hard-failure alerts")
    parser.add_argument("--alert-log-path", default=None, help="Optional alert log file path")
    return parser.parse_args(argv)


def parse_force_period(force_period: str | None, report_type: str) -> datetime.datetime:
    if not force_period:
        now = datetime.datetime.now()
        if report_type == "EOM":
            last_day = calendar.monthrange(now.year, now.month)[1]
            return datetime.datetime(now.year, now.month, last_day)
        return datetime.datetime(now.year, now.month, now.day)

    value = force_period.strip()
    if len(value) == 7:
        year, month = map(int, value.split("-"))
        day = calendar.monthrange(year, month)[1] if report_type == "EOM" else 1
        return datetime.datetime(year, month, day)

    if len(value) == 10:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%d")
        if report_type == "EOM":
            last_day = calendar.monthrange(parsed.year, parsed.month)[1]
            return datetime.datetime(parsed.year, parsed.month, last_day)
        return parsed

    raise ValueError("--force-period must be YYYY-MM or YYYY-MM-DD")


SP_EXTRACTS_PATH = "/sites/DATAANDREPORTING/Shared Documents/SAP Extracts"


def _build_sp_handler(project_root: Path):
    """Return an initialised SharePointHandler if SP credentials are configured, else None."""
    load_dotenv(project_root / ".env")
    site_url = os.getenv("SHAREPOINT_SITE_URL")
    client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
    if not all([site_url, client_id, client_secret]):
        return None
    tenant = os.getenv("SHAREPOINT_TENANT_ID") or os.getenv("SHAREPOINT_TENANT_DOMAIN")
    try:
        request_timeout = float(os.getenv("SHAREPOINT_REQUEST_TIMEOUT_SECONDS", "30"))
    except ValueError:
        request_timeout = 30.0
    return SharePointHandler(
        site_url, client_id, client_secret,
        quiet=True, tenant=tenant, request_timeout=request_timeout,
    )


def resolve_input_file(
    filename: str,
    local_path: Path,
    sp_handler,
    retries: int = 1,
    retry_backoff_seconds: float = 1.0,
) -> Path:
    """Return a local path for *filename*, downloading from SharePoint SAP Extracts if needed.

    Priority: SharePoint (if credentials configured) → local fallback.
    """
    if sp_handler is not None:
        try:
            temp_dir = Path(tempfile.mkdtemp(prefix="v2_input_"))
            dest = temp_dir / filename
            download_unified_with_retry(
                sp_handler,
                f"{SP_EXTRACTS_PATH}/{filename}",
                str(dest),
                retries=retries,
                base_backoff_seconds=retry_backoff_seconds,
            )
            logging.info("Downloaded from SharePoint: %s", filename)
            return dest
        except Exception as exc:
            logging.warning(
                "SharePoint download failed for %s: %s — trying local fallback", filename, exc
            )

    if local_path.exists():
        logging.info("Using local file: %s", local_path)
        return local_path

    raise FileNotFoundError(
        f"Required data file not found: {filename}. "
        "Ensure it exists in SharePoint SAP Extracts or locally in data/inputs/."
    )


def resolve_mapping_file(
    project_root: Path,
    explicit_path: str | None,
    retries: int = 1,
    retry_backoff_seconds: float = 1.0,
) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Mapping file not found: {path}")
        return path

    sp_handler = _build_sp_handler(project_root)
    for filename, local_rel in [
        ("entity_mappings.csv", "data/inputs/mappings/entity_mappings.csv"),
        ("py25_regional_mappings.csv", "data/inputs/mappings/py25_regional_mappings.csv"),
    ]:
        try:
            return resolve_input_file(
                filename,
                project_root / local_rel,
                sp_handler,
                retries=retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
        except FileNotFoundError:
            continue

    raise FileNotFoundError(
        "No mapping file found. Provide --mapping-file, configure SharePoint credentials, "
        "or ensure mapping files exist in data/inputs/mappings/."
    )


def resolve_unified_filename(report_type: str) -> str:
    return "new_unified_dbo_qry_mtd.csv" if report_type == "MTD" else "new_unified_dbo_qry_eom.csv"


def resolve_output_dir(project_root: Path, output_dir_arg: str | None) -> Path:
    if output_dir_arg:
        path = Path(output_dir_arg)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    env_dir = os.environ.get("REPORT_OUTPUT_DIR")
    if env_dir:
        path = Path(env_dir)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    path = project_root / "data" / "outputs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_partitioned_output_dir(base_output_dir: Path, report_type: str, generated_at: datetime.datetime) -> Path:
    partition = (
        base_output_dir
        / f"report_type={report_type.upper()}"
        / f"date={generated_at.strftime('%Y-%m-%d')}"
        / f"time={generated_at.strftime('%H%M%S')}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    return partition


def classify_retryable_error(exc: Exception) -> bool:
    retryable_terms = [
        "timeout",
        "tempor",
        "connection",
        "429",
        "503",
        "rate",
        "throttle",
        "unavailable",
    ]
    text = str(exc).lower()
    return any(term in text for term in retryable_terms)


def emit_alert(message: str, severity: str = "error", context: dict | None = None, webhook_url: str | None = None, log_path: str | None = None) -> None:
    payload = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "severity": severity,
        "message": message,
        "context": context or {},
    }

    logging.error("ALERT[%s] %s | %s", severity.upper(), message, payload["context"])

    sink_path = log_path or os.environ.get("V2_ALERT_LOG_PATH")
    if sink_path:
        path = Path(sink_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")

    target = webhook_url or os.environ.get("V2_ALERT_WEBHOOK_URL")
    if target:
        try:
            req = request.Request(
                target,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            request.urlopen(req, timeout=10)
        except Exception as webhook_exc:
            logging.warning("Failed to send alert webhook: %s", webhook_exc)


def write_run_summary(run_summary: dict, output_dir: Path, summary_path_arg: str | None) -> Path:
    if summary_path_arg:
        summary_path = Path(summary_path_arg)
        if not summary_path.is_absolute():
            summary_path = (output_dir / summary_path).resolve()
    else:
        summary_path = output_dir / f"{run_summary['run_id']}_summary.json"

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as handle:
        json.dump(run_summary, handle, indent=2, default=str)
    return summary_path


def download_unified_with_retry(
    sp_handler,
    sharepoint_path: str,
    local_path: str,
    retries: int,
    base_backoff_seconds: float,
) -> None:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            sp_handler.download_file(sharepoint_path, local_path)
            return
        except Exception as exc:
            last_error = exc
            logging.warning("Download failed (%s/%s): %s", attempt, retries, exc)
            if not classify_retryable_error(exc):
                raise RuntimeError(f"Non-retryable download failure: {exc}") from exc
            if attempt < retries:
                sleep_seconds = max(0.0, base_backoff_seconds) * (2 ** (attempt - 1))
                time.sleep(sleep_seconds)
    raise RuntimeError(f"Failed to download unified source after {retries} attempts: {last_error}")


def resolve_unified_source_path(
    project_root: Path,
    report_type: str,
    input_unified_csv: str | None,
    retries: int,
    retry_backoff_seconds: float,
) -> Path:
    if input_unified_csv:
        path = Path(input_unified_csv)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Unified input file not found: {path}")
        return path

    load_dotenv(project_root / ".env")
    site_url = os.getenv("SHAREPOINT_SITE_URL")
    client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
    tenant = os.getenv("SHAREPOINT_TENANT_ID") or os.getenv("SHAREPOINT_TENANT_DOMAIN")
    timeout_raw = os.getenv("SHAREPOINT_REQUEST_TIMEOUT_SECONDS", "30")
    require_sharepoint = os.getenv("V2_UNIFIED_REQUIRE_SHAREPOINT", "false").strip().lower() in {
        "1", "true", "yes", "on"
    }

    try:
        request_timeout = float(timeout_raw)
    except ValueError:
        request_timeout = 30.0

    filename = resolve_unified_filename(report_type)

    if require_sharepoint and not all([site_url, client_id, client_secret]):
        raise RuntimeError(
            "V2_UNIFIED_REQUIRE_SHAREPOINT is enabled, but SharePoint credentials are incomplete. "
            "Set SHAREPOINT_SITE_URL, SHAREPOINT_CLIENT_ID, and SHAREPOINT_CLIENT_SECRET."
        )

    if all([site_url, client_id, client_secret]):
        temp_dir = Path(tempfile.mkdtemp(prefix="v2_unified_qry_"))
        local_path = temp_dir / filename

        sp_handler = SharePointHandler(
            site_url,
            client_id,
            client_secret,
            quiet=True,
            tenant=tenant,
            request_timeout=request_timeout,
        )
        sp_path = f"/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/{filename}"
        download_unified_with_retry(
            sp_handler,
            sp_path,
            str(local_path),
            retries=retries,
            base_backoff_seconds=retry_backoff_seconds,
        )
        return local_path

    fallback = project_root / "data" / "inputs" / filename
    if fallback.exists():
        if require_sharepoint:
            raise RuntimeError(
                "V2_UNIFIED_REQUIRE_SHAREPOINT is enabled; local fallback is disabled. "
                "Fix SharePoint connectivity/credentials to continue."
            )
        return fallback

    raise FileNotFoundError(
        "No unified source path available. Provide --input-unified-csv or configure SharePoint credentials."
    )


def apply_report_date_anchor(generator, report_date: datetime.datetime):
    generator._report_date = report_date
    generator.now = report_date
    generator.current_year = report_date.year
    generator.prior_year = report_date.year - 1
    generator.current_month = report_date.month


def main(argv=None):
    args = parse_args(argv)
    report_type = args.report_type.upper()

    project_root = Path(__file__).resolve().parents[1]
    output_root = resolve_output_dir(project_root, args.output_dir)
    report_date = parse_force_period(args.force_period, report_type=report_type)

    generated_at = datetime.datetime.now()
    timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
    output_dir = build_partitioned_output_dir(output_root, report_type=report_type, generated_at=generated_at)
    period_token = build_period_token(report_type, report_date)
    run_id = compose_output_name(
        prefix="v2_run",
        year=report_date.year,
        timestamp=timestamp,
        period_token=period_token,
        output_tag=args.output_tag,
    )

    run_summary = {
        "run_id": run_id,
        "report_type": report_type,
        "report_date": report_date.strftime("%Y-%m-%d"),
        "schema_mode": args.schema_mode,
        "schema_manifest_version": get_schema_manifest_version(args.schema_manifest),
        "dry_run": bool(args.dry_run),
        "source": {},
        "output_root": str(output_root),
        "output_dir": str(output_dir),
        "validation": {"warnings": []},
        "counts": {
            "rows_in": 0,
            "rows_filtered": 0,
            "rows_mapped": 0,
        },
        "outputs": {},
        "status": "running",
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    print("=" * 80)
    print("UNIFIED QRY V2 REPORT GENERATION")
    print("=" * 80)
    print(f"report_type: {report_type}")
    print(f"report_date: {report_date.strftime('%Y-%m-%d')}")
    print(f"output_dir:  {output_dir}")
    print(f"schema_manifest_version: {run_summary['schema_manifest_version']}")

    try:
        unified_source = resolve_unified_source_path(
            project_root,
            report_type=report_type,
            input_unified_csv=args.input_unified_csv,
            retries=max(1, args.download_retries),
            retry_backoff_seconds=max(0.0, args.retry_backoff_seconds),
        )
    except Exception as exc:
        emit_alert(
            "Unified source resolution failed",
            context={"report_type": report_type, "error": str(exc)},
            webhook_url=args.alert_webhook_url,
            log_path=args.alert_log_path,
        )
        raise

    print(f"unified_source: {unified_source}")
    run_summary["source"] = {
        "path": str(unified_source),
        "name": unified_source.name,
    }

    canonical_df = load_unified_qry_csv(
        str(unified_source),
        report_type=report_type,
        report_date=report_date,
        schema_mode=args.schema_mode,
        schema_manifest_path=args.schema_manifest,
    )
    run_summary["counts"]["rows_in"] = int(len(canonical_df))

    try:
        validation_warnings = run_ingestion_validations(
            canonical_df,
            report_type=report_type,
            report_date=report_date,
            strict=not args.relaxed_validation,
            eom_policy=args.eom_completeness_policy,
        )
    except ValidationError as exc:
        emit_alert(
            "Unified validation failed",
            context={
                "report_type": report_type,
                "strict": not args.relaxed_validation,
                "error": str(exc),
            },
            webhook_url=args.alert_webhook_url,
            log_path=args.alert_log_path,
        )
        raise

    run_summary["validation"]["warnings"] = list(validation_warnings)
    for warning in validation_warnings:
        logging.warning("Validation warning: %s", warning)

    mapped_base = compose_output_name(
        prefix="qry_unified_mapped",
        year=report_date.year,
        timestamp=timestamp,
        period_token=period_token,
        output_tag=args.output_tag,
    )
    usa_base = compose_output_name(
        prefix="management_report_usa_spa",
        year=report_date.year,
        timestamp=timestamp,
        period_token=period_token,
        output_tag=args.output_tag,
    )
    core_base = compose_output_name(
        prefix="management_report_core_markets",
        year=report_date.year,
        timestamp=timestamp,
        period_token=period_token,
        output_tag=args.output_tag,
    )
    combined_base = compose_output_name(
        prefix="combined_management_report",
        year=report_date.year,
        timestamp=timestamp,
        period_token=period_token,
        output_tag=args.output_tag,
    )

    run_summary["outputs"] = {
        "mapped_csv": str(output_dir / f"{mapped_base}.csv"),
        "usa_csv": str(output_dir / f"{usa_base}.csv"),
        "core_csv": str(output_dir / f"{core_base}.csv"),
        "combined_csv": str(output_dir / f"{combined_base}.csv"),
    }

    if args.dry_run:
        run_summary["status"] = "dry-run"
        run_summary["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        print("\n[DRY-RUN] Validation and naming completed. No files written.")
        print(f"[DRY-RUN] mapped_data: {mapped_base}.csv")
        print(f"[DRY-RUN] usa_export:  {usa_base}.csv")
        print(f"[DRY-RUN] core_export: {core_base}.csv")
        print(f"[DRY-RUN] combined:    {combined_base}.csv")
        return

    _sp = _build_sp_handler(project_root)
    _retries = max(1, args.download_retries)
    _backoff = max(0.0, args.retry_backoff_seconds)

    mapping_path = resolve_mapping_file(
        project_root,
        args.mapping_file,
        retries=_retries,
        retry_backoff_seconds=_backoff,
    )
    py_mapping_path = resolve_input_file(
        "py25_regional_mappings.csv",
        project_root / "data/inputs/mappings/py25_regional_mappings.csv",
        _sp, _retries, _backoff,
    )
    mapping_df = pd.read_csv(mapping_path)

    mapped_df = apply_mappings(canonical_df, mapping_df, output_dir=str(output_dir))
    run_summary["counts"]["rows_mapped"] = int(len(mapped_df))
    mapped_path = output_dir / f"{mapped_base}.csv"
    mapped_df.to_csv(mapped_path, index=False)

    current_year = report_date.year
    prior_year = report_date.year - 1

    budget_path = resolve_input_file(
        f"budget_{current_year}_processed.csv",
        project_root / f"data/inputs/budget/budget_{current_year}_processed.csv",
        _sp, _retries, _backoff,
    )
    prior_path = resolve_input_file(
        f"prior_sales_{prior_year}_processed.csv",
        project_root / f"data/inputs/prior_years/prior_sales_{prior_year}_processed.csv",
        _sp, _retries, _backoff,
    )
    gvl_budget_path = resolve_input_file(
        f"budget_GVL_{current_year}.csv",
        project_root / f"data/inputs/budget/budget_GVL_{current_year}.csv",
        _sp, _retries, _backoff,
    )
    gvl_prior_path = resolve_input_file(
        f"prior_sales_{prior_year}_gvl.csv",
        project_root / f"data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv",
        _sp, _retries, _backoff,
    )
    usa_budget_path = resolve_input_file(
        f"budget_USA_spa_{current_year}.csv",
        project_root / f"data/inputs/budget/budget_USA_spa_{current_year}.csv",
        _sp, _retries, _backoff,
    )
    usa_prior_path = resolve_input_file(
        f"prior_sales_{prior_year}_usa.csv",
        project_root / f"data/inputs/prior_years/prior_sales_{prior_year}_usa.csv",
        _sp, _retries, _backoff,
    )

    receivables = ManagementReportGenerator(
        str(project_root / "src/config/report_structure.json"),
        str(mapped_path),
        str(budget_path),
        str(prior_path),
    )
    apply_report_date_anchor(receivables, report_date)

    usa_kwargs = {}
    if "report_date" in inspect.signature(USASpaReportGenerator.__init__).parameters:
        usa_kwargs["report_date"] = report_date

    usa = USASpaReportGenerator(
        str(project_root / "src/config/usa_spa_report_structure.json"),
        str(mapped_path),
        str(usa_budget_path),
        str(usa_prior_path),
        **usa_kwargs,
    )
    apply_report_date_anchor(usa, report_date)

    core_kwargs = {}
    if "report_date" in inspect.signature(CoreMarketReportGenerator.__init__).parameters:
        core_kwargs["report_date"] = report_date

    core = CoreMarketReportGenerator(
        str(project_root / "src/config/core_market_report_structure.json"),
        str(mapped_path),
        str(gvl_budget_path),
        str(gvl_prior_path),
        py_mapping_path=str(py_mapping_path),
        entity_mapping_path=str(mapping_path),
        **core_kwargs,
    )
    apply_report_date_anchor(core, report_date)

    receivables_df = receivables.calculate_report()
    usa_df = usa.calculate_report()
    core_df = core.calculate_report()

    receivables.render_report(receivables_df)
    usa.render_report(usa_df)
    core.render_report(core_df)

    export_service = V2ExportService()

    export_service.export_once(usa, usa_df, output_dir / f"{usa_base}.csv")
    export_service.export_once(core, core_df, output_dir / f"{core_base}.csv")

    combined_df = build_combined_dataframe(receivables_df, usa_df, core_df)
    export_service.export_once(receivables, combined_df, output_dir / f"{combined_base}.csv")

    run_summary["status"] = "success"
    run_summary["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    summary_path = write_run_summary(run_summary, output_dir=output_dir, summary_path_arg=args.summary_path)

    print("\n[OK] V2 run complete")
    print(f"[OK] mapped_data: {mapped_path.name}")
    print(f"[OK] usa_export:  {usa_base}")
    print(f"[OK] core_export: {core_base}")
    print(f"[OK] combined:    {combined_base}")
    print(f"[OK] run_summary: {summary_path.name}")


if __name__ == "__main__":
    try:
        main()
    except ValidationError as err:
        logging.error("Validation failed: %s", err)
        raise SystemExit(2) from err
