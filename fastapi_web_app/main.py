from fastapi import FastAPI, Request, Form, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import subprocess
import os
import zipfile
import re
import base64
import shutil
import csv
import glob
import logging
import json
from pathlib import Path
from datetime import datetime
import asyncio
import time
from dotenv import load_dotenv
from sse_starlette.sse import EventSourceResponse, ServerSentEvent

try:
    from azure.storage.blob import BlobServiceClient
except Exception:  # pragma: no cover
    BlobServiceClient = None

logging.basicConfig(level=logging.INFO)

# Get the directory where main.py is located
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
AUTO_REFRESH_ENABLED = os.getenv("AUTO_REFRESH_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
BLOB_CONNECTION_STRING = os.getenv("REPORT_OUTPUT_BLOB_CONNECTION_STRING", "").strip() or os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
BLOB_CONTAINER_NAME = os.getenv("REPORT_OUTPUT_BLOB_CONTAINER", "").strip()
BLOB_PREFIX = os.getenv("REPORT_OUTPUT_BLOB_PREFIX", "").strip().strip("/")
BLOB_CACHE_DIR = BASE_DIR / "static" / "blob_cache"
METRICS_CACHE_TTL_SECONDS = float(os.getenv("METRICS_CACHE_TTL_SECONDS", "5"))

app = FastAPI(title="Sales Report Generator")

# Mount static files using absolute path
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# Templates using absolute path
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Global state
PRE_RUN_OUTLINE = """Pipeline outline:
1. Pull latest SharePoint tables
2. Clean & harmonize SKUs
3. Generate combined management reports
4. Export CSV, Excel, HTML, PDF bundles
"""

report_status = {
    "running": False,
    "error": False,
    "output": "",
    "csv_url": "",
    "txt_url": "",
    "html_url": "",
    "xlsx_url": "",
    "pdf_url": "",
    "zip_url": "",
    "unmapped_url": "",
    "core_market_csv_url": "",
    "core_market_html_url": "",
    "usa_spa_csv_url": "",
    "usa_spa_html_url": "",
    "last_run": None,
    "metrics": {
        "timestamp": None,
        "segments": {
            "Core Markets": {"sales": None, "budget_pct": None},
            "UK": {"sales": None, "budget_pct": None},
            "Export": {"sales": None, "budget_pct": None},
            "US": {"sales": None, "budget_pct": None},
            "Ecommerce": {"sales": None, "budget_pct": None}
        }
    }
}

_metrics_cache = {"signature": None, "expires_at": 0.0, "value": None}
_segment_metrics_cache = {"signature": None, "expires_at": 0.0, "value": None}


def _parse_email_list(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {
        token.strip().lower()
        for token in re.split(r"[;,]", raw)
        if token and token.strip()
    }


def _extract_user_email(request: Request) -> str | None:
    direct = (request.headers.get("x-ms-client-principal-name") or "").strip().lower()
    if "@" in direct:
        return direct

    encoded = (request.headers.get("x-ms-client-principal") or "").strip()
    if not encoded:
        return None

    try:
        padding = "=" * (-len(encoded) % 4)
        decoded = base64.b64decode(encoded + padding).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        return None

    preferred_types = {
        "preferred_username",
        "upn",
        "email",
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
    }

    fallback_email = None
    for claim in payload.get("claims", []):
        typ = str(claim.get("typ", "")).strip().lower()
        val = str(claim.get("val", "")).strip().lower()
        if "@" not in val:
            continue
        if typ in preferred_types:
            return val
        if fallback_email is None:
            fallback_email = val

    return fallback_email


def _authorized_emails_for_path(path: str) -> set[str]:
    management = _parse_email_list(os.getenv("REPORT_DISPATCH_RECIPIENTS"))
    core = _parse_email_list(os.getenv("CORE_MARKET_DISPATCH_RECIPIENTS"))
    usa = _parse_email_list(os.getenv("USA_SPA_DISPATCH_RECIPIENTS"))
    all_allowed = management | core | usa

    if path == "/" or path == "/run-report":
        return management
    if path.startswith("/coremarkets"):
        return core
    if path.startswith("/usaspa"):
        return usa
    if path in {"/stream-logs", "/status", "/metrics", "/segment-metrics"}:
        return all_allowed
    if path.startswith("/download/"):
        return all_allowed
    return set()


@app.middleware("http")
async def enforce_slot_email_access(request: Request, call_next):
    path = request.url.path

    if (
        path.startswith("/.auth")
        or path.startswith("/static/")
        or path in {"/version", "/health"}
    ):
        return await call_next(request)

    allowed_emails = _authorized_emails_for_path(path)
    if not allowed_emails:
        return await call_next(request)

    user_email = _extract_user_email(request)
    if user_email and user_email in allowed_emails:
        return await call_next(request)

    return JSONResponse(status_code=403, content={"detail": "Forbidden"})


@app.on_event("startup")
async def startup_event():
    """Pre-populate report_status from disk on every startup so views work after server restarts."""
    _recover_status_from_disk()


def _parse_to_float(s):
    """Parse a CSV cell string to float. Treats '-' and empty as 0.0, returns None on failure."""
    if s is None:
        return None
    stripped = str(s).strip().rstrip('%').replace(',', '')
    if stripped in ('', '-', '\u2014', 'n/a', 'N/A'):
        return 0.0
    try:
        return float(stripped)
    except ValueError:
        return None


def _resolve_outputs_dir() -> Path:
    configured = os.getenv("REPORT_OUTPUT_DIR")
    if configured:
        configured_path = Path(configured)
        if configured_path.is_absolute():
            return configured_path
        return (BASE_DIR.parent / configured_path).resolve()
    return BASE_DIR.parent / "data" / "outputs"


def _blob_enabled() -> bool:
    return bool(BlobServiceClient and BLOB_CONNECTION_STRING and BLOB_CONTAINER_NAME)


def _blob_container_client():
    if not _blob_enabled():
        return None
    service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    return service.get_container_client(BLOB_CONTAINER_NAME)


def _find_latest_blob_name(filename_glob: str) -> str | None:
    if not _blob_enabled():
        return None

    import fnmatch

    client = _blob_container_client()
    if client is None:
        return None

    latest_blob = None
    for blob in client.list_blobs(name_starts_with=(BLOB_PREFIX + "/") if BLOB_PREFIX else None):
        name = blob.name
        base_name = name.rsplit("/", 1)[-1]
        if not fnmatch.fnmatch(base_name, filename_glob):
            continue
        if latest_blob is None or blob.last_modified > latest_blob.last_modified:
            latest_blob = blob

    return latest_blob.name if latest_blob else None


def _download_blob_to_cache(blob_name: str) -> Path:
    client = _blob_container_client()
    if client is None:
        raise RuntimeError("Blob client unavailable")

    BLOB_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    local_path = BLOB_CACHE_DIR / blob_name.rsplit("/", 1)[-1]

    with open(local_path, "wb") as handle:
        stream = client.download_blob(blob_name)
        handle.write(stream.readall())

    return local_path


def _latest_output_file(filename_glob: str) -> Path | None:
    if _blob_enabled():
        blob_name = _find_latest_blob_name(filename_glob)
        if blob_name:
            try:
                return _download_blob_to_cache(blob_name)
            except Exception as exc:
                logging.warning(f"Blob download failed for {blob_name}: {exc}")

    output_dir = _resolve_outputs_dir()
    matches = sorted(output_dir.rglob(filename_glob), key=lambda x: x.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _resolve_run_artifacts(output_dir: Path, timestamp: str) -> tuple[list[Path], Path | None]:
    report_files = sorted(
        [
            path for path in output_dir.rglob("*")
            if path.is_file()
            and timestamp in path.name
            and ("combined" in path.name or "core_market" in path.name or "usa_spa" in path.name)
        ],
        key=lambda path: path.name,
    )

    if not report_files:
        return [], None

    run_root = Path(os.path.commonpath([str(path.parent) for path in report_files]))
    unmapped_candidates = sorted(
        [path for path in run_root.rglob("unmapped_entities_*.csv") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    return report_files, (unmapped_candidates[0] if unmapped_candidates else None)


def _path_signature(path: Path | None):
    if path is None:
        return None
    try:
        stat = path.stat()
    except OSError:
        return (str(path), None, None)
    return (str(path), stat.st_mtime_ns, stat.st_size)


def _extract_segment_metrics_from_files(core_csv: Path | None, usa_csv: Path | None):
    result = {
        "core_markets": {"sales": None, "budget_pct": None},
        "usa_spa": {"sales": None, "budget_pct": None},
    }

    if core_csv:
        try:
            with open(core_csv, 'r', encoding='utf-8') as f:
                for row in csv.reader(f):
                    if not row:
                        continue
                    if row[0].strip().startswith('Total Core Market'):
                        sales = _parse_to_float(row[1]) if len(row) > 1 else None
                        pct = None
                        for cell in reversed(row):
                            if '%' in cell:
                                pct = _parse_to_float(cell)
                                break
                        result["core_markets"] = {"sales": sales, "budget_pct": pct}
                        break
        except Exception as e:
            logging.error(f"extract_latest_segment_metrics (core markets): {e}")

    if usa_csv:
        try:
            with open(usa_csv, 'r', encoding='utf-8') as f:
                rows = list(csv.reader(f))
            if len(rows) >= 2:
                header = rows[0]
                budget_col_idx = next(
                    (i for i, h in enumerate(header)
                     if h.strip().startswith('%') and 'vs' in h.lower() and h.strip()[-1] == 'B'),
                    4
                )
                for row in rows[1:]:
                    if row and row[0].strip() == 'USA Spa':
                        sales = _parse_to_float(row[1]) if len(row) > 1 else None
                        pct   = _parse_to_float(row[budget_col_idx]) if len(row) > budget_col_idx else None
                        result["usa_spa"] = {"sales": sales, "budget_pct": pct}
                        break
        except Exception as e:
            logging.error(f"extract_latest_segment_metrics (usa spa): {e}")

    return result


def _extract_total_metrics_from_file(latest_csv: Path | None):
    if not latest_csv:
        logging.warning("No CSV report files found")
        return {"total_sales": 0, "budget_pct": 0}

    logging.info(f"Reading metrics from: {latest_csv}")

    with open(latest_csv, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip() == "Total Sales":
                try:
                    if len(row) >= 2:
                        total_sales = float(row[1])
                        budget_str = row[-1].strip().rstrip('%')
                        budget_pct = float(budget_str) if budget_str else 0
                        logging.info(f"Extracted metrics - Total Sales: {total_sales}, Budget %: {budget_pct}")
                        return {"total_sales": total_sales, "budget_pct": budget_pct}
                except (ValueError, IndexError) as e:
                    logging.warning(f"Could not parse Total Sales row: {e}")

    logging.warning("Total Sales row not found in CSV")
    return {"total_sales": 0, "budget_pct": 0}


async def _get_metrics_cached_async():
    latest_csv = await asyncio.to_thread(_latest_output_file, "combined_management_report_*.csv")
    signature = _path_signature(latest_csv)

    now = time.monotonic()
    if (
        _metrics_cache["value"] is not None
        and _metrics_cache["signature"] == signature
        and _metrics_cache["expires_at"] > now
    ):
        return _metrics_cache["value"]

    value = await asyncio.to_thread(_extract_total_metrics_from_file, latest_csv)
    _metrics_cache["signature"] = signature
    _metrics_cache["value"] = value
    _metrics_cache["expires_at"] = now + METRICS_CACHE_TTL_SECONDS
    return value


async def _get_segment_metrics_cached_async():
    core_csv = await asyncio.to_thread(_latest_output_file, "management_report_core_markets_*.csv")
    usa_csv = await asyncio.to_thread(_latest_output_file, "management_report_usa_spa_*.csv")
    signature = (_path_signature(core_csv), _path_signature(usa_csv))

    now = time.monotonic()
    if (
        _segment_metrics_cache["value"] is not None
        and _segment_metrics_cache["signature"] == signature
        and _segment_metrics_cache["expires_at"] > now
    ):
        return _segment_metrics_cache["value"]

    value = await asyncio.to_thread(_extract_segment_metrics_from_files, core_csv, usa_csv)
    _segment_metrics_cache["signature"] = signature
    _segment_metrics_cache["value"] = value
    _segment_metrics_cache["expires_at"] = now + METRICS_CACHE_TTL_SECONDS
    return value


def extract_latest_segment_metrics():
    """Read the latest audience-specific CSVs from data/outputs/ and return per-segment totals."""
    core_csv = _latest_output_file("management_report_core_markets_*.csv")
    usa_csv = _latest_output_file("management_report_usa_spa_*.csv")
    return _extract_segment_metrics_from_files(core_csv, usa_csv)


def _recover_status_from_disk():
    """On startup, scan data/outputs/ for the latest run and pre-populate report_status.
    This keeps the UI functional across server restarts."""
    static_dir = BASE_DIR / "static"

    def _publish_to_static(file_path: Path) -> str:
        static_dir.mkdir(parents=True, exist_ok=True)
        destination = static_dir / file_path.name
        shutil.copy(file_path, destination)
        return f"/download/{file_path.name}"

    try:
        latest_combined_csv = _latest_output_file("combined_management_report_*.csv")
        if not latest_combined_csv:
            return

        # Extract timestamp from filename
        ts_match = re.search(r'_(\d{8}_\d{6})\.csv$', latest_combined_csv.name)
        if not ts_match:
            return
        timestamp = ts_match.group(1)
        report_status["last_run"] = timestamp
        logging.info(f"Recovered last_run from disk: {timestamp}")

        latest_files = {
            "csv_url": _latest_output_file("combined_management_report_*.csv"),
            "txt_url": _latest_output_file("combined_management_report_*.txt"),
            "html_url": _latest_output_file("combined_management_report_*.html"),
            "xlsx_url": _latest_output_file("combined_management_report_*.xlsx"),
            "pdf_url": _latest_output_file("combined_management_report_*.pdf"),
            "core_market_csv_url": _latest_output_file("management_report_core_markets_*.csv"),
            "core_market_html_url": _latest_output_file("management_report_core_markets_*.html"),
            "usa_spa_csv_url": _latest_output_file("management_report_usa_spa_*.csv"),
            "usa_spa_html_url": _latest_output_file("management_report_usa_spa_*.html"),
        }

        for status_key, latest_file in latest_files.items():
            report_status[status_key] = _publish_to_static(latest_file) if latest_file else ""

        # Recover segment metrics from disk
        seg = extract_latest_segment_metrics()
        report_status["metrics"]["segments"]["Core Markets"] = seg["core_markets"]
        report_status["metrics"]["segments"]["US"]           = seg["usa_spa"]
        report_status["metrics"]["timestamp"] = timestamp
        logging.info(f"Recovered segment metrics from disk: {seg}")
    except Exception as e:
        logging.warning(f"_recover_status_from_disk failed: {e}")


def extract_metrics_from_csv():
    """Extract total sales and budget percentage from the latest CSV report."""
    try:
        latest_csv = _latest_output_file("combined_management_report_*.csv")
        return _extract_total_metrics_from_file(latest_csv)
        
    except Exception as e:
        logging.error(f"Error extracting metrics from CSV: {e}")
        return {"total_sales": 0, "budget_pct": 0}

def get_version_info():
    """Get version information from version.json or git"""
    version_file = BASE_DIR.parent / "version.json"
    
    # Try to load from version.json first
    if version_file.exists():
        try:
            with open(version_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Could not read version.json: {e}")
    
    # Fallback: try to get git info
    try:
        git_hash = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=BASE_DIR.parent,
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()
        
        git_branch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            cwd=BASE_DIR.parent,
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()
        
        return {
            "version": f"{git_branch}@{git_hash}",
            "git_commit": git_hash,
            "git_branch": git_branch,
            "deployed_at": datetime.now().isoformat()
        }
    except Exception:
        return {
            "version": "dev",
            "git_commit": "unknown",
            "deployed_at": datetime.now().isoformat()
        }

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "view": "management", "auto_refresh": AUTO_REFRESH_ENABLED})

@app.get("/coremarkets", response_class=HTMLResponse)
async def core_markets_view(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "view": "coremarkets", "auto_refresh": AUTO_REFRESH_ENABLED})

@app.get("/usaspa", response_class=HTMLResponse)
async def usa_spa_view(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "view": "usaspa", "auto_refresh": AUTO_REFRESH_ENABLED})

@app.get("/version")
async def version():
    """Return version information"""
    return JSONResponse(get_version_info())

@app.post("/run-report")
async def run_report(background_tasks: BackgroundTasks):
    if report_status["running"]:
        raise HTTPException(status_code=400, detail="Report is already running")

    background_tasks.add_task(execute_report)
    return {"message": "Report generation started"}


@app.get("/stream-logs")
async def stream_logs():
    async def event_generator():
        last_len = 0
        completed = False
        yield ServerSentEvent(data=PRE_RUN_OUTLINE, event="outline")

        while True:
            current_output = report_status["output"]

            if report_status["running"]:
                if len(current_output) > last_len:
                    chunk = current_output[last_len:]
                    last_len = len(current_output)
                    cleaned = chunk.rstrip('\n')
                    if cleaned:
                        yield ServerSentEvent(data=cleaned, event="log")
                completed = False
            else:
                if current_output and not completed:
                    yield ServerSentEvent(data="Run complete. Summary refreshed below.", event="complete")
                    completed = True
                last_len = len(current_output)

            await asyncio.sleep(0.4)

    return EventSourceResponse(event_generator())

@app.get("/status")
async def get_status():
    if AUTO_REFRESH_ENABLED and not report_status["running"]:
        _recover_status_from_disk()
    report_status["auto_refresh_enabled"] = AUTO_REFRESH_ENABLED
    return report_status

@app.get("/metrics")
async def get_metrics():
    """Get the total sales and budget percentage from the latest report."""
    return await _get_metrics_cached_async()


@app.get("/segment-metrics")
async def get_segment_metrics():
    """Return per-segment metrics from the latest audience-specific CSV files."""
    return await _get_segment_metrics_cached_async()

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = BASE_DIR / "static" / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/octet-stream'
    )

def execute_report():
    global report_status

    report_status["running"] = True
    report_status["error"] = False
    report_status["output"] = ""
    report_status["csv_url"] = ""
    report_status["txt_url"] = ""
    report_status["html_url"] = ""
    report_status["xlsx_url"] = ""
    report_status["pdf_url"] = ""
    report_status["zip_url"] = ""
    report_status["unmapped_url"] = ""
    report_status["core_market_csv_url"] = ""
    report_status["core_market_html_url"] = ""
    report_status["usa_spa_csv_url"] = ""
    report_status["usa_spa_html_url"] = ""

    try:
        # Path to the full_report_v2.py script
        script_path = Path(__file__).parent.parent / "src" / "full_report_v2.py"

        # Run the script with live output
        process = subprocess.Popen(
            ['python', str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=script_path.parent
        )

        # Read output line by line
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                report_status["output"] += output

        # Wait for process to complete
        returncode = process.poll()

        if returncode == 0:
            # V2 script prints:  [OK] combined:    <combined_base>
            # Extract the base name, then pull the YYYYMMDD_HHMMSS timestamp from it.
            combined_match = re.search(
                r'\[OK\] combined:\s+(\S+)', report_status["output"]
            )
            timestamp_match = re.search(r'(\d{8}_\d{6})', combined_match.group(1) if combined_match else "")

            if timestamp_match:
                timestamp = timestamp_match.group(1)
                report_status["last_run"] = timestamp

                # Output directory (absolute)
                output_dir = _resolve_outputs_dir()
                static_dir = BASE_DIR / "static"
                static_dir.mkdir(exist_ok=True)

                generated_paths, unmapped_path = _resolve_run_artifacts(output_dir, timestamp)

                if generated_paths:
                    # Create zip file
                    zip_path = static_dir / f'combined_reports_{timestamp}.zip'

                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        for file_path in generated_paths:
                            zipf.write(file_path, file_path.name)
                        if unmapped_path:
                            zipf.write(unmapped_path, unmapped_path.name)

                    report_status["zip_url"] = f'/download/combined_reports_{timestamp}.zip'

                    # Copy individual files to static
                    for file_path in generated_paths:
                        file = file_path.name
                        if 'core_market' in file:
                            if file.endswith('.csv'):
                                shutil.copy(file_path, static_dir / file)
                                report_status["core_market_csv_url"] = f'/download/{file}'
                            elif file.endswith('.html'):
                                shutil.copy(file_path, static_dir / file)
                                report_status["core_market_html_url"] = f'/download/{file}'
                            elif file.endswith('.txt'):
                                shutil.copy(file_path, static_dir / file)
                            elif file.endswith('.xlsx'):
                                shutil.copy(file_path, static_dir / file)
                            elif file.endswith('.pdf'):
                                shutil.copy(file_path, static_dir / file)
                        elif 'usa_spa' in file:
                            shutil.copy(file_path, static_dir / file)
                            if file.endswith('.csv'):
                                report_status["usa_spa_csv_url"] = f'/download/{file}'
                            elif file.endswith('.html'):
                                report_status["usa_spa_html_url"] = f'/download/{file}'
                        elif file.endswith('.csv'):
                            shutil.copy(file_path, static_dir / file)
                            report_status["csv_url"] = f'/download/{file}'
                        elif file.endswith('.txt'):
                            shutil.copy(file_path, static_dir / file)
                            report_status["txt_url"] = f'/download/{file}'
                        elif file.endswith('.html'):
                            shutil.copy(file_path, static_dir / file)
                            report_status["html_url"] = f'/download/{file}'
                        elif file.endswith('.xlsx'):
                            shutil.copy(file_path, static_dir / file)
                            report_status["xlsx_url"] = f'/download/{file}'
                        elif file.endswith('.pdf'):
                            shutil.copy(file_path, static_dir / file)
                            report_status["pdf_url"] = f'/download/{file}'

                if unmapped_path:
                    shutil.copy(unmapped_path, static_dir / unmapped_path.name)
                    report_status["unmapped_url"] = f'/download/{unmapped_path.name}'
                
                # Populate per-segment metrics in memory
                seg = extract_latest_segment_metrics()
                report_status["metrics"]["segments"]["Core Markets"] = seg["core_markets"]
                report_status["metrics"]["segments"]["US"]           = seg["usa_spa"]
                report_status["metrics"]["timestamp"] = timestamp
                logging.info(f"Segment metrics populated after run: {seg}")

                if not generated_paths and not unmapped_path:
                    report_status["output"] += "\n\nNo generated files found."
            else:
                report_status["output"] += "\n\nCould not parse timestamp from output."
        else:
            report_status["error"] = True
            report_status["output"] += f"\n\nScript failed with return code {returncode}"

    except Exception as e:
        report_status["error"] = True
        report_status["output"] = f"Error running report: {str(e)}"

    report_status["running"] = False