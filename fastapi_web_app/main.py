from __future__ import annotations

# Load .env FIRST — must run before any local module (auth.py, middleware.py) is
# imported, because auth.py builds its frozensets at module-import time.
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()  # reads fastapi_web_app/.env into os.environ
except ImportError:
    pass  # python-dotenv not installed — rely on shell env vars

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException
import subprocess
import os
import zipfile
import re
import shutil
import csv
import glob
import logging
import json
from pathlib import Path
from datetime import datetime
import asyncio
from sse_starlette.sse import EventSourceResponse, ServerSentEvent
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO)

# Get the directory where main.py is located
BASE_DIR = Path(__file__).resolve().parent

# ── Lifespan: startup tasks (replaces @app.on_event) ──────────────────────────

def _prune_telemetry() -> None:
    """Sync DB call — run in a thread pool so it never blocks the event loop."""
    import sys as _sys
    _sys.path.insert(0, str(BASE_DIR.parent))
    from src.database import engine as _engine
    _sess = sessionmaker(bind=_engine)()
    try:
        _sess.execute(text(
            "DELETE FROM telemetry_logs WHERE timestamp < DATEADD(day, -90, GETDATE())"
        ))
        _sess.commit()
    finally:
        _sess.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Recover metrics from disk — always safe, no DB needed
    _recover_status_from_disk()

    # 2. Telemetry prune — risky (DB may be unavailable); run in threadpool
    #    so it never stalls the event loop, and always allow the app to start.
    try:
        await run_in_threadpool(_prune_telemetry)
        logging.info("[startup] Telemetry prune successful.")
    except Exception as _e:
        logging.warning("[startup] Telemetry prune failed (non-fatal): %s", _e)

    yield  # app is running


app = FastAPI(title="Sales Report Generator", lifespan=lifespan)

# ── Auth middleware (runs once per request, populates request.state.user) ──────
try:
    from middleware import AuthMiddleware
    app.add_middleware(AuthMiddleware)
    logging.info("✅ AuthMiddleware registered")
except Exception as e:
    logging.warning(f"⚠️  AuthMiddleware not loaded: {e}")

# ── 403 exception handler — branded 'Request Access' page ──────────────────────
@app.exception_handler(403)
async def forbidden_handler(request: Request, exc):
    user = getattr(request.state, "user", None)
    return templates.TemplateResponse(
        "403.html",
        {"request": request, "user": user},
        status_code=403,
    )

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 403:
        return await forbidden_handler(request, exc)
    raise exc

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    import traceback
    logging.error(
        "💥 Unhandled exception on %s %s\n%s",
        request.method, request.url.path,
        traceback.format_exc(),
    )
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": type(exc).__name__},
    )

# ── Route helpers ────────────────────────────────────────────────────────────
try:
    from access import check_access, assert_admin  # noqa: F401
except Exception as e:
    logging.warning(f"⚠️  access.py not loaded: {e}")
    def check_access(request, *tiers): pass  # type: ignore
    def assert_admin(request): pass  # type: ignore

try:
    from telemetry import log_page_view, log_export  # noqa: F401
except Exception as e:
    logging.warning(f"⚠️  telemetry.py not loaded: {e}")
    def log_page_view(*a, **kw): pass  # type: ignore
    def log_export(*a, **kw): pass  # type: ignore

# ── Include admin routes ─────────────────────────────────────────────────────
try:
    from admin_routes import router as admin_router
    app.include_router(admin_router)
    logging.info("✅ Admin UI routes loaded successfully")
except Exception as e:
    logging.warning(f"⚠️  Admin UI routes not available: {e}")

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


# startup_event replaced by the lifespan context manager above


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


def extract_latest_segment_metrics():
    """Read the latest audience-specific CSVs from data/outputs/ and return per-segment totals."""
    output_dir = Path(__file__).parent.parent / "data" / "outputs"
    result = {
        "core_markets": {"sales": None, "budget_pct": None},
        "usa_spa":      {"sales": None, "budget_pct": None},
    }

    # ---- Core Markets -------------------------------------------------------
    core_csvs = sorted(
        output_dir.glob("management_report_core_markets_*.csv"),
        key=lambda x: x.stat().st_mtime, reverse=True
    )
    if core_csvs:
        try:
            with open(core_csvs[0], 'r', encoding='utf-8') as f:
                for row in csv.reader(f):
                    if not row:
                        continue
                    if row[0].strip().startswith('Total Core Market'):
                        sales = _parse_to_float(row[1]) if len(row) > 1 else None
                        # % vs Budget is the last cell containing '%'
                        pct = None
                        for cell in reversed(row):
                            if '%' in cell:
                                pct = _parse_to_float(cell)
                                break
                        result["core_markets"] = {"sales": sales, "budget_pct": pct}
                        break
        except Exception as e:
            logging.error(f"extract_latest_segment_metrics (core markets): {e}")

    # ---- USA Spa ------------------------------------------------------------
    usa_csvs = sorted(
        output_dir.glob("management_report_usa_spa_*.csv"),
        key=lambda x: x.stat().st_mtime, reverse=True
    )
    if usa_csvs:
        try:
            with open(usa_csvs[0], 'r', encoding='utf-8') as f:
                rows = list(csv.reader(f))
            if len(rows) >= 2:
                header = rows[0]
                # Preferred column: 'vs Budget' or '% 26A vs 26B' pattern
                budget_col_idx = next(
                    (i for i, h in enumerate(header)
                     if h.strip().startswith('%') and 'vs' in h.lower() and h.strip()[-1] == 'B'),
                    4  # fallback to column 4 (known format)
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


def _recover_status_from_disk():
    """On startup, scan data/outputs/ for the latest run and pre-populate report_status.
    This keeps the UI functional across server restarts."""
    output_dir = Path(__file__).parent.parent / "data" / "outputs"
    static_dir = BASE_DIR / "static"
    try:
        # Find latest combined CSV to get the last timestamp
        combined_csvs = sorted(
            output_dir.glob("combined_management_report_*.csv"),
            key=lambda x: x.stat().st_mtime, reverse=True
        )
        if not combined_csvs:
            return

        # Extract timestamp from filename
        ts_match = re.search(r'_(\d{8}_\d{6})\.csv$', combined_csvs[0].name)
        if not ts_match:
            return
        timestamp = ts_match.group(1)
        report_status["last_run"] = timestamp
        logging.info(f"Recovered last_run from disk: {timestamp}")

        # Rebuild URLs for all files that exist in static/
        for f in static_dir.iterdir():
            name = f.name
            if timestamp not in name:
                continue
            url = f'/download/{name}'
            if 'core_market' in name:
                if name.endswith('.csv'):
                    report_status["core_market_csv_url"] = url
                elif name.endswith('.html'):
                    report_status["core_market_html_url"] = url
            elif 'usa_spa' in name:
                if name.endswith('.csv'):
                    report_status["usa_spa_csv_url"] = url
                elif name.endswith('.html'):
                    report_status["usa_spa_html_url"] = url
            elif 'combined' in name:
                if name.endswith('.csv'):
                    report_status["csv_url"] = url
                elif name.endswith('.txt'):
                    report_status["txt_url"] = url
                elif name.endswith('.html'):
                    report_status["html_url"] = url
                elif name.endswith('.xlsx'):
                    report_status["xlsx_url"] = url
                elif name.endswith('.pdf'):
                    report_status["pdf_url"] = url
                elif name.endswith('.zip'):
                    report_status["zip_url"] = url

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
        # Find all CSV files in the data/outputs directory
        output_dir = Path(__file__).parent.parent / "data" / "outputs"
        csv_files = sorted(output_dir.glob("combined_management_report_*.csv"), 
                          key=lambda x: x.stat().st_mtime, reverse=True)
        
        if not csv_files:
            logging.warning("No CSV report files found")
            return {"total_sales": 0, "budget_pct": 0}
        
        latest_csv = csv_files[0]
        logging.info(f"Reading metrics from: {latest_csv}")
        
        # Parse the CSV to find "Total Sales" row
        with open(latest_csv, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0].strip() == "Total Sales":
                    # Row format: ["Total Sales", "sales_value", "budget_col", "col3", "budget_pct%"]
                    # Expected format based on CSV: ["Total Sales", "926", "1866", "1030", "49.6%"]
                    try:
                        if len(row) >= 2:
                            # First numeric column after "Total Sales" is the sales value
                            total_sales = float(row[1])
                            # Last column should contain budget percentage
                            budget_str = row[-1].strip().rstrip('%')
                            budget_pct = float(budget_str) if budget_str else 0
                            
                            logging.info(f"Extracted metrics - Total Sales: {total_sales}, Budget %: {budget_pct}")
                            return {"total_sales": total_sales, "budget_pct": budget_pct}
                    except (ValueError, IndexError) as e:
                        logging.warning(f"Could not parse Total Sales row: {e}")
        
        logging.warning("Total Sales row not found in CSV")
        return {"total_sales": 0, "budget_pct": 0}
        
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

def _get_pending_unmapped_count() -> int:
    """Return count of unmapped entities with status='pending' for the nav badge."""
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR.parent))
        from src.database import engine as _engine
        from src.models import UnmappedLog
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker(bind=_engine)()
        try:
            return session.query(UnmappedLog).filter_by(status='pending').count()
        finally:
            session.close()
    except Exception:
        return 0


@app.get("/", response_class=HTMLResponse)
async def home(request: Request, background_tasks: BackgroundTasks):
    user = getattr(request.state, "user", None)
    if user and user.email != "anonymous@unknown":
        background_tasks.add_task(_bg_log_page_view, user.email, "/")
        background_tasks.add_task(_bg_log_login, user.email)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "view": "management",
        "user": user,
        "pending_unmapped": _get_pending_unmapped_count(),
    })

@app.get("/coremarkets", response_class=HTMLResponse)
async def core_markets_view(request: Request, background_tasks: BackgroundTasks):
    check_access(request, "core", "management", "admin")
    user = getattr(request.state, "user", None)
    background_tasks.add_task(
        _bg_log_page_view, user.email if user else "anonymous", "/coremarkets"
    )
    return templates.TemplateResponse("index.html", {
        "request": request,
        "view": "coremarkets",
        "user": user,
        "pending_unmapped": _get_pending_unmapped_count(),
    })

@app.get("/usaspa", response_class=HTMLResponse)
async def usa_spa_view(request: Request, background_tasks: BackgroundTasks):
    check_access(request, "usa", "management", "admin")
    user = getattr(request.state, "user", None)
    background_tasks.add_task(
        _bg_log_page_view, user.email if user else "anonymous", "/usaspa"
    )
    return templates.TemplateResponse("index.html", {
        "request": request,
        "view": "usaspa",
        "user": user,
        "pending_unmapped": _get_pending_unmapped_count(),
    })

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
    return report_status

@app.get("/metrics")
async def get_metrics():
    """Get the total sales and budget percentage from the latest report."""
    return extract_metrics_from_csv()


@app.get("/segment-metrics")
async def get_segment_metrics():
    """Return per-segment metrics from the latest audience-specific CSV files."""
    return extract_latest_segment_metrics()

@app.get("/download/{filename}")
async def download_file(request: Request, filename: str, background_tasks: BackgroundTasks):
    check_access(request, "core", "usa", "management", "admin")
    file_path = Path("static") / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    user = getattr(request.state, "user", None)
    ext = Path(filename).suffix.lstrip(".").lower() or "unknown"
    report_type = (
        "core_markets" if "core_market" in filename
        else "usa_spa" if "usa_spa" in filename
        else "combined"
    )
    background_tasks.add_task(
        _bg_log_export,
        user.email if user else "anonymous",
        ext,
        report_type,
    )

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/octet-stream'
    )


# ── Background telemetry helpers (use their own DB session, errors swallowed) ──

def _bg_log_page_view(user_email: str, page_id: str) -> None:
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR.parent))
        from src.database import engine as _engine
        _sess = sessionmaker(bind=_engine)()
        try:
            log_page_view(_sess, user_email, page_id)
        finally:
            _sess.close()
    except Exception as _e:
        logging.warning("[telemetry] _bg_log_page_view failed: %s", _e)


def _bg_log_login(user_email: str) -> None:
    """Record a login event (fires once per home page visit)."""
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR.parent))
        from src.database import engine as _engine
        from src.models import TelemetryLog as _TLog
        _sess = sessionmaker(bind=_engine)()
        try:
            _sess.add(_TLog(
                user_email=user_email.lower().strip(),
                event_type="login",
                page_id="/",
            ))
            _sess.commit()
        finally:
            _sess.close()
    except Exception as _e:
        logging.warning("[telemetry] _bg_log_login failed: %s", _e)


def _bg_log_export(user_email: str, file_format: str, report_type: str) -> None:
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR.parent))
        from src.database import engine as _engine
        _sess = sessionmaker(bind=_engine)()
        try:
            log_export(_sess, user_email, file_format, report_type)
        finally:
            _sess.close()
    except Exception as _e:
        logging.warning("[telemetry] _bg_log_export failed: %s", _e)

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
        # Path to the full_report.py script
        script_path = Path(__file__).parent.parent / "src" / "full_report.py"

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
            # Parse timestamp from output
            timestamp_match = re.search(r'Timestamp: (\d{8}_\d{6})', report_status["output"])
            if timestamp_match:
                timestamp = timestamp_match.group(1)
                report_status["last_run"] = timestamp

                # Output directory
                output_dir = script_path.parent.parent / "data" / "outputs"
                static_dir = Path("static")
                static_dir.mkdir(exist_ok=True)

                # Find generated files (now combined and core_market)
                generated_files = [f for f in os.listdir(output_dir) if timestamp in f and ('combined' in f or 'core_market' in f or 'usa_spa' in f)]

                if generated_files:
                    # Create zip file
                    zip_path = static_dir / f'combined_reports_{timestamp}.zip'

                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        for file in generated_files:
                            file_path = output_dir / file
                            zipf.write(file_path, file)

                    report_status["zip_url"] = f'/download/combined_reports_{timestamp}.zip'

                    # Copy individual files to static
                    for file in generated_files:
                        if 'core_market' in file:
                            if file.endswith('.csv'):
                                shutil.copy(output_dir / file, static_dir / file)
                                report_status["core_market_csv_url"] = f'/download/{file}'
                            elif file.endswith('.html'):
                                shutil.copy(output_dir / file, static_dir / file)
                                report_status["core_market_html_url"] = f'/download/{file}'
                            elif file.endswith('.txt'):
                                shutil.copy(output_dir / file, static_dir / file)
                            elif file.endswith('.xlsx'):
                                shutil.copy(output_dir / file, static_dir / file)
                            elif file.endswith('.pdf'):
                                shutil.copy(output_dir / file, static_dir / file)
                        elif 'usa_spa' in file:
                            shutil.copy(output_dir / file, static_dir / file)
                            if file.endswith('.csv'):
                                report_status["usa_spa_csv_url"] = f'/download/{file}'
                            elif file.endswith('.html'):
                                report_status["usa_spa_html_url"] = f'/download/{file}'
                        elif file.endswith('.csv'):
                            shutil.copy(output_dir / file, static_dir / file)
                            report_status["csv_url"] = f'/download/{file}'
                        elif file.endswith('.txt'):
                            shutil.copy(output_dir / file, static_dir / file)
                            report_status["txt_url"] = f'/download/{file}'
                        elif file.endswith('.html'):
                            shutil.copy(output_dir / file, static_dir / file)
                            report_status["html_url"] = f'/download/{file}'
                        elif file.endswith('.xlsx'):
                            shutil.copy(output_dir / file, static_dir / file)
                            report_status["xlsx_url"] = f'/download/{file}'
                        elif file.endswith('.pdf'):
                            shutil.copy(output_dir / file, static_dir / file)
                            report_status["pdf_url"] = f'/download/{file}'
                
                # Find and copy unmapped entities file (latest one)
                unmapped_files = sorted(output_dir.glob("unmapped_entities_*.csv"), 
                                       key=lambda x: x.stat().st_mtime, reverse=True)
                if unmapped_files:
                    latest_unmapped = unmapped_files[0]
                    shutil.copy(latest_unmapped, static_dir / latest_unmapped.name)
                    report_status["unmapped_url"] = f'/download/{latest_unmapped.name}'
                
                # Populate per-segment metrics in memory
                seg = extract_latest_segment_metrics()
                report_status["metrics"]["segments"]["Core Markets"] = seg["core_markets"]
                report_status["metrics"]["segments"]["US"]           = seg["usa_spa"]
                report_status["metrics"]["timestamp"] = timestamp
                logging.info(f"Segment metrics populated after run: {seg}")

                if not generated_files and not unmapped_files:
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