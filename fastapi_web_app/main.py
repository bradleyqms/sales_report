from fastapi import FastAPI, Request, Form, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import subprocess
import os
import zipfile
import re
import shutil
import csv
import glob
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Sales Report Generator")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Templates
templates = Jinja2Templates(directory="templates")

# Global state
report_status = {
    "running": False,
    "output": "",
    "csv_url": "",
    "txt_url": "",
    "html_url": "",
    "xlsx_url": "",
    "pdf_url": "",
    "zip_url": "",
    "last_run": None,
    "metrics": {
        "timestamp": None,
        "segments": {
            "Core Markets": {"sales": 0, "budget_pct": 0},
            "UK": {"sales": 0, "budget_pct": 0},
            "Export": {"sales": 0, "budget_pct": 0},
            "US": {"sales": 0, "budget_pct": 0},
            "Ecommerce": {"sales": 0, "budget_pct": 0}
        }
    }
}

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

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "status": report_status
    })

@app.post("/run-report")
async def run_report(background_tasks: BackgroundTasks):
    if report_status["running"]:
        raise HTTPException(status_code=400, detail="Report is already running")

    background_tasks.add_task(execute_report)
    return {"message": "Report generation started"}

@app.get("/status")
async def get_status():
    return report_status

@app.get("/metrics")
async def get_metrics():
    """Get the total sales and budget percentage from the latest report."""
    return extract_metrics_from_csv()

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = Path("static") / filename
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
    report_status["output"] = ""
    report_status["csv_url"] = ""
    report_status["txt_url"] = ""
    report_status["html_url"] = ""
    report_status["xlsx_url"] = ""
    report_status["pdf_url"] = ""
    report_status["zip_url"] = ""

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

                # Find generated files (now combined)
                generated_files = [f for f in os.listdir(output_dir) if timestamp in f and 'combined' in f]

                if generated_files:
                    # Create zip file
                    static_dir = Path("static")
                    static_dir.mkdir(exist_ok=True)
                    zip_path = static_dir / f'combined_reports_{timestamp}.zip'

                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        for file in generated_files:
                            file_path = output_dir / file
                            zipf.write(file_path, file)

                    report_status["zip_url"] = f'/download/combined_reports_{timestamp}.zip'

                    # Copy individual files to static
                    for file in generated_files:
                        if file.endswith('.csv'):
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
                else:
                    report_status["output"] += "\n\nNo generated files found."
            else:
                report_status["output"] += "\n\nCould not parse timestamp from output."
        else:
            report_status["output"] += f"\n\nScript failed with return code {returncode}"

    except Exception as e:
        report_status["output"] = f"Error running report: {str(e)}"

    report_status["running"] = False