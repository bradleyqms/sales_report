# QMS Sales Reporting Hub

Enterprise sales reporting system with real-time SharePoint integration and interactive web dashboard. Generates comprehensive management reports with automated data retrieval, processing, and multi-format exports.

## Features

### Core Reports
- **Receivables Management Report**: MTD sales by region/channel with budget vs. actual comparison
- **Core Market Report**: European sub-region breakdown with Existing/New customer segmentation
- **USA Spa Report**: Regional territory analysis with USD currency handling
- **GVL Report**: Individual employee sales performance tracking
- **Combined Report**: Unified export with all sections in CSV, TXT, HTML, Excel, and PDF formats

### Technical Capabilities
- **Real-time SharePoint Integration**: Automated QRY file retrieval and processing
- **Multi-format Export**: CSV, TXT, HTML, XLSX (formatted), PDF with professional styling
- **Interactive Web Dashboard**: Server-sent events for live progress streaming
- **Report Inheritance Architecture**: Shared export logic via `BaseReportGenerator` pattern
- **Data Quality**: Entity mapping validation with unmapped entity detection

## Technology Stack

- **Backend**: FastAPI, Python 3.12
- **Data Processing**: Pandas, NumPy
- **Export Formats**: ReportLab (PDF), openpyxl (Excel), Jinja2 (HTML)
- **SharePoint**: MSAL authentication with Office365-REST-Python-Client
- **Frontend**: Vanilla JS with SSE, responsive CSS
- **Deployment**: Azure App Service with GitHub Actions CI/CD

## Quick Start

### Prerequisites
- Python 3.12+
- SharePoint access credentials (Client ID, Client Secret)
- Azure AD application registration with SharePoint API permissions

### Installation

1. **Clone and setup environment**:
```bash
git clone https://github.com/bradleyqms/sales_report.git
cd sales_report_v2_independent
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure SharePoint credentials** (`.env` file):
```env
SHAREPOINT_SITE_URL=https://yoursite.sharepoint.com/sites/DATAANDREPORTING
SHAREPOINT_CLIENT_ID=your-azure-app-client-id
SHAREPOINT_CLIENT_SECRET=your-azure-app-secret
```

3. **Run the web application**:
```bash
cd fastapi_web_app
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

4. **Access dashboard**: Open `http://127.0.0.1:8000` in your browser

### Command-Line Usage

Generate reports directly from CLI:
```bash
# Full combined report (all sections)
python src/full_report.py

# Individual reports
python src/receivables_report_generator.py
python src/core_market_report.py
python src/usa_spa_report.py
python src/gvl_report.py
```

Outputs saved to `data/outputs/` with timestamp in filename.

## Architecture

### Project Structure
```
sales_report_v2_independent/
├── src/                              # Core report generation
│   ├── base_report_generator.py     # Abstract base with shared export logic
│   ├── receivables_report_generator.py  # Main management report
│   ├── core_market_report.py        # Sub-region breakdown (EU markets)
│   ├── usa_spa_report.py            # USA regional territories
│   ├── gvl_report.py                # Employee-level sales tracking
│   ├── full_report.py               # Orchestrator for combined generation
│   ├── sharepoint_client.py         # SharePoint file operations
│   ├── qry_data_ingestion.py       # QRY file processing
│   ├── qry_data_mapping.py         # Entity mapping & transformation
│   ├── utils.py                     # Date helpers, formatting utilities
│   └── config/
│       ├── report_structure.json    # Receivables report hierarchy
│       ├── core_market_report_structure.json  # Core market hierarchy
│       └── usa_spa_report_structure.json      # USA regional structure
├── fastapi_web_app/                 # Web interface
│   ├── main.py                      # FastAPI app with SSE streaming
│   ├── templates/index.html         # Dashboard UI
│   ├── static/                      # CSS, JS, images, recent exports
│   └── Dockerfile                   # Container configuration
├── data/
│   ├── inputs/                      # Budget, prior year, mappings (gitignored)
│   │   ├── budget/
│   │   ├── prior_years/
│   │   └── mappings/
│   └── outputs/                     # Generated reports (gitignored)
├── tests/                           # Unit & integration tests
├── .github/workflows/               # CI/CD pipelines
├── requirements.txt                 # Python dependencies
├── runtime.txt                      # Python version (Azure)
└── version.json                     # Build version tracking
```

### Report Generation Flow
1. **Data Ingestion**: Pull QRY files from SharePoint (`QRY_AR_MTD_*.csv`, `QRY_CN_MTD_*.csv`, `QRY_SO_*.csv`)
2. **Entity Mapping**: Apply mappings from `entity_mappings.csv` to standardize region/channel names
3. **Report Calculation**: Each generator computes sales, budget, prior year metrics per report structure
4. **Export**: `BaseReportGenerator.export_report()` generates CSV, TXT, HTML, XLSX, PDF formats
5. **Web Delivery**: FastAPI streams progress via SSE and serves download links

## Deployment

### Production (Azure App Service)
- **URL**: `https://qms-sales-report.azurewebsites.net`
- **Deployment**: Automatic via GitHub Actions on push to `main`
- **Runtime**: Python 3.12 on Linux container

### Environment Variables (Azure Configuration)
Set in Azure Portal → App Service → Configuration:
```
SHAREPOINT_SITE_URL=<sharepoint-site>
SHAREPOINT_CLIENT_ID=<azure-app-id>
SHAREPOINT_CLIENT_SECRET=<azure-app-secret>
SCM_DO_BUILD_DURING_DEPLOYMENT=true
```

### CI/CD Pipeline
`.github/workflows/main_qms-sales-report.yml`:
1. Checkout code
2. Setup Python 3.12
3. Install dependencies
4. Deploy to Azure using publish profile
5. Restart app service

## Data Sources

### SharePoint Files (Auto-fetched)
- **QRY Files**: AR, CN, SO tables (`/SAP Extracts/QRY_*.csv`)
- **Budget Files**: `budget_2026_processed.csv`, `budget_GVL_2026.csv`, `budget_USA_spa_2026.csv`
- **Prior Year**: `prior_sales_2025_processed.csv`, `prior_sales_2025_gvl.csv`, `prior_sales_2025_usa.csv`
- **Mappings**: `entity_mappings.csv` (standardizes region/channel names)

### Local Fallback
If SharePoint unavailable, reads from `data/inputs/` directories.

## Report Details

### Receivables Management Report
- **Sections**: Core Markets (6), UK (2), USA (2), Distributors (7), eCommerce (4), Company totals (3)
- **Metrics**: MTD Sales, Budget, Prior Year, % vs Budget
- **Hierarchy**: Defined in `src/config/report_structure.json`

### Core Market Report (Sub-Region Breakdown)
- **Markets**: Germany (7 regions), Benelux (3), Switzerland (3), Spain, France, Italy
- **Segmentation**: Existing vs New customers
- **Metrics**: Total Sales, Existing, New, Budget (split), Prior YoY, % vs Budget

### USA Spa Report
- **Regions**: Northeast, Central, Southeast, West
- **Currency**: Auto-converts kEUR → kUSD (rate: 1.07)
- **Metrics**: Budget vs Actual, Prior Year comparison

### GVL Report
- **Level**: Individual sales employee
- **Metrics**: MTD Sales, Budget, Prior Year, % vs Budget

## Development

### Adding a New Report
1. Create generator class inheriting from `BaseReportGenerator`
2. Implement abstract methods: `calculate_report()`, `render_report()`, `get_report_headers()`, `get_report_title()`, `format_row_for_export()`
3. Add to `src/full_report.py` orchestration
4. Update `fastapi_web_app/main.py` for web integration

### Running Tests
```bash
pytest tests/ -v
```

## Troubleshooting

### SharePoint Connection Issues
- Verify Azure AD app has `Sites.Read.All` permission
- Check Client ID/Secret in `.env` or Azure config
- Confirm SharePoint site URL is correct

### Report Generation Errors
- Check `data/inputs/mappings/entity_mappings.csv` exists
- Verify budget/prior year files match current year
- Review unmapped entities in output logs

### Web Dashboard Not Loading
- Ensure port 8000 not in use: `netstat -ano | findstr :8000`
- Check FastAPI logs for startup errors
- Verify static files exist in `fastapi_web_app/static/`

## Automated Report Dispatch

- **Function:** `dispatch_reports` runs on a timer (07:00 UTC by default) and ensures the latest HTML exports for both the combined management report and the core markets report are fresh before sending.
- **Refresh Step:** The function executes `src/full_report.py` (configurable via `REPORT_DISPATCH_REFRESH_COMMAND`) so every dispatch reflects the newest data, then uploads any required exports to `data/outputs`.
- **Attachment Selection:** Two dedicated glob patterns can optionally pin the files that always get attached (`REPORT_DISPATCH_ATTACHMENT_PATTERNS`), while the legacy `REPORT_DISPATCH_ATTACHMENT_GLOB`/`REPORT_DISPATCH_MAX_ATTACHMENTS` pair remains available as a fallback.
- **Transport:** Graph client credentials using MSAL (`GRAPH_CLIENT_ID`, `GRAPH_CLIENT_SECRET`, `GRAPH_TENANT_ID`).

### Required Settings

Add these keys to your Azure Function configuration or to `azure_functions/local.settings.json` when testing locally:

| Variable | Description |
|---|---|
| `REPORT_DISPATCH_RECIPIENTS` | Comma/semicolon-separated recipient list (e.g., `sales-team@qms.com`). |
| `REPORT_DISPATCH_GRAPH_SENDER` | User principal name that will send the mail (must match the Graph service principal permission). |
| `REPORT_DISPATCH_ATTACHMENT_PATTERNS` | Semicolon-delimited globs for key files (default `combined_management_report_*.html;management_report_core_markets_*.html`). |
| `REPORT_DISPATCH_ATTACHMENTS_PER_PATTERN` | Max matches to include per pattern (default `1`). |
| `REPORT_DISPATCH_ATTACHMENT_GLOB` | Legacy glob for more general attachments (still used when `REPORT_DISPATCH_ATTACHMENT_PATTERNS` is unset; default `management_report_*.html`). |
| `REPORT_DISPATCH_MAX_ATTACHMENTS` | Limits attachments when relying on the legacy glob (default `3`). |
| `REPORT_DISPATCH_REFRESH_COMMAND` | Command that refreshes reports before dispatch (empty string disables it; default uses the current Python interpreter to run `src/full_report.py`). |
| `REPORT_DISPATCH_REFRESH_TIMEOUT_SECONDS` | Time limit for the refresh command (default `1800`). |
| `REPORT_DISPATCH_BODY` | Body text for the dispatch email. |
| `REPORT_DISPATCH_SUBJECT` | Subject template for dispatched emails. |

The function also relies on the Microsoft Graph credentials already required elsewhere in the project (`GRAPH_TENANT_ID`, `GRAPH_CLIENT_ID`, `GRAPH_CLIENT_SECRET`).

Override `REPORT_DISPATCH_OUTPUTS_PATH` if your reports live outside `data/outputs`; the function logs a warning and falls back to `data/outputs` if the directory is missing.
## License

Proprietary - QMS Medicosmetics

## Support

For issues or questions, contact the development team.
