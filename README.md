# QMS Sales Reporting Hub

FastAPI-based web application for generating comprehensive sales reports with SharePoint integration.

## Features

- **Receivables Report**: Track outstanding payments and customer balances
- **GVL Report**: Employee sales performance analysis with budget comparison
- **USA Spa Report**: Regional sales analysis with territory breakdown
- **Full Report**: Combined report generation with all metrics
- **SharePoint Integration**: Automatic data retrieval from SharePoint lists
- **Interactive Dashboard**: Web-based interface for report generation and download

## Technology Stack

- **Backend**: FastAPI, Python 3.11
- **PDF Generation**: ReportLab
- **Data Processing**: Pandas
- **Authentication**: MSAL (Microsoft Authentication Library)
- **Deployment**: Azure App Service with GitHub Actions CI/CD

## Local Development

### Prerequisites

- Python 3.11+
- SharePoint credentials (Client ID, Client Secret)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd sales_report_v2_independent
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create `.env` file with SharePoint credentials:
```env
SHAREPOINT_SITE_URL=https://yoursite.sharepoint.com/sites/yoursite
SHAREPOINT_CLIENT_ID=your-client-id
SHAREPOINT_CLIENT_SECRET=your-client-secret
```

5. Run the application:
```bash
cd fastapi_web_app
python -m uvicorn main:app --reload
```

6. Open browser to `http://localhost:8000`

## Project Structure

```
sales_report_v2_independent/
├── src/                          # Core report generation logic
│   ├── full_report.py           # Main orchestrator
│   ├── receivables_report_generator.py
│   ├── gvl_report.py
│   ├── usa_spa_report.py
│   ├── sharepoint_client.py     # SharePoint integration
│   ├── qry_data_ingestion.py    # Data retrieval
│   └── qry_data_mapping.py      # Data transformation
├── fastapi_web_app/             # Web interface
│   ├── main.py                  # FastAPI application
│   ├── templates/               # Jinja2 templates
│   └── static/                  # CSS, JS, images
├── data/
│   ├── inputs/                  # Budget files, mappings
│   │   ├── budget/
│   │   ├── prior_year/
│   │   └── mappings/
│   └── outputs/                 # Generated reports (gitignored)
├── requirements.txt
├── startup.sh                   # Azure startup script
└── .github/workflows/           # GitHub Actions
```

## Deployment

### Azure App Service

Application is deployed to: `https://qms-sales-report.azurewebsites.net`

### GitHub Actions Workflow

Automatic deployment on push to `main` branch:
1. Checkout code
2. Set up Python 3.11
3. Install dependencies
4. Deploy to Azure using publish profile

### Environment Variables (Azure Configuration)

Required in Azure Portal → Configuration → Application settings:
- `SHAREPOINT_SITE_URL`: SharePoint site URL
- `SHAREPOINT_CLIENT_ID`: Azure AD application client ID
- `SHAREPOINT_CLIENT_SECRET`: Azure AD application client secret
- `SCM_DO_BUILD_DURING_DEPLOYMENT`: `true`

## Budget Data Integration

### GVL Report Budget
Budget data located in `data/inputs/budget/budget_GVL_2025.csv`:
- Columns: Year, Month, Date, Market_Group, Region, Sales Employee/Account, Value_kEUR
- Filter by Month=11 for November reporting period

## Report Descriptions

### Receivables Report
Tracks customer payment status, outstanding balances, and aging analysis.

### GVL Report (Employee Sales)
Individual sales performance by employee with:
- Month-to-date sales
- Budget comparison
- Prior year comparison
- Variance analysis

### USA Spa Report (Regional Sales)
Territory-level sales breakdown for USA market with regional performance metrics.

### Full Report
Comprehensive report combining all metrics with executive summary.

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
