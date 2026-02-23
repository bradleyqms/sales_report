# UK/USA Unmapped Entities Check - SharePoint Integration

This tool checks UK and USA sales input files for unmapped customer entities that cannot be properly categorized in management reports.

## Features

- **SharePoint Integration**: Downloads latest QRY files from SharePoint automatically
- **Fallback Support**: Uses local `automated_extracts` directory if SharePoint unavailable
- **Smart Parsing**: Handles different QRY file formats (USA AR vs others)
- **Detailed Reporting**: Generates CSV and text reports of unmapped entities

## SharePoint Setup

### Environment Variables

Set these environment variables to enable SharePoint integration:

```bash
SHAREPOINT_SITE_URL=https://qmsmedicosmetics.sharepoint.com/sites/DATAANDREPORTING
SHAREPOINT_CLIENT_ID=your-client-id-here
SHAREPOINT_CLIENT_SECRET=your-client-secret-here
```

### Test SharePoint Connection

Before running the main script, test your SharePoint credentials:

```bash
cd sales_report_v2_independent
python src/test_sharepoint_unmapped.py
```

## Usage

### Run Unmapped Entities Check

```bash
cd sales_report_v2_independent
python src/uk_usa_unmapped_check.py
```

The script will:
1. Check for SharePoint credentials
2. Download latest QRY files from SharePoint if available
3. Fall back to local files if SharePoint fails
4. Process UK and USA QRY files
5. Generate reports in `data/outputs/`

## File Sources

### SharePoint Path
- **Location**: `/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/`
- **Files**: All QRY_AR_*, QRY_CN_*, QRY_SO_* files for UK and USA

### Local Fallback
- **Location**: `../sales_report_v2/automated_extracts/`
- **Files**: Timestamped QRY files (e.g., `QRY_AR_MTD_USA_20260206_162303.csv`)

## Output

Reports are saved to `data/outputs/` with timestamped filenames:
- `uk_usa_sales_unmapped_entities_check_YYYYMMDD_HHMMSS.csv`
- `uk_usa_sales_unmapped_entities_check_YYYYMMDD_HHMMSS.txt`

## QRY File Format Support

The script handles different QRY file formats:
- **USA AR files**: `CustomerCode=CustomerName=Amount,CurrencyCode=`
- **Other files**: `CustomerName=Amount,CurrencyCode=`

## Dependencies

- pandas
- pathlib
- sharepoint_client (for SharePoint integration)
- Environment variables for SharePoint access