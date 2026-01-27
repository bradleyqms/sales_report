# SharePoint Upload Instructions for Budget & Prior Years Files

## Overview
To fix the production issue where support files are missing, upload the budget and prior years CSV files to SharePoint. The code now automatically downloads them as part of the report generation pipeline.

## Files to Upload

### Budget Files (3 files)
- `budget_2026_processed.csv`
- `budget_GVL_2026.csv`
- `budget_USA_spa_2026.csv`

### Prior Years Files (3 files)
- `prior_sales_2025_processed.csv`
- `prior_sales_2025_gvl.csv`
- `prior_sales_2025_usa.csv`

## Step-by-Step Instructions

### Step 1: Access SharePoint
1. Open your browser and go to your SharePoint site
2. Navigate to the document library where your QRY files are stored
3. URL should be similar to: `https://[tenant].sharepoint.com/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/`

### Step 2: Create Folder Structure
Create the following folder structure if it doesn't already exist:

```
SAP Extracts/
├── Support Files/
    ├── Budget/
    └── Prior Years/
```

**How to create folders:**
1. Click the "New" button in SharePoint
2. Select "Folder"
3. Name it "Support Files"
4. Inside "Support Files", create two folders: "Budget" and "Prior Years"

### Step 3: Upload Budget Files
1. Navigate to `SAP Extracts/Support Files/Budget/`
2. Click "Upload"
3. Select all 3 budget files from your local machine:
   - `c:\Users\bradley\OneDrive - QMS Medicosmetics\Desktop\python_projects\sales_report_v2_independent\data\inputs\budget\budget_2026_processed.csv`
   - `c:\Users\bradley\OneDrive - QMS Medicosmetics\Desktop\python_projects\sales_report_v2_independent\data\inputs\budget\budget_GVL_2026.csv`
   - `c:\Users\bradley\OneDrive - QMS Medicosmetics\Desktop\python_projects\sales_report_v2_independent\data\inputs\budget\budget_USA_spa_2026.csv`
4. Click "Open" and wait for upload to complete

### Step 4: Upload Prior Years Files
1. Navigate to `SAP Extracts/Support Files/Prior Years/`
2. Click "Upload"
3. Select all 3 prior years files:
   - `c:\Users\bradley\OneDrive - QMS Medicosmetics\Desktop\python_projects\sales_report_v2_independent\data\inputs\prior_years\prior_sales_2025_processed.csv`
   - `c:\Users\bradley\OneDrive - QMS Medicosmetics\Desktop\python_projects\sales_report_v2_independent\data\inputs\prior_years\prior_sales_2025_gvl.csv`
   - `c:\Users\bradley\OneDrive - QMS Medicosmetics\Desktop\python_projects\sales_report_v2_independent\data\inputs\prior_years\prior_sales_2025_usa.csv`
4. Click "Open" and wait for upload to complete

### Step 5: Verify Upload
1. Confirm all 6 files are now in SharePoint:
   - `/Support Files/Budget/` should contain: `budget_2026_processed.csv`, `budget_GVL_2026.csv`, `budget_USA_spa_2026.csv`
   - `/Support Files/Prior Years/` should contain: `prior_sales_2025_processed.csv`, `prior_sales_2025_gvl.csv`, `prior_sales_2025_usa.csv`

## How the Code Works

The `full_report.py` script now includes these files in the automatic download process:

```python
other_paths = {
    'mapping': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/entity_mappings.csv',
    'budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_{current_year}_processed.csv',
    'prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_{prior_year}_processed.csv',
    'gvl_budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/Support Files/Budget/budget_GVL_{current_year}.csv',
    'gvl_prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/Support Files/Prior Years/prior_sales_{prior_year}_gvl.csv',
    'usa_spa_budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/Support Files/Budget/budget_USA_spa_{current_year}.csv',
    'usa_spa_prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/Support Files/Prior Years/prior_sales_{prior_year}_usa.csv'
}
```

## What Happens During Report Generation

1. **Download Phase**: The script downloads all 7 support files from SharePoint (mapping + 6 new budget/prior files)
2. **Fallback Logic**: If a file can't be downloaded from SharePoint, it automatically falls back to local files in `data/inputs/`
3. **Report Generation**: All three reports (Receivables, GVL, USA Spa) now have required data and will generate successfully

## Troubleshooting

### Issue: File not found error in production
**Solution**: Check that all 6 files have been uploaded to the correct SharePoint paths:
- Budget files: `SAP Extracts/Support Files/Budget/`
- Prior files: `SAP Extracts/Support Files/Prior Years/`

### Issue: File names don't match
**Solution**: Ensure the file names in SharePoint exactly match:
- `budget_2026_processed.csv` (not `budget_2026.csv` or `budget_processed.csv`)
- `budget_GVL_2026.csv` (not `gvl_budget_2026.csv`)
- `budget_USA_spa_2026.csv` (not `usa_spa_budget_2026.csv`)
- `prior_sales_2025_processed.csv`
- `prior_sales_2025_gvl.csv`
- `prior_sales_2025_usa.csv`

### Issue: SharePoint credentials not working
**Solution**: Verify your `.env` file has correct values:
```
SHAREPOINT_SITE_URL=https://[tenant].sharepoint.com/sites/DATAANDREPORTING
SHAREPOINT_CLIENT_ID=your_client_id
SHAREPOINT_CLIENT_SECRET=your_client_secret
```

## Code Changes Made

Modified: `src/full_report.py`

Changes:
1. Extended `other_paths` dictionary to include all 4 new SharePoint paths
2. Added fallback logic for each new file type in the exception handler
3. Updated GVL report generation to use downloaded paths from `local_paths` dict
4. Updated USA Spa report generation to use downloaded paths from `local_paths` dict
5. Updated local-files-only path to create `local_paths` dict with all 6 support file paths

This ensures production has access to all required files, whether from SharePoint or local fallback.
