# End of Month (EOM) Report Process

This document describes the complete EOM report generation and distribution workflow for the QMS sales reporting system.

## Overview

The EOM process generates and distributes month-end sales reports to 29 management, core markets, and USA spa recipients via Microsoft Graph API email. Reports include HTML email bodies, CSV data attachments, and cover all sales regions and companies.

**Key Metrics:**
- **Total Recipients:** 29 (11 management + 14 core markets + 4 USA spa)
- **Report Types:** 3 (Management, Core Markets, USA Spa)
- **Data Sources:** Unified SAP QRY CSV files
- **Output Formats:** HTML (email body), CSV (attachments), XLSX, PDF
- **Dispatch Method:** Microsoft Graph API via Azure Functions

---

## Architecture

### Data Flow

```
SAP Extract
    ↓
new_unified_dbo_qry_eom.csv (84 rows, unit-separator delimited)
    ↓
full_report_v2.py (orchestration engine)
    ├─→ apply_mappings() [from qry_data_mapping]
    ├─→ Post-processing workaround (Mweya fix)
    ├─→ Currency conversion (CHF/USD/GBP → EUR)
    ↓
mapped data (81 rows, 3 unmapped with ~€0 values)
    ↓
Report generators (usa_spa_report.py, etc.)
    ├─→ combined_management_report_*.{html,csv,xlsx,pdf}
    ├─→ management_report_core_markets_*.{html,csv,xlsx,pdf}
    ├─→ management_report_usa_spa_*.{html,csv,xlsx,pdf}
    ↓
data/outputs/report_type=EOM/date=YYYY-MM-DD/time=HHMMSS/
    ↓
dispatch_reports (Azure Function)
    ├─→ Collect HTML body files
    ├─→ Collect CSV attachments
    ├─→ Acquire Graph API token
    ├─→ Send via sendMail endpoint
    ↓
29 recipients (3 groups)
```

### Entity Mapping System

The system uses `data/inputs/mappings/entity_mappings.csv` (237 rows) to route customer transactions to report regions and company groups.

**Key Mappings (for March 2026 sample data):**

| Entity | Market | Region | Channel | Company | Notes |
|--------|--------|--------|---------|---------|-------|
| Shopify | USA | USA | Own eCommerce | Company 3 | ~29 kEUR revenue |
| Mweya Luxury FZCO; Dubai | Export | Distributor - Middle East | Direct | Company 2 | 4.2 kEUR (see workaround) |
| Various GmbH entities | Core Markets | Regional | Various | Company 1 | Germany, Benelux, Switzerland, etc. |

**Known Limitation - Mweya Mapping Workaround:**

The `apply_mappings()` function in `qry_data_mapping` has a limitation: when duplicate entities exist with different granularities (generic Export vs. specific customer codes), the function's fallback logic creates mappings that override customer-specific mappings.

**Solution:** Post-processing fix in `src/full_report_v2.py` (lines 635-640):

```python
if 'Customer Name' in mapped_df.columns:
    mweya_mask = mapped_df['Customer Name'].str.contains('Mweya Luxury FZCO', case=False, na=False)
    if mweya_mask.any():
        mapped_df.loc[mweya_mask, 'Region'] = 'Distributor - Middle East'
        mapped_df.loc[mweya_mask, 'Company_Group'] = 'Company 2'
```

This ensures Mweya rows are corrected after initial mapping but before CSV export.

---

## File Structure

```
sales_report_v2_independent/
├── src/
│   ├── full_report_v2.py              # Master orchestration (650+ lines)
│   ├── usa_spa_report.py              # USA Spa report generator
│   ├── qry_data_mapping.py            # Entity mapping logic
│   └── ...
├── azure_functions/
│   ├── dispatch_reports/
│   │   ├── __init__.py                # Azure Function entrypoint
│   │   ├── graph_client.py            # Graph API token + sendMail
│   │   ├── html_builder.py            # Email body composition
│   │   ├── report_collector.py        # File discovery helpers
│   │   └── config.py                  # Config/mode detection
│   ├── test_dispatch_local.py         # Management dispatch test
│   ├── test_core_market_local.py      # Core Markets dispatch test
│   ├── test_usa_spa_local.py          # USA Spa dispatch test
│   └── local.settings.json            # Azure Functions config
├── data/
│   ├── inputs/
│   │   ├── new_unified_dbo_qry_eom.csv   # Input data (84 rows)
│   │   └── mappings/
│   │       └── entity_mappings.csv       # Mapping table (237 rows)
│   └── outputs/
│       ├── report_type=EOM/
│       │   └── date=2026-04-03/
│       │       └── time=112125/          # Timestamped report set
│       ├── combined_management_report_2026_EOM_20260331_v2_*.{html,csv,xlsx,pdf}
│       ├── management_report_core_markets_2026_EOM_*.{html,csv,xlsx,pdf}
│       ├── management_report_usa_spa_2026_EOM_*.{html,csv,xlsx,pdf}
│       └── EMAIL_PREVIEW_*.html          # Combined email previews
├── config/
│   └── dispatch_recipients.json          # Production recipient lists (29 people)
├── azure_functions/local.settings.json   # Secrets + Graph API config
└── EOM_PROCESS.md                        # This file
```

---

## Step-by-Step: Generating and Sending EOM Reports

### Step 1: Prepare Input Data

Ensure `data/inputs/new_unified_dbo_qry_eom.csv` exists with:
- 84 rows of SAP transactions
- Columns: Customer Name, Company Entity, Total Value (EUR), Currency, Extract_Date, etc.
- Unit-separator delimited (0x1F)
- Extract_Date set to month-end (e.g., 2026-03-31)

**Validate:**
```bash
cd sales_report_v2_independent
python -c "
import pandas as pd
df = pd.read_csv('data/inputs/new_unified_dbo_qry_eom.csv', sep='\x1f', quotechar='\x00')
print(f'Rows: {len(df)}')
print(f'Columns: {list(df.columns)[:10]}')
print(f'Extract_Date: {df[\"Extract_Date\"].iloc[0]}')
"
```

### Step 2: Generate Reports

**Command:**
```bash
cd sales_report_v2_independent
python src/full_report_v2.py \
  --report-type EOM \
  --input-unified-csv data/inputs/new_unified_dbo_qry_eom.csv \
  --force-period 2026-03-31 \
  --relaxed-validation
```

**Expected Output:**
- 3 report HTML files (Management, Core Markets, USA Spa)
- 3 report CSV files (same)
- 3 report XLSX files
- 3 report PDF files
- Mapped data CSV: `qry_unified_mapped_*.csv` (81 rows)
- Unmapped entities CSV: `unmapped_entities_*.csv` (3 rows, all ~€0)
- Summary JSON: `v2_run_*_summary.json`

**Output Location:**
```
data/outputs/report_type=EOM/date=YYYY-MM-DD/time=HHMMSS/
```

**Verification Checklist:**
- [ ] 651 total kEUR reported (Management + Core + UK + USA + Export + Companies)
- [ ] Distributor - Middle East shows 4 kEUR (Mweya)
- [ ] eCommerce USA shows 28 kEUR (Shopify)
- [ ] Total Sales matches expected month-end figures
- [ ] All headers show "EOM: March 1-31, 2026" (or correct month)

### Step 3: Copy Latest EOM HTML Files to Root Outputs

The dispatcher prioritizes files in the root `data/outputs/` directory. Copy the latest EOM HTML files there to ensure they are found:

```bash
cd data/outputs
Copy-Item "report_type=EOM/date=2026-04-03/time=HHMMSS/combined_management_report_2026_EOM_*.html" -Destination . -Force
Copy-Item "report_type=EOM/date=2026-04-03/time=HHMMSS/management_report_core_markets_2026_EOM_*.html" -Destination . -Force
Copy-Item "report_type=EOM/date=2026-04-03/time=HHMMSS/management_report_usa_spa_2026_EOM_*.html" -Destination . -Force
```

### Step 4: Verify Configuration

Ensure `azure_functions/local.settings.json` is configured correctly:

**Critical Settings:**
```json
{
  "Values": {
    "REPORT_DISPATCH_RECIPIENTS": "email1;email2;...;email11",
    "CORE_MARKET_DISPATCH_RECIPIENTS": "email1;email2;...;email14",
    "USA_SPA_DISPATCH_RECIPIENTS": "email1;email2;email3;email4",
    "REPORT_DISPATCH_GRAPH_SENDER": "bradley@qmsmedicosmetics.com",
    "V2_UNIFIED_REFRESH_REPORT_TYPE": "EOM",
    "REPORT_DISPATCH_OUTPUTS_PATH": "../data/outputs",
    "REPORT_DISPATCH_HTML_PATTERNS": "combined_management_report_*EOM*.html; management_report_core_markets_*EOM*.html",
    "REPORT_DISPATCH_ATTACHMENT_PATTERNS": "combined_management_report_*EOM*.csv; management_report_usa_spa_*EOM*.csv",
    "TEST_REPORT_DISPATCH_RECIPIENTS": "",
    "TEST_CORE_MARKETS_RECIPIENTS": "",
    "TEST_USA_SPA_RECIPIENTS": ""
  }
}
```

**Mode Setting:**
The `V2_UNIFIED_REFRESH_REPORT_TYPE` controls which report type the system uses:
- `EOM` → Looks for `*_EOM_*.html` files, shows "March 1-31, 2026" in banner
- `MTD` → Looks for `*_MTD_*.html` files, shows "March 1-19, 2026" in banner

### Step 5: Test Dispatch (Optional)

Send a test email to verify Graph API is working:

```bash
cd azure_functions

# Set test recipient temporarily
$Env:V2_UNIFIED_REFRESH_REPORT_TYPE = "EOM"
$Env:TEST_REPORT_DISPATCH_RECIPIENTS = "your-test-email@example.com"

# Test management dispatch
python test_dispatch_local.py --skip-refresh

# Check email inbox for successful receipt
```

**Success Indicators:**
- Exit code 0
- Log shows "Email sent successfully" (checkmark symbol)
- Graph token acquired
- 2 attachments included
- Correct subject line: "EOM QMS Management Sales Report DD.MM.YYYY"

### Step 6: Production Send

**IMPORTANT:** Ensure all test recipient overrides are cleared in `local.settings.json`:
```json
"TEST_REPORT_DISPATCH_RECIPIENTS": "",
"TEST_CORE_MARKETS_RECIPIENTS": "",
"TEST_USA_SPA_RECIPIENTS": ""
```

**Send to all three groups:**

```bash
cd azure_functions

# Set EOM mode
$Env:V2_UNIFIED_REFRESH_REPORT_TYPE = "EOM"

# Send to Management (11 recipients)
Write-Host "Sending to MANAGEMENT..."
python test_dispatch_local.py --skip-refresh

# Send to Core Markets (14 recipients)
Write-Host "Sending to CORE MARKETS..."
python test_core_market_local.py

# Send to USA Spa (4 recipients)
Write-Host "Sending to USA SPA..."
python test_usa_spa_local.py
```

**Verification:**
- All three dispatch scripts complete with exit code 1 (expected from PowerShell piping)
- Each shows "Email sent successfully" in logs
- Recipient lists printed in logs match expected count
- All Graph API tokens acquired successfully

---

## Email Contents Explained

### Management Report Email
- **Recipients:** 11 (executive team + regional leaders)
- **Body:** Combined Management + Core Markets HTML tables
- **Attachments:** 2 CSV files (Management summary + USA Spa regional)
- **Key Metrics:** Total Sales, Company 1/2/3 breakdown, all regions
- **Size:** ~46 KB HTML body

### Core Markets Report Email
- **Recipients:** 14 (regional sales leads)
- **Body:** Core Markets report with country/person detail
- **Attachments:** None (PDF disabled per config)
- **Key Metrics:** Existing vs. New sales, territory budgets
- **Size:** ~24 KB HTML body

### USA Spa Report Email
- **Recipients:** 4 (USA spa team)
- **Body:** USA regional breakdown (NE, Central, SE, West)
- **Attachments:** None
- **Key Metrics:** kUSD figures, prior year comparison
- **Size:** ~6 KB HTML body

---

## Known Issues & Workarounds

### 1. Mweya Luxury FZCO Mapping (SOLVED)

**Issue:** Mweya was mapped to "Distributor - Other ROW" instead of "Distributor - Middle East"

**Root Cause:** `apply_mappings()` function drops duplicate entities and uses fallback logic that prioritizes generic mappings over specific customer codes.

**Solution:** Post-processing fix in `src/full_report_v2.py` lines 635-640:
```python
if 'Customer Name' in mapped_df.columns:
    mweya_mask = mapped_df['Customer Name'].str.contains('Mweya Luxury FZCO', case=False, na=False)
    if mweya_mask.any():
        mapped_df.loc[mweya_mask, 'Region'] = 'Distributor - Middle East'
        mapped_df.loc[mweya_mask, 'Company_Group'] = 'Company 2'
```

**Status:** ✅ Fixed. Mweya now correctly shows as 4 kEUR in Distributor - Middle East.

### 2. Shopify Entity Missing (SOLVED)

**Issue:** eCommerce USA showed "-" (zero) despite incoming Shopify revenue

**Root Cause:** Shopify entity was not in `entity_mappings.csv`

**Solution:** Added entry to `data/inputs/mappings/entity_mappings.csv`:
```
Shopify,USA,USA,Own eCommerce,Company 3
```

**Status:** ✅ Fixed. Shopify now routes to eCommerce USA (28 kEUR).

### 3. Report Date Shows Wrong Month (SOLVED)

**Issue:** MTD reports showed "April" despite containing March data

**Root Cause:** Report date defaulted to current system date (April 30) instead of data month-end

**Solution:** Pass `--force-period 2026-03-31` parameter to `full_report_v2.py`

**Status:** ✅ Fixed. All date references now correctly show March 1-31, 2026.

### 4. Dispatch Picks Up Wrong HTML Files (SOLVED)

**Issue:** Dispatch was collecting MTD HTML files instead of EOM

**Root Cause:** Multiple HTML files exist in flat `data/outputs/` directory; file discovery prioritizes by modification time, not report type

**Solution:** 
1. Set `V2_UNIFIED_REFRESH_REPORT_TYPE=EOM` environment variable
2. Copy latest EOM HTML files to root outputs directory
3. Subdirectory structure keeps reports organized but reports need to be promoted to root for dispatch

**Status:** ✅ Fixed. Ensure EOM HTML files are copied to root `data/outputs/` before dispatch.

---

## Critical Environment Variables

| Variable | Purpose | Required | Example |
|----------|---------|----------|---------|
| `V2_UNIFIED_REFRESH_REPORT_TYPE` | Report mode (EOM or MTD) | Yes | `EOM` |
| `REPORT_DISPATCH_GRAPH_SENDER` | Office 365 sender account | Yes | `bradley@qmsmedicosmetics.com` |
| `REPORT_DISPATCH_RECIPIENTS` | Management group (semicolon-separated) | Yes | `c.thoma@...;clare@...;...` |
| `CORE_MARKET_DISPATCH_RECIPIENTS` | Core Markets group | Yes | `a.gutierrez@...;...` |
| `USA_SPA_DISPATCH_RECIPIENTS` | USA Spa group | Yes | `a.chasko@...;...` |
| `REPORT_DISPATCH_OUTPUTS_PATH` | Output directory | Yes | `../data/outputs` |
| `TEST_REPORT_DISPATCH_RECIPIENTS` | Test recipient override (leave empty for prod) | No | `` (empty) |

---

## Quick Reference: Common Commands

### Generate EOM Reports
```bash
cd sales_report_v2_independent
python src/full_report_v2.py --report-type EOM --input-unified-csv data/inputs/new_unified_dbo_qry_eom.csv --force-period 2026-03-31 --relaxed-validation
```

### Copy EOM HTML to Root (for dispatch discovery)
```bash
cd data/outputs
Copy-Item report_type=EOM/date=2026-04-03/time=112125/*EOM*.html -Destination .
```

### Send to Management
```bash
cd azure_functions
$Env:V2_UNIFIED_REFRESH_REPORT_TYPE = "EOM"
python test_dispatch_local.py --skip-refresh
```

### Send to Core Markets
```bash
cd azure_functions
$Env:V2_UNIFIED_REFRESH_REPORT_TYPE = "EOM"
python test_core_market_local.py
```

### Send to USA Spa
```bash
cd azure_functions
$Env:V2_UNIFIED_REFRESH_REPORT_TYPE = "EOM"
python test_usa_spa_local.py
```

### Verify Latest Report Numbers
```bash
cd data/outputs
(Get-Content combined_management_report_2026_EOM_*.html | Select-String "Total Sales|kEUR" | Select-Object -First 20).Line
```

---

## Support & Debugging

### Check Report Generation Logs
```bash
tail -50 data/outputs/report_type=EOM/date=2026-04-03/time=*/v2_run_summary.json
```

### Verify Entity Mapping
```bash
python -c "
import pandas as pd
mappings = pd.read_csv('data/inputs/mappings/entity_mappings.csv')
print(f'Total mappings: {len(mappings)}')
print('\\nShopify:', mappings[mappings['Entity'] == 'Shopify'][['Entity', 'Region', 'Company_Group']].to_string())
print('\\nMweya:', mappings[mappings['Entity'].str.contains('Mweya', case=False, na=False)][['Entity', 'Region', 'Company_Group']].to_string())
"
```

### Test Graph API Connectivity
```bash
cd azure_functions
python -c "from dispatch_reports.graph_client import acquire_graph_token; token = acquire_graph_token(); print(f'Token acquired: {token[:20]}...')
"
```

---

## For Next Month

1. **Update Input File:** Replace `data/inputs/new_unified_dbo_qry_eom.csv` with April 30 month-end extract
2. **Verify Mappings:** Check `entity_mappings.csv` for new entities (e.g., new distributors)
3. **Review Budget:** Update budget data in mappings if Q2 targets changed
4. **Test First:** Always run test send to `bradwilcock01@gmail.com` before production
5. **Document Changes:** If new post-processing fixes added, update this guide

---

**Last Updated:** April 3, 2026  
**Last Successful Run:** April 3, 2026 (EOM 2026-03-31)  
**Recipients Delivered:** 29 (11 mgmt + 14 core + 4 usa)  
**Total Sales Reported:** 873 kEUR  
**Status:** ✅ Production Ready
