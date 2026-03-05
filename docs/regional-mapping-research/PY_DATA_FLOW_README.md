# Prior Year Data Flow - Complete Documentation Index
## sales_report_v2_independent

**Generated:** February 27, 2026  
**System Date Context:** Current Year=2026, Prior Year=2025, Current Month=February

---

## 📚 Documentation Files Overview

This repository contains three comprehensive documentation files that collectively document the complete PY data flow:

### 1. **PY_DATA_FLOW_DOCUMENTATION.md** (Main Document - 600+ lines)
   **Scope:** High-level overview to detailed technical reference
   
   **Content:**
   - Complete data flow overview and business context
   - PY data file locations and naming conventions
   - Data loading pipeline (BaseReportGenerator)
   - 3-step transformation pipeline with code examples
   - Entity mapping integration
   - Report calculation patterns for all 4 generators
   - Files & dependencies matrix
   - Mermaid flow diagrams (embedded)
   - Common issues & solutions
   - Testing & validation checklist
   - Performance considerations
   - Code references with line numbers
   
   **Best For:** Understanding the complete system, debugging issues, finding code references
   
   **Quick Links:**
   - Data File Locations: [Link](#py-data-file-locations)
   - Transformation Steps: [Link](#transformation-steps)
   - Entity Mappings: [Link](#entity-mapping-integration)
   - Report Calculations: [Link](#report-calculations)

---

### 2. **PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md** (Diagrams - 400+ lines)
   **Scope:** Visual representation of all major flows
   
   **Diagrams Included:**
   1. **End-to-End Pipeline** - Full data journey from CSV to report output
   2. **Data Loading & Parsing** - CSV read to DataFrame transformations
   3. **Region-Level Aggregation** - USA Spa report aggregation pattern
   4. **Employee-Level Lookup** - GVL report lookup mechanism
   5. **Multi-Market Management** - Complex multi-segment report flow
   6. **YoY Calculation Pattern** - Metric calculation workflow
   7. **Error Handling & Fallbacks** - Robust error handling paths
   8. **Data Column Transformations** - Input to output column conversions
   9. **Report Generation Command Flow** - Full execution sequence
   10. **Timestamp & Date Context** - Timeline visualization
   
   **Diagram Features:**
   - Color-coded by transformation stage
   - Shows data volumes at each step
   - Includes memory/performance indicators
   - Fallback and error paths included
   
   **Best For:** Understanding data flow visually, presentations, troubleshooting workflows

---

### 3. **PY_DATA_FILE_TOUCH_MAP.md** (Implementation Details - 700+ lines)
   **Scope:** Exhaustive file-by-file mapping of every PY data interaction
   
   **Content:**
   - **Step 1: File Ingestion**
     - BaseReportGenerator.__init__
     - BaseReportGenerator._load_data_files
     - Data state after loading
   
   - **Step 2: Date Preparation**
     - PreparedDates calculations
     - Utility functions (get_prior_year, get_current_month)
   
   - **Step 3: Data Preparation (Report-Specific)**
     - USA Spa Report: Date parsing, numeric conversion, regional aggregation
     - GVL Report: Text cleanup, employee mapping, filtering
     - Core Market Report: Sub-region cleaning, type filtering
     - Management Report: Specialized prior loading
   
   - **Step 4: Report Calculation**
     - USA Spa: Region-level YoY calculations
     - GVL: Employee lookup and metrics
   
   - **Step 5: Rendering & Export**
     - Formatting functions
     - Display and export mechanisms
   
   - **Complete File Touch Summary Table** - Every file, method, line number
   - **Data Transformation Timeline** - State at each T0-T7 stage
   - **Tracing a Single Data Point** - Complete journey of one PY value (Northeast region example)
   
   **Best For:** Code review, adding features, debugging specific transformations, exact line references

---

## 🔄 Quick Reference: Data Flow Summary

```
CSV File (11,230 rows)
  ↓ [base_report_generator.py:85 - pd.read_csv()]
self.prior_df (DataFrame, object dtypes)
  ↓ [report._prepare_data() - Lines 140-151]
self.prior_month (850 rows, filtered to Feb 2025)
  ↓ [report._prepare_data() - Lines 165-173]
Lookup Dictionary (4-30 aggregated entries)
  ↓ [report.calculate_report() - Lines 265-300]
YoY Metrics (diff_prior, pct_prior)
  ↓ [report.render_report() - Lines 310-400]
Formatted Report Output (CSV, PDF, HTML)
```

---

## 📊 Key Files Touched by Analysis

### Core Implementation Files (5 files)

| File | Role | PY Operations |
|------|------|---|
| `src/base_report_generator.py` | Base class for all reports | Load, parse dates, convert numeric |
| `src/usa_spa_report.py` | USA Spa regional report | Regional aggregation, YoY calculation |
| `src/gvl_report.py` | Employee sales report | Employee mapping, lookup, YoY |
| `src/core_market_report.py` | Core markets report | Sub-region aggregation |
| `src/receivables_report_generator.py` | Management report | Multi-segment handling |

### Supporting Files (2 files)

| File | Role | PY Operations |
|------|------|---|
| `src/utils.py` | Utility functions | Date calculations (get_prior_year, get_current_month) |
| `src/full_report.py` | Orchestration | File path construction, report coordination |

### Configuration Files (1 folder)

| Location | Role | PY Usage |
|----------|------|---|
| `src/config/*.json` | Report structure definitions | Defines sections/items for aggregation |

### Data Files (2 folders)

| Location | Contents | PY Data |
|----------|----------|---------|
| `data/inputs/prior_years/` | PY CSV files | 6 files: raw and processed, market-specific |
| `data/inputs/mappings/` | Entity mappings | Links PY data to market segments |

---

## 🎯 Finding Specific Information

### I need to...

**Find where PY data is loaded**
→ See: [PY_DATA_FLOW_DOCUMENTATION.md - Data Loading Pipeline](#data-loading-pipeline)  
→ Code: [base_report_generator.py L71-102](#)

**Understand date filtering logic**
→ See: [PY_DATA_FILE_TOUCH_MAP.md - Step 3: Filter by Month](#step-3-data-preparation--filtering-report-specific)  
→ Code: [usa_spa_report.py L140-151](#)

**See how numeric values are converted**
→ See: [PY_DATA_FLOW_DOCUMENTATION.md - Transform 2: Numeric Conversion](#transform-2-numeric-value-conversion)  
→ Code: [usa_spa_report.py L165-173](#)

**Understand YoY calculations**
→ See: [PY_DATA_FLOW_DOCUMENTATION.md - YoY Analysis Pattern](#yoy-analysis-pattern)  
→ Diagrams: [PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md - Diagram 6](#)

**Debug report generation**
→ See: [PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md - Diagram 9](#)  
→ Details: [PY_DATA_FILE_TOUCH_MAP.md - Step 4: Report Calculation](#)

**Find all mentions of entity mappings with PY**
→ See: [PY_DATA_FLOW_DOCUMENTATION.md - Entity Mapping Integration](#entity-mapping-integration)  
→ Code: [gvl_report.py L60-82](#) or [qry_data_mapping.py L70-145](#)

**Add a new report generator**
→ Use: [PY_DATA_FILE_TOUCH_MAP.md - Step 3 Examples](#step-3-data-preparation--filtering-report-specific)  
→ Template: Follow pattern in [usa_spa_report.py](#) or [gvl_report.py](#)

**Trace a data point through the system**
→ See: [PY_DATA_FILE_TOUCH_MAP.md - Tracing a Single PY Data Point](#tracing-a-single-py-data-point)  
→ Full example: Northeast region walkthrough

---

## 📈 Data Pipeline Overview

### File Locations

```
sales_report_v2_independent/
├── data/
│   ├── inputs/
│   │   ├── prior_years/              ← PY DATA SOURCE
│   │   │   ├── prior_sales_2025_processed.csv      (11,230 rows)
│   │   │   ├── prior_sales_2025_usa.csv             (2,340 rows)
│   │   │   ├── prior_sales_2025_gvl.csv             (850 rows)
│   │   │   └── prior_sales_2024_*.csv              (historical)
│   │   └── mappings/
│   │       └── entity_mappings.csv                 (entity mapping context)
│   └── outputs/                       ← REPORT OUTPUT
│       ├──*.csv
│       ├── *.pdf
│       └── *.html
├── src/
│   ├── base_report_generator.py      (Load, filter, convert)
│   ├── usa_spa_report.py             (Regional aggregation)
│   ├── gvl_report.py                 (Employee aggregation)
│   ├── core_market_report.py         (Sub-region aggregation)
│   ├── receivables_report_generator.py (Multi-segment)
│   ├── utils.py                      (Date utilities)
│   ├── full_report.py                (Orchestration)
│   └── config/
│       ├── usa_spa_report_structure.json
│       ├── gvl_report_structure.json
│       └── ...
└── [THIS DOCUMENTATION]
    ├── PY_DATA_FLOW_DOCUMENTATION.md              ← START HERE
    ├── PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md      ← VISUALIZE
    └── PY_DATA_FILE_TOUCH_MAP.md                  ← DEEP DIVE
```

---

## 🔍 Transformation Steps Quick Reference

### Step 1: Load (base_report_generator.py:85)
**Input:** File path string  
**Output:** self.prior_df (11,230 rows, object dtypes)

### Step 2: Parse Dates (report._prepare_data:140)
**Input:** self.prior_df (Date as string "02/15/2025")  
**Output:** self.prior_month (850 rows, Date as datetime64)

### Step 3: Parse Numeric (report._prepare_data:165)
**Input:** self.prior_month (Value_kEUR as string "156,340.50")  
**Output:** self.prior_month (Value_kEUR as float64 156340.5)

### Step 4: Aggregate (report._prepare_data:172)
**Input:** self.prior_month (850 rows)  
**Output:** prior_region_dict ({Region: summed_value})

### Step 5: Calculate YoY (report.calculate_report:298)
**Input:** val_prior from lookup dict  
**Output:** diff_prior (float), pct_prior (float)

### Step 6: Format (report.render_report:350)
**Input:** diff_prior (float -11110.5), pct_prior (float -7.1)  
**Output:** Formatted strings ("-11111", "-7.1%")

### Step 7: Export (report.export_*:file ops)
**Input:** Formatted report data  
**Output:** CSV, PDF, HTML files

---

## 🧪 Testing & Validation Checklist

When adding or modifying PY data functionality:

- [ ] **File Existence:** Verify prior_sales_*.csv files in `data/inputs/prior_years/`
- [ ] **Schema Validation:** Confirm required columns (Date, Value_kEUR, Region, etc.)
- [ ] **Date Parsing:** Test DD/MM/YYYY → datetime conversion with sample dates
- [ ] **Numeric Conversion:** Verify thousands separator handling (comma, space)
- [ ] **Month Filtering:** Validate correct month/year is selected
- [ ] **Aggregation:** Confirm groupby sums match expected totals
- [ ] **YoY Calculations:** Verify no division by zero, correct percentage formula
- [ ] **Entity Mapping:** Test employee/customer mapping application
- [ ] **Output Format:** Verify CSV/PDF/HTML exports have correct values
- [ ] **Performance:** Confirm <500ms load time per file

**Test Entry Points:**
```bash
python src/gvl_report.py           # GVL with PY
python src/usa_spa_report.py       # USA Spa with market-specific PY
python src/full_report.py          # All reports with all PY files
```

---

## 📝 Key Metrics Summary

### Data Volumes
- **Input:** 11,230 PY rows (full year)
- **Filtered:** 850 rows (single month = 7%)
- **Aggregated:** 4-30 groups (0.5% of filtered)

### Transformation Types
1. CSV read (pandas)
2. Date parsing (DD/MM/YYYY → datetime64)
3. Numeric conversion (string → float64)
4. Text normalization (strip whitespace)
5. Filtering (year+month)
6. Aggregation (groupby sum)
7. Calculation (diff, percentage)
8. Formatting (float → string)

### Performance
- **Load:** <500ms per CSV file
- **Filter:** <100ms per report
- **Aggregate:** <50ms per report  
- **Calculate:** <100ms per report
- **Render:** <500ms per report
- **Export:** <1000ms per format

---

## 🚀 Getting Started

### For New Team Members
1. Read: [PY_DATA_FLOW_DOCUMENTATION.md - Overview](#overview)
2. View: [PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md - Diagram 1](#)
3. Study: One specific report flow (e.g., USA Spa or GVL)

### For Code Review
1. Check: [PY_DATA_FILE_TOUCH_MAP.md - File Touch Summary Table](#file-touch-summary-table)
2. Reference: Exact line numbers in implementation
3. Validate: Against documented transformations

### For Adding Features
1. Locate: Similar functionality in existing report
2. Copy: Transformation pattern (e.g., from [PY_DATA_FILE_TOUCH_MAP.md](#))
3. Adapt: To new market/dimension
4. Test: Using validation checklist

### For Debugging Issues
1. Trace: Data journey through [PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md](#)
2. Reference: Exact transformation in [PY_DATA_FILE_TOUCH_MAP.md](#)
3. Check: Common issues in [PY_DATA_FLOW_DOCUMENTATION.md - Issues & Solutions](#common-issues--solutions)

---

## 💡 Important Details

### Dynamic File Selection (CRITICAL)
PY file paths are **calculated dynamically** based on current date:
```python
prior_year = get_prior_year()  # Always current_year - 1
prior_path = f'...prior_sales_{prior_year}_processed.csv'
```

**Implication:** As we move to 2027, files automatically reference 2026 PY data.

### Month Filtering (DYNAMIC)
Prior month filtering is **dynamically calculated**:
```python
current_month = get_current_month()  # Current Gregorian month (1-12)
filter: year == prior_year AND month == current_month
```

**Implication:** Reports always compare same month YoY.

### Entity Mapping (STATIC)
Entity mappings are **static lookup files** that provide context:
- Map sales employees to market segments
- Map customers to regions
- Provides Channel_Level, Market_Group attributes

**Implication:** PY data inherits same attributes for consistency.

---

## 📚 Reference Card

### File Paths (Dynamic)
| Item | Path | Variable |
|------|------|----------|
| Primary PY | `data/inputs/prior_years/prior_sales_{PRIOR_YEAR}_processed.csv` | prior_year = 2025 |
| USA Spa PY | `data/inputs/prior_years/prior_sales_{PRIOR_YEAR}_usa.csv` | prior_year = 2025 |
| GVL PY | `data/inputs/prior_years/prior_sales_{PRIOR_YEAR}_gvl.csv` | prior_year = 2025 |
| Mappings | `data/inputs/mappings/entity_mappings.csv` | Static |
| Outputs | `data/outputs/{format}/report.*` | Dynamic timestamp |

### Key Variables
| Variable | Example | Source |
|----------|---------|--------|
| current_year | 2026 | get_current_year() |
| prior_year | 2025 | get_prior_year() |
| current_month | 2 | get_current_month() |
| now | 2026-02-27 09:45:00 | datetime.now() |

### Column Names (Standardized)
| Column | Type | Example |
|--------|------|---------|
| Date | datetime64/str | "02/15/2025" |
| Region | str | "Northeast" |
| Value_kEUR | float | 156340.5 |
| Value_kUSD | float | 167123.4 |
| Sales Employee / Account | str | "John Smith" |
| Sub Region | str | "Germany" |
| Company_Group | str | "GmbH" |

---

## ✅ Document Completeness Checklist

This documentation covers:

- ✅ Where PY data files are loaded → [PY_DATA_FLOW_DOCUMENTATION.md - Data File Locations](#)
- ✅ How PY flows through BaseReportGenerator → [PY_DATA_FILE_TOUCH_MAP.md - Step 1-3](#)
- ✅ Each transformation step with code examples → [PY_DATA_FILE_TOUCH_MAP.md - Complete table](#)
- ✅ Where PY connects to entity mappings → [PY_DATA_FLOW_DOCUMENTATION.md - Entity Mapping Integration](#)
- ✅ How PY is used in report calculations → [PY_DATA_FLOW_DOCUMENTATION.md - Report Calculations](#)
- ✅ Which files touch/transform PY at each step → [PY_DATA_FILE_TOUCH_MAP.md - File touch summary table](#)
- ✅ Data flow: File → Load → Parse → Map → Filter → Aggregate → Report → Output
  - File: CSV location → [#py-data-file-locations]
  - Load: pd.read_csv()  → [PY_DATA_FILE_TOUCH_MAP.md - Step 1](#step-1-file-ingestion)
  - Parse: Date/Numeric → [PY_DATA_FILE_TOUCH_MAP.md - Step 3](#step-3-data-preparation--filtering-report-specific)
  - Map: Entity matching → [PY_DATA_FLOW_DOCUMENTATION.md - Entity Mapping](#)
  - Filter: Month selection → [PY_DATA_FILE_TOUCH_MAP.md - Filter by Month](#section-4-filter-prior-for-same-month-last-year-)
  - Aggregate: Groupby sum → [PY_DATA_FILE_TOUCH_MAP.md - Parse Numeric Values](#section-5-parse-numeric-values-)
  - Report: YoY calc → [PY_DATA_FILE_TOUCH_MAP.md - Step 4](#step-4-report-calculation)
  - Output: Export → [PY_DATA_FILE_TOUCH_MAP.md - Step 5](#step-5-report-rendering--export)

---

## 🔗 Cross-References

### Within Documentation
- Main document: [PY_DATA_FLOW_DOCUMENTATION.md](PY_DATA_FLOW_DOCUMENTATION.md)
- Diagrams: [PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md](PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md)
- File touch map: [PY_DATA_FILE_TOUCH_MAP.md](PY_DATA_FILE_TOUCH_MAP.md)

### In Codebase
- Data loading: `src/base_report_generator.py` L71-102
- Filtering: `src/usa_spa_report.py` L140-151
- Aggregation: `src/usa_spa_report.py` L165-173
- Calculation: `src/usa_spa_report.py` L265-300
- Rendering: `src/usa_spa_report.py` L310-400
- Entry point: `src/full_report.py` main()

---

## 📞 Questions & Support

For questions about:
- **Architecture & Flow:** See [PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md](PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md)
- **Implementation Details:** See [PY_DATA_FILE_TOUCH_MAP.md](PY_DATA_FILE_TOUCH_MAP.md)
- **Troubleshooting:** See [PY_DATA_FLOW_DOCUMENTATION.md - Common Issues](#common-issues--solutions)
- **Code References:** See [PY_DATA_FLOW_DOCUMENTATION.md - Appendix](#appendix-code-references)

---

**Document Index Version:** 1.0  
**Created:** February 27, 2026  
**Pages:** 3 comprehensive files (1,700+ combined lines)  
**Format:** Markdown with Mermaid diagrams  
**Coverage:** Complete PY data pipeline from ingestion through report generation
