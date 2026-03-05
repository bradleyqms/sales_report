# Prior Year Data - File Touch Map & Transformation Matrix
## sales_report_v2_independent

**Purpose:** Map every file that touches or transforms PY data through the entire pipeline, with exact line numbers and method names.

---

## Overview: File Touch Sequence

```
PY CSV File
    ↓
BaseReportGenerator (LOAD)
    ↓
Report.__init__ (LOAD)
    ↓
Report._prepare_data (FILTER & PARSE)
    ↓
Report.calculate_report (AGGREGATE & TRANSFORM)
    ↓
Report.render_report (FORMAT & CALCULATE YOY)
    ↓
Report.export_* (EXPORT)
```

---

## Step 1: File Ingestion

### File: `src/base_report_generator.py`

**Purpose:** Abstract base class that loads all data files including PY

#### Method: `__init__` (Lines 27-57)

```python
def __init__(self, config_path: str, sales_path: str, budget_path: str, prior_path: str):
    """
    Initialize the report generator.
    Args:
        prior_path: Path to prior year data CSV ← PARAMETER
    """
    self.config = self._load_config(config_path)
    self._load_data_files(sales_path, budget_path, prior_path)  # ← CALLS LOAD METHOD
    self._prepare_dates()
```

**Responsibilities:**
- Accept `prior_path` parameter (passed from subclass or full_report.py)
- Call `_load_data_files()` with prior_path

**Input Data:** File path string  
**Output Data:** None (triggers loading)

#### Method: `_load_data_files` (Lines 71-102)

```python
def _load_data_files(self, sales_path: str, budget_path: str, prior_path: str) -> None:
    """
    Load CSV data files into DataFrames.
    
    Args:
        prior_path: Path to prior year data CSV ← PARAMETER
    
    Raises:
        FileNotFoundError: If any data file doesn't exist
        pd.errors.EmptyDataError: If any data file is empty
    """
    try:
        self.df = pd.read_csv(sales_path)              # Current year
        self.budget_df = pd.read_csv(budget_path)      # Budget
        self.prior_df = pd.read_csv(prior_path)        # ← PRIOR YEAR LOADED HERE ★★★
        # prior_df is now a DataFrame with columns: Date, Region, Value_kEUR, Value_kUSD, etc.
    except FileNotFoundError as e:
        logging.error(f"Required data file not found: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logging.error(f"Data file is empty: {e}")
        raise
```

**Responsibilities:**
- Read CSV file from disk
- Parse into pandas DataFrame
- Store in `self.prior_df`
- Validate file exists and is not empty

**Input Data:**
- File path: `data/inputs/prior_years/prior_sales_2025_processed.csv` (or market-specific variant)
- Raw CSV with columns: Date, Region, Value_kEUR, Value_kUSD, Sales Employee, etc.

**Output Data:**
- `self.prior_df`: DataFrame (11,230 rows, 12 columns, mixed dtypes)
- Columns still as strings initially (not parsed)

**Data State:**
```
                       Date         Region  Value_kEUR  Value_kUSD Sales Employee
0          02/15/2025  Northeast      156,340.50   167,123.40  John Smith
1          02/15/2025  Central        203,450.25   218,127.77  Jane Doe
...
11229      02/28/2025  West           234,567.89   251,248.94  Bob Wilson
```

---

## Step 2: Date Preparation

### File: `src/base_report_generator.py`

#### Method: `_prepare_dates` (Lines 104-116)

```python
def _prepare_dates(self) -> None:
    """
    Calculate and store commonly used date values.
    
    Sets instance variables:
    - current_year: Current year (int) → 2026
    - prior_year: Prior year (int) → 2025
    - current_month: Current month (int, 1-12) → 2
    - now: Current datetime object
    """
    self.now = datetime.datetime.now()
    self.current_year = get_current_year()      # ← Calls utils.get_current_year()
    self.prior_year = get_prior_year()          # ← Calls utils.get_prior_year()
    self.current_month = get_current_month()    # ← Calls utils.get_current_month()
```

**Responsibilities:**
- Calculate temporal context for filtering
- Establish current_month and prior_year

**Input Data:** Current system date

**Output Data:** Instance variables (used by _prepare_data methods)

### Helper File: `src/utils.py`

#### Function: `get_prior_year` (Lines 50-60)

```python
def get_prior_year() -> int:
    """
    Get the prior year (current year - 1) dynamically.
    
    Returns:
        Prior year as integer (e.g., 2025 when current year is 2026)
    """
    return datetime.datetime.now().year - 1
```

**Use:** Returns 2025 for 2026 reports

#### Function: `get_current_month` (Lines 28-38)

```python
def get_current_month() -> int:
    """
    Get the current month (1-12).
    
    Returns:
        Month as integer (1=January, 12=December)
    """
    now = datetime.datetime.now()
    return now.month
```

**Use:** Returns 2 for February reports

---

## Step 3: Data Preparation & Filtering (Report-Specific)

### File: `src/usa_spa_report.py`

**Class:** `USASpaReportGenerator(BaseReportGenerator)`

#### Method: `_prepare_data` (Lines 75-191)

**Responsibilities:**
1. Filter sales data to AR (Accounts Receivable)
2. Filter to USA Spa market segment
3. Override budget file if market-specific version available
4. **Filter prior year data to current month**
5. Parse dates in prior data
6. Aggregate prior by region for lookup

##### Section 1: Filter Sales (Lines 77-92)
```python
# Filter Sales to AR (for QRY data, Document Type is 'AR')
self.df = self.df[self.df['Document Type'] == 'AR'].copy()

# Filter to USA Spa
self.df = self.df[(self.df['Market_Group'] == 'USA') & (self.df['Channel_Level'] == 'Spa')].copy()
```

**Impact:** Narrows sales focus; does NOT directly touch prior_df

##### Section 2: Budget File Selection (Lines 93-134)
```python
# Prefer local USA-specific budget file if present
repo_root = Path(__file__).parent.parent
local_budget_dir = repo_root / 'data' / 'inputs' / 'budget'

# Look for current year USA Spa budget file first
current_year_usa_spa_file = local_budget_dir / f'budget_USA_spa_{self.current_year}.csv'
if current_year_usa_spa_file.exists():
    try:
        alt_budget = pd.read_csv(current_year_usa_spa_file)
        logging.info(f"Preferring local budget file: {current_year_usa_spa_file.name}")
        self.budget_df = alt_budget
    except Exception:
        pass
```

**Impact:** Does NOT touch prior_df (budget only)

##### Section 3: Filter Budget for Current Month (Lines 135-138)
```python
# Filter Budget for Current Month
# Budget Date is DD/MM/YYYY
self.budget_df['Date'] = pd.to_datetime(self.budget_df['Date'], format='%d/%m/%Y')
self.budget_month = self.budget_df[self.budget_df['Date'].dt.month == self.current_month].copy()
```

**Impact:** Does NOT touch prior_df (budget only)

##### Section 4: **FILTER PRIOR FOR SAME MONTH LAST YEAR** ★★★ (Lines 140-151)

```python
def _prepare_data(self):
    # ...
    
    # Filter Prior for Same Month Last Year
    # Prior file may have Date in DD/MM/YYYY format — try to parse safely
    try:
        self.prior_df['Date'] = pd.to_datetime(
            self.prior_df['Date'], 
            format='%d/%m/%Y'
        )
        self.prior_month = self.prior_df[
            (self.prior_df['Date'].dt.year == self.prior_year) &        # 2025
            (self.prior_df['Date'].dt.month == self.current_month)      # 2 (February)
        ].copy()
    except Exception:
        # Fallback: string-based date matching
        target_prior_date = f"{self.prior_year}-{self.current_month:02d}"  # "2025-02"
        self.prior_month = self.prior_df[
            self.prior_df['Date'].astype(str).str.startswith(target_prior_date)
        ].copy()
```

**TRANSFORMATION 1: DATE PARSING & FILTERING**

**Input Data:**
- `self.prior_df` (11,230 rows from CSV)
- Column `Date` as object/string: "02/15/2025", "02/16/2025", etc.
- `self.prior_year = 2025` (from _prepare_dates)
- `self.current_month = 2` (from _prepare_dates)

**Processing:**
1. Parse `Date` column: string → datetime64
2. Extract year and month from datetime
3. Filter WHERE year == 2025 AND month == 2
4. Copy to new DataFrame (avoid SettingWithCopyWarning)

**Output Data:**
- `self.prior_month` (≈850 rows, same columns)
- Only Feb 2025 records remain
- Date now datetime64[ns]

**Example Output:**
```python
                    Date         Region  Value_kEUR Value_kUSD Sales Employee
0        2025-02-15  Northeast      156,340.50   167,123.40  John Smith
1        2025-02-15  Central        203,450.25   218,127.77  Jane Doe
...
847      2025-02-28  West           234,567.89   251,248.94  Bob Wilson

# Filtering: 11,230 rows → 850 rows (7%)
```

##### Section 5: **PARSE NUMERIC VALUES** ★★★ (Lines 165-173)

```python
# Pre-aggregate budget and prior by Region for quick lookups
def sum_numeric(df_section, col):
    # Robustly parse numeric columns (may have thousands separators or quoted strings)
    if col not in df_section.columns:
        return pd.Series(dtype=float)
    
    tmp = df_section[['Region', col]].copy()
    
    # Normalize values: remove non-breaking spaces, regular spaces, commas
    tmp[col] = tmp[col].astype(str).str.replace('\u00a0', '', regex=False) \
                                    .str.replace(' ', '', regex=False) \
                                    .str.replace(',', '', regex=False)
    
    # Replace empty-like strings with '0'
    tmp[col] = tmp[col].replace({'nan': '', 'None': '', '': '0'})
    
    # Convert to numeric
    tmp[col] = pd.to_numeric(tmp[col], errors='coerce').fillna(0.0)
    
    # Group by Region and sum
    grouped = tmp.groupby('Region')[col].sum()
    return grouped

# Apply aggregation to prior data
self.prior_region_kusd = sum_numeric(self.prior_month, 'Value_kUSD')  # ← PY AGGREGATION
self.prior_region_keur = sum_numeric(self.prior_month, 'Value_kEUR')  # ← PY AGGREGATION
```

**TRANSFORMATION 2: NUMERIC CONVERSION & AGGREGATION**

**Input Data:**
- `self.prior_month` (850 rows)
- Columns: Region (str), Value_kUSD (str like "156,340.50"), Value_kEUR (str like "156340")

**Processing:**
1. Extract Region and target value column
2. Strip whitespace and thousands separators (comma, space, \u00a0)
3. Convert string → float (NaN/errors → 0)
4. Groupby Region and sum all values
5. Create indexed Series (dict-like)

**Output Data:**
- `self.prior_region_kusd` (Series, 4-8 regions)
- `self.prior_region_keur` (Series, 4-8 regions)
- Values now float64 in thousands (e.g., 156340.50 = 156,340.50 kUSD)

**Example Output:**
```python
# self.prior_region_kusd
Region
Northeast          156340.50
Central            203450.25
Southeast          189230.10
West               234567.89
Name: Value_kUSD, dtype: float64

# This becomes lookup dict-like structure:
{
    'Northeast': 156340.50,
    'Central': 203450.25,
    'Southeast': 189230.10,
    'West': 234567.89
}
```

---

### File: `src/gvl_report.py`

**Class:** `GVLReportGenerator(BaseReportGenerator)`

#### Method: `_prepare_data` (Lines 40-117)

**Responsibilities:**
1. Filter sales to AR (Accounts Receivable)
2. Convert sales to kEUR
3. Load and apply employee mappings
4. **Filter prior to current month**
5. Clean prior employee names for matching

##### Section 1: Sales Filter (Lines 47-49)
```python
# Filter Sales to AR
self.df = self.df[self.df['Document Type'] == 'AR'].copy()

# Convert Sales to kEUR
value_col = 'Value_in_EUR_converted' if 'Value_in_EUR_converted' in self.df.columns else 'Total Value (EUR)'
self.df['kEUR'] = self.df[value_col].fillna(0) / 1000
```

**Impact:** Does NOT touch prior_df

##### Section 2: **CLEAN PRIOR EMPLOYEE NAMES** ★★ (Lines 52-53)

```python
# Clean Sales Employee in budget and prior
self.budget_df['Sales_Employee_Cleaned'] = self.budget_df['Sales Employee / Account'].fillna('').str.strip()
self.prior_df['Sales_Employee_Cleaned'] = self.prior_df['Sales Employee / Account'].fillna('').str.strip()
```

**TRANSFORMATION: TEXT CLEANUP**

**Input Data:**
- `self.prior_df['Sales Employee / Account']` (object/string with potential whitespace)
- Example: "  John Smith  ", "Jane Doe", etc.

**Processing:**
1. Fill NaN with empty string
2. Strip leading/trailing whitespace

**Output Data:**
- `self.prior_df['Sales_Employee_Cleaned']` (new column)
- Example: "John Smith", "Jane Doe", etc.

##### Section 3: **FILTER PRIOR FOR MONTH** ★★★ (Lines 110-112)

```python
# Filter Prior for Same Month Last Year
# Prior Date is DD/MM/YYYY
self.prior_df['Date'] = pd.to_datetime(self.prior_df['Date'], format='%d/%m/%Y')
self.prior_month = self.prior_df[
    (self.prior_df['Date'].dt.year == self.prior_year) & 
    (self.prior_df['Date'].dt.month == self.current_month)
].copy()
```

**TRANSFORMATION: DATE PARSING & FILTERING**

**Input Data:**
- `self.prior_df` (11,230 rows)
- Column `Date` as string: "02/15/2025"
- `self.prior_year = 2025`, `self.current_month = 2`

**Processing:**
1. Parse Date string → datetime64
2. Filter WHERE year == 2025 AND month == 2

**Output Data:**
- `self.prior_month` (≈850 rows)
- Contains only Feb 2025 data

##### Section 4: **EMPLOYEE MAPPING** ★★ (Lines 60-82)

```python
# Map Sales Employees to Region using entity mappings
repo_root = Path(__file__).parent.parent
mapping_path = repo_root / 'data/inputs/mappings/entity_mappings.csv'
self.employee_region_map = {}

if mapping_path.exists():
    try:
        mapping_df = pd.read_csv(mapping_path)
        mapping_df['Sales_Employee'] = mapping_df['Sales_Employee'].fillna('').str.strip()
        mapping_df['Sales_Employee_Cleaned'] = mapping_df['Sales_Employee_Cleaned'].fillna('').str.strip()
        mapping_df['Region'] = mapping_df['Region'].fillna('').str.strip()
        
        # Create lookup dictionary from mapping
        cleaned_map = mapping_df[mapping_df['Sales_Employee_Cleaned'] != ''].drop_duplicates(subset=['Sales_Employee_Cleaned'])
        raw_map = mapping_df[mapping_df['Sales_Employee'] != ''].drop_duplicates(subset=['Sales_Employee'])
        
        self.employee_region_map.update(dict(zip(cleaned_map['Sales_Employee_Cleaned'], cleaned_map['Region'])))
        self.employee_region_map.update({
            k: v for k, v in dict(zip(raw_map['Sales_Employee'], raw_map['Region'])).items()
            if k not in self.employee_region_map
        })
        
        # Apply mapping to prior data ★
        self.prior_df['Region'] = self.prior_df['Sales_Employee_Cleaned'].map(self.employee_region_map).fillna('')
```

**TRANSFORMATION: APPLY ENTITY MAPPING TO PRIOR**

**Input Data:**
- `self.employee_region_map` (dict from entity_mappings.csv)
- Example: {'John Smith': 'Northeast', 'Jane Doe': 'Central', ...}
- `self.prior_df['Sales_Employee_Cleaned']` (cleaned employee names)

**Processing:**
1. Look up each employee name in mapping dict
2. Replace with mapped Region value
3. Fill unmapped (NaN) with empty string

**Output Data:**
- `self.prior_df['Region']` (updated column)
- Now contains regional segmentation from mappings

---

### File: `src/core_market_report.py`

**Class:** `CoreMarketReportGenerator(BaseReportGenerator)`

#### Method: `_prepare_data` (Lines 45-155)

**Responsibilities:**
1. Filter sales to AR and CN (Credit Notes)
2. Tag new customer sales
3. Clean mapping columns in budget and prior
4. **Filter prior to current month**
5. Set up sub-region mapping

##### Section 1: **FILTER AND CLEAN PRIOR** ★★★ (Lines 90-118)

```python
# Clean Sub_Region in budget and prior - handle both column name variants
if 'Sub Region' in self.prior_df.columns:
    self.prior_df['Sub_Region_Cleaned'] = self.prior_df['Sub Region'].fillna('').str.strip()
elif 'Subchannel / Partner' in self.prior_df.columns:
    self.prior_df['Sub_Region_Cleaned'] = self.prior_df['Subchannel / Partner'].fillna('').str.strip()
else:
    self.prior_df['Sub_Region_Cleaned'] = ''

# Filter Prior for Same Month Last Year
# Prior Date is DD/MM/YYYY
self.prior_df['Date'] = pd.to_datetime(self.prior_df['Date'], format='%d/%m/%Y')
self.prior_month = self.prior_df[
    (self.prior_df['Date'].dt.year == self.prior_year) & 
    (self.prior_df['Date'].dt.month == self.current_month)
].copy()

# Ensure prior numeric columns are parsed correctly
if 'Value_kEUR' in self.prior_month.columns:
    self.prior_month['Value_kEUR'] = pd.to_numeric(
        self.prior_month['Value_kEUR'], 
        errors='coerce'
    ).fillna(0)
```

**TRANSFORMATIONS:**

1. **Sub-Region Cleaning:**
   - Input: `Self.prior_df['Sub Region']` (string with potential whitespace)
   - Processing: fillna('') → str.strip()
   - Output: `self.prior_df['Sub_Region_Cleaned']` (clean string)

2. **Date Parsing & Filtering:**
   - Input: `self.prior_df['Date']` (string "02/15/2025")
   - Processing: pd.to_datetime(..., format='%d/%m/%Y') → filter by year/month
   - Output: `self.prior_month` (≈850 rows, Feb 2025 only)

3. **Numeric Parsing:**
   - Input: `self.prior_month['Value_kEUR']` (object/string)
   - Processing: pd.to_numeric(..., errors='coerce').fillna(0)
   - Output: `self.prior_month['Value_kEUR']` (float64)

---

### File: `src/receivables_report_generator.py`

**Class:** `ManagementReportGenerator(BaseReportGenerator)`

#### Method: `_prepare_data` (Lines 58-120)

**Responsibilities:**
1. Load USA-specific prior file if available (overwrites prior_df)
2. Apply date filtering via base class method
3. Filter monthly data
4. Set up for multi-segment reporting

##### Section 1: **LOAD OR USE STANDARD PRIOR** ★★ (Lines 70-78)

```python
# Load USA-specific prior if available, otherwise use provided prior
usa_prior_path = repo_root / f'data/inputs/prior_years/prior_sales_{self.prior_year}_usa.csv'
if usa_prior_path.exists():
    try:
        self.usa_prior_df = pd.read_csv(usa_prior_path)
        logging.info(f"[DEBUG] Loaded USA-specific prior file: {usa_prior_path.name}")
    except Exception as e:
        logging.warning(f"[DEBUG] Failed to load USA prior file: {e}")
        self.usa_prior_df = None
else:
    self.usa_prior_df = None
```

**TRANSFORMATION: LOAD SPECIALIZED PRIOR**

**Input Data:**
- File path: `data/inputs/prior_years/prior_sales_2025_usa.csv`
- OR None if file doesn't exist

**Processing:**
1. Check if market-specific prior file exists
2. If yes, read CSV into separate DataFrame
3. If no, set to None (use standard prior_df instead)

**Output Data:**
- `self.usa_prior_df` (DataFrame or None)
- Used for USA-specific sections of management report

##### Section 2: **FILTER PRIOR** ★★★ (Lines 104-105)

```python
# Filter Budget and Prior for Current Month using base class methods
self.budget_month = self._filter_budget_for_month()
self.prior_month = self._filter_prior_for_month()
```

**Calls Base Class Method:** `BaseReportGenerator._filter_prior_for_month()`

---

## Step 4: Report Calculation

### File: `src/usa_spa_report.py`

#### Method: `calculate_report` (Lines 200-350)

**Responsibilities:**
1. Loop through config sections and items
2. For each item, retrieve actual, budget, and prior values
3. Calculate YoY metrics
4. Build report data structure

##### Key Calculation Section (Lines 265-300)

```python
def calculate_report(self):
    report_data = []
    section_totals = {}
    grand_total = {'actual': 0, 'budget': 0, 'prior': 0, 'diff_budget': 0, 
                   'pct_budget': 0, 'diff_prior': 0, 'pct_prior': 0}
    
    for section in self.config['sections']:
        # ... handle special sections ...
        
        # Regular Section
        if 'items' in section:
            # Section with items (regions)
            for item in section['items']:
                label = item['label']                    # e.g., "Northeast"
                filter_val = item.get('filter_value')    # Region value
                
                if filter_val:
                    # Get ACTUAL sales
                    s_mask = (self.df['Region'] == filter_val)
                    val_actual = self.df[s_mask]['kVAL'].sum()
                    
                    # Get PRIOR value from pre-computed lookup
                    val_prior = 0
                    if filter_val in self.prior_region_kusd.index and self.prior_region_kusd.get(filter_val, 0) != 0:
                        val_prior = float(self.prior_region_kusd.get(filter_val, 0))  # ← USES PRIOR LOOKUP
                    elif filter_val in self.prior_region_keur.index and self.prior_region_keur.get(filter_val, 0) != 0:
                        val_prior = float(self.prior_region_keur.get(filter_val, 0))  # ← USES PRIOR LOOKUP
                    
                    # CALCULATE YOY METRICS ★★★
                    val_diff_prior = val_actual - val_prior
                    val_pct_prior = (val_actual / val_prior * 100) - 100 if val_prior != 0 else 0
                    
                    # Build row
                    rows.append({
                        'label': label,
                        'actual': val_actual,
                        'budget': val_budget,
                        'prior': val_prior,
                        'diff_budget': val_diff_budget,
                        'pct_budget': val_pct_budget,
                        'diff_prior': val_diff_prior,    # ← YOY DIFFERENCE
                        'pct_prior': val_pct_prior,      # ← YOY PERCENTAGE
                        'is_total': False,
                        'is_spacer': False
                    })
```

**TRANSFORMATION: CREATE YOY METRICS**

**Input Data:**
- `val_actual` (float): Current sales sum for region
- `val_prior` (float): Prior year sum from `prior_region_kusd` or `prior_region_keur`

**Processing:**
1. Calculate absolute difference: `val_actual - val_prior`
2. Calculate percentage: `(val_actual / val_prior * 100) - 100` IF prior != 0 ELSE 0
3. Build row dict with both metrics

**Output Data:**
- Row dict with keys: 'actual', 'prior', 'diff_prior', 'pct_prior', etc.
- Ready for formatting and export

---

### File: `src/gvl_report.py`

#### Method: `calculate_report` (Lines 150-250)

**Responsibilities:**
1. Loop through employees in config
2. Get actual sales, budget, and prior for each employee
3. Calculate metrics
4. Build report data

##### Key Calculation Section (Lines 170-210)

```python
def calculate_report(self):
    report_data = []
    section_totals = {}
    grand_total = {'Sales': 0, 'Budget': 0, 'Prior': 0}
    
    for section in self.config['sections']:
        # ... handle special sections ...
        
        # Regular Section
        if 'items' in section:
            # Section with items (sales employees)
            for item in section['items']:
                label = item['label']                    # e.g., "John Smith"
                filter_val = item.get('filter_value')    # Employee name
                
                # Get ACTUAL sales
                s_mask = (self.df['Sales_Employee_Cleaned'] == filter_val)
                sales = self.df[s_mask]['kEUR'].sum()
                
                # Get BUDGET and PRIOR
                budget = self._get_budget_value(filter_val)
                prior = self._get_prior_value(filter_val)  # ← CALLS PRIOR LOOKUP METHOD
                
                # Add to section
                report_data.append({
                    'label': label,
                    'sales': sales,
                    'budget': budget,
                    'prior': prior,  # ← PRIOR FROM LOOKUP
                    'is_total': False,
                    'is_spacer': False
                })
```

#### Method: `_get_prior_value` (Lines 145-152)

```python
def _get_prior_value(self, salesperson):
    """Get prior year value for a salesperson for the same month."""
    if salesperson in self.prior_month['Sales_Employee_Cleaned'].values:
        prior_row = self.prior_month[self.prior_month['Sales_Employee_Cleaned'] == salesperson]
        return prior_row['Value_kEUR'].iloc[0] if not prior_row.empty else 0
    return 0
```

**TRANSFORMATION: EMPLOYEE LOOKUP FROM PRIOR**

**Input Data:**
- `salesperson` (str): Employee name to look up
- `self.prior_month` (DataFrame): Filtered Feb 2025 prior data

**Processing:**
1. Check if employee exists in prior_month
2. Filter prior_month rows for that employee
3. Extract Value_kEUR from first matching row
4. Return value or 0 if not found

**Output Data:**
- Prior year value for employee (float)
- Example: 45.67 kEUR

---

## Step 5: Report Rendering & Export

### File: `src/base_report_generator.py`

#### Methods: Formatting Functions

**Method:** `format_number` (Lines 208-228)

```python
def format_number(self, value: float, zero_placeholder: str = "-") -> str:
    """
    Format a numeric value for display.
    
    Args:
        value: Number to format
        zero_placeholder: String to display for zero values (default "-")
    
    Returns:
        Formatted string (rounded integer or placeholder)
    """
    if abs(value) >= 0.5:
        return f"{int(round(value))}"
    elif value == 0:
        return zero_placeholder
    else:
        return "0"
```

**Method:** `format_percentage` (Lines 230-253)

```python
def format_percentage(self, numerator: float, denominator: float, 
                     zero_placeholder: str = "-") -> str:
    """
    Format a percentage value for display.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value (often prior value)
        zero_placeholder: String to display when denominator is zero
    
    Returns:
        Formatted percentage string like "12.5%" or placeholder
    """
    if denominator and denominator != 0:
        pct = (numerator / denominator * 100) - 100
        return f"{pct:.1f}%"
    return zero_placeholder
```

**Purpose:** Format prior-derived metrics for display

---

### File: `src/usa_spa_report.py`

#### Method: `render_report` (Lines 310-400)

**Purpose:** Display report to console with formatted output

**Input Data:**
- Report data structure with metrics including 'prior', 'diff_prior', 'pct_prior'

**Processing:**
1. Loop through report rows
2. Format each numeric value using format_number()
3. Format percentages using format_percentage()
4. Print to console with aligned columns

#### Method: Export to CSV (inherited pattern)

```python
def to_csv(self, filepath: str):
    report_df = pd.DataFrame(self.report_data)
    report_df.to_csv(filepath, index=False)
```

**Prior Data in Output:**
- Column: 'prior' (float formatted as integer)
- Column: 'diff_prior' (float formatted as integer)
- Column: 'pct_prior' (formatted as "±X.X%")

---

## Step 6: Data Dependencies & Import Chain

### Primary Entry Point: `src/full_report.py`

#### Function: `main` (Lines 19-350)

```python
def main():
    # ...
    current_year = get_current_year()    # 2026
    prior_year = get_prior_year()        # 2025
    
    # Build file paths dynamically
    budget_path = str(project_root / f'data/inputs/budget/budget_{current_year}_processed.csv')
    prior_path = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv')
    gvl_prior_path = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv')
    usa_spa_prior_path = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_usa.csv')
    
    # ... download or use local files ...
    
    # Generate reports
    gvl_gen = GVLReportGenerator(
        project_root / 'src/config/gvl_report_structure.json',
        mapped_path,
        budget_path,
        prior_path  # ← PASS PRIOR FILE PATH
    )
    
    usa_spa_gen = USASpaReportGenerator(
        project_root / 'src/config/usa_spa_report_structure.json',
        mapped_path,
        budget_path,
        usa_spa_prior_path  # ← PASS MARKET-SPECIFIC PRIOR
    )
    
    # ... more reports ...
```

**Prior File Paths Passed:**
1. `prior_sales_2025_processed.csv` → Standard reports (GVL, Core Market)
2. `prior_sales_2025_usa.csv` → USA Spa Report
3. `prior_sales_2025_gvl.csv` → Optional GVL-specific (loaded in method override)

---

## File Touch Summary Table

| Step | File | Method | Line(s) | Operation | Input | Output |
|------|------|--------|---------|-----------|-------|--------|
| 1 | base_report_generator.py | __init__ | 27-57 | Accept prior_path parameter | File path | None |
| 1 | base_report_generator.py | _load_data_files | 71-102 | Read CSV → DataFrame | CSV file | self.prior_df |
| 2 | base_report_generator.py | _prepare_dates | 104-116 | Calculate prior_year, current_month | System date | Instance vars |
| 2 | utils.py | get_prior_year | 50-60 | Calculate prior year | System date | 2025 |
| 2 | utils.py | get_current_month | 28-38 | Get current month | System date | 2 |
| 3 | usa_spa_report.py | _prepare_data | 140-151 | Parse date, filter month | self.prior_df | self.prior_month |
| 3 | usa_spa_report.py | _prepare_data | 165-173 | Parse numeric, aggregate by region | self.prior_month | prior_region_kusd |
| 3 | gvl_report.py | _prepare_data | 52-53 | Clean employee names | self.prior_df | self.prior_df['Sales_Employee_Cleaned'] |
| 3 | gvl_report.py | _prepare_data | 110-112 | Filter month, parse date | self.prior_df | self.prior_month |
| 3 | gvl_report.py | _prepare_data | 60-82 | Apply employee mapping | self.prior_df | self.prior_df['Region'] |
| 3 | core_market_report.py | _prepare_data | 90-118 | Clean sub-region, filter month, parse numeric | self.prior_df | self.prior_month |
| 3 | receivables_report_generator.py | _prepare_data | 70-78 | Load USA-specific prior | CSV file | self.usa_prior_df |
| 3 | receivables_report_generator.py | _prepare_data | 104-105 | Filter month (base class) | self.prior_df | self.prior_month |
| 4 | usa_spa_report.py | calculate_report | 265-300 | Lookup prior, calculate YoY | prior_region_kusd | Row dict |
| 4 | gvl_report.py | calculate_report | 170-210 | Call _get_prior_value() | self.prior_month | Prior value |
| 4 | gvl_report.py | _get_prior_value | 145-152 | Employee lookup in prior | self.prior_month | Prior value or 0 |
| 5 | base_report_generator.py | format_percentage | 230-253 | Format prior-derived % | pct_prior | "±X.X%" string |
| 5 | usa_spa_report.py | render_report | 310-400 | Display to console | Report data | Console output |
| 6 | usa_spa_report.py | export_* | Various | Export to CSV/PDF/HTML | Report data | File output |

---

## Data Transformation Timeline

```
T0: Load from Disk
├─ prior_sales_2025_processed.csv
├─ 11,230 rows
├─ Columns: object dtype (strings)
└─ Example Date: "02/15/2025"

T1: In-Memory DataFrame
├─ self.prior_df
├─ 11,230 rows, 12 columns
├─ Columns: Date (str), Region (str), Value_kEUR (str), etc.
└─ Memory: ~5-10 MB

T2: Filter by Month
├─ self.prior_month
├─ 850 rows (7% of original)
├─ Columns: Date (datetime64), Region (str), Value_kEUR (str), etc.
└─ Memory: ~400 KB

T3: Parse Numeric
├─ self.prior_month
├─ 850 rows, columns updated
├─ Value_kEUR: object → float64
├─ Values: "156,340.50" → 156340.5
└─ Memory: ~400 KB (same)

T4: Aggregate by Region (USA Spa)
├─ self.prior_region_kusd (Series/Dict)
├─ 4-8 entries (one per region)
├─ Index: Region names (str)
├─ Values: Sum of Value_kUSD per region (float64)
└─ Memory: <1 KB

T5: Calculate YoY Metrics
├─ Report rows with prior metrics
├─ diff_prior = actual - prior (float)
├─ pct_prior = (actual/prior*100)-100 (float)
└─ Memory: ~1-10 KB per report

T6: Format for Display
├─ str representations
├─ Numbers: "156340" (formatted integer)
├─ Percentages: "-7.1%" (formatted string)
└─ Memory: ~1-10 KB

T7: Export to File
├─ CSV: data/outputs/report.csv
├─ PDF: data/outputs/report.pdf
├─ HTML: data/outputs/report.html
└─ File size: 10-100 KB each
```

---

## Key Metrics Summary

### **Data Volume**
- **Input:** 11,230 rows (full year prior data)
- **Filtered:** ~850 rows (single month)
- **Aggregated:** 4-8 regions or 20-30 employees
- **Size Reduction:** 7% for time filter, then <1% for aggregation

### **Transformation Types**
1. **Date Parsing:** String (DD/MM/YYYY) → datetime64
2. **Numeric Conversion:** String with separators → float64
3. **Text Cleaning:** Strip whitespace, normalize names
4. **Filtering:** 11,230 → 850 rows (by year+month)
5. **Aggregation:** 850 → 4-30 groups (by region/employee)
6. **Calculation:** Numeric diff and percentage
7. **Formatting:** Float → String (number, percentage)

### **Files Modified**
- `self.prior_df`: 3 new columns (Date, Region, Sales_Employee_Cleaned)
- `self.prior_month`: All columns (filtered subset)
- `prior_region_kusd`: New Series (aggregated)
- Report rows: dicts with prior metrics

---

## Tracing a Single PY Data Point

**Example:** Northeast region, February 2025, Value_kUSD = 156,340.50

### Path Through System:

1. **CSV File:** `data/inputs/prior_years/prior_sales_2025_usa.csv` (row 42)
   - Date: "02/15/2025"
   - Region: "Northeast"
   - Value_kUSD: "156,340.50"

2. **Load:** `base_report_generator.py` L85
   - Read CSV with pd.read_csv()
   - Store in `self.prior_df[41]` (row 42)
   - All columns object dtype

3. **Filter Month:** `usa_spa_report.py` L146
   - Parse Date: "02/15/2025" → datetime(2025, 2, 15)
   - Filter: year==2025? YES, month==2? YES → Keep row
   - Result: Row retained in `self.prior_month`

4. **Format & Convert:** `usa_spa_report.py` L170
   - Parse Value_kUSD: "156,340.50" (remove ",") → 156340.5
   - Convert str → float64
   - Result: `self.prior_month['Value_kUSD'][41] = 156340.5`

5. **Aggregate:** `usa_spa_report.py` L172
   - Groupby Region, sum all Value_kUSD
   - All "Northeast" rows: 156340.5 + ... other Northeast values
   - Result: `self.prior_region_kusd['Northeast'] = 156340.5±others = TOTAL`

6. **Report Calculation:** `usa_spa_report.py` L283
   - val_prior = `self.prior_region_kusd.get('Northeast', 0)`
   - val_prior = 156340.5 (plus sum of any other Northeast rows)
   - Lookup: Northeast region matches in config
   - Result: prior value retrieved

7. **YoY Metrics:** `usa_spa_report.py` L298-299
   - val_actual = 145,230 (current sales for Northeast Feb 2026)
   - val_prior = 156,340 (prior sales for Northeast Feb 2025)
   - diff_prior = 145,230 - 156,340 = -11,110
   - pct_prior = (145,230 / 156,340 * 100) - 100 = -7.1%

8. **Format:** `usa_spa_report.py` L304 (format_row_for_export)
   - diff_prior: -11110 (float) → "-11110" (str)
   - pct_prior: -7.1 (float) → "-7.1%" (str)

9. **Render:** `usa_spa_report.py` L310 (render_report)
   - Display: "Northeast | 145,230 | 152,000 | -6,770 | -4.5% | 156,340 | -7.1%"

10. **Export:** `usa_spa_report.py` (export methods)
    - CSV: "Northeast,145230,152000,-6770,-4.5%,156340,-7.1%"
    - PDF/HTML: Styled table with same values

---

**Document Version:** 1.0  
**Created:** February 27, 2026  
**File Count:** 5 core files + utils
**Total Lines of Code:** ~3,200 (directly touching PY data)
