# Salesperson-to-Region Mapping Research
## sales_report_v2_independent Project

---

## 1. Entity Mappings CSV Structure

### File Location
**Primary:** `data/inputs/mappings/entity_mappings.csv`
**Backup:** `../inputs_backup/entity_mappings.csv` (in parent directory)
**SharePoint Path:** `/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/entity_mappings.csv`

### CSV Column Definition
The mapping file contains 10 columns:

| Column | Type | Purpose | Example |
|--------|------|---------|---------|
| `Entity` | String | Company entity type | `Inc.`, `GmbH`, `AG`, `Descomed`, `Export` |
| `Market_Group` | String | Market/country grouping | `USA`, `Core Markets`, `UK`, `Export` |
| `Region` | String | **Region for salesperson assignment** | `Northeast`, `West`, `Germany`, `Switzerland` |
| `Sub Region` | String | Sub-region detail (optional) | `North East`, `NL Central`, `Bayern` |
| `Channel_Level` | String | Sales channel | `Spa`, `Retail`, `eCommerce EU (incl. UK)`, `Interco` |
| `Company_Group` | String | Internal company grouping | `Company 1`, `Company 2`, `Company 3` |
| `Sales_Employee` | String | **Raw salesperson name (lookup key #1)** | `John Doe`, `A. Gutierrez`, `Amy` |
| `Customer_Code` | String | Customer code (if applicable) | `25000`, `51206` |
| `Customer_Name` | String | Customer name | `West Coast Lifestyles Inc.; CA`, `Harrods (UK) Ltd.` |
| `Sales_Employee_Cleaned` | String | **Cleaned salesperson name (lookup key #2)** | `John Doe`, `Aracelli`, `Amy` |

#### Key Notes:
- **Sales_Employee**: Original/raw employee name format from source systems
- **Sales_Employee_Cleaned**: Normalized/standardized employee name (often a first name or simplified name)
- **Region**: The geographic or regional assignment for the salesperson
- **Dual keys exist** because employee names come from different source systems in different formats

---

## 2. Where employee_region_map is Created and Built

### 2.1 GVL Report (`gvl_report.py`, lines 70-104)

**Location:** [gvl_report.py](gvl_report.py#L70-L104)

#### Code:
```python
# Lines 70-104
# Map Sales Employees to Region using entity mappings (for roll-up alignment)
repo_root = Path(__file__).parent.parent
mapping_path = repo_root / 'data/inputs/mappings/entity_mappings.csv'
self.employee_region_map = {}
if mapping_path.exists():
    try:
        mapping_df = pd.read_csv(mapping_path)
        mapping_df['Sales_Employee'] = mapping_df['Sales_Employee'].fillna('').str.strip()
        mapping_df['Sales_Employee_Cleaned'] = mapping_df['Sales_Employee_Cleaned'].fillna('').str.strip()
        mapping_df['Region'] = mapping_df['Region'].fillna('').str.strip()

        # Step 1: Build cleaned employee map (deduplicated by Sales_Employee_Cleaned)
        cleaned_map = mapping_df[mapping_df['Sales_Employee_Cleaned'] != ''].drop_duplicates(subset=['Sales_Employee_Cleaned'])
        
        # Step 2: Build raw employee map (deduplicated by Sales_Employee)
        raw_map = mapping_df[mapping_df['Sales_Employee'] != ''].drop_duplicates(subset=['Sales_Employee'])

        # Step 3: Update dictionary - cleaned keys first, then raw keys (if not already present)
        self.employee_region_map.update(dict(zip(cleaned_map['Sales_Employee_Cleaned'], cleaned_map['Region'])))
        self.employee_region_map.update({
            k: v for k, v in dict(zip(raw_map['Sales_Employee'], raw_map['Region'])).items()
            if k not in self.employee_region_map
        })

        # Step 4: Apply mapping to budget and prior dataframes
        self.budget_df['Region'] = self.budget_df['Sales_Employee_Cleaned'].map(self.employee_region_map).fillna('')
        self.prior_df['Region'] = self.prior_df['Sales_Employee_Cleaned'].map(self.employee_region_map).fillna('')
    except Exception as e:
        logging.warning(f"Could not apply employee-region mapping: {e}")
else:
    logging.warning(f"Mapping file not found: {mapping_path}")
```

#### Build Process:
1. **Load CSV**: Read entity_mappings.csv from `data/inputs/mappings/`
2. **Data Cleaning**: Strip whitespace from string columns
3. **Create two dictionaries**: 
   - **cleaned_map**: Deduplicated on `Sales_Employee_Cleaned` (unique identifier)
   - **raw_map**: Deduplicated on `Sales_Employee` (original format)
4. **Priority merging**: Cleaned keys take priority, raw keys fill gaps
5. **Execute mappings**: Apply the dictionary to `budget_df` and `prior_df` using `.map()` method
6. **Fallback handling**: Unmapped employees get empty string `''`

#### Used in:
- Budget dataframe row filtering: `self.budget_df['Sales_Employee_Cleaned'].map(self.employee_region_map)`
- Prior year dataframe row filtering: `self.prior_df['Sales_Employee_Cleaned'].map(self.employee_region_map)`

---

### 2.2 USA Spa Report (`usa_spa_report.py`, lines 150-194)

**Location:** [usa_spa_report.py](usa_spa_report.py#L150-L194)

#### Code:
```python
# Lines 153-173: Region-level aggregation (NOT employee-level mapping)
# Pre-aggregate budget and prior by Region for quick lookups (support both kUSD and kEUR)
def sum_numeric(df_section, col):
    # Robustly parse numeric columns
    tmp = df_section[['Region', col]].copy()
    # Normalize values and convert to numeric
    tmp[col] = pd.to_numeric(...).fillna(0.0)
    # Group by Region and sum
    grouped = tmp.groupby('Region')[col].sum()
    return grouped

self.budget_region_kusd = sum_numeric(self.budget_month, 'Value_kUSD')
self.budget_region_keur = sum_numeric(self.budget_month, 'Value_kEUR')
self.prior_region_kusd = sum_numeric(self.prior_month, 'Value_kUSD')
self.prior_region_keur = sum_numeric(self.prior_month, 'Value_kEUR')
```

#### Build Process:
1. **Budget is NOT individually mapped to regions** in USA Spa report
2. Instead, **pre-aggregate by Region column** that must already exist in budget_df
3. The Region column comes from the mapping applied earlier (via qry_data_mapping.apply_mappings)
4. **Region lookup happens dynamically** during report calculation
5. **Lookup keys used**: `filter_val` from config (line 274): `self.df['Region'] == filter_val`

#### Note:
- USA Spa report does NOT build employee_region_map
- It assumes regions are already present in the data via earlier mapping
- It groups budget/prior data by existing Region column

---

### 2.3 Core Market Report (`core_market_report.py`)

Uses similar pattern to GVL - builds employee_region_map for Core Markets regions
**Location:** Lines ~570-600 (similar structure to GVL)

---

## 3. Multiple Examples of Sales_Employee → Region Mappings

### Example Set 1: USA Spa Regional Assignments
From entity_mappings.csv lines 49-75:

| Sales_Employee | Sales_Employee_Cleaned | Region | Channel_Level | Entity | Notes |
|---|---|---|---|---|---|
| `(blank)` | `Amy` | `West` | `Spa` | `Inc.` | Amy represents West region spas |
| `(blank)` | `Bridget` | `Northeast` | `Spa` | `Inc.` | Bridget represents Northeast spas |
| `(blank)` | `Lisa` | `Southeast` | `Spa` | `Inc.` | Lisa represents Southeast spas |
| `(blank)` | `Melissa` | (varies) | `Spa` | `Inc.` | Melissa handles special/other accounts |

**Real Example from CSV:**
```csv
Inc.,USA,West,,Spa,Company 1,,25000,West Coast Lifestyles Inc.; CA,Amy
Inc.,USA,Northeast,,Spa,Company 1,,25001,Four Seasons Hotel Philadelphia,Bridget
Inc.,USA,Southeast,,Spa,Company 1,,25002,Porto Vita/Villa Grande Club; FL,Lisa
Inc.,USA,West,,Spa,Company 1,,25004,Teton Mountain Lodge,Amy
Inc.,USA,West,,Spa,Company 1,,25009,Glo Rejuvenation; Oregon,Amy
```

### Example Set 2: Core Markets (Europe) Regional Assignments
From entity_mappings.csv lines 48-50, 205+:

| Sales_Employee | Sales_Employee_Cleaned | Region | Sub Region | Entity | Notes |
|---|---|---|---|---|---|
| `A. Gutierrez` | `Aracelli` | `Germany` | `North East` | `GmbH` | Aracelli covers North East Germany |
| `A. Gutierrez Neukd` | `Aracelli` | `Germany` | `North East` | `GmbH` | Same person, alternate format |
| `C. da Costa Campos` | `Benelux - Other` | `Other NL` | `NL Other` | `GmbH` | Costa covers Benelux Other |
| `K. Brunbauer` | `Kerstin` | `Germany` | `North` | `GmbH` | Kerstin covers North Germany |
| `M. Pfauch` | `Marina` | `Germany` | `NRW` | `GmbH` | Marina covers NRW region |

**From CSV:**
```csv
GmbH,Core Markets,Germany,North East,,Company 1,A. Gutierrez,,,Aracelli
GmbH,Core Markets,Germany,North East,,Company 1,A. Gutierrez Neukd,,,Aracelli
GmbH,Core Markets,Germany,North,,Company 1,K. Brunbauer,,,Kerstin
GmbH,Core Markets,Germany,NRW,,Company 1,M. Pfauch,,,Marina
GmbH,Core Markets,Germany,NRW,,Company 1,M. Pfauch Neukd,,,Marina
GmbH,Core Markets,Benelux,NL Central,,Company 1,M. Mijnheer NL,,,Marjelein
GmbH,Core Markets,Switzerland,German Switzerland,,Company 1,Ch. Rose,,,Christiane
```

### Example Set 3: Format Variations (Same Salesperson, Multiple Formats)
From entity_mappings.csv:

```csv
# Both formats of "Marina" map to the same region
GmbH,Core Markets,Germany,NRW,,Company 1,M. Pfauch,,,Marina
GmbH,Core Markets,Germany,NRW,,Company 1,M. Pfauch Neukd,,,Marina

# Both formats of "Kerstin" map to Germany North
GmbH,Core Markets,Germany,North,,Company 1,K. Brunbauer,,,Kerstin
GmbH,Core Markets,Germany,North,,Company 1,K. Brunbauer Neukd,,,Kerstin
```

**Purpose**: The "Neukd" (New Customer Discount?) format captures the same salesperson under alternative account structures

---

## 4. Lookup Keys Used: Sales_Employee vs Sales_Employee_Cleaned

### Key #1: Sales_Employee_Cleaned (PRIMARY)
**Pattern**: First name or simplified identifier

```python
# Line 98 in gvl_report.py - Primary lookup
self.budget_df['Region'] = self.budget_df['Sales_Employee_Cleaned'].map(self.employee_region_map)
```

**How it's used**:
- Budget dataframes have column: `Sales Employee / Account`
- Cleaned in gvl_report.py line 76:
  ```python
  self.budget_df['Sales_Employee_Cleaned'] = self.budget_df['Sales Employee / Account'].fillna('').str.strip()
  ```
- Mapped using `.map()` on the dictionary created from CSV
- Examples: `Amy`, `Bridget`, `Lisa`, `Kerstin`, `Marina`, `Aracelli`

### Key #2: Sales_Employee (FALLBACK)
**Pattern**: Full or partial name with potential account suffix

```python
# Lines 92-96 in gvl_report.py - Fallback for unmapped cleaned names
self.employee_region_map.update({
    k: v for k, v in dict(zip(raw_map['Sales_Employee'], raw_map['Region'])).items()
    if k not in self.employee_region_map  # Only if not already mapped by cleaned key
})
```

**How it's used**:
- When `Sales_Employee_Cleaned` lookup fails (returns NaN)
- Retry with original `Sales_Employee` format from mapping
- Examples: `A. Gutierrez`, `K. Brunbauer`, `M. Pfauch`, `Ch. Rose`

### Reconciliation in Code (qry_data_mapping.py, lines 111-138)

The core mapping module handles **both** customer and employee approaches:

```python
# Line 100-115: Employee mapping (for GmbH/AG entities)
if 'Sales_Employee' in mapping_df.columns:
    emp_cols = ['Sales_Employee', 'Market_Group', 'Region', ...]
    map_emp = mapping_df[emp_cols].dropna(subset=['Sales_Employee']).drop_duplicates(subset=['Sales_Employee'])
    
    sales_df['temp_employee'] = sales_df['Sales Employee Name']  # Source field
    sales_df.loc[~sales_df['Company Entity'].isin(['GmbH', 'AG']), 'temp_employee'] = pd.NA
    sales_df = sales_df.merge(map_emp, left_on='temp_employee', right_on='Sales_Employee', ...)

# Line 116-138: Customer mapping (for other entities)
if 'Customer_Name' in mapping_df.columns:
    cust_cols = ['Customer_Name', 'Market_Group', 'Region', ...]
    map_cust = mapping_df[cust_cols].dropna(subset=['Customer_Name']).drop_duplicates(subset=['Customer_Name'])
    ...
```

**Priority Logic**:
1. If GmbH/AG entity → try `Sales_Employee` match
2. If other entity → try `Customer_Name` match
3. If failed → try `Sales Employee Name` field
4. If still failed → log as unmapped

---

## 5. Error Handling & Fallbacks

### 5.1 GVL Report Error Handling

**File:** [gvl_report.py](gvl_report.py#L80-L104)

```python
# Lines 80-104
if mapping_path.exists():
    try:
        mapping_df = pd.read_csv(mapping_path)
        mapping_df['Sales_Employee'] = mapping_df['Sales_Employee'].fillna('').str.strip()
        mapping_df['Sales_Employee_Cleaned'] = mapping_df['Sales_Employee_Cleaned'].fillna('').str.strip()
        mapping_df['Region'] = mapping_df['Region'].fillna('').str.strip()

        # Dropna handling with deduplication
        cleaned_map = mapping_df[mapping_df['Sales_Employee_Cleaned'] != ''].drop_duplicates(...)
        raw_map = mapping_df[mapping_df['Sales_Employee'] != ''].drop_duplicates(...)
        
        # ... apply mappings ...
        
    except Exception as e:
        logging.warning(f"Could not apply employee-region mapping: {e}")
else:
    logging.warning(f"Mapping file not found: {mapping_path}")
```

#### Fallback Behaviors:
1. **File not found**: Log warning, continue with empty `employee_region_map = {}`
2. **Parsing error**: Log warning, continue without mapping
3. **Missing columns**: Won't prevent mapping, missing data becomes NaN
4. **Unmapped employee**: 
   - `.map()` returns NaN → filled with empty string: `.fillna('')`
   - No exception raised, silently becomes blank region

### 5.2 Data Cleaning (Null Handling)

**File:** [gvl_report.py](gvl_report.py#L74-L76)

```python
# Line 74-76
self.budget_df['Sales_Employee_Cleaned'] = self.budget_df['Sales Employee / Account'].fillna('').str.strip()
self.prior_df['Sales_Employee_Cleaned'] = self.prior_df['Sales Employee / Account'].fillna('').str.strip()
```

All null values are converted to empty strings **before** mapping

### 5.3 Unmapped Entity Tracking (qry_data_mapping.py)

**File:** [qry_data_mapping.py](qry_data_mapping.py#L70-L230)

```python
# Lines 102-130: Unmapped employee tracking
unmapped_emp = sales_df[sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
if not unmapped_emp.empty:
    logging.warning(f"Found {len(unmapped_emp)} unmapped employee records (GmbH/AG)")
    emp_stats = collect_unmapped_stats(unmapped_emp, 'employee', 'Sales Employee Name')
    unmapped_entities.update(emp_stats)

# Lines 133-180: Unmapped customer tracking  
unmapped_cust = sales_df[~sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
if not unmapped_cust.empty:
    logging.warning(f"Found {len(unmapped_cust)} unmapped customer records (non-GmbH/AG)")
    cust_stats = collect_unmapped_stats(unmapped_cust, 'customer', 'Customer Name')
    unmapped_entities.update(cust_stats)
```

#### Unmapped Entities Export:
```python
# Lines 208-215
unmapped_df = unmapped_df.sort_values(['entity_type', 'count'], ascending=[True, False])
timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
unmapped_path = output_dir / f"unmapped_entities_{timestamp}.csv"
unmapped_df.to_csv(unmapped_path, index=False)

logging.info(f"Exported {len(unmapped_records)} unmapped entities to {unmapped_path}")
```

**Exported fields**:
- `entity_type`: 'employee' or 'customer'
- `entity_name`: Actual name from source data
- `count`: Number of transactions
- `first_seen`: Earliest date in data
- `last_seen`: Latest date in data
- `customer_code`: (optional) Associated customer code

### 5.4 Query Integration & Alternative Paths

**File:** [qry_data_ingestion.py](qry_data_ingestion.py)

The complete pipeline includes:
1. **Sales data ingestion** from QRY files (process_qry_files)
2. **Mapping application** (apply_mappings) - adds Region, Market_Group, Channel_Level
3. **Report-specific cleanup** (in each report generator)

---

## 6. Configuration-Driven Region Lookups

### GVL Report Region Lookups
**File:** [gvl_report.json](src/config/gvl_report_structure.json)

```json
{
  "sections": [
    {
      "title": "Germany",
      "show_total": true,
      "items": [
        {"label": "Kerstin", "filter_value": "Kerstin"},
        {"label": "Marina", "filter_value": "Marina"},
        {"label": "Sibylle", "filter_value": "Sibylle"},
        ...
      ]
    }
  ]
}
```

**How used in report generation** (gvl_report.py line 196):
```python
s_mask = (self.df['Sales_Employee_Cleaned'] == filter_val)  # filter_val = "Kerstin"
val_sales = self.df[s_mask]['kEUR'].sum()
```

### USA Spa Report Region Lookups
**File:** [usa_spa_report.json](src/config/usa_spa_report_structure.json)

```json
{
  "sections": [
    {
      "title": "Northeast",
      "items": [
        {"label": "Northeast", "filter_value": "Northeast"}
      ]
    },
    {
      "title": "West",
      "items": [
        {"label": "West", "filter_value": "West"}
      ]
    }
  ]
}
```

**How used in report generation** (usa_spa_report.py line 274):
```python
s_mask = (self.df['Region'] == filter_val)  # filter_val = "Northeast"
val_actual = self.df[s_mask]['kVAL'].sum()
```

---

## 7. Summary Table: Mapping Flow Diagram

```
entity_mappings.csv (246 rows)
    ↓
[GVL Report]
    ├─ Load mapping_df from CSV
    ├─ Deduplicate on Sales_Employee_Cleaned
    ├─ Deduplicate on Sales_Employee 
    ├─ Build employee_region_map dictionary
    │   ├─ Key: Sales_Employee_Cleaned (e.g., "Kerstin")
    │   └─ Value: Region (e.g., "Germany")
    ├─ Apply to budget_df['Region'] 
    ├─ Apply to prior_df['Region']
    └─ Use in Section+Item→Region calculation
    
[USA Spa Report]
    ├─ Regions already in data (from qry_data_mapping)
    ├─ Pre-aggregate budget by Region
    ├─ Pre-aggregate prior by Region
    └─ Use Region from config (Northeast, West, etc.)

[Core Market Report]
    └─ Similar to GVL (employee→region mapping)

[Unmapped Tracking]
    └─ For any entity not found in mappings.csv
        → Export to unmapped_entities_{timestamp}.csv
```

---

## 8. Test Examples

**File:** [test_mapping.py](tests/test_mapping.py#L85-L125)

```python
def test_apply_mappings_known_employees(sample_mapping_df, sample_sales_df_employees, temp_output_dir):
    """Test that known employees are mapped correctly."""
    result = apply_mappings(sample_sales_df_employees.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Check that John Doe is mapped
    john_row = result[result['Sales Employee Name'] == 'John Doe'].iloc[0]
    assert john_row['Market_Group'] == 'Europe'
    assert john_row['Region'] == 'Germany'
    assert john_row['Channel_Level'] == 'Direct'
    
    # Check that Jane Smith is mapped
    jane_row = result[result['Sales Employee Name'] == 'Jane Smith'].iloc[0]
    assert jane_row['Market_Group'] == 'USA'
    assert jane_row['Region'] == 'USA-East'
    assert jane_row['Channel_Level'] == 'Spa'
```

---

## 9. Key Takeaways

| Aspect | Details |
|--------|---------|
| **Primary mapping file** | entity_mappings.csv (246 rows) |
| **Primary key (cleaned)** | `Sales_Employee_Cleaned` (first name/ID) |
| **Fallback key (raw)** | `Sales_Employee` (full format) |
| **Mapping output** | Region assignment (e.g., "Northeast", "Germany", "West") |
| **GVL report** | Builds employee_region_map, applies to budget/prior data |
| **USA Spa report** | Aggregates by existing Region column (from qry_data_mapping) |
| **Error handling** | Missing files logged, unmapped entities exported to CSV |
| **Deduplication** | One region per unique Sales_Employee_Cleaned value |
| **Data cleaning** | All string fields trimmed, nulls → empty strings |
| **Fallback values** | Unmapped employees → empty string Region (silently) |

