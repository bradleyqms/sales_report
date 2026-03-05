# Prior Year Data Flow - Detailed Architecture Diagrams
## sales_report_v2_independent

---

## 1. End-to-End Data Pipeline

```mermaid
graph TD
    Start["🚀 Report Generation Started<br/>current_year=2026<br/>prior_year=2025<br/>current_month=02"]
    
    subgraph Source["📂 DATA INGESTION"]
        PY_FILES["Prior Year Files<br/>data/inputs/prior_years/"]
        PY_2025["prior_sales_2025<br/>_processed.csv<br/>11,230 rows"]
        PY_USA["prior_sales_2025<br/>_usa.csv<br/>2,340 rows"]
        PY_GVL["prior_sales_2025<br/>_gvl.csv<br/>850 rows"]
        MAPPINGS["entity_mappings.csv<br/>Sales Employees<br/>Customers"]
    end
    
    subgraph Load["🔧 INITIALIZATION (BaseReportGenerator.__init__)"]
        LoadCSV["pd.read_csv()<br/>→ self.prior_df"]
        LoadConfig["Load config from JSON<br/>→ self.config"]
        CalcDates["Calculate Dates<br/>→ prior_year=2025<br/>→ current_month=02"]
    end
    
    subgraph Prepare["⚙️ DATA PREPARATION (Report._prepare_data())"]
        ParseDate["Parse Dates<br/>DD/MM/YYYY → datetime<br/>self.prior_df['Date']"]
        FilterMonth["Filter to Month<br/>Year=2025 AND Month=02<br/>→ self.prior_month"]
        ParseNumeric["Parse Numeric<br/>Value_kEUR: str → float<br/>fillna(0)"]
        Aggregate["Aggregate by Dimension<br/>Groupby Region/Employee<br/>→ Lookup Dictionary"]
    end
    
    subgraph Reports["📊 REPORT GENERATORS"]
        USA["USASpaReportGenerator<br/>Region aggregation<br/>4-8 regions"]
        GVL["GVLReportGenerator<br/>Employee aggregation<br/>20-30 employees"]
        CORE["CoreMarketReportGenerator<br/>Sub-region aggregation<br/>10-15 sub-regions"]
        MGMT["ManagementReportGenerator<br/>Multi-segment<br/>5-8 segments"]
    end
    
    subgraph Calc["🧮 YOY CALCULATIONS (calculate_report())"]
        GetActual["val_actual = df[filter]<br/>.kVAL.sum()"]
        GetPrior["val_prior = lookup_dict<br/>.get(region_key)"]
        CalcDiff["val_diff_prior =<br/>val_actual - val_prior"]
        CalcPct["val_pct_prior =<br/>val_actual/val_prior*100-100"]
        BuildRow["Build Report Row<br/>{label, actual,<br/>budget, prior,<br/>diff_prior, pct_prior}"]
    end
    
    subgraph Output["📈 REPORT OUTPUT (render_report())"]
        CSV["CSV Export<br/>data/outputs/"]
        PDF["PDF Export<br/>styled table"]
        HTML["HTML Export<br/>web format"]
        Console["Console Output<br/>formatted text"]
    end
    
    Start --> Source
    PY_FILES --> PY_2025
    PY_FILES --> PY_USA
    PY_FILES --> PY_GVL
    Source --> Load
    
    LoadCSV --> LoadConfig
    LoadConfig --> CalcDates
    
    Load --> Prepare
    ParseDate --> FilterMonth
    FilterMonth --> ParseNumeric
    ParseNumeric --> Aggregate
    
    Prepare --> Reports
    
    USA --> Calc
    GVL --> Calc
    CORE --> Calc
    MGMT --> Calc
    
    Calc --> GetActual
    GetActual --> GetPrior
    GetPrior --> CalcDiff
    CalcDiff --> CalcPct
    CalcPct --> BuildRow
    
    BuildRow --> Output
    Output --> CSV
    Output --> PDF
    Output --> HTML
    Output --> Console
    
    style Source fill:#e1f5ff,stroke:#01579b
    style Load fill:#fff3e0,stroke:#e65100
    style Prepare fill:#e8f5e9,stroke:#1b5e20
    style Reports fill:#f3e5f5,stroke:#4a148c
    style Calc fill:#fff9c4,stroke:#f57f17
    style Output fill:#c8e6c9,stroke:#1b5e20
```

---

## 2. Data Loading & Parsing Flow

```mermaid
graph LR
    A["CSV File<br/>prior_sales_2025<br/>_processed.csv<br/>11,230 rows"] 
    
    B1["Raw DataFrame<br/>Date: object<br/>Value_kEUR: object<br/>Region: object"]
    B2["memory: ~5-10 MB<br/>11,230 rows<br/>×12 columns"]
    
    C1["Parse Date<br/>pd.to_datetime<br/>format='%d/%m/%Y'"]
    C2["Date column<br/>datetime64[ns]"]
    
    D1["Filter Rows<br/>year == 2025<br/>month == 2"]
    D2["850 rows selected<br/>~7% of input"]
    
    E1["Parse Numeric<br/>str.replace(',', '')<br/>→ pd.to_numeric<br/>→ fillna(0)"]
    E2["Value_kEUR: float64<br/>No NaN values<br/>0 = missing data"]
    
    F1["Clean Text<br/>str.strip()<br/>Remove whitespace"]
    F2["Region: clean string<br/>Sales_Employee:<br/>standardized"]
    
    A -->|pd.read_csv| B1
    B1 --> B2
    
    B1 -->|Date transformation| C1
    C1 --> C2
    C2 ---|dt.year, dt.month| D1
    D1 --> D2
    
    B1 ---|Value_kEUR transformation| E1
    E1 --> E2
    
    B1 ---|Text cleanup| F1
    F1 --> F2
    
    D2 -->|Output DataFrame| G["self.prior_month<br/>850 rows, full columns<br/>Ready for aggregation"]
    E2 --> G
    F2 --> G
    
    style A fill:#e3f2fd
    style B1 fill:#fff3e0
    style B2 fill:#fff3e0
    style C1 fill:#e8f5e9
    style C2 fill:#c8e6c9
    style D1 fill:#c8e6c9
    style D2 fill:#a5d6a7
    style E1 fill:#fff9c4
    style E2 fill:#fdd835
    style F1 fill:#ffe0b2
    style F2 fill:#ffb74d
    style G fill:#81c784
```

---

## 3. Region-Level Aggregation (USA Spa Report)

```mermaid
graph TD
    A["self.prior_month<br/>850 rows<br/>Feb 2025 data"]
    
    B["Aggregation:<br/>groupby('Region').sum()<br/>Value_kUSD and Value_kEUR"]
    
    C1["Region: Northeast<br/>Value_kUSD: 156,340"]
    C2["Region: Central<br/>Value_kUSD: 203,450"]
    C3["Region: Southeast<br/>Value_kUSD: 189,230"]
    C4["Region: West<br/>Value_kUSD: 234,560"]
    
    D["self.prior_region_kusd<br/>Index dict<br/>{<br/>  'Northeast': 156.34,<br/>  'Central': 203.45,<br/>  'Southeast': 189.23,<br/>  'West': 234.56<br/>}"]
    
    E["Calculate Report<br/>for region in config:<br/>val_prior = prior_region_kusd.get(region, 0)"]
    
    F1["Region: Northeast<br/>actual: 145.23<br/>prior: 156.34<br/>diff: -11.11<br/>pct: -7.1%"]
    
    F2["Region: Central<br/>actual: 210.45<br/>prior: 203.45<br/>diff: +7.00<br/>pct: +3.4%"]
    
    A --> B
    B --> C1
    B --> C2
    B --> C3
    B --> C4
    
    C1 --> D
    C2 --> D
    C3 --> D
    C4 --> D
    
    D --> E
    E --> F1
    E --> F2
    
    style A fill:#e3f2fd
    style B fill:#fff9c4
    style C1 fill:#f1f8e9
    style C2 fill:#f1f8e9
    style C3 fill:#f1f8e9
    style C4 fill:#f1f8e9
    style D fill:#fff176
    style E fill:#ffccbc
    style F1 fill:#b2dfdb
    style F2 fill:#b2dfdb
```

---

## 4. Employee-Level Lookup (GVL Report)

```mermaid
graph TD
    A["self.prior_month<br/>850 rows<br/>Feb 2025"]
    
    B["Build Lookup Function:<br/>def _get_prior_value(emp_name)<br/>Filter by Sales_Employee_Cleaned"]
    
    C["Enter Report Loop"]
    
    D["Item: John Smith<br/>(filter_value)"]
    
    E["prior_mask = prior_month<br/>['Sales_Employee_Cleaned']<br/>== 'John Smith'"]
    
    F["Match Result:<br/>12 rows for John Smith<br/>Feb 2025"]
    
    G["prior_value = prior_month<br/>[prior_mask]<br/>['Value_kEUR'].iloc[0]<br/>= 45.67 kEUR"]
    
    H["Report Calculation:<br/>sales_actual: 48.23 kEUR<br/>prior: 45.67 kEUR<br/>diff: +2.56<br/>pct: +5.6%"]
    
    I["Row Output<br/>{label: 'John Smith',<br/>sales: 48.23,<br/>prior: 45.67}"]
    
    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    
    style A fill:#e3f2fd
    style B fill:#fff9c4
    style C fill:#ffe0b2
    style D fill:#f3e5f5
    style E fill:#f3e5f5
    style F fill:#e1bee7
    style G fill:#ce93d8
    style H fill:#ffccbc
    style I fill:#b2dfdb
```

---

## 5. Multi-Market Management Report Flow

```mermaid
graph TD
    Prior["self.prior_df<br/>Prior Year Data"]
    Prior_USA["self.usa_prior_df<br/>Optional: USA Specific PY"]
    Prior_Month["self.prior_month<br/>Filtered to Feb 2025"]
    
    subgraph USA_Section["USA SECTION"]
        USA_Load["Load usa_prior_df<br/>if exists"]
        USA_Filter["Filter to Feb 2025"]
        USA_Agg["Aggregate by<br/>Channel_Level"]
        USA_Lookup["usa_prior_spa<br/>usa_prior_retail"]
    end
    
    subgraph Europe_Section["EUROPE SECTION"]
        EUR_Filter["Filter prior_month<br/>market_group='Germany'"]
        EUR_Agg["Aggregate by<br/>Channel_Level"]
        EUR_Lookup["eur_prior_retail<br/>eur_prior_online"]
    end
    
    subgraph UK_Section["UK SECTION"]
        UK_Filter["Filter prior_month<br/>market_group='UK'"]
        UK_Agg["Aggregate by<br/>Channel_Level"]
        UK_Lookup["uk_prior_retail<br/>uk_prior_online"]
    end
    
    subgraph GVL_Section["GVL SECTION"]
        GVL_Filter["Filter prior_month<br/>Company_Group='GmbH'"]
        GVL_Build["Build lookup by<br/>Sales_Employee_Cleaned"]
        GVL_Lookup["gvl_priors<br/>{emp: value}"]
    end
    
    Report["calculate_report()"]
    
    Output["<br/>Report Output<br/><br/>USA Spa: sales, prior<br/>USA Retail: sales, prior<br/>Germany: sales, prior<br/>Switzerland: sales, prior<br/>UK: sales, prior<br/>GVL: sales, prior"]
    
    Prior --> USA_Load
    Prior_USA --> USA_Load
    USA_Load --> USA_Filter
    USA_Filter --> USA_Agg
    USA_Agg --> USA_Lookup
    
    Prior_Month --> EUR_Filter
    EUR_Filter --> EUR_Agg
    EUR_Agg --> EUR_Lookup
    
    Prior_Month --> UK_Filter
    UK_Filter --> UK_Agg
    UK_Agg --> UK_Lookup
    
    Prior_Month --> GVL_Filter
    GVL_Filter --> GVL_Build
    GVL_Build --> GVL_Lookup
    
    USA_Lookup --> Report
    EUR_Lookup --> Report
    UK_Lookup --> Report
    GVL_Lookup --> Report
    
    Report --> Output
    
    style Prior fill:#e3f2fd
    style Prior_USA fill:#e3f2fd
    style Prior_Month fill:#e3f2fd
    style USA_Section fill:#fff3e0
    style Europe_Section fill:#f3e5f5
    style UK_Section fill:#e8f5e9
    style GVL_Section fill:#ffe0b2
    style Report fill:#fff9c4
    style Output fill:#b2dfdb
```

---

## 6. YoY Calculation Pattern

```mermaid
graph TD
    A1["Previous Month<br/>Actual (Feb 2026)"]
    A2["Budget (Feb 2026)"]
    A3["Prior (Feb 2025)<br/>← FROM PRIOR_MONTH"]
    
    B1["actual = df[filter]<br/>.sum()"]
    B2["budget = budget_dict<br/>.get(region)"]
    B3["prior = prior_dict<br/>.get(region) OR<br/>sum(prior_month[filter])"]
    
    C1["val_diff_budget =<br/>actual - budget"]
    
    C2["val_pct_budget =<br/>IF budget != 0:<br/>(actual/budget*100)-100<br/>ELSE: 0"]
    
    C3["val_diff_prior =<br/>actual - prior"]
    
    C4["val_pct_prior =<br/>IF prior != 0:<br/>(actual/prior*100)-100<br/>ELSE: 0"]
    
    D["Row Dict<br/>{<br/>  'label': 'Northeast',<br/>  'actual': 145.23,<br/>  'budget': 152.00,<br/>  'prior': 156.34,<br/>  'diff_budget': -6.77,<br/>  'pct_budget': -4.5%,<br/>  'diff_prior': -11.11,<br/>  'pct_prior': -7.1%<br/>}"]
    
    A1 --> B1
    A2 --> B2
    A3 --> B3
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
    
    B1 --> C4
    B3 --> C4
    
    C1 --> D
    C2 --> D
    C3 --> D
    C4 --> D
    
    style A1 fill:#e3f2fd
    style A2 fill:#fff3e0
    style A3 fill:#c8e6c9
    style B1 fill:#fff9c4
    style B2 fill:#fff9c4
    style B3 fill:#fff9c4
    style C1 fill:#ffccbc
    style C2 fill:#ffccbc
    style C3 fill:#f8bbd0
    style C4 fill:#f8bbd0
    style D fill:#b2dfdb
```

---

## 7. Error Handling & Fallbacks

```mermaid
graph TD
    A["Load prior_df<br/>from CSV"]
    
    B{"File<br/>Exists?"}
    
    B1["⚠️ FileNotFoundError<br/>Exit with error"]
    
    C["Read CSV<br/>pd.read_csv()"]
    
    D{"CSV<br/>Valid?"}
    D1["⚠️ EmptyDataError<br/>Exit with error"]
    
    E["Parse Date<br/>pd.to_datetime<br/>format='%d/%m/%Y'"]
    
    F{"Parse<br/>Success?"}
    F1["Fallback:<br/>String prefix match<br/>startswith('2025-02')"]
    
    G["Filter Month<br/>year==2025<br/>month==2"]
    
    H["Convert Numeric<br/>pd.to_numeric<br/>errors='coerce'"]
    
    I["NaN Values?"]
    I1["Replace with 0<br/>.fillna(0)"]
    
    J["Group by Region<br/>sum()"]
    
    K{"Lookup<br/>Region?"]
    K1["Return 0<br/>.get(region, 0)"]
    
    L["YoY Calculation"]
    
    M{"Prior==0?"]
    M1["Set percent to 0<br/>if prior != 0"]
    
    N["Report Row"]
    
    A --> B
    B -->|No| B1
    B -->|Yes| C
    C --> D
    D -->|Empty| D1
    D -->|Valid| E
    E --> F
    F -->|Fail| F1
    F -->|Success| G
    F1 --> G
    G --> H
    H --> I
    I -->|NaN| I1
    I -->|Valid| J
    I1 --> J
    J --> K
    K -->|Not found| K1
    K -->|Found| L
    K1 --> L
    L --> M
    M -->|Yes| M1
    M -->|No| M1
    M1 --> N
    
    style A fill:#e3f2fd
    style B fill:#fff9c4
    style B1 fill:#ffcccc
    style C fill:#e8f5e9
    style D fill:#fff9c4
    style D1 fill:#ffcccc
    style E fill:#f3e5f5
    style F fill:#fff9c4
    style F1 fill:#ffe0b2
    style G fill:#c8e6c9
    style H fill:#fff9c4
    style I fill:#fff9c4
    style I1 fill:#ffe0b2
    style J fill:#ffccbc
    style K fill:#fff9c4
    style K1 fill:#ffe0b2
    style L fill:#b2dfdb
    style M fill:#fff9c4
    style M1 fill:#ffe0b2
    style N fill:#81c784
```

---

## 8. Data Column Transformations

```mermaid
graph LR
    A["Input Columns<br/>from CSV"]
    
    B1["Date<br/>Type: object<br/>Example:<br/>'02/15/2025'"]
    B2["Region<br/>Type: object<br/>Example:<br/>'Northeast'"]
    B3["Value_kEUR<br/>Type: object<br/>Example:<br/>'156,340.50'"]
    B4["Value_kUSD<br/>Type: object<br/>Example:<br/>'167,123.40'"]
    B5["Sales Employee<br/>Type: object<br/>Example:<br/>'  John Smith  '"]
    
    C1["pd.to_datetime<br/>format='%d/%m/%Y'"]
    C2[".str.strip()"]
    C3[".str.replace',')<br/>pd.to_numeric"]
    C4[".str.replace',')<br/>pd.to_numeric"]
    C5[".str.strip()"]
    
    D1["Date<br/>Type: datetime64<br/>Example:<br/>2025-02-15"]
    D2["Region<br/>Type: object<br/>Example:<br/>'Northeast'"]
    D3["Value_kEUR<br/>Type: float64<br/>Example:<br/>156340.50"]
    D4["Value_kUSD<br/>Type: float64<br/>Example:<br/>167123.40"]
    D5["Sales Employee<br/>Type: object<br/>Example:<br/>'John Smith'"]
    
    E["Output Columns<br/>Ready for aggregation"]
    
    A -->|input| B1
    A -->|input| B2
    A -->|input| B3
    A -->|input| B4
    A -->|input| B5
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
    B4 --> C4
    B5 --> C5
    
    C1 --> D1
    C2 --> D2
    C3 --> D3
    C4 --> D4
    C5 --> D5
    
    D1 --> E
    D2 --> E
    D3 --> E
    D4 --> E
    D5 --> E
    
    style A fill:#e3f2fd
    style B1 fill:#fff3e0
    style B2 fill:#fff3e0
    style B3 fill:#fff3e0
    style B4 fill:#fff3e0
    style B5 fill:#fff3e0
    style C1 fill:#fff9c4
    style C2 fill:#fff9c4
    style C3 fill:#fff9c4
    style C4 fill:#fff9c4
    style C5 fill:#fff9c4
    style D1 fill:#c8e6c9
    style D2 fill:#c8e6c9
    style D3 fill:#c8e6c9
    style D4 fill:#c8e6c9
    style D5 fill:#c8e6c9
    style E fill:#81c784
```

---

## 9. Report Generation Command Flow

```mermaid
graph TD
    Start["python src/full_report.py<br/>OR<br/>python src/usa_spa_report.py"]
    
    Init["Initialize Report Generator<br/>ReportGenerator(config, sales, budget, prior)"]
    
    Base["BaseReportGenerator.__init__<br/>- Load config<br/>- Load all CSV files (including prior)<br/>- Prepare dates"]
    
    Prepare["Report._prepare_data()<br/>- Filter prior to current month<br/>- Convert numeric values<br/>- Aggregate by dimension"]
    
    Calc["Report.calculate_report()<br/>- For each config section:<br/>  - Get actual sales<br/>  - Get prior value<br/>  - Calculate YoY metrics<br/>  - Build row dict"]
    
    Render["report.render_report()<br/>- Format rows<br/>- Style output"]
    
    Export["report.export_formats()<br/>- to_csv()<br/>- to_pdf()<br/>- to_html()"]
    
    Output["Save to<br/>data/outputs/<br/>/{format}/"]
    
    Start --> Init
    Init --> Base
    Base --> Prepare
    Prepare --> Calc
    Calc --> Render
    Render --> Export
    Export --> Output
    
    style Start fill:#fff3e0
    style Init fill:#f3e5f5
    style Base fill:#fff9c4
    style Prepare fill:#fff9c4
    style Calc fill:#ffccbc
    style Render fill:#f8bbd0
    style Export fill:#b2dfdb
    style Output fill:#81c784
```

---

## 10. Timestamp & Date Context

```mermaid
timeline
    title Current System Date vs PY Filtering
    
    2024-02-15 : Historical Year 1
    2024-03-15 : Historical Year 1
    2025-02-15 : Prior Year (target for filtering)
    2025-03-15 : Prior Year
    2026-02-15 : Current Date (TODAY)
    2026-03-15 : Future Reporting
    
    section Report Context
    2025-02-15 : Filter prior_month to HERE
    2026-02-15 : Current actual sales for comparison
    
    section Calculations
    2026-02-15 : Actual = Sum(Feb 2026 sales)
    2025-02-15 : Prior = Sum(Feb 2025 sales)
    2026-02-15 : YoY = (Actual - Prior) / Prior * 100
```

---

**Document Version:** 1.0  
**Diagrams Created:** February 27, 2026  
**Tools:** Mermaid.js v10+
