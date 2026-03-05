# Regional Mapping Research Documentation

**Ticket**: DNR-5 - Implement Regional Mapping for Prior Year (PY) Core Markets Data  
**Date**: February 27, 2026  
**Status**: Research Complete

---

## Documentation Overview

This folder contains comprehensive research and analysis for implementing regional mapping changes to Prior Year 2025 (PY25) data in the sales_report_v2_independent system.

### Main Research Document

See `.ai/features/DNR-5-regional-mapping-research/` for the comprehensive ticket research overview.

Technical documentation in this folder covers:

---

## Supporting Research Documents

### Data Flow Research

📄 **[PY_DATA_FLOW_README.md](PY_DATA_FLOW_README.md)** — PY Data Flow Quick Reference  
Quick-start guide to the Prior Year data pipeline:
- 3-document overview
- Quick reference tables
- How to navigate the research
- Data pipeline overview
- Testing & validation checklist

📄 **[PY_DATA_FLOW_DOCUMENTATION.md](PY_DATA_FLOW_DOCUMENTATION.md)** — Complete PY Data Flow  
800+ line detailed reference covering:
- PY data file locations and naming conventions
- Data loading pipeline through BaseReportGenerator
- 7-step transformation process with line numbers
- Entity mapping integration
- Report calculations for all 4 report types
- Performance analysis and optimization
- Common issues and solutions

📄 **[PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md](PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md)** — Visual Architecture  
10 detailed Mermaid diagrams showing:
1. End-to-End Pipeline
2. Data Loading & Parsing
3. Region-Level Aggregation (USA Spa)
4. Employee-Level Lookup (GVL)
5. Multi-Market Management Flow
6. YoY Calculation Pattern
7. Error Handling & Fallbacks
8. Data Column Transformations
9. Report Generation Command Flow
10. Timeline & Date Context

📄 **[PY_DATA_FILE_TOUCH_MAP.md](PY_DATA_FILE_TOUCH_MAP.md)** — Detailed Implementation Map  
800+ lines documenting exact code mappings:
- Step-by-step file touch sequence
- Complete file touch summary table with all operations
- Data transformation timeline (T0-T7 states)
- Tracing a single data point through the pipeline
- Dynamic file selection logic
- All 5 core files analyzed with line numbers

### Entity Mapping Research

📄 **[SALESPERSON_REGION_MAPPING_RESEARCH.md](SALESPERSON_REGION_MAPPING_RESEARCH.md)** — Current Mapping Logic  
Comprehensive breakdown of employee-to-region mapping:
- Entity mappings CSV structure (10 columns)
- Where `employee_region_map` is created in each report
- Multiple mapping examples with real data
- Lookup key priority (Cleaned vs Raw names)
- Error handling and fallback behavior
- Code snippets with line numbers
- Visual flow diagrams

📄 **[ENTITY_MAPPINGS_STRUCTURE.md](ENTITY_MAPPINGS_STRUCTURE.md)** — Mapping File Specification  
Detailed analysis of entity_mappings.csv:
- Complete header with 10 columns defined
- Employee vs Customer column patterns
- Regional mapping details
- Null/empty value handling patterns
- GmbH/AG vs other entity types
- Critical relationships between columns
- 283+ record examples from actual data

---

## How to Use This Research

### For Understanding Current State
1. Start with **Main Research Document** (Section: "Summary")
2. Review **Current Mapping Logic** for how employee mapping works
3. Check **PY Data Flow README** for quick reference

### For Implementation
1. Review **Implementation Prerequisites** in Main Research Document
2. Study **File Touch Map** to see exactly where code changes are needed
3. Use **Architecture Diagrams** to understand data transformations
4. Reference **Code References Summary** for exact file locations and line numbers

### For Testing & Validation
1. Check **PY Data Flow Documentation** (Common Issues & Solutions section)
2. Review test locations in **File Touch Map**
3. Follow **testing checklist** in PY_DATA_FLOW_README.md

---

## Quick Reference

### Files That Need Changes
- `src/base_report_generator.py` — Add PY mapping loader
- `src/gvl_report.py` — Use new PY25 mapping
- `src/core_market_report.py` — Use new PY25 mapping
- `src/usa_spa_report.py` — Validate region grouping
- `data/inputs/mappings/` — Add new mapping file(s)
- `tests/test_mapping.py` — Add PY25 mapping tests

### Data File Locations
- **Current mappings**: `data/inputs/mappings/entity_mappings.csv`
- **PY25 data**: `data/inputs/prior_years/` (3 files: gvl, usa, processed)
- **Reports**: `src/` (gvl_report.py, usa_spa_report.py, core_market_report.py, full_report.py)

### Key Findings
- PY25 files **already contain Region data** as direct columns
- Current system uses **shared mapping** for both current and prior year
- **Recommended approach**: Create `py25_regional_mappings.csv` (backward compatible)
- **Files touching PY data**: 7 core files (5 Python + 2 utility)

---

## Folder Structure

```
sales_report_v2_independent/docs/regional-mapping-research/
├── INDEX.md (this file)
├── PY_DATA_FLOW_README.md
├── PY_DATA_FLOW_DOCUMENTATION.md
├── PY_DATA_FLOW_ARCHITECTURE_DIAGRAMS.md
├── PY_DATA_FILE_TOUCH_MAP.md
└── SALESPERSON_REGION_MAPPING_RESEARCH.md

/.ai/features/DNR-5-regional-mapping-research/
└── 2026-02-27-DNR-5-regional-mapping-research.md (Main research)
```

## Document Structure

Each technical documentation file follows this pattern:

```
├── Overview/Summary
├── Detailed Findings
├── Code References with Line Numbers
├── Architecture/Flow Diagrams (where applicable)
├── Implementation Guidance
└── Related Documentation Links
```

---

## Next Steps

1. **Audit PY25 region assignments** in `data/inputs/prior_years/`
   - Determine if current Region values need updating
   - Identify which fields are salesperson-dependent

2. **Define new regional mapping logic**
   - Specify how employees map to new regions
   - Document Sub Region hierarchy changes (if any)

3. **Choose implementation approach**
   - Option A: Enhance entity_mappings.csv
   - Option B: Create py25_regional_mappings.csv (recommended)
   - Option C: Pre-process prior_sales_2025_*.csv files

4. **Implement and test**
   - Use code references for exact locations
   - Run test suite across all report types
   - Validate YoY comparisons

---

## Questions or Need More Details?

Each document is self-contained with complete context. Cross-references between documents are provided for deeper investigation.

**Document created**: February 27, 2026  
**Researcher**: GitHub Copilot  
**Ticket**: DNR-5
