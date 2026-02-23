# Automated Extracts Timestamp Tool

This tool adds timestamps to QRY files in the `automated_extracts` directory to track when they were created.

## Usage

Run the timestamp script after generating new automated extracts:

```bash
cd sales_report_v2_independent
python src/timestamp_extracts.py
```

## What it does

1. **Renames files** with timestamp: `QRY_AR_MTD_USA.csv` → `QRY_AR_MTD_USA_20260206_162303.csv`
2. **Creates metadata file** (`extract_timestamps.txt`) with creation information
3. **Timestamp format**: `YYYYMMDD_HHMMSS` (e.g., `20260206_162303` = February 6, 2026, 16:23:03)

## File Structure After Timestamping

```
automated_extracts/
├── extract_timestamps.txt                    # Metadata file
├── QRY_AR_MTD_CH_20260206_162303.csv        # Timestamped files
├── QRY_AR_MTD_Export_20260206_162303.csv
├── QRY_AR_MTD_Gmbh_20260206_162303.csv
├── QRY_AR_MTD_UK_20260206_162303.csv
├── QRY_AR_MTD_USA_20260206_162303.csv
└── ... (all other QRY files with timestamps)
```

## Benefits

- **Track creation time**: Know exactly when each extract was generated
- **Version control**: Multiple extracts can coexist with different timestamps
- **Audit trail**: Clear history of data extraction times
- **Debugging**: Correlate issues with specific extract generations

## Integration

The timestamping is compatible with other tools like the unmapped entities checker, which will automatically work with timestamped files.