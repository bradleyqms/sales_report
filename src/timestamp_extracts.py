#!/usr/bin/env python3
"""
Add timestamps to automated extract files.

This script adds timestamps to QRY files in the automated_extracts directory
to track when they were created.
"""

import os
import datetime
from pathlib import Path
import shutil

def add_timestamps_to_extracts(extracts_dir: Path, timestamp_format: str = "%Y%m%d_%H%M%S"):
    """
    Add timestamps to automated extract files.

    Args:
        extracts_dir: Directory containing the extract files
        timestamp_format: Format for the timestamp (default: YYYYMMDD_HHMMSS)
    """
    if not extracts_dir.exists():
        print(f"Directory {extracts_dir} does not exist!")
        return

    # Get current timestamp
    now = datetime.datetime.now()
    timestamp = now.strftime(timestamp_format)

    print(f"Adding timestamp {timestamp} to files in {extracts_dir}")

    # Find all QRY files
    qry_files = list(extracts_dir.glob("QRY_*.csv"))

    if not qry_files:
        print("No QRY files found!")
        return

    print(f"Found {len(qry_files)} QRY files to timestamp")

    for file_path in qry_files:
        # Create new filename with timestamp
        stem = file_path.stem  # filename without extension
        suffix = file_path.suffix  # .csv
        new_filename = f"{stem}_{timestamp}{suffix}"
        new_path = file_path.parent / new_filename

        # Rename the file
        shutil.move(str(file_path), str(new_path))
        print(f"Renamed: {file_path.name} -> {new_filename}")

    print(f"\nTimestamping complete! Added {timestamp} to {len(qry_files)} files.")

def create_timestamp_metadata_file(extracts_dir: Path):
    """
    Create a metadata file with timestamp information.
    """
    metadata_file = extracts_dir / "extract_timestamps.txt"
    now = datetime.datetime.now()

    with open(metadata_file, 'w') as f:
        f.write(f"Automated Extracts Timestamp Information\n")
        f.write(f"Created: {now.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Timestamp format: YYYYMMDD_HHMMSS\n")
        f.write(f"Files in this directory have been timestamped with creation date/time.\n")

    print(f"Created metadata file: {metadata_file}")

if __name__ == "__main__":
    # Path to automated extracts directory (sibling to current project)
    extracts_dir = Path(__file__).parent.parent.parent / "sales_report_v2" / "automated_extracts"

    print("Automated Extracts Timestamp Tool")
    print("=" * 40)

    # Add timestamps to files
    add_timestamps_to_extracts(extracts_dir)

    # Create metadata file
    create_timestamp_metadata_file(extracts_dir)

    print("\nDone! All automated extracts now have timestamps.")