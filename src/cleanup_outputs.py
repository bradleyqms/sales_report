#!/usr/bin/env python
"""
Clean up output files older than 24 hours.

This script removes files from the data/outputs folder that are older than 24 hours
to prevent disk space issues during testing.
"""

import os
import time
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def cleanup_old_files(folder, hours=24):
    """
    Remove files older than specified hours from the given folder.
    
    Args:
        folder (str): Path to the folder to clean
        hours (int): Age threshold in hours (default: 24)
    
    Returns:
        tuple: (files_deleted, total_size_freed)
    """
    folder_path = Path(folder)
    
    if not folder_path.exists():
        logging.warning(f"Folder not found: {folder}")
        return 0, 0
    
    if not folder_path.is_dir():
        logging.error(f"Not a directory: {folder}")
        return 0, 0
    
    # Calculate cutoff time (24 hours ago)
    cutoff_time = time.time() - (hours * 3600)
    
    files_deleted = 0
    total_size = 0
    
    try:
        for file_path in folder_path.glob('*'):
            if file_path.is_file():
                file_mtime = file_path.stat().st_mtime
                
                # Check if file is older than cutoff
                if file_mtime < cutoff_time:
                    try:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        files_deleted += 1
                        total_size += file_size
                        
                        age_hours = (time.time() - file_mtime) / 3600
                        logging.info(f"Deleted: {file_path.name} ({file_size:,} bytes, {age_hours:.1f} hours old)")
                    except Exception as e:
                        logging.error(f"Error deleting {file_path.name}: {e}")
    except Exception as e:
        logging.error(f"Error scanning folder: {e}")
        return 0, 0
    
    return files_deleted, total_size

def format_bytes(bytes_size):
    """Format bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"

def main():
    # Get the output folder path
    script_dir = Path(__file__).parent.parent
    output_folder = script_dir / 'data' / 'outputs'
    
    logging.info(f"Starting cleanup of {output_folder}")
    logging.info(f"Removing files older than 24 hours...")
    
    files_deleted, total_size = cleanup_old_files(str(output_folder), hours=24)
    
    # Print summary
    print("\n" + "=" * 60)
    print("CLEANUP SUMMARY")
    print("=" * 60)
    print(f"Folder: {output_folder}")
    print(f"Files deleted: {files_deleted}")
    print(f"Space freed: {format_bytes(total_size)}")
    print("=" * 60 + "\n")
    
    if files_deleted == 0:
        logging.info("No files to clean up.")
    else:
        logging.info(f"Successfully deleted {files_deleted} file(s), freed {format_bytes(total_size)}")

if __name__ == '__main__':
    main()
