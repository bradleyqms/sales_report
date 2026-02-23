#!/usr/bin/env python3
"""
Test SharePoint connectivity for unmapped entities check.

This script tests if SharePoint credentials are available and can connect
to download the latest QRY files for unmapped entities checking.
"""

import os
import sys
from pathlib import Path

def test_sharepoint_connectivity():
    """Test SharePoint connectivity and download a sample file."""
    print("Testing SharePoint connectivity for unmapped entities check...")
    print("=" * 60)

    # Check environment variables
    SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
    CLIENT_ID = os.getenv('SHAREPOINT_CLIENT_ID')
    CLIENT_SECRET = os.getenv('SHAREPOINT_CLIENT_SECRET')

    print(f"SHAREPOINT_SITE_URL: {'✓ Set' if SHAREPOINT_SITE_URL else '✗ Not set'}")
    print(f"SHAREPOINT_CLIENT_ID: {'✓ Set' if CLIENT_ID else '✗ Not set'}")
    print(f"SHAREPOINT_CLIENT_SECRET: {'✓ Set' if CLIENT_SECRET else '✗ Not set'}")

    use_sharepoint = all([SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET])

    if not use_sharepoint:
        print("\n❌ SharePoint credentials not available.")
        print("The unmapped check will use local automated_extracts files.")
        print("\nTo enable SharePoint integration:")
        print("1. Set environment variables: SHAREPOINT_SITE_URL, SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET")
        print("2. Or create a .env file in the project root with these variables")
        return False

    print("\n✓ SharePoint credentials found. Testing connection...")

    try:
        # Import SharePoint handler
        sys.path.append(str(Path(__file__).parent))
        from sharepoint_client import SharePointHandler
        import tempfile

        # Initialize SharePoint handler
        sp_handler = SharePointHandler(SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET, quiet=True)
        print("✓ SharePoint connection successful")

        # Test downloading a small file
        test_file = "QRY_CN_MTD_USA.csv"  # Usually small/empty
        sp_path = f"/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/{test_file}"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / test_file
            sp_handler.download_file(sp_path, str(local_path))
            print(f"✓ Test download successful: {test_file}")

        print("\n✅ SharePoint integration is working!")
        print("The unmapped check will download the latest QRY files from SharePoint.")

    except Exception as e:
        print(f"\n❌ SharePoint test failed: {e}")
        print("The unmapped check will fall back to local files.")
        return False

    return True

if __name__ == "__main__":
    test_sharepoint_connectivity()