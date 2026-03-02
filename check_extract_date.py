"""
DNR-55 Step 0A Verification Script
------------------------------------
Reads raw QRY files (local or from SharePoint) and confirms whether
the Extract_Date_Int column/row is present and survives the ingestion regex.

Usage:
    python check_extract_date.py                     # uses local automated_extracts/
    python check_extract_date.py --folder path/to/   # explicit local folder
    python check_extract_date.py --sharepoint        # download from SharePoint first
"""

import os
import re
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────

# Same regex used by qry_data_ingestion.py
QRY_LINE_PATTERN = re.compile(r'^(.+?)=([0-9,.\-]+)=?$')

ANCHOR_NAMES = [
    "Extract_Date_Int",   # expected column name from SAP
    "QMS_EXTRACT_DATE",   # alternative from original plan
    "extract_date",       # case variants
    "Extract_Date",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def check_file(path: Path) -> dict:
    """
    Inspect a single QRY file. Returns a summary dict.
    """
    result = {
        "file": path.name,
        "total_lines": 0,
        "regex_matched": 0,
        "regex_skipped": 0,
        "anchor_lines_raw": [],       # lines containing any anchor name (raw text)
        "anchor_lines_matched": [],   # anchor lines that ALSO match the regex
        "anchor_lines_skipped": [],   # anchor lines that do NOT match the regex
        "sample_skipped": [],         # first 5 skipped non-anchor lines (for context)
    }

    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception as e:
        result["error"] = str(e)
        return result

    result["total_lines"] = len(lines)
    skipped_sample_count = 0

    for raw_line in lines:
        line = raw_line.rstrip("\r\n")
        is_anchor = any(name.lower() in line.lower() for name in ANCHOR_NAMES)
        match = QRY_LINE_PATTERN.match(line)

        if match:
            result["regex_matched"] += 1
            if is_anchor:
                result["anchor_lines_matched"].append(line)
        else:
            result["regex_skipped"] += 1
            if is_anchor:
                result["anchor_lines_skipped"].append(line)
            elif skipped_sample_count < 5:
                result["sample_skipped"].append(line)
                skipped_sample_count += 1

        if is_anchor:
            result["anchor_lines_raw"].append(line)

    return result


def print_report(results: list[dict]) -> None:
    sep = "─" * 70

    print("\n" + sep)
    print("  DNR-55 Step 0A — Extract_Date_Int Verification")
    print(sep)

    found_in_any = False

    for r in results:
        print(f"\n📄 {r['file']}")

        if "error" in r:
            print(f"   ❌ Error reading file: {r['error']}")
            continue

        print(f"   Lines total   : {r['total_lines']}")
        print(f"   Regex matched : {r['regex_matched']}")
        print(f"   Regex skipped : {r['regex_skipped']}  "
              f"({'header/blank/non-numeric' if r['regex_skipped'] <= 3 else 'check skipped lines below'})")

        if r["anchor_lines_raw"]:
            found_in_any = True
            print(f"\n   ✅ ANCHOR FOUND ({len(r['anchor_lines_raw'])} line(s)):")
            for line in r["anchor_lines_raw"]:
                print(f"      RAW  : {repr(line)}")

            if r["anchor_lines_matched"]:
                print(f"\n   ✅ Regex MATCHES anchor (will be ingested correctly):")
                for line in r["anchor_lines_matched"]:
                    m = QRY_LINE_PATTERN.match(line)
                    print(f"      entity='{m.group(1)}'  value='{m.group(2)}'")
            else:
                print(f"\n   ⚠️  Regex does NOT match anchor lines.")
                print(f"      The anchor is present but will be silently discarded")
                print(f"      by the current qry_data_ingestion.py regex.")
                print(f"      FORMAT CHECK needed — see details below.")

            if r["anchor_lines_skipped"]:
                print(f"\n   Skipped anchor line(s) (regex failed):")
                for line in r["anchor_lines_skipped"]:
                    print(f"      '{line}'")
                print(f"\n   DIAGNOSIS:")
                for line in r["anchor_lines_skipped"]:
                    if "," in line and "=" not in line:
                        print(f"      → Looks like a CSV column header row.")
                        print(f"        The SAP query added Extract_Date_Int as a standard")
                        print(f"        CSV column, not as an Entity=Value row.")
                        print(f"        Step 0B must use pd.read_csv header detection,")
                        print(f"        not the entity row lookup approach.")
                    elif "=" in line:
                        parts = line.split("=")
                        val = parts[1] if len(parts) > 1 else ""
                        if not re.match(r'^[0-9,.\-]+$', val.strip()):
                            print(f"      → Entity=Value format found BUT value '{val}' is")
                            print(f"        non-numeric. Consider using CONVERT(INT,...) in SAP")
                            print(f"        to produce 'Extract_Date_Int=20260227='.")
        else:
            print(f"   ⚠️  No anchor line found in this file.")

        if r["sample_skipped"]:
            print(f"\n   Sample skipped non-anchor lines (first {len(r['sample_skipped'])}):")
            for line in r["sample_skipped"]:
                print(f"      '{line}'")

    print("\n" + sep)
    if found_in_any:
        print("  RESULT: Extract_Date_Int anchor detected in at least one file.")
    else:
        print("  RESULT: ⚠️  No anchor found in any file. Step 0A may not be complete yet.")
    print(sep + "\n")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    load_dotenv(Path(__file__).parent / ".env")

    parser = argparse.ArgumentParser(description="Verify Extract_Date_Int in QRY files")
    parser.add_argument("--folder", default=None,
                        help="Local folder containing QRY CSV files")
    parser.add_argument("--sharepoint", action="store_true",
                        help="Download QRY files from SharePoint before checking")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only check this many files (useful for quick spot-check)")
    args = parser.parse_args()

    # ── Resolve folder ────────────────────────────────────────────────────────
    if args.sharepoint:
        print("Downloading QRY files from SharePoint...")
        try:
            import tempfile, sys as _sys
            _sys.path.insert(0, str(Path(__file__).parent / "src"))
            from sharepoint_client import SharePointHandler

            SITE = os.getenv("SHAREPOINT_SITE_URL")
            CID  = os.getenv("SHAREPOINT_CLIENT_ID")
            CSEC = os.getenv("SHAREPOINT_CLIENT_SECRET")
            if not all([SITE, CID, CSEC]):
                print("ERROR: SharePoint credentials not found in .env. "
                      "Run without --sharepoint to use local files.")
                sys.exit(1)

            sp = SharePointHandler(SITE, CID, CSEC, quiet=True)
            tmp = tempfile.mkdtemp()
            qry_files = [
                "QRY_AR_MTD_CH.csv", "QRY_AR_MTD_Export.csv", "QRY_AR_MTD_Gmbh.csv",
                "QRY_AR_MTD_UK.csv",  "QRY_AR_MTD_USA.csv",
                "QRY_CN_MTD_CH.csv",  "QRY_CN_MTD_GmbH.csv", "QRY_CN_MTD_GmbH1.csv",
                "QRY_CN_MTD_UK.csv",  "QRY_CN_MTD_USA.csv",
                "QRY_SO_OPEN_MTD_CH.csv",    "QRY_SO_OPEN_MTD_Gmbh.csv",   "QRY_SO_OPEN_MTD_USA.csv",
                "QRY_SO_TOTAL_MTD_CH.csv",   "QRY_SO_TOTAL_MTD_Gmbh.csv",  "QRY_SO_TOTAL_MTD_USA.csv",
            ]
            sp_base = "/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/"
            downloaded = 0
            for fn in qry_files:
                try:
                    sp.download_file(sp_base + fn, os.path.join(tmp, fn))
                    downloaded += 1
                except Exception as e:
                    print(f"  Warning: could not download {fn}: {e}")
            print(f"Downloaded {downloaded} files to {tmp}")
            folder = Path(tmp)
        except ImportError:
            print("ERROR: Could not import SharePointHandler. Check src/ path.")
            sys.exit(1)

    elif args.folder:
        folder = Path(args.folder)
    else:
        # Default: local automated_extracts adjacent to this script
        folder = Path(__file__).parent / "automated_extracts"
        if not folder.exists():
            # Fallback: data/inputs directory
            folder = Path(__file__).parent / "data" / "inputs"

    if not folder.exists():
        print(f"ERROR: Folder not found: {folder}")
        print("Use --folder to specify the path to your QRY files, "
              "or --sharepoint to download them.")
        sys.exit(1)

    # ── Find QRY files ────────────────────────────────────────────────────────
    qry_paths = sorted(
        p for p in folder.iterdir()
        if p.is_file() and "QRY" in p.name.upper() and p.suffix.lower() == ".csv"
    )

    if not qry_paths:
        print(f"ERROR: No QRY CSV files found in: {folder}")
        sys.exit(1)

    if args.limit:
        qry_paths = qry_paths[:args.limit]

    print(f"Found {len(qry_paths)} QRY file(s) in: {folder}")

    # ── Run checks ────────────────────────────────────────────────────────────
    results = [check_file(p) for p in qry_paths]
    print_report(results)


if __name__ == "__main__":
    main()
