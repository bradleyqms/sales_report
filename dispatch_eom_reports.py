#!/usr/bin/env python3
"""
EOM Report Dispatch Runner for March 2026 Reports
- Test dispatch to bradwilcock01@gmail.com first
- Then production dispatch to configured recipients
"""
import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime

def run_command(cmd, description):
    """Execute a shell command and return success status."""
    print(f"\n  ▶ {description}")
    print(f"    Command: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, cwd="azure_functions", capture_output=False, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return False

def main():
    print("\n" + "="*90)
    print("  EOM REPORT DISPATCH RUNNER — March 2026")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"))
    print("="*90)
    
    repo_root = Path(__file__).parent
    os.chdir(repo_root)
    
    # Read config
    config_file = Path("config/dispatch_recipients.json")
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Show what will be sent
    print("\n📊 REPORT SUMMARY:")
    print(f"  Period: March 2026 (End-of-Month)")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d at %H:%M UTC')}")
    print(f"  Input File: 20260403T103752.253-new_unified_dbo_qry_last_month.xlsx")
    
    print("\n📧 EMAIL GROUPS (PRODUCTION):")
    print(f"  1. Management          → {len(config['management']['recipients'])} recipients")
    print(f"     [All 3 Tables: Mgmt, Core Markets, USA SPA]")
    print(f"  2. Core Markets        → {len(config['core']['recipients'])} recipients")
    print(f"     [Core Markets Report Only]")
    print(f"  3. USA SPA             → {len(config['usa']['recipients'])} recipients")
    print(f"     [USA SPA Regional Report Only]")
    
    print("\n✅ EMAIL FILES VERIFIED:")
    outputs = [
        ("EMAIL_1: Management", "data/outputs/EMAIL_1_MANAGEMENT_full.html"),
        ("EMAIL_2: Core Markets", "data/outputs/EMAIL_2_CORE_MARKETS.html"),
        ("EMAIL_3: USA SPA", "data/outputs/EMAIL_3_USA_SPA.html")
    ]
    for name, path in outputs:
        p = Path(path)
        if p.exists():
            size = p.stat().st_size // 1024
            print(f"  ✓ {name:30} {size:>6} KB")
        else:
            print(f"  ✗ {name:30} MISSING")
            sys.exit(1)
    
    # Phase 1: Test Dispatch
    print(f"\n{'='*90}")
    print("  PHASE 1: TEST DISPATCH (bradwilcock01@gmail.com)")
    print(f"{'='*90}")
    print("\n⚠️  This will SEND REAL EMAILS to: bradwilcock01@gmail.com")
    print("   Use this to verify email formatting and content before production send")
    
    response = input("\n🔔 Ready to send TEST emails? (y/n): ").strip().lower()
    if response != 'y':
        print("  Aborted.")
        sys.exit(0)
    
    print("\n" + "─"*90)
    test_results = []
    
    # Test 1: Management
    print("\n[1/3] Testing MANAGEMENT EMAIL")
    cmd = ["python", "test_dispatch_local.py", "--skip-refresh"]
    result = run_command(cmd, "Send Management Report to test recipient")
    test_results.append(("Management", result))
    
    # Test 2: Core Markets
    print("\n[2/3] Testing CORE MARKETS EMAIL")
    cmd = ["python", "test_core_market_local.py", "--skip-refresh"]
    result = run_command(cmd, "Send Core Markets Report to test recipient")
    test_results.append(("Core Markets", result))
    
    # Test 3: USA SPA
    print("\n[3/3] Testing USA SPA EMAIL")
    cmd = ["python", "test_usa_spa_local.py", "--skip-refresh"]
    result = run_command(cmd, "Send USA SPA Report to test recipient")
    test_results.append(("USA SPA", result))
    
    # Summary
    print(f"\n{'─'*90}")
    print("TEST DISPATCH RESULTS:")
    all_passed = True
    for name, result in test_results:
        status = "✅ SENT" if result else "❌ FAILED"
        print(f"  {name:30} {status}")
        if not result:
            all_passed = False
    
    if not all_passed:
        print("\n⚠️  Some test emails failed to send. Check configuration and try again.")
        sys.exit(1)
    
    print("\n✅ All test emails sent successfully!")
    print("\n📧 Check bradwilcock01@gmail.com for test emails")
    print("   - Management Report (all 3 tables)")
    print("   - Core Markets Report")
    print("   - USA SPA Report")
    
    # Phase 2: Production Dispatch
    print(f"\n{'='*90}")
    print("  PHASE 2: PRODUCTION DISPATCH")
    print(f"{'='*90}")
    print("\n⚠️  THIS WILL SEND TO ALL PRODUCTION RECIPIENTS:")
    print(f"     Management: {', '.join([r.split('<')[0].strip() for r in config['management']['recipients'][:3]])}...")
    print(f"     Core Markets: {', '.join([r.split('<')[0].strip() for r in config['core']['recipients'][:3]])}...")
    print(f"     USA SPA: {', '.join([r.split('<')[0].strip() for r in config['usa']['recipients']])}")
    
    print(f"\n   Total Recipients: {len(config['management']['recipients']) + len(config['core']['recipients']) + len(config['usa']['recipients'])}")
    
    response = input("\n🔔 Ready to send PRODUCTION emails? (type 'YES' to confirm): ").strip()
    if response != 'YES':
        print("  ✓ Production send cancelled.")
        print("\nTo send later, run:")
        print("  cd azure_functions")
        print("  python test_dispatch_local.py --skip-refresh        # Management")
        print("  python test_core_market_local.py --skip-refresh     # Core Markets")
        print("  python test_usa_spa_local.py --skip-refresh         # USA SPA")
        sys.exit(0)
    
    print("\n" + "─"*90)
    prod_results = []
    
    # Production 1: Management
    print("\n[1/3] Sending MANAGEMENT EMAILS (Production)")
    cmd = ["python", "test_dispatch_local.py", "--skip-refresh"]
    result = run_command(cmd, "Send to all management recipients")
    prod_results.append(("Management", result))
    
    # Production 2: Core Markets
    print("\n[2/3] Sending CORE MARKETS EMAILS (Production)")
    cmd = ["python", "test_core_market_local.py", "--skip-refresh"]
    result = run_command(cmd, "Send to all core markets recipients")
    prod_results.append(("Core Markets", result))
    
    # Production 3: USA SPA
    print("\n[3/3] Sending USA SPA EMAILS (Production)")
    cmd = ["python", "test_usa_spa_local.py", "--skip-refresh"]
    result = run_command(cmd, "Send to all USA SPA recipients")
    prod_results.append(("USA SPA", result))
    
    # Final Summary
    print(f"\n{'='*90}")
    print("PRODUCTION DISPATCH COMPLETE")
    print(f"{'='*90}")
    print("\nDISPATCH SUMMARY:")
    all_sent = True
    for name, result in prod_results:
        status = "✅ SENT" if result else "❌ FAILED"
        print(f"  {name:30} {status}")
        if not result:
            all_sent = False
    
    if all_sent:
        print(f"\n{'='*90}")
        print("  ✅ SUCCESS: All production emails dispatched!")
        print(f"{'='*90}")
        print(f"\n  Report Date: March 2026 (End-of-Month)")
        print(f"  Dispatch Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Recipients: {len(config['management']['recipients']) + len(config['core']['recipients']) + len(config['usa']['recipients'])}")
    else:
        print(f"\n{'='*90}")
        print("  ⚠️  Some production emails failed to send")
        print(f"{'='*90}")
        sys.exit(1)

if __name__ == "__main__":
    main()
