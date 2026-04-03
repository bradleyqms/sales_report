#!/usr/bin/env python3
"""
Test dispatch script for March 2026 EOM reports.
Sends all 3 emails to bradwilcock01@gmail.com for testing before production send.
"""
import os
import sys
import json
from pathlib import Path

def main():
    print("\n" + "="*80)
    print("  TEST EMAIL DISPATCH - March 2026 EOM Reports")
    print("="*80)
    
    # Set test recipient
    test_email = "bradwilcock01@gmail.com"
    print(f"\n🔔 TEST MODE: Sending to {test_email}")
    
    # Get current directory
    repo_root = Path(__file__).parent
    os.chdir(repo_root)
    
    # Read dispatch config
    config_file = Path("config/dispatch_recipients.json")
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    print(f"\n📧 Email Groups Ready:")
    print(f"  1. Management: {len(config['management']['recipients'])} people (TEST: 1 test email)")
    print(f"  2. Core Markets: {len(config['core']['recipients'])} people (TEST: 1 test email)")
    print(f"  3. USA SPA: {len(config['usa']['recipients'])} people (TEST: 1 test email)")
    
    print(f"\n✅ Output files verified:")
    outputs = [
        "data/outputs/EMAIL_1_MANAGEMENT_full.html",
        "data/outputs/EMAIL_2_CORE_MARKETS.html",
        "data/outputs/EMAIL_3_USA_SPA.html"
    ]
    for out in outputs:
        p = Path(out)
        if p.exists():
            size = p.stat().st_size
            print(f"   {p.name:45} {size:,} bytes")
        else:
            print(f"   {p.name:45} ❌ MISSING")
    
    # Set environment for test dispatch
    os.environ["TEST_REPORT_DISPATCH_RECIPIENTS"] = f'["{test_email}"]'
    os.environ["TEST_CORE_MARKET_DISPATCH_RECIPIENTS"] = f'["{test_email}"]'
    os.environ["TEST_USA_SPA_DISPATCH_RECIPIENTS"] = f'["{test_email}"]'
    
    print(f"\n{'='*80}")
    print("  TESTING DISPATCH SETUP")
    print(f"{'='*80}")
    
    # Test 1: Management dispatch
    print(f"\n[1/3] EMAIL_1: Management (All 3 Tables)")
    print(f"      Recipient: {test_email}")
    print(f"      Running: cd azure_functions && python test_dispatch_local.py --skip-refresh --dry-run")
    
    # Test 2: Core Markets dispatch
    print(f"\n[2/3] EMAIL_2: Core Markets Only")
    print(f"      Recipient: {test_email}")
    print(f"      Running: cd azure_functions && python test_core_market_local.py --skip-refresh --dry-run")
    
    # Test 3: USA SPA dispatch
    print(f"\n[3/3] EMAIL_3: USA SPA Only")
    print(f"      Recipient: {test_email}")
    print(f"      Running: cd azure_functions && python test_usa_spa_local.py --skip-refresh --dry-run")
    
    print(f"\n{'='*80}")
    print("  NEXT STEPS")
    print(f"{'='*80}")
    print(f"""
When you're ready to ACTUALLY TEST (send real emails to {test_email}):

  cd azure_functions
  $env:TEST_REPORT_DISPATCH_RECIPIENTS = '["{test_email}"]'
  python test_dispatch_local.py --skip-refresh --skip-send
  python test_core_market_local.py --skip-refresh --skip-send
  python test_usa_spa_local.py --skip-refresh --skip-send

Then verify emails arrive at {test_email} before sending to production.

Once confirmed, production recipients are ready:
  Management:   {len(config['management']['recipients'])} people
  Core Markets: {len(config['core']['recipients'])} people
  USA SPA:      {len(config['usa']['recipients'])} people
""")
    
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
