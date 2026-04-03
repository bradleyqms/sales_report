import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, 'azure_functions')
from pathlib import Path
from dispatch_reports.html_builder import build_html_body

# Get the updated report files
mgmt_file = list(Path('data/outputs').glob('combined_management_report_2026_EOM*eom_march_final_updated*.html'))[0]
core_file = list(Path('data/outputs').glob('*core_markets_2026_EOM*eom_march_final_updated*.html'))[0]
usa_file = list(Path('data/outputs').glob('*usa_spa_2026_EOM*eom_march_final_updated*.html'))[0]

print(f"Using updated reports:")
print(f"  Management: {mgmt_file.name}")
print(f"  Core Markets: {core_file.name}")
print(f"  USA SPA: {usa_file.name}")
print()

# EMAIL 1: Management (all 3 tables)
print("[EMAIL 1] Generating EMAIL_1_MANAGEMENT_full.html...")
content_type, email_body = build_html_body(
    html_files=[mgmt_file, core_file, usa_file],
    plain_intro='Please find the latest QMS sales data for March 2026 (End-of-Month). This report includes Management performance, Core Markets analysis, and USA SPA regional breakdown.',
    banner_title='Management Sales Report — March 2026 (EOM)',
    banner_subtitle='March 1-31, 2026 • End-of-Month Results • All Markets',
    footer_note='Full CSV and PDF export files are available in the attachments.'
)
with open('data/outputs/EMAIL_1_MANAGEMENT_full.html', 'w', encoding='utf-8') as f:
    f.write(email_body)
print(f"   [OK] {Path('data/outputs/EMAIL_1_MANAGEMENT_full.html').stat().st_size:,} bytes")

# EMAIL 2: Core Markets only
print("[EMAIL 2] Generating EMAIL_2_CORE_MARKETS.html...")
content_type, email_body = build_html_body(
    html_files=[core_file],
    plain_intro='Please find the latest Core Markets sales report for March 2026 (End-of-Month).',
    banner_title='Core Markets Sales Report — March 2026 (EOM)',
    banner_subtitle='March 1-31, 2026 • End-of-Month Results',
    footer_note='Full CSV and PDF export files are available in the attachments.'
)
with open('data/outputs/EMAIL_2_CORE_MARKETS.html', 'w', encoding='utf-8') as f:
    f.write(email_body)
print(f"   [OK] {Path('data/outputs/EMAIL_2_CORE_MARKETS.html').stat().st_size:,} bytes")

# EMAIL 3: USA SPA only
print("[EMAIL 3] Generating EMAIL_3_USA_SPA.html...")
content_type, email_body = build_html_body(
    html_files=[usa_file],
    plain_intro='Please find the latest USA SPA regional sales report for March 2026 (End-of-Month).',
    banner_title='USA SPA Regional Sales Report — March 2026 (EOM)',
    banner_subtitle='March 1-31, 2026 • End-of-Month Results',
    footer_note='Full CSV and PDF export files are available in the attachments.'
)
with open('data/outputs/EMAIL_3_USA_SPA.html', 'w', encoding='utf-8') as f:
    f.write(email_body)
print(f"   [OK] {Path('data/outputs/EMAIL_3_USA_SPA.html').stat().st_size:,} bytes")

print("\n[OK] All 3 emails regenerated with updated reports")
