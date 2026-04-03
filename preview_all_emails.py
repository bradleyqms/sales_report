import json
from pathlib import Path

# Read recipients config
with open('config/dispatch_recipients.json', 'r') as f:
    recipients = json.load(f)

# Get the three email files
mgmt_email = Path('data/outputs/EMAIL_1_MANAGEMENT_full.html')
core_email = Path('data/outputs/EMAIL_2_CORE_MARKETS.html')
usa_email = Path('data/outputs/EMAIL_3_USA_SPA.html')

# Define subject lines and descriptions
emails_info = [
    {
        'name': 'EMAIL_1_MANAGEMENT_full.html',
        'subject': 'QMS Sales Report - March 2026 (EOM) - Management',
        'description': 'All 3 Tables: Management Report + Core Markets + USA SPA Regional',
        'recipients': recipients['management']['recipients'],
        'file': mgmt_email
    },
    {
        'name': 'EMAIL_2_CORE_MARKETS.html',
        'subject': 'QMS Sales Report - March 2026 (EOM) - Core Markets',
        'description': 'Core Markets Report Only',
        'recipients': recipients['core']['recipients'],
        'file': core_email
    },
    {
        'name': 'EMAIL_3_USA_SPA.html',
        'subject': 'QMS Sales Report - March 2026 (EOM) - USA SPA',
        'description': 'USA SPA Regional Report Only',
        'recipients': recipients['usa']['recipients'],
        'file': usa_email
    }
]

# Create preview document
preview_html = '''<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body { font-family: Arial, sans-serif; background: #f5f5f5; margin: 0; padding: 20px; }
  .email-group { background: white; margin: 30px auto; max-width: 1000px; border: 1px solid #ddd; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
  .email-header { background: #1a365d; color: white; padding: 20px; border-bottom: 3px solid #2c5282; }
  .email-header h2 { margin: 0 0 8px 0; font-size: 18px; }
  .email-meta { background: #f9f9f9; padding: 15px 20px; border-bottom: 1px solid #ddd; }
  .meta-row { margin: 8px 0; }
  .meta-label { font-weight: bold; color: #333; display: inline-block; width: 100px; }
  .recipients { background: #f0f4f8; padding: 12px; border-radius: 4px; margin-top: 8px; }
  .recipient-item { padding: 4px 0; font-size: 13px; color: #555; }
  .email-content { padding: 20px; max-height: 400px; overflow-y: auto; border-top: 1px solid #ddd; }
  .email-content iframe { border: none; width: 100%; height: 400px; }
  .preview-link { display: inline-block; margin-top: 10px; padding: 8px 14px; background: #2c5282; color: white; text-decoration: none; border-radius: 4px; font-size: 13px; }
  .preview-link:hover { background: #1a365d; }
  h1 { text-align: center; color: #1a365d; margin-bottom: 40px; }
</style>
</head>
<body>
<h1>📧 EOM March 2026 Email Preview - All 3 Reports</h1>
'''

for i, email_info in enumerate(emails_info, 1):
    recipients_list = email_info['recipients']
    
    preview_html += f'''
<div class="email-group">
  <div class="email-header">
    <h2>EMAIL {i}: {email_info['name']}</h2>
    <div style="font-size: 12px; opacity: 0.9;">{email_info['description']}</div>
  </div>
  
  <div class="email-meta">
    <div class="meta-row">
      <span class="meta-label">Subject:</span>
      <span style="color: #1a365d; font-weight: 500;">{email_info['subject']}</span>
    </div>
    
    <div class="meta-row">
      <span class="meta-label">Recipients:</span>
      <span style="color: #d35400; font-weight: 500;">{len(recipients_list)} people</span>
    </div>
    
    <div class="recipients">
      <div style="font-weight: bold; margin-bottom: 8px; color: #1a365d;">Recipient List:</div>
'''
    
    for recipient in recipients_list:
        preview_html += f'      <div class="recipient-item">✓ {recipient}</div>\n'
    
    preview_html += f'''
    </div>
    
    <div style="margin-top: 12px;">
      <a href="file:///{email_info['file'].absolute()}" class="preview-link">🔍 Open Full Preview</a>
    </div>
  </div>
</div>
'''

preview_html += '''
</body>
</html>
'''

# Save preview
preview_file = Path('data/outputs/EMAIL_PREVIEW_ALL_RECIPIENTS.html')
with open(preview_file, 'w', encoding='utf-8') as f:
    f.write(preview_html)

print(f"✅ Created comprehensive preview: {preview_file.name}")
print(f"   File: {preview_file}")

# Also print summary to console
for i, email_info in enumerate(emails_info, 1):
    print(f"\n{'='*80}")
    print(f"EMAIL {i}: {email_info['name']}")
    print(f"{'='*80}")
    print(f"Subject: {email_info['subject']}")
    print(f"Description: {email_info['description']}")
    print(f"Recipients ({len(email_info['recipients'])} people):")
    for recipient in email_info['recipients']:
        print(f"  • {recipient}")
    print(f"File: {email_info['file'].name}")
