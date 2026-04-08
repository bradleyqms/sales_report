import re

# Read the email file
with open('data/outputs/EMAIL_1_MANAGEMENT_full.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the first USA Spa section (the simple one with just 4 regions)
# It appears between Total Sales and Core Markets Report

first_h3_usa = content.find('<h3 style="margin:8px 0 8px 0;font-size:14px;color:#1a365d;font-family:Arial,sans-serif;border-top:2px solid #e2e8f0;padding-top:16px;">USA Spa')

if first_h3_usa > 0:
    # Find the </div> that closes this section
    section_end = content.find('</div>', first_h3_usa)
    
    # Debug: show what we're removing
    section_to_remove = content[first_h3_usa:section_end+6]
    print(f"Section length to remove: {len(section_to_remove)} chars")
    print(f"First 200 chars: {section_to_remove[:200]}")
    
    # Remove it
    new_content = content[:first_h3_usa] + content[section_end+6:]
    
    # Write back
    with open('data/outputs/EMAIL_1_MANAGEMENT_full.html', 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"\n✅ Removed duplicate USA Spa table")
    print(f"New file size: {len(new_content)} bytes (was {len(content)})")
else:
    print("First USA Spa heading not found")
