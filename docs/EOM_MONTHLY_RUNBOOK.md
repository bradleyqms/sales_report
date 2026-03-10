# EOM Monthly Runbook

## 1) Branch/Ticket Close-Out

### Scope to commit for this feature branch

Include:
- src/eom_unified_workbook_report.py
- src/base_report_generator.py
- src/usa_spa_report.py
- azure_functions/dispatch_reports/html_builder.py
- azure_functions/dispatch_reports/__init__.py
- azure_functions/core_market_reports/__init__.py
- azure_functions/dispatch_usa_spa_reports/__init__.py
- azure_functions/test_dispatch_local.py
- azure_functions/test_core_market_local.py
- azure_functions/validate_dispatch_dry_run.py
- run_eom_monthly_dryrun.ps1
- tests/test_eom_unified_workbook_report.py
- docs/EOM_MONTHLY_RUNBOOK.md

Delete:
- src/eom_workbook_report.py
- tests/test_eom_workbook_report.py

Do not include:
- root xlsx/sql scratch files
- data/outputs artifacts
- trigger_run_status.txt
- retrigger_failed_invocations.sh (unless explicitly part of release)

### Suggested close-out commands

1. Verify branch and status
   git branch --show-current
   git status --short

2. Stage only feature files
   git add src/eom_unified_workbook_report.py
   git add src/base_report_generator.py
   git add src/usa_spa_report.py
   git add azure_functions/dispatch_reports/html_builder.py
   git add azure_functions/dispatch_reports/__init__.py
   git add azure_functions/core_market_reports/__init__.py
   git add azure_functions/dispatch_usa_spa_reports/__init__.py
   git add azure_functions/test_dispatch_local.py
   git add azure_functions/test_core_market_local.py
   git add azure_functions/validate_dispatch_dry_run.py
   git add run_eom_monthly_dryrun.ps1
   git add tests/test_eom_unified_workbook_report.py
   git add docs/EOM_MONTHLY_RUNBOOK.md
   git rm src/eom_workbook_report.py
   git rm tests/test_eom_workbook_report.py

3. Commit
   git commit -m "feat(eom): unify workbook EOM flow, strict dry-run validation, and EOM dispatch subjects"

4. Push
   git push -u origin feature/eom-workbook

5. PR + ticket closure
- Link ticket in PR description.
- Include validation evidence (subjects, recipients, title month check).
- Close ticket only after PR merged and one production-confirmed send.


## 2) Next-Month Reuse (Operator Steps)

Scenario example: on April 5, send March EOM.

### A. Generate anchored report outputs

From repo root:
python src/eom_unified_workbook_report.py --input-xlsx "FULL_PATH_TO_MARCH_WORKBOOK.xlsx" --force-period 2026-03 --strict-sheets --output-tag eom_2026_03

### B. Dry-run validation (required gate)

From azure_functions:
python validate_dispatch_dry_run.py --outputs-dir ../data/outputs --force-period 2026-03 --test-recipient bradwilcock01@gmail.com

Checks enforced:
- recipients resolve
- subjects resolve
- body renders
- title month/year matches forced period

### C. Test send to one inbox (optional but recommended)

Management:
python test_dispatch_local.py --skip-refresh

Core:
python test_core_market_local.py --skip-refresh

USA (if needed):
use TEST_USA_SPA_RECIPIENTS and send in local mode after loading local.settings.json

### D. Production send (management + core only when requested)

- Use config/dispatch_recipients.json as source of truth.
- Disable TEST overrides.
- Send management then core.
- Keep USA excluded unless explicitly requested.

### E. Archive/cleanup

Use local side script:
python ../cleanup_sales_report_outputs.py --organize-all --recursive --include-nontimestamped --apply

Resulting structure:
- data/outputs/archive/YYYY-MM-DD/HH-MM-SS/report_type/


## 3) Known Settings to Verify Before Send

- REPORT_DISPATCH_GRAPH_SENDER is set
- CORE_MARKET_SEND_PDF is true/false as intended
- TEST_* recipient vars are empty for production send
- REPORT_DISPATCH_OUTPUTS_PATH points to repo data/outputs
- Core and management recipient lists are current in config/dispatch_recipients.json
