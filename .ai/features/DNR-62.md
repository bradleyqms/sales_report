# EOM CLI Workflow: Workbook Ingestion, Report Generation, and Multi-Dispatch Email Automation

## Ticket
- DNR-62

## Summary
Implemented an end-to-end CLI-driven EOM February reporting workflow in `sales_report_v2_independent`, including workbook-based data ingestion, report generation, and Graph email dispatch testing.

## Scope Delivered
- Added standalone EOM report runner from multi-sheet workbook:
  - `src/eom_workbook_report.py`
  - Supports forced period (`--force-period`), strict sheet validation, output tagging, and optional output directory.
- Added workbook ingestion support for sheets:
  - Required: `GmbH`, `AG`, `UK`, `Inc`
  - Optional: `Export`
  - Added alias handling for `Sales Amount` on Export sheet.
- Ensured EOM date anchoring across outputs:
  - Filenames include period token (e.g., `EOM_20260228`)
  - Export/report title date handling aligned to forced report date.
- Updated report date consistency in render/export paths:
  - `src/base_report_generator.py`
  - `src/receivables_report_generator.py`
- Added lightweight PowerShell wrappers:
  - `run_eom_report.ps1` (run EOM workbook report)
  - `send_dispatch_email.ps1` (send dispatch email from CLI)
- Added CLI email dispatch utility:
  - `src/dispatch_email_cli.py`
  - Dispatch types: `management`, `core`, `usa`
  - Sender override support (`--sender`)
  - Mode-specific attachments:
    - Management: combined PDF + combined XLSX
    - Core: core markets PDF
    - USA: HTML body only
  - Improved subject clarity format:
    - `QMS Dispatch | <Report Type> | DD.MM.YYYY`
- Fixed management body composition to include both tables:
  - Combined management HTML + core markets HTML in one management email.
- Fixed email/table title date mismatch (`1-4` vs `1-28`) by passing anchored `report_date` through HTML body builder and dispatch entrypoints.

## Testing / Validation
- Automated tests:
  - `pytest tests/test_eom_workbook_report.py tests/test_date_logic.py -q` passed.
- Live dispatch tests to `bradwilcock01@gmail.com` completed successfully for:
  - Management (2 attachments: PDF + XLSX)
  - Core Markets (1 attachment: PDF)
  - USA Spa (0 attachments, HTML inline)
- Terminal logs confirm successful Graph send and attachment selection.

## Notes
- USA Spa remained lower than expected due to mapping/channel allocation (significant USA value mapped to `eCommerce USA`/`Amazon`, not `Spa`), not ingestion failure.
- Workflow is ready for repeatable CLI-triggered EOM runs and targeted dispatches.

For future months (e.g., EOM March), the flow is repeatable with 3 steps.

1) Prepare the monthly workbook

Use the same template with sheets:
Required: GmbH, AG, UK, Inc
Optional: Export
Keep value columns in supported names (Total AR Invoice, Sales Amount, etc.).
2) Generate the EOM report pack

From project root:
.\run_eom_report.ps1 -InputXlsx "EOM_March_2026.xlsx" -ForcePeriod "2026-03" -StrictSheets -OutputTag "mar_eom"
This creates outputs in data/outputs with EOM token in filenames, e.g. EOM_20260331.
3) Dispatch emails by audience

Management:
.[send_dispatch_email.ps1](http://_vscodecontentref_/1) -DispatchType management -RecipientGroup management -Sender "bradley@qmsmedicosmetics.com"
Core:
.[send_dispatch_email.ps1](http://_vscodecontentref_/2) -DispatchType core -RecipientGroup core -Sender "bradley@qmsmedicosmetics.com"
USA:
.[send_dispatch_email.ps1](http://_vscodecontentref_/3) -DispatchType usa -RecipientGroup usa -Sender "bradley@qmsmedicosmetics.com"
Safety / best practice

Preview first (no send):
add -DryRun to each command.
Recipient groups are in config/dispatch_recipients.json; keep enabled=false until approved.
Subjects auto-format by period:
EOM Management Sales Report DD.MM.YYYY
EOM Core Market Sales Report DD.MM.YYYY
EOM USA Sales Report DD.MM.YYYY