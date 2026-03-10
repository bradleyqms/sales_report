param(
    [Parameter(Mandatory=$true)]
    [string]$InputXlsx,

    [Parameter(Mandatory=$true)]
    [string]$ForcePeriod,

    [string]$OutputTag = "monthly_dryrun",
    [string]$TestRecipient = "bradwilcock01@gmail.com"
)

$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

Write-Host "=== EOM Generate (Unified Workbook) ==="
python "src/eom_unified_workbook_report.py" `
  --input-xlsx $InputXlsx `
  --force-period $ForcePeriod `
  --strict-sheets `
  --output-tag $OutputTag

Write-Host "`n=== Validate report HTML titles ==="
$periodCompact = ($ForcePeriod -replace "-", "")
$periodToken = "EOM_${periodCompact}"
$patterns = @(
  "data/outputs/management_report_usa_spa_*${periodToken}*${OutputTag}*.html",
  "data/outputs/management_report_core_markets_*${periodToken}*${OutputTag}*.html",
  "data/outputs/combined_management_report_*${periodToken}*${OutputTag}*.html"
)

foreach ($pattern in $patterns) {
    $f = Get-ChildItem $pattern | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($null -eq $f) {
        throw "Missing expected HTML output for pattern: $pattern"
    }
    Write-Host "--- $($f.Name) ---"
    Select-String -Path $f.FullName -Pattern "<h2>" | Select-Object -First 1 | ForEach-Object { $_.Line }
}

Write-Host "`n=== Dry-run dispatch validation (subject/recipients/body) ==="
$env:TEST_REPORT_DISPATCH_RECIPIENTS = $TestRecipient
$env:TEST_CORE_MARKETS_RECIPIENTS = $TestRecipient
$env:TEST_USA_SPA_RECIPIENTS = $TestRecipient

Set-Location "azure_functions"
python "validate_dispatch_dry_run.py" --test-recipient $TestRecipient --outputs-dir "../data/outputs" --force-period $ForcePeriod

Write-Host "`n[OK] Monthly dry-run pipeline completed"
