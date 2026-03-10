<#
.SYNOPSIS
    EOM production send: generate (optional), validate, dispatch to real recipients.

.DESCRIPTION
    Reads production recipients from config/dispatch_recipients.json.
    Runs the dispatch validator with the --force-period month guard before sending.
    Prompts for explicit YES confirmation before any email is dispatched.

.PARAMETER ForcePeriod
    Reporting period in YYYY-MM format (e.g. 2026-02). Required.
    Used as the month guard in validation and logged in the confirmation prompt.

.PARAMETER Groups
    Comma-separated list of recipient groups to send to.
    Valid values: management, core, usa   (usa requires test_usa_spa_local.py)
    Default: "management,core"

.PARAMETER InputXlsx
    Path to the EOM source workbook (.xlsx). Required unless -SkipGenerate is set.

.PARAMETER OutputTag
    Tag appended to generated report filenames. Defaults to eom_YYYYMM.

.PARAMETER SkipGenerate
    Skip the report generation step. Use when outputs already exist in data/outputs/.

.EXAMPLE
    # Full pipeline: generate + validate + send
    .\run_eom_production_send.ps1 -ForcePeriod 2026-02 -InputXlsx "20260309T_feb26_workbook.xlsx"

.EXAMPLE
    # Skip generation (already generated), send management only
    .\run_eom_production_send.ps1 -ForcePeriod 2026-02 -SkipGenerate -Groups "management"

.EXAMPLE
    # All three groups
    .\run_eom_production_send.ps1 -ForcePeriod 2026-02 -SkipGenerate -Groups "management,core,usa"
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$ForcePeriod,

    [string]$Groups = "management,core",

    [string]$InputXlsx = "",

    [string]$OutputTag = "",

    [switch]$SkipGenerate
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Derive default output tag from period (e.g. 2026-02 -> eom_202602)
if (-not $OutputTag) {
    $OutputTag = "eom_$($ForcePeriod -replace '-', '')"
}

# Parse and validate group list
$groupList = $Groups.ToLower().Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
$validGroups = @("management", "core", "usa")
foreach ($g in $groupList) {
    if ($g -notin $validGroups) {
        Write-Error "Invalid group '$g'. Valid groups: $($validGroups -join ', ')"
        exit 1
    }
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  EOM PRODUCTION SEND  |  Period: $ForcePeriod" -ForegroundColor Cyan
Write-Host "  Groups: $($groupList -join ', ')" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# ─────────────────────────────────────────────────────────────
# 1. Generate reports (unless skipped)
# ─────────────────────────────────────────────────────────────
if (-not $SkipGenerate) {
    if (-not $InputXlsx) {
        Write-Error "-InputXlsx is required unless -SkipGenerate is used."
        exit 1
    }
    Write-Host "`n=== [1/4] EOM Report Generation ===" -ForegroundColor Cyan
    python "src/eom_unified_workbook_report.py" `
        --input-xlsx $InputXlsx `
        --force-period $ForcePeriod `
        --strict-sheets `
        --output-tag $OutputTag
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Report generation failed (exit $LASTEXITCODE)."
        exit $LASTEXITCODE
    }
    Write-Host "  [OK] Reports generated with tag: $OutputTag" -ForegroundColor Green
}
else {
    Write-Host "`n=== [1/4] Generation skipped (-SkipGenerate) ===" -ForegroundColor Yellow
}

# ─────────────────────────────────────────────────────────────
# 2. Load production recipients from dispatch_recipients.json
# ─────────────────────────────────────────────────────────────
Write-Host "`n=== [2/4] Loading production recipients ===" -ForegroundColor Cyan
$cfgPath = Join-Path $PSScriptRoot "config/dispatch_recipients.json"
if (-not (Test-Path $cfgPath)) {
    Write-Error "Recipients config not found: $cfgPath"
    exit 1
}
$recipientsCfg = Get-Content $cfgPath -Raw | ConvertFrom-Json

$recipientMap = @{}
foreach ($g in $groupList) {
    $node = $recipientsCfg.$g
    if ($null -eq $node -or $node.recipients.Count -eq 0) {
        Write-Error "No recipients found for group '$g' in $cfgPath"
        exit 1
    }
    # Join with semicolons — parse_recipients() accepts ; or ,
    $recipientMap[$g] = $node.recipients -join ";"
    Write-Host "  [$g] $($node.recipients.Count) recipients loaded" -ForegroundColor White
}

# ─────────────────────────────────────────────────────────────
# 3. Dispatch validation (force-period month guard)
# ─────────────────────────────────────────────────────────────
Write-Host "`n=== [3/4] Dispatch validation (force-period: $ForcePeriod) ===" -ForegroundColor Cyan
Push-Location "azure_functions"
try {
    # Run validator without test-recipient override so it reads from env/settings
    python "validate_dispatch_dry_run.py" `
        --outputs-dir "../data/outputs" `
        --force-period $ForcePeriod
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Validation failed — aborting production send."
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
Write-Host "  [OK] Validation passed." -ForegroundColor Green

# ─────────────────────────────────────────────────────────────
# 4. Confirmation prompt — list every recipient before sending
# ─────────────────────────────────────────────────────────────
Write-Host "`n=== [4/4] Production send confirmation ===" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Period  : $ForcePeriod" -ForegroundColor White
Write-Host "  Groups  : $($groupList -join ', ')" -ForegroundColor White
Write-Host ""
foreach ($g in $groupList) {
    $node = $recipientsCfg.$g
    Write-Host "  [$g]  ($($node.recipients.Count) recipients):" -ForegroundColor Cyan
    $node.recipients | ForEach-Object { Write-Host "    - $_" -ForegroundColor Gray }
}
Write-Host ""
Write-Host "  WARNING: This will send real emails to the addresses above." -ForegroundColor Red
Write-Host ""
$confirm = Read-Host "  Type YES to confirm and dispatch"
if ($confirm -ne "YES") {
    Write-Host "`n  Aborted — no emails sent." -ForegroundColor Yellow
    exit 0
}

# ─────────────────────────────────────────────────────────────
# 5. Clear test overrides, set production recipients, dispatch
# ─────────────────────────────────────────────────────────────
# Ensure test short-circuit vars are absent so scripts fall through to production vars
Remove-Item Env:TEST_REPORT_DISPATCH_RECIPIENTS -ErrorAction SilentlyContinue
Remove-Item Env:TEST_CORE_MARKETS_RECIPIENTS    -ErrorAction SilentlyContinue
Remove-Item Env:TEST_USA_SPA_RECIPIENTS         -ErrorAction SilentlyContinue

foreach ($g in $groupList) {
    $count = $recipientsCfg.$g.recipients.Count
    Write-Host "`n>>> [$g] Dispatching to $count recipients..." -ForegroundColor Green
    Push-Location "azure_functions"
    try {
        switch ($g) {
            "management" {
                $env:REPORT_DISPATCH_RECIPIENTS = $recipientMap[$g]
                python "test_dispatch_local.py" --skip-refresh
            }
            "core" {
                $env:CORE_MARKET_DISPATCH_RECIPIENTS = $recipientMap[$g]
                python "test_core_market_local.py" --skip-refresh
            }
            "usa" {
                # Placeholder: uncomment when test_usa_spa_local.py is available
                # $env:USA_SPA_DISPATCH_RECIPIENTS = $recipientMap[$g]
                # python "test_usa_spa_local.py" --skip-refresh
                Write-Warning "USA Spa: test_usa_spa_local.py not yet available — skipped."
            }
        }
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Dispatch failed for '$g' (exit $LASTEXITCODE)"
            exit $LASTEXITCODE
        }
    }
    finally {
        Pop-Location
    }
    Write-Host "  [OK] $g dispatched." -ForegroundColor Green
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "  EOM Production Send COMPLETE" -ForegroundColor Green
Write-Host "  Period: $ForcePeriod  |  Groups: $($groupList -join ', ')" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
