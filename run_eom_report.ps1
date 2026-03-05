param(
    [Parameter(Mandatory = $true)]
    [string]$InputXlsx,

    [Parameter(Mandatory = $true)]
    [string]$ForcePeriod,

    [string]$OutputTag = "",
    [switch]$StrictSheets
)

Set-Location $PSScriptRoot

$cmd = @(
    "src/eom_workbook_report.py",
    "--input-xlsx", $InputXlsx,
    "--force-period", $ForcePeriod
)

if ($StrictSheets) {
    $cmd += "--strict-sheets"
}

if ($OutputTag -ne "") {
    $cmd += @("--output-tag", $OutputTag)
}

python @cmd
