param(
    [Parameter(Mandatory=$true)]
    [string]$SessionCookie,
    [string]$WebAppName = "qms-sales-report",
    [string]$ResourceGroup = "DefaultResourceGroup-DEWC",
    [string]$Slot = "staging"
)

$ErrorActionPreference = "Stop"

$webHost = az webapp show --name $WebAppName --resource-group $ResourceGroup --slot $Slot --query defaultHostName -o tsv
if (-not $webHost) {
    throw "Could not resolve web host"
}

$headers = @{
    "Cookie" = "AppServiceAuthSession=$SessionCookie"
    "Accept" = "application/json"
}

Write-Host "Host: $webHost"

$statusResp = Invoke-WebRequest -Uri ("https://" + $webHost + '/status') -Headers $headers -Method GET -UseBasicParsing -TimeoutSec 30
Write-Host "/status -> $($statusResp.StatusCode)"

$metricsResp = Invoke-WebRequest -Uri ("https://" + $webHost + '/metrics') -Headers $headers -Method GET -UseBasicParsing -TimeoutSec 30
Write-Host "/metrics -> $($metricsResp.StatusCode)"

$raw = $statusResp.Content
if ($raw -match "<title>Sign in to your account</title>" -or $raw -match "login.microsoftonline.com") {
    Write-Host ""
    Write-Host "AUTH RESULT: Session cookie was not accepted for API auth (received AAD sign-in HTML)."
    Write-Host "Likely causes: expired cookie, cookie copied from wrong domain, or partial copy."
    Write-Host ""
    Write-Host "Body preview:"
    Write-Host ($raw.Substring(0, [Math]::Min(300, $raw.Length)))
    exit 2
}

try {
    $statusJson = $raw | ConvertFrom-Json
}
catch {
    Write-Host ""
    Write-Host "AUTH RESULT: Non-JSON response received from /status."
    Write-Host "Body preview:"
    Write-Host ($raw.Substring(0, [Math]::Min(300, $raw.Length)))
    exit 3
}
Write-Host "last_run: $($statusJson.last_run)"
Write-Host "next_auto_refresh_at: $($statusJson.next_auto_refresh_at)"
Write-Host "running: $($statusJson.running)"
Write-Host "error: $($statusJson.error)"
Write-Host "output_length: $($statusJson.output.Length)"

Write-Host ""
Write-Host "Raw /status JSON:"
$statusResp.Content
