param(
    [string]$WebAppName = "qms-sales-report",
    [string]$ResourceGroup = "DefaultResourceGroup-DEWC",
    [string]$Slot = "staging",
    [string]$TenantId = "61fe0a1f-d1f0-4745-a200-a176d479ed9d",
    [string]$ClientId = "a0d2083a-fa75-4980-bb6a-1997e30c9a6f"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/4] Resolving host..."
$webHost = az webapp show --name $WebAppName --resource-group $ResourceGroup --slot $Slot --query defaultHostName -o tsv
if (-not $webHost) {
    throw "Could not resolve web host"
}
Write-Host "Host: $webHost"

Write-Host "[2/4] Getting AAD token for app audience..."
$resource = "api://$ClientId"
$token = az account get-access-token --tenant $TenantId --resource $resource --query accessToken -o tsv
if (-not $token) {
    throw "Failed to acquire token for $resource. Run: az login --tenant $TenantId --scope '$resource/.default'"
}

Write-Host "[3/4] Probing /.auth/me ..."
$me = Invoke-WebRequest -Uri ("https://" + $webHost + '/.auth/me') -Headers @{ Authorization = "Bearer $token" } -Method GET -TimeoutSec 30
Write-Host "/.auth/me -> $($me.StatusCode)"
$meJson = $me.Content | ConvertFrom-Json
$principal = $null
if ($meJson -and $meJson.Count -gt 0) {
    $principal = $meJson[0].user_id
}
Write-Host "Principal: $principal"

Write-Host "[4/4] Probing /status ..."
$statusResp = Invoke-WebRequest -Uri ("https://" + $webHost + '/status') -Headers @{ Authorization = "Bearer $token"; Accept = "application/json" } -Method GET -TimeoutSec 30
Write-Host "/status -> $($statusResp.StatusCode)"
$statusJson = $statusResp.Content | ConvertFrom-Json

Write-Host "last_run: $($statusJson.last_run)"
Write-Host "next_auto_refresh_at: $($statusJson.next_auto_refresh_at)"
Write-Host "running: $($statusJson.running)"
Write-Host "error: $($statusJson.error)"

Write-Host ""
Write-Host "Raw status JSON:"
$statusResp.Content
