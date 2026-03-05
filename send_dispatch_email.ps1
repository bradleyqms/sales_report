param(
    [string]$To = "",

    [ValidateSet("management", "core", "usa")]
    [string]$DispatchType = "management",

    [string]$RecipientGroup = "",
    [string]$RecipientsJson = "config/dispatch_recipients.json",

    [string]$Sender = "",

    [string]$Subject = "",
    [string]$Body = "Please find the latest QMS sales data attached.",
    [switch]$Refresh,
    [switch]$DryRun
)

Set-Location $PSScriptRoot

$cmd = @(
    "src/dispatch_email_cli.py",
    "--dispatch-type", $DispatchType,
    "--body", $Body
)

if ($RecipientGroup -ne "") {
    $cmd += @("--recipient-group", $RecipientGroup, "--recipients-json", $RecipientsJson)
} elseif ($To -ne "") {
    $cmd += @("--to", $To)
} else {
    throw "Provide either -To or -RecipientGroup"
}

if ($Sender -ne "") {
    $cmd += @("--sender", $Sender)
}

if ($Subject -ne "") {
    $cmd += @("--subject", $Subject)
}

if ($Refresh) {
    $cmd += "--refresh"
}

if ($DryRun) {
    $cmd += "--dry-run"
}

python @cmd
