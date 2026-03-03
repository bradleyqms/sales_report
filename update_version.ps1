# Update version.json with current git info
$ErrorActionPreference = 'Stop'

Write-Host 'Updating version.json...' -ForegroundColor Cyan

try {
    $gitCommit = git rev-parse --short HEAD 2>$null
    $gitBranch = git rev-parse --abbrev-ref HEAD 2>$null
    $deployedAt = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
    $rawTag = git describe --tags --exact-match 2>$null
    if ($rawTag) {
        $version = $rawTag -replace '^v', ''
    } else {
        $version = "dev-$gitCommit"
    }
    
    $versionInfo = @{
        version = $version
        git_commit = $gitCommit
        git_branch = $gitBranch
        deployed_at = $deployedAt
        description = 'Sales Report v2'
    }
    
    $versionJson = $versionInfo | ConvertTo-Json -Depth 10
    $versionJson | Out-File -FilePath 'version.json' -Encoding UTF8 -NoNewline
    
    Write-Host 'Version updated!' -ForegroundColor Green
    Write-Host "  Version : $version"
    Write-Host "  Branch  : $gitBranch"
    Write-Host "  Commit  : $gitCommit"
    if ($rawTag) {
        Write-Host "  Source  : git tag ($rawTag)" -ForegroundColor DarkGray
    } else {
        Write-Host "  Source  : no tag found, using commit SHA" -ForegroundColor Yellow
    }
    
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}
