# ==============================================================================
# MailBaby Compilation & Build Script (Windows PowerShell)
# ==============================================================================

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $RootDir "build\bin"
$BinaryName = "mailbaby.exe"
$OutputBin = Join-Path $OutputDir $BinaryName

$Version = if ($env:VERSION) { $env:VERSION } else { "1.0.0" }
$Commit = try { (git rev-parse --short HEAD 2>$null) } catch { "dev" }
if (-not $Commit) { $Commit = "dev" }
$BuildDate = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host " Building MailBaby (Windows)" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host " Version    : $Version"
Write-Host " Commit     : $Commit"
Write-Host " Build Date : $BuildDate"
Write-Host " Output     : $OutputBin"
Write-Host "=================================================="

if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

Set-Location $RootDir

$GoPath = "go"
if (Test-Path "C:\Users\ZhenYi\sdk\go1.26.6\bin\go.exe") {
    $GoPath = "C:\Users\ZhenYi\sdk\go1.26.6\bin\go.exe"
}

$env:CGO_ENABLED = "0"
$LdFlags = "-s -w -X mailbaby/internal/cmd.Version=$Version -X mailbaby/internal/cmd.Commit=$Commit -X mailbaby/internal/cmd.BuildDate=$BuildDate"

& $GoPath build -trimpath -ldflags "$LdFlags" -o "$OutputBin" .

if ($LASTEXITCODE -eq 0) {
    Write-Host "[SUCCESS] Binary compiled successfully: $OutputBin" -ForegroundColor Green
    Write-Host "Usage: .\build\bin\mailbaby.exe server -c config.yaml" -ForegroundColor Yellow
} else {
    Write-Host "[ERROR] Build failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}
