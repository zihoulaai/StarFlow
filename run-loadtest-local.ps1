<# 
.SYNOPSIS
    Local full-stack load test for StarFlow on a single Windows machine.
    Spins up gateway + loadgen with sensible defaults for the host hardware.

.DESCRIPTION
    This script:
    1. Builds gateway and loadgen binaries
    2. Starts gateway with GOMEMLIMIT/GOMAXPROCS tuned for 12C/24T + 32 GB RAM
    3. Waits for readiness
    4. Creates tenant/app via admin API
    5. Runs loadgen with configurable connections/QPS/channels
    6. Emits JSON report and exits non-zero on SLA miss

    Requirements:
    - Go 1.22+ in PATH
    - Run from repo root (D:\trae_workspace\StarFlow)
    - Admin PowerShell for port range expansion (optional, see -ExpandPorts)

.PARAMETER Connections
    Total subscriber connections to open (default 10000)
.PARAMETER PublishQPS
    Target publish rate across all channels (default 500)
.PARAMETER Duration
    Steady-state measurement window (default 60s)
.PARAMETER Ramp
    Connection ramp-up window (default 10s)
.PARAMETER ChannelSpan
    Spread subscribers/publishes over N channels (default 20)
.PARAMETER Probes
    Connections that contribute latency samples (default 16)
.PARAMETER Concurrency
    Concurrent publish workers (default 8)
.PARAMETER GatewayMemoryGB
    GOMEMLIMIT for gateway in GiB (default 4)
.PARAMETER GatewayProcs
    GOMAXPROCS for gateway (default 10)
.PARAMETER ExpandPorts
    Run netsh to expand ephemeral port range (requires Admin, persists across reboot)
.PARAMETER ReportPath
    Output JSON report path (default .\capacity-<timestamp>.json)
.PARAMETER KeepGateway
    Leave gateway running after test (default $false)
.PARAMETER Verbose
    Verbose output
#>

[CmdletBinding()]
param(
    [int]$Connections = 10000,
    [int]$PublishQPS = 500,
    [string]$Duration = "60s",
    [string]$Ramp = "10s",
    [int]$ChannelSpan = 20,
    [int]$Probes = 16,
    [int]$Concurrency = 8,
    [int]$GatewayMemoryGB = 4,
    [int]$GatewayProcs = 10,
    [switch]$ExpandPorts,
    [string]$ReportPath = "",
    [switch]$KeepGateway,
    [switch]$VerboseOutput
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-VerboseEx { if ($VerboseOutput) { Write-Verbose @_ } }
function Write-Step { Write-Host "==> $@" -ForegroundColor Cyan }

# --- expand ephemeral port range (requires Admin) ---
if ($ExpandPorts) {
    Write-Step "Expanding ephemeral port range (requires Admin)..."
    try {
        netsh int ipv4 set dynamicport tcp start=10000 num=55535
        netsh int ipv6 set dynamicport tcp start=10000 num=55535
        Write-Host "  Done. Requires reboot to take effect." -ForegroundColor Yellow
    } catch {
        Write-Warning "  Failed (run as Admin): $($Error[0])"
    }
}

# --- repo root ---
$RepoRoot = (Resolve-Path .).Path
Write-VerboseEx "Repo root: $RepoRoot"

# --- build binaries ---
Write-Step "Building binaries..."
$binDir = Join-Path $RepoRoot "bin"
if (-not (Test-Path $binDir)) { New-Item -ItemType Directory -Path $binDir | Out-Null }

$env:GOFLAGS = "-mod=vendor"
go build -o (Join-Path $binDir "gateway.exe") ./cmd/gateway
go build -o (Join-Path $binDir "loadgen.exe") ./cmd/loadgen
Write-VerboseEx "Binaries in $binDir"

# --- env ---
$secret = (1..32 | ForEach-Object { ("{0:x}" -f (Get-Random -Max 16)) }) -join ""
$env:GOMEMLIMIT_BYTES = ($GatewayMemoryGB * 1024 * 1024 * 1024).ToString()
$env:GOMAXPROCS = $GatewayProcs.ToString()
$env:CONTROL_DSN = "starflow.db?_pragma=foreign_keys(1)"
$env:SECRET_SOURCE = "env"
$env:SECRET_REF = "PUSH_TOKEN_SECRET"
$env:PUSH_TOKEN_SECRET = $secret
$env:BOOTSTRAP_TENANT = "t1"
$env:BOOTSTRAP_SUBJECT = "ops"
$env:BOOTSTRAP_ROLE = "admin"
$env:INSTANCE_ID = "1"
$env:LISTEN = ":8080"
$env:ADMIN_LISTEN = ":8081"
$env:READY_LISTEN = ":8082"

Write-VerboseEx "GOMEMLIMIT=$env:GOMEMLIMIT_BYTES GOMAXPROCS=$env:GOMAXPROCS"

# --- start gateway ---
Write-Step "Starting gateway..."
$gatewayLog = Join-Path $RepoRoot "gateway-local.log"
$gatewayErrLog = Join-Path $RepoRoot "gateway-local-err.log"

# Start gateway.exe directly; it inherits $env: vars from this process
$gatewayExe = Join-Path $binDir "gateway.exe"
$gatewayProc = Start-Process -FilePath $gatewayExe `
    -WindowStyle Hidden -PassThru `
    -WorkingDirectory $RepoRoot `
    -RedirectStandardOutput $gatewayLog `
    -RedirectStandardError $gatewayErrLog

Write-VerboseEx "Gateway PID: $($gatewayProc.Id)"

function Stop-Gateway {
    if ($gatewayProc -and !$gatewayProc.HasExited) {
        Write-Step "Stopping gateway (PID $($gatewayProc.Id))..."
        Stop-Process -Id $gatewayProc.Id -Force -ErrorAction SilentlyContinue
    }
}
if (-not $KeepGateway) { Set-Alias -Name Cleanup -Value Stop-Gateway -Option AllScope }

# --- wait for ready ---
Write-Step "Waiting for /readyz..."
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    try {
        $resp = Invoke-RestMethod -Uri "http://127.0.0.1:8082/readyz" -Method Get -TimeoutSec 2 -ErrorAction Stop
        if ($resp.Ready -eq $true) { $ready = $true; break }
    } catch { }
    Start-Sleep -Seconds 1
}
if (-not $ready) {
    Write-Error "Gateway did not become ready in 60s. Check $gatewayLog"
    Get-Content $gatewayLog -Tail 50
    Stop-Gateway
    exit 1
}
Write-Host "  Gateway ready" -ForegroundColor Green

# --- create tenant/app via admin API ---
Write-Step "Creating tenant & application..."
$adminHeaders = @{
    "X-Tenant-Id" = "t1"
    "X-Admin-Subject" = "ops"
    "Content-Type" = "application/json"
}

$tenantResp = Invoke-RestMethod -Uri "http://127.0.0.1:8081/admin/tenants" -Method Post -Headers $adminHeaders -Body '{"name":"loadtest"}'
Write-VerboseEx "Tenant: $($tenantResp | ConvertTo-Json -Depth 3)"

$appResp = Invoke-RestMethod -Uri "http://127.0.0.1:8081/admin/applications" -Method Post -Headers $adminHeaders -Body '{"name":"loadgen","can_publish":true,"can_subscribe":true}'
$appKey = $appResp.application.app_key
$appSecret = $appResp.secret
Write-Host "  AppKey: $appKey" -ForegroundColor Green
Write-Host "  Secret: $appSecret" -ForegroundColor Green

# --- channels (set limits via PATCH) ---
$subsPerCh = [math]::Ceiling($Connections / $ChannelSpan)
$maxSubs = $subsPerCh * 2
for ($i = 0; $i -lt $ChannelSpan; $i++) {
    $ch = "loadtest:$i"
    $channels += $ch
    $chBody = @{max_subscribers=$maxSubs} | ConvertTo-Json -Compress
    Invoke-RestMethod -Uri "http://127.0.0.1:8081/admin/channels?channel=$ch" -Method Patch -Headers $adminHeaders -Body $chBody | Out-Null
}
Write-VerboseEx "Set limits for $ChannelSpan channels"

# --- run loadgen ---
if (-not $ReportPath) {
    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $ReportPath = Join-Path $RepoRoot "capacity-$ts.json"
}

Write-Step "Running loadgen..."
Write-Host "  Connections: $Connections | QPS: $PublishQPS | Duration: $Duration | Channels: $ChannelSpan | Probes: $Probes"

$loadgenArgs = @(
    "--publish-url", "http://127.0.0.1:8080/publish",
    "--tenant", "t1",
    "--app-key", $appKey,
    "--secret", $appSecret,
    "--channel", "loadtest:0",
    "--connections", $Connections.ToString(),
    "--publish-qps", $PublishQPS.ToString(),
    "--duration", $Duration,
    "--ramp", $Ramp,
    "--channel-span", $ChannelSpan.ToString(),
    "--probes", $Probes.ToString(),
    "--concurrency", $Concurrency.ToString(),
    "--report", $ReportPath
)

$loadgenExit = 0
try {
    & (Join-Path $binDir "loadgen.exe") $loadgenArgs
    $loadgenExit = $LASTEXITCODE
} catch {
    Write-Error "loadgen failed: $($Error[0])"
    $loadgenExit = 1
}

# --- cleanup ---
if (-not $KeepGateway) { Stop-Gateway }

# --- result ---
if ($loadgenExit -eq 0) {
    Write-Host "`n=== LOAD TEST PASSED ===" -ForegroundColor Green
    Write-Host "Report: $ReportPath"
} else {
    Write-Host "`n=== LOAD TEST FAILED (exit $loadgenExit) ===" -ForegroundColor Red
    Write-Host "Report: $ReportPath"
}

exit $loadgenExit