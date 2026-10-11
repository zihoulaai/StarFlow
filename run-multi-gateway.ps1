<# 
.SYNOPSIS
    Start multiple gateway instances for multi-instance fanout testing.

.DESCRIPTION
    Spins up N gateway instances on consecutive ports with different INSTANCE_ID.
    Uses memory bus (single-node) or Redis bus (multi-node).

.PARAMETER Instances
    Number of gateway instances (default 3)
.PARAMETER BasePort
    Starting port for data plane (default 8080). Each instance gets +3 ports.
.PARAMETER MemoryGB
    GOMEMLIMIT per instance in GiB (default 2)
.PARAMETER Procs
    GOMAXPROCS per instance (default 4)
.PARAMETER BusKind
    "memory" (single-node fanout) or "redis" (multi-node)
.PARAMETER RedisAddr
    Redis address for bus (default 127.0.0.1:6379)
.PARAMETER KeepRunning
    Leave gateways running after script ends
.PARAMETER Verbose
    Verbose output
#>

[CmdletBinding()]
param(
    [int]$Instances = 3,
    [int]$BasePort = 8080,
    [int]$MemoryGB = 2,
    [int]$Procs = 4,
    [string]$BusKind = "memory",
    [string]$RedisAddr = "127.0.0.1:6379",
    [switch]$KeepRunning,
    [switch]$VerboseOutput
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-VerboseEx { if ($VerboseOutput) { Write-Verbose @_ } }
function Write-Step { Write-Host "==> $@" -ForegroundColor Cyan }
function Write-OK { Write-Host "  $@" -ForegroundColor Green }
function Write-ERR { Write-Host "  $@" -ForegroundColor Red }

$RepoRoot = (Resolve-Path .).Path
$binDir = Join-Path $RepoRoot "bin"
$gatewayExe = Join-Path $binDir "gateway.exe"

# Common secret
$secret = (1..32 | ForEach-Object { ("{0:x}" -f (Get-Random -Max 16)) }) -join ""

$global:GatewayProcs = @()

function Stop-AllGateways {
    Write-Step "Stopping all gateways..."
    foreach ($p in $global:GatewayProcs) {
        if (-not $p.HasExited) {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
if (-not $KeepRunning) { Set-Alias -Name Cleanup -Value Stop-AllGateways -Option AllScope }

# Start instances
for ($i = 0; $i -lt $Instances; $i++) {
    $instanceId = $i + 1
    $listenPort = $BasePort + $i * 3
    $adminPort = $listenPort + 1
    $readyPort = $listenPort + 2

    $gatewayLog = Join-Path $RepoRoot "gateway-$instanceId.log"
    $gatewayErrLog = Join-Path $RepoRoot "gateway-$instanceId-err.log"

    $env:GOMEMLIMIT_BYTES = ($MemoryGB * 1024 * 1024 * 1024).ToString()
    $env:GOMAXPROCS = $Procs.ToString()
    $env:CONTROL_DSN = "starflow.db?_pragma=foreign_keys(1)"
    $env:SECRET_SOURCE = "env"
    $env:SECRET_REF = "PUSH_TOKEN_SECRET"
    $env:PUSH_TOKEN_SECRET = $secret
    $env:BOOTSTRAP_TENANT = "t1"
    $env:BOOTSTRAP_SUBJECT = "ops"
    $env:BOOTSTRAP_ROLE = "admin"
    $env:INSTANCE_ID = $instanceId.ToString()
    $env:LISTEN = ":$listenPort"
    $env:ADMIN_LISTEN = ":$adminPort"
    $env:READY_LISTEN = ":$readyPort"
    $env:BUS_KIND = $BusKind
    $env:BUS_ADDR = $RedisAddr
    $env:BUS_CHANNEL = "push:admitted"
    $env:BUS_CONFIG_CHANNEL = "push:config"

    Write-Step "Starting gateway instance $instanceId (ports $listenPort/$adminPort/$readyPort)..."
    
    $proc = Start-Process -FilePath $gatewayExe `
        -WindowStyle Hidden -PassThru `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $gatewayLog `
        -RedirectStandardError $gatewayErrLog

    $global:GatewayProcs += $proc
    Write-VerboseEx "  PID: $($proc.Id), Logs: $gatewayLog / $gatewayErrLog"
}

# Wait for all to be ready
Write-Step "Waiting for all gateways to become ready..."
$allReady = $false
for ($attempt = 0; $attempt -lt 60; $attempt++) {
    $readyCount = 0
    foreach ($p in $global:GatewayProcs) {
        $idx = $global:GatewayProcs.IndexOf($p)
        $readyPort = $BasePort + $idx * 3 + 2
        try {
            $resp = Invoke-RestMethod -Uri "http://127.0.0.1:$readyPort/readyz" -Method Get -TimeoutSec 1 -ErrorAction Stop
            if ($resp.Ready -eq $true) { $readyCount++ }
        } catch { }
    }
    if ($readyCount -eq $Instances) {
        $allReady = $true
        break
    }
    Write-Host "  $readyCount/$Instances ready..." -NoNewline
    Start-Sleep -Seconds 1
}
if (-not $allReady) {
    Write-ERR "Not all gateways became ready"
    foreach ($p in $global:GatewayProcs) {
        $idx = $global:GatewayProcs.IndexOf($p)
        $errLog = Join-Path $RepoRoot "gateway-$($idx+1)-err.log"
        Write-ERR "--- Instance $($idx+1) stderr ---"
        Get-Content $errLog -Tail 30 -ErrorAction SilentlyContinue
    }
    exit 1
}
Write-OK "All $Instances gateways ready!"

# Output connection info
Write-Host ""
Write-Host "Gateway endpoints:" -ForegroundColor Yellow
for ($i = 0; $i -lt $Instances; $i++) {
    $listenPort = $BasePort + $i * 3
    $adminPort = $listenPort + 1
    $readyPort = $listenPort + 2
    Write-Host "  Instance $($i+1): data=:$listenPort admin=:$adminPort ready=:$readyPort"
}
Write-Host ""
Write-Host "Publish URL (use any instance): http://127.0.0.1:$BasePort/publish" -ForegroundColor Green
Write-Host "Admin URL: http://127.0.0.1:$($BasePort+1)/admin/*" -ForegroundColor Green
Write-Host ""
Write-Host "Press Ctrl+C to stop all gateways..." -ForegroundColor Yellow

# Keep running
try {
    while ($true) { Start-Sleep -Seconds 10 }
} finally {
    Stop-AllGateways
}