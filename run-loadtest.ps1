<# 
.SYNOPSIS
Runs StarFlow gateway + loadgen locally without Docker.

.DESCRIPTION
This script:
1. Starts the gateway with in-memory bus + SQLite
2. Bootstraps an admin principal
3. Creates a tenant, application, and channel via admin API
4. Runs loadgen against the local gateway

.REQUIREMENTS
- Go 1.22+
- Windows (PowerShell)
- No Docker required

.EXAMPLE
.\run-loadtest.ps1 -Connections 1000 -PublishQPS 10 -Duration 30s
#>

param(
    [int]$Connections = 1000,
    [int]$PublishQPS = 10,
    [string]$Duration = "30s",
    [string]$ReportPath = "capacity-report.json",
    [int]$ChannelSpan = 1,
    [int]$Probes = 8,
    [string]$Ramp = "10s",
    [string]$GatewayListen = ":8080",
    [string]$AdminListen = ":8081",
    [string]$ReadyListen = ":8082",
    [string]$DBPath = "starflow.db",
    [string]$Secret = "dev-secret-key-at-least-32-chars-long-for-signing",
    [string]$TenantId = "loadtest",
    [string]$AdminSubject = "loadtest-admin",
    [string]$AppName = "loadtest-app",
    [string]$ChannelName = "room:1"
)

$ErrorActionPreference = "Stop"

function Write-Section($title) {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════"
    Write-Host "  $title"
    Write-Host "═══════════════════════════════════════════════════════════"
}

function Start-Gateway {
    Write-Section "Starting Gateway"

    # Clean up old DB
    try {
        if (Test-Path $DBPath) { Remove-Item $DBPath -Force }
    } catch {
        Write-Warning "Could not remove old DB: $_"
    }

    # Create a temporary PowerShell script to start the gateway with proper env vars
    $gatewayScriptPath = Join-Path (Get-Location).Path "start-gateway.ps1"
    $logPath = Join-Path (Get-Location).Path "gateway.log"
    $global:gatewayLogPath = $logPath

    # Build script content using StringBuilder to avoid here-string issues
    $sb = [System.Text.StringBuilder]::new()
    $sb.AppendLine('$env:BUS_KIND = "memory"')
    $sb.AppendLine('$env:BUS_CHANNEL = "push:admitted"')
    $sb.AppendLine('$env:BUS_CONFIG_CHANNEL = "push:config"')
    $sb.AppendLine('$env:CONTROL_DSN = "file:{0}?cache=shared&mode=rwc"' -f $DBPath)
    $sb.AppendLine('$env:CONTROL_MAX_OPEN_CONNS = "16"')
    $sb.AppendLine('$env:CONTROL_POLL_INTERVAL = "15s"')
    $sb.AppendLine('$env:SECRET_SOURCE = "env"')
    $sb.AppendLine('$env:SECRET_REF = "PUSH_TOKEN_SECRET"')
    $sb.AppendLine('$env:PUSH_TOKEN_SECRET = "{0}"' -f $Secret)
    $sb.AppendLine('$env:BOOTSTRAP_TENANT = "{0}"' -f $TenantId)
    $sb.AppendLine('$env:BOOTSTRAP_SUBJECT = "{0}"' -f $AdminSubject)
    $sb.AppendLine('$env:BOOTSTRAP_ROLE = "admin"')
    $sb.AppendLine('$env:INSTANCE_ID = "1"')
    $sb.AppendLine('$env:LISTEN = "{0}"' -f $GatewayListen)
    $sb.AppendLine('$env:ADMIN_LISTEN = "{0}"' -f $AdminListen)
    $sb.AppendLine('$env:READY_LISTEN = "{0}"' -f $ReadyListen)
    $sb.AppendLine('$env:GOMEMLIMIT_BYTES = "4294967296"')
    $sb.AppendLine('$env:GOGC = "400"')
    $sb.AppendLine('$env:DELIVERY_FLUSH_INTERVAL = "10ms"')
    $sb.AppendLine('$env:DELIVERY_MERGE_RATIO = "20"')
    $sb.AppendLine('$env:DELIVERY_COMPRESS = "true"')
    $sb.AppendLine('$env:DELIVERY_COMPRESS_MIN_BATCH = "5"')
    $sb.AppendLine('$env:DELIVERY_PER_VIEWER_BPS = "2048"')
    $sb.AppendLine('$env:DELIVERY_HEARTBEAT_INTERVAL = "25s"')
    $sb.AppendLine('$env:DELIVERY_HEARTBEAT_TIMEOUT = "10s"')
    $sb.AppendLine('$env:DELIVERY_DRAIN_GRACE = "30s"')
    $sb.AppendLine('$env:DELIVERY_SHARDS = "1024"')
    $sb.AppendLine('$env:AUTH_TOKEN_TTL = "5m"')
    $sb.AppendLine('$env:AUTH_TOKEN_SKEW = "2m"')
    $sb.AppendLine('')
    # Run go directly without pipeline to ensure env vars are inherited
    $sb.AppendLine('go run ./cmd/gateway > "{0}" 2>&1' -f $logPath)
    
    $scriptContent = $sb.ToString()

    Set-Content -Path $gatewayScriptPath -Value $scriptContent -Encoding UTF8

    Write-Host "Starting gateway on $GatewayListen (admin: $AdminListen, ready: $ReadyListen)..."
    Write-Host "DB: $DBPath"
    Write-Host "Tenant: $TenantId, Admin: $AdminSubject"
    Write-Host "Gateway output logged to: $logPath"

    # Start gateway via the script file
    $gatewayProcess = Start-Process -FilePath "powershell" -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $gatewayScriptPath -PassThru -WorkingDirectory (Get-Location)
    $global:gatewayWrapperPid = $gatewayProcess.Id
    
    # The actual gateway is a child process (go run), we need to find it
    Write-Host "Gateway wrapper PID: $($global:gatewayWrapperPid)"
    
    # Wait a bit for the child process to start
    Start-Sleep -Seconds 3
    
    # Find the child go process
    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $global:gatewayWrapperPid" -ErrorAction SilentlyContinue
    if ($children) {
        $global:gatewayPid = $children.ProcessId
        Write-Host "Gateway PID (go run): $($global:gatewayPid)"
    } else {
        $global:gatewayPid = $global:gatewayWrapperPid
        Write-Host "Gateway PID: $($global:gatewayPid)"
    }

    # Wait for readiness
    Write-Host "Waiting for gateway to be ready..."
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $resp = Invoke-RestMethod -Uri "http://127.0.0.1:8082/readyz" -Method GET -ErrorAction Stop -TimeoutSec 2
            if ($resp.Ready -eq $true) { $ready = $true; break }
        } catch { }
        Start-Sleep -Seconds 1
        Write-Host "  ... waiting ($($i+1)/30)"
    }
    if (-not $ready) {
        throw "Gateway did not become ready in time"
    }
    Write-Host "Gateway ready!"
}

function Setup-TenantAndApp {
    Write-Section "Setting up Tenant, Application, and Channel"

    $headers = @{
        "X-Tenant-Id" = $TenantId
        "X-Admin-Subject" = $AdminSubject
        "Content-Type" = "application/json"
    }

    # Create tenant (might already exist from bootstrap)
    try {
        Write-Host "Creating tenant '$TenantId'..."
        $body = @{ id = $TenantId; name = "Load Test Tenant" } | ConvertTo-Json
        Invoke-RestMethod -Uri "http://127.0.0.1:8081/admin/tenants" -Method POST -Headers $headers -Body $body | Out-Null
        Write-Host "  Tenant created"
    } catch {
        if ($_.Exception.Response.StatusCode.value__ -eq 409) {
            Write-Host "  Tenant already exists (bootstrap)"
        } else {
            throw
        }
    }

    # Create application
    Write-Host "Creating application '$AppName'..."
    $body = @{ name = $AppName; can_publish = $true; can_subscribe = $true } | ConvertTo-Json
    $appResponse = Invoke-RestMethod -Uri "http://127.0.0.1:8081/admin/applications" -Method POST -Headers $headers -Body $body
    $global:AppKey = $appResponse.application.app_key
    $global:AppSecret = $appResponse.secret
    Write-Host "  App Key: $global:AppKey"
    Write-Host "  App Secret: $global:AppSecret (save this - shown only once!)"

    # Configure quota for high connection ceiling
    Write-Host "Setting quota for $Connections connections..."
    $body = @{
        max_connections = $Connections * 2  # headroom
        max_publish_qps = $PublishQPS * 2
        max_message_bytes = 4096
        publish_failure_policy = "degrade"
    } | ConvertTo-Json
    Invoke-RestMethod -Uri "http://127.0.0.1:8081/admin/quotas/$TenantId" -Method PUT -Headers $headers -Body $body | Out-Null
    Write-Host "  Quota set"

    # Configure channel (channel is passed as query parameter)
    Write-Host "Configuring channel '$ChannelName'..."
    $body = @{
        max_subscribers = $Connections * 2
        coalescing_enabled = $true
        merge_ratio_override = 20
        receive_rate_target = 50
    } | ConvertTo-Json
    $channelUri = "http://127.0.0.1:8081/admin/channels/?channel=$([System.Uri]::EscapeDataString($ChannelName))"
    Invoke-RestMethod -Uri $channelUri -Method PATCH -Headers $headers -Body $body | Out-Null
    Write-Host "  Channel configured"
}

function Run-LoadGen {
    Write-Section "Running Load Generator"

    $publishUrl = "http://127.0.0.1:8080/publish"
    Write-Host "Target: $publishUrl"
    Write-Host "Connections: $Connections | Publish QPS: $PublishQPS | Duration: $Duration"
    Write-Host "Channel: $ChannelName | Channel Span: $ChannelSpan | Probes: $Probes"
    Write-Host "Report: $ReportPath"

    $args = @(
        "run", "./cmd/loadgen",
        "--publish-url", $publishUrl,
        "--tenant", $TenantId,
        "--app-key", $global:AppKey,
        "--secret", $global:AppSecret,
        "--channel", $ChannelName,
        "--connections", $Connections,
        "--publish-qps", $PublishQPS,
        "--duration", $Duration,
        "--ramp", $Ramp,
        "--channel-span", $ChannelSpan,
        "--probes", $Probes,
        "--report", $ReportPath
    )

    Write-Host "Executing: go $($args -join ' ')"
    Write-Host ""

    # Run loadgen and capture exit code
    $process = Start-Process -FilePath "go" -ArgumentList $args -Wait -PassThru -WorkingDirectory (Get-Location) -NoNewWindow
    $exitCode = $process.ExitCode

    Write-Host ""
    Write-Host "Loadgen exited with code: $exitCode"

    if (Test-Path $ReportPath) {
        Write-Host ""
        Write-Section "Report Summary"
        $jsonContent = Get-Content $ReportPath -Raw
        # Use JavaScriptSerializer as ConvertFrom-Json has issues
        Add-Type -AssemblyName System.Web.Extensions
        $serializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer
        $report = $serializer.DeserializeObject($jsonContent)
        
        Write-Host "Duration: $($report["duration_seconds"])s"
        Write-Host "Target Connections: $($report["target_connections"])"
        Write-Host "Target QPS: $($report["target_publish_qps"])"
        Write-Host "Offered QPS: $($report["offered_qps"])"
        Write-Host "Sent: $($report["sent"]) | Accepted: $($report["accepted"]) | Refused: $($report["refused"]) | Failed: $($report["failed"])"
        Write-Host "Accept Rate: $("{0:P3}" -f $report["accept_rate"])"
        Write-Host "Latency p50/p95/p99 (publish): $($report["latency_p50_ms"])/$($report["latency_p95_ms"])/$($report["latency_p99_ms"]) ms"
        if ($report.ContainsKey("delivery")) {
            $d = $report["delivery"]
            Write-Host "Connections Ready: $($d["ready"])/$($d["target_connections"])"
            Write-Host "Delivered: $($d["delivered"]) | Bytes: $($d["bytes_delivered"])"
            Write-Host "Egress: $([math]::Round($d["egress_bytes_per_second"])) B/s total, $([math]::Round($d["egress_per_viewer_bytes_per_second"])) B/s per viewer"
            Write-Host "Delivery Latency p50/p95/p99: $($d["delivery_p50_ms"])/$($d["delivery_p95_ms"])/$($d["delivery_p99_ms"]) ms"
            Write-Host "Gap Notices: $($d["gap_notices"])"
            Write-Host "Fanout Factor: $("{0:F2}" -f $report["fanout_factor"])"
        }
        Write-Host "Conforms: $($report["conforms"])"
        if ($report.ContainsKey("failures")) {
            Write-Host "Failures:"
            foreach ($f in $report["failures"]) {
                Write-Host "  - $f"
            }
        }
    }

    return $exitCode
}

function Stop-Gateway {
    Write-Section "Stopping Gateway"
    try {
        # Kill the gateway child process (go run) - handle array of PIDs
        if ($global:gatewayPid) {
            $pids = @($global:gatewayPid)
            foreach ($pid in $pids) {
                $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
                if ($proc) {
                    $proc.Kill()
                    $proc.WaitForExit(5000)
                    Write-Host "Gateway child process (PID: $pid) stopped"
                }
            }
        }
        # Kill the wrapper PowerShell process
        if ($global:gatewayWrapperPid) {
            $proc = Get-Process -Id $global:gatewayWrapperPid -ErrorAction SilentlyContinue
            if ($proc) {
                $proc.Kill()
                $proc.WaitForExit(5000)
                Write-Host "Gateway wrapper process (PID: $global:gatewayWrapperPid) stopped"
            }
        }
        # Clean up any remaining go processes started recently
        Get-Process -Name "go" -ErrorAction SilentlyContinue | Where-Object { $_.StartTime -gt (Get-Date).AddMinutes(-10) } | Stop-Process -Force -ErrorAction SilentlyContinue
        # Also kill any powershell processes running start-gateway.ps1
        Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe' AND CommandLine LIKE '%start-gateway%'" -ErrorAction SilentlyContinue | ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }
        Start-Sleep -Seconds 1  # Allow file handles to release
        Write-Host "Gateway stopped"
    } catch {
        Write-Warning "Failed to stop gateway: $_"
    }
}

# Main execution
try {
    Write-Host "StarFlow Local Load Test Runner"
    Write-Host "================================="

    # Check Go is available
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
        throw "Go not found in PATH"
    }

    Start-Gateway
    Setup-TenantAndApp
    $exitCode = Run-LoadGen

    if ($exitCode -eq 0) {
        Write-Host ""
        Write-Host "✅ Load test PASSED (conforms to all targets)"
    } else {
        Write-Host ""
        Write-Host "❌ Load test FAILED (does not conform)"
    }

    exit $exitCode
}
finally {
    Stop-Gateway
}