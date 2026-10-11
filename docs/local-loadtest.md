# Local Load Testing Without Docker

Run the full StarFlow load test pipeline on your machine without Docker.

## Prerequisites

- **Go 1.22+** installed and in PATH
- **Windows**: PowerShell 5.1+ (built-in)
- **Linux/macOS**: Bash + `jq` (`apt install jq` / `brew install jq`)
- **~4 GB RAM** for the gateway (configurable via `GOMEMLIMIT_BYTES`)

## Quick Start

### Windows (PowerShell)

```powershell
# Basic run (1000 connections, 10 msg/s, 30s)
.\run-loadtest.ps1

# Custom parameters
.\run-loadtest.ps1 -Connections 5000 -PublishQPS 50 -Duration 60s -ReportPath my-report.json
```

### Linux/macOS (Bash)

```bash
# Basic run
./run-loadtest.sh

# Custom parameters via environment variables
CONNECTIONS=5000 PUBLISH_QPS=50 DURATION=60s ./run-loadtest.sh
```

## What It Does

1. **Starts the gateway** with:
   - In-memory message bus (no Redis needed)
   - SQLite database (`starflow.db`)
   - Bootstrap admin principal
   - 4 GiB memory limit, tuned GC

2. **Provisions via admin API**:
   - Creates tenant (or uses bootstrapped one)
   - Creates application → returns `app_key` + `app_secret` (shown once!)
   - Sets quota for target connection count
   - Configures channel with coalescing enabled

3. **Runs loadgen** against local gateway:
   - Opens subscriber WebSocket connections (ramped over `--ramp`)
   - Publishes messages at `--publish-qps`
   - Measures: admission latency, delivery latency, egress, gap notices
   - Produces JSON report + conformance verdict (exit code)

## Key Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CONNECTIONS` | 1000 | Subscriber connections to open |
| `PUBLISH_QPS` | 10 | Publish rate (per channel) |
| `DURATION` | 30s | Steady-state measurement window |
| `RAMP` | 10s | Connection ramp window |
| `CHANNEL_SPAN` | 1 | Spread load over N channels |
| `PROBES` | 8 | Connections that sample latency |
| `GOMEMLIMIT_BYTES` | 4294967296 | Gateway memory limit (4 GiB) |

## Per-Channel Publish Ceiling

The per-viewer bandwidth budget (2 KB/s) means a **single channel** can only sustain ~10 msg/s at 200 B/message before viewers hit their budget.

To test the **platform target of 1,000 msg/s**, you must spread across channels:

```bash
# 1000 msg/s across 100 channels = 10 msg/s per channel (within budget)
CHANNEL_SPAN=100 PUBLISH_QPS=1000 CONNECTIONS=10000 ./run-loadtest.sh
```

## Understanding Results

### Conformance Targets (from `cmd/loadgen/main.go`)

| Metric | Target |
|--------|--------|
| Publish p99 latency | ≤ 50 ms |
| Publish acceptance rate | ≥ 99% |
| Delivery p50 | ≤ 50 ms |
| Delivery p95 | ≤ 100 ms |
| Delivery p99 | ≤ 200 ms |
| Connection readiness | 100% of target |
| Gap notices | 0 |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All targets met (conforms) |
| 1 | One or more targets missed |
| 2 | Fatal error (setup failed) |

### Sample Passing Output

```
=== capacity report ===
duration            30.0s
target connections   1000
target publish rate  10/s
offered rate         10/s
sent / accepted     300 / 300 (1.000)
refused / failed     0 / 0
latency p50/p95/p99  2.1 / 4.3 / 8.7 ms
multi-instance path  validated=false

--- delivery ---
connections ready   1000 of 1000 target
messages delivered  300000
bytes delivered     60000000
egress              2000000 B/s total, 2000 B/s per viewer over 30s
delivery p50        12.3 ms  (100 samples)
delivery p95        18.7 ms
delivery p99        24.1 ms

RESULT: conforms to every documented target
```

## Troubleshooting

### Gateway fails to start
- Check `GOMEMLIMIT_BYTES` — must be set and positive
- Verify port 8080/8081/8082 are free
- Ensure `SECRET` is at least 32 chars

### Loadgen can't connect
- Wait for `/readyz` to return `{"status":"ready"}`
- Check gateway logs for errors

### Connection count limited (~16k on macOS/Linux)
- Ephemeral port exhaustion: `sysctl -w net.ipv4.ip_local_port_range="1024 65535"`
- Or run loadgen on a separate machine

### "accepted 1000 messages and delivered none"
- Channel not configured or quota too low
- Verify channel exists: `curl -H "X-Tenant-Id: t1" -H "X-Admin-Subject: admin" http://localhost:8081/admin/channels/room:1`

## Extending for Multi-Instance Tests

1. Start gateway #1: `INSTANCE_ID=1 LISTEN=:8080 ADMIN_LISTEN=:8081 READY_LISTEN=:8082 go run ./cmd/gateway`
2. Start gateway #2: `INSTANCE_ID=2 LISTEN=:8083 ADMIN_LISTEN=:8084 READY_LISTEN=:8085 go run ./cmd/gateway`
3. Both use same `CONTROL_DSN` and `BUS_KIND=redis` with a Redis instance
4. Run loadgen against a load balancer in front of both

## Files Created

| File | Purpose |
|------|---------|
| `run-loadtest.ps1` | Windows PowerShell runner |
| `run-loadtest.sh` | Linux/macOS Bash runner |
| `starflow.db` | SQLite database (auto-created) |
| `capacity-report.json` | Loadgen JSON output |