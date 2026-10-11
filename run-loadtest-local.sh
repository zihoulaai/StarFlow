#!/usr/bin/env bash
# Local full-stack load test for StarFlow on a single Linux/macOS/WSL machine.
# Spins up gateway + loadgen with sensible defaults for the host hardware.
#
# Usage:
#   ./run-loadtest-local.sh [options]
#
# Options:
#   -c, --connections N       Total subscriber connections (default: 10000)
#   -q, --publish-qps N       Target publish rate across all channels (default: 500)
#   -d, --duration DUR        Steady-state measurement window (default: 60s)
#   -r, --ramp DUR            Connection ramp-up window (default: 10s)
#   -s, --channel-span N      Spread subscribers/publishes over N channels (default: 20)
#   -p, --probes N            Connections that contribute latency samples (default: 16)
#   -w, --concurrency N       Concurrent publish workers (default: 8)
#   -m, --gateway-mem GB      GOMEMLIMIT for gateway in GiB (default: 4)
#   -P, --gateway-procs N     GOMAXPROCS for gateway (default: 10)
#   -e, --expand-ports        Expand ephemeral port range (requires sudo, persists)
#   -o, --report PATH         Output JSON report path (default: ./capacity-<ts>.json)
#   -k, --keep-gateway        Leave gateway running after test
#   -v, --verbose             Verbose output
#   -h, --help                Show help

set -euo pipefail

# Defaults
CONNECTIONS=10000
PUBLISH_QPS=500
DURATION="60s"
RAMP="10s"
CHANNEL_SPAN=20
PROBES=16
CONCURRENCY=8
GATEWAY_MEM_GB=4
GATEWAY_PROCS=10
EXPAND_PORTS=false
REPORT_PATH=""
KEEP_GATEWAY=false
VERBOSE=false

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--connections) CONNECTIONS="$2"; shift 2 ;;
        -q|--publish-qps) PUBLISH_QPS="$2"; shift 2 ;;
        -d|--duration) DURATION="$2"; shift 2 ;;
        -r|--ramp) RAMP="$2"; shift 2 ;;
        -s|--channel-span) CHANNEL_SPAN="$2"; shift 2 ;;
        -p|--probes) PROBES="$2"; shift 2 ;;
        -w|--concurrency) CONCURRENCY="$2"; shift 2 ;;
        -m|--gateway-mem) GATEWAY_MEM_GB="$2"; shift 2 ;;
        -P|--gateway-procs) GATEWAY_PROCS="$2"; shift 2 ;;
        -e|--expand-ports) EXPAND_PORTS=true; shift ;;
        -o|--report) REPORT_PATH="$2"; shift 2 ;;
        -k|--keep-gateway) KEEP_GATEWAY=true; shift ;;
        -v|--verbose) VERBOSE=true; shift ;;
        -h|--help)
            grep '^#' "$0" | cut -c4-
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo -e "\033[1;36m==>\033[0m $*"; }
vlog() { [[ $VERBOSE == true ]] && echo -e "\033[1;30m   $*\033[0m" || true; }
ok() { echo -e "\033[1;32m$*\033[0m"; }
err() { echo -e "\033[1;31m$*\033[0m" >&2; }
warn() { echo -e "\033[1;33m$*\033[0m"; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$REPO_ROOT/bin"
mkdir -p "$BIN_DIR"

# --- expand ephemeral port range (Linux only, requires sudo) ---
if [[ $EXPAND_PORTS == true ]]; then
    if [[ "$(uname)" == "Linux" ]]; then
        log "Expanding ephemeral port range (requires sudo)..."
        sudo sysctl -w net.ipv4.ip_local_port_range="10000 65535" >/dev/null
        sudo sysctl -w net.ipv4.tcp_tw_reuse=1 >/dev/null
        ok "  Done (runtime only; add to /etc/sysctl.d/ for persistence)"
    else
        warn "  Port expansion only supported on Linux; skipping"
    fi
fi

# --- build ---
log "Building binaries..."
export GOFLAGS="-mod=vendor"
go build -o "$BIN_DIR/gateway" ./cmd/gateway
go build -o "$BIN_DIR/loadgen" ./cmd/loadgen
vlog "Binaries in $BIN_DIR"

# --- env ---
SECRET=$(openssl rand -hex 32)
export GOMEMLIMIT_BYTES=$((GATEWAY_MEM_GB * 1024 * 1024 * 1024))
export GOMAXPROCS=$GATEWAY_PROCS
export CONTROL_DSN="starflow.db?_pragma=foreign_keys(1)"
export SECRET_SOURCE=env
export SECRET_REF=PUSH_TOKEN_SECRET
export PUSH_TOKEN_SECRET="$SECRET"
export BOOTSTRAP_TENANT=t1
export BOOTSTRAP_SUBJECT=ops
export BOOTSTRAP_ROLE=admin
export INSTANCE_ID=1
export LISTEN=:8080
export ADMIN_LISTEN=:8081
export READY_LISTEN=:8082

vlog "GOMEMLIMIT=$GOMEMLIMIT_BYTES GOMAXPROCS=$GOMAXPROCS"

# --- start gateway ---
log "Starting gateway..."
GATEWAY_LOG="$REPO_ROOT/gateway-local.log"
"$BIN_DIR/gateway" >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!
vlog "Gateway PID: $GATEWAY_PID"

cleanup() {
    if [[ $KEEP_GATEWAY == false ]] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
        log "Stopping gateway (PID $GATEWAY_PID)..."
        kill "$GATEWAY_PID" 2>/dev/null || true
        wait "$GATEWAY_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# --- wait for ready ---
log "Waiting for /readyz..."
READY=false
for i in {1..60}; do
    if curl -sf "http://127.0.0.1:8082/readyz" | grep -q '"status":"ready"'; then
        READY=true
        break
    fi
    sleep 1
done
if [[ $READY != true ]]; then
    err "Gateway did not become ready in 60s. Check $GATEWAY_LOG"
    tail -50 "$GATEWAY_LOG"
    exit 1
fi
ok "  Gateway ready"

# --- create tenant/app via admin API ---
log "Creating tenant & application..."
ADMIN_HDR=(-H "X-Tenant-Id: t1" -H "X-Admin-Subject: ops" -H "Content-Type: application/json")
curl -sf -X POST "${ADMIN_HDR[@]}" http://127.0.0.1:8081/admin/tenants -d '{"name":"loadtest"}' >/dev/null

APP_RESP=$(curl -sf -X POST "${ADMIN_HDR[@]}" http://127.0.0.1:8081/admin/applications -d '{"name":"loadgen","can_publish":true,"can_subscribe":true}')
APP_KEY=$(echo "$APP_RESP" | jq -r '.application.app_key')
APP_SECRET=$(echo "$APP_RESP" | jq -r '.secret')
ok "  AppKey: $APP_KEY"
ok "  Secret: $APP_SECRET"

# --- create channels ---
vlog "Creating $CHANNEL_SPAN channels..."
SUBS_PER_CH=$(( (CONNECTIONS + CHANNEL_SPAN - 1) / CHANNEL_SPAN ))
MAX_SUBS=$(( SUBS_PER_CH * 2 ))
for i in $(seq 0 $((CHANNEL_SPAN - 1))); do
    CH="loadtest:$i"
    curl -sf -X POST "${ADMIN_HDR[@]}" http://127.0.0.1:8081/admin/channels \
        -d "$(jq -n --arg name "$CH" --argjson max "$MAX_SUBS" '{name:$name,max_subscribers:$max}')" >/dev/null
done

# --- report path ---
if [[ -z $REPORT_PATH ]]; then
    TS=$(date +%Y%m%d-%H%M%S)
    REPORT_PATH="$REPO_ROOT/capacity-$TS.json"
fi

# --- run loadgen ---
log "Running loadgen..."
echo "  Connections: $CONNECTIONS | QPS: $PUBLISH_QPS | Duration: $DURATION | Channels: $CHANNEL_SPAN | Probes: $PROBES"

LOADGEN_ARGS=(
    --publish-url "http://127.0.0.1:8080/publish"
    --tenant t1
    --app-key "$APP_KEY"
    --secret "$APP_SECRET"
    --channel "loadtest:0"
    --connections "$CONNECTIONS"
    --publish-qps "$PUBLISH_QPS"
    --duration "$DURATION"
    --ramp "$RAMP"
    --channel-span "$CHANNEL_SPAN"
    --probes "$PROBES"
    --concurrency "$CONCURRENCY"
    --report "$REPORT_PATH"
)

LOADGEN_EXIT=0
"$BIN_DIR/loadgen" "${LOADGEN_ARGS[@]}" || LOADGEN_EXIT=$?

# --- result ---
echo
if [[ $LOADGEN_EXIT -eq 0 ]]; then
    ok "=== LOAD TEST PASSED ==="
    echo "Report: $REPORT_PATH"
else
    err "=== LOAD TEST FAILED (exit $LOADGEN_EXIT) ==="
    echo "Report: $REPORT_PATH"
fi

exit $LOADGEN_EXIT