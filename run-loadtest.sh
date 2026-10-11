#!/usr/bin/env bash
# StarFlow Local Load Test Runner (Linux/macOS)
# Runs gateway + loadgen without Docker

set -euo pipefail

# Configuration (override via env vars)
CONNECTIONS="${CONNECTIONS:-1000}"
PUBLISH_QPS="${PUBLISH_QPS:-10}"
DURATION="${DURATION:-30s}"
REPORT_PATH="${REPORT_PATH:-capacity-report.json}"
CHANNEL_SPAN="${CHANNEL_SPAN:-1}"
PROBES="${PROBES:-8}"
RAMP="${RAMP:-10s}"
GATEWAY_LISTEN="${GATEWAY_LISTEN:-:8080}"
ADMIN_LISTEN="${ADMIN_LISTEN:-:8081}"
READY_LISTEN="${READY_LISTEN:-:8082}"
DB_PATH="${DB_PATH:-starflow.db}"
SECRET="${SECRET:-dev-secret-key-at-least-32-chars-long-for-signing}"
TENANT_ID="${TENANT_ID:-loadtest}"
ADMIN_SUBJECT="${ADMIN_SUBJECT:-loadtest-admin}"
APP_NAME="${APP_NAME:-loadtest-app}"
CHANNEL_NAME="${CHANNEL_NAME:-room:1}"

section() {
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  $1"
    echo "═══════════════════════════════════════════════════════════"
}

start_gateway() {
    section "Starting Gateway"

    export BUS_KIND="memory"
    export BUS_CHANNEL="push:admitted"
    export BUS_CONFIG_CHANNEL="push:config"
    export CONTROL_DSN="file:${DB_PATH}?cache=shared&mode=rwc"
    export CONTROL_MAX_OPEN_CONNS="16"
    export CONTROL_POLL_INTERVAL="15s"
    export SECRET_SOURCE="env"
    export SECRET_REF="PUSH_TOKEN_SECRET"
    export PUSH_TOKEN_SECRET="${SECRET}"
    export BOOTSTRAP_TENANT="${TENANT_ID}"
    export BOOTSTRAP_SUBJECT="${ADMIN_SUBJECT}"
    export BOOTSTRAP_ROLE="admin"
    export INSTANCE_ID="1"
    export LISTEN="${GATEWAY_LISTEN}"
    export ADMIN_LISTEN="${ADMIN_LISTEN}"
    export READY_LISTEN="${READY_LISTEN}"
    export GOMEMLIMIT_BYTES="4294967296"  # 4 GiB
    export GOGC="400"
    export DELIVERY_FLUSH_INTERVAL="10ms"
    export DELIVERY_MERGE_RATIO="20"
    export DELIVERY_COMPRESS="true"
    export DELIVERY_COMPRESS_MIN_BATCH="5"
    export DELIVERY_PER_VIEWER_BPS="2048"
    export DELIVERY_HEARTBEAT_INTERVAL="25s"
    export DELIVERY_HEARTBEAT_TIMEOUT="10s"
    export DELIVERY_DRAIN_GRACE="30s"
    export DELIVERY_SHARDS="1024"
    export AUTH_TOKEN_TTL="5m"
    export AUTH_TOKEN_SKEW="2m"

    # Clean up old DB
    rm -f "${DB_PATH}"

    echo "Starting gateway on ${GATEWAY_LISTEN} (admin: ${ADMIN_LISTEN}, ready: ${READY_LISTEN})..."
    echo "DB: ${DB_PATH}"
    echo "Tenant: ${TENANT_ID}, Admin: ${ADMIN_SUBJECT}"

    # Start gateway in background
    go run ./cmd/gateway &
    GATEWAY_PID=$!
    echo "Gateway PID: ${GATEWAY_PID}"

    # Wait for readiness
    echo "Waiting for gateway to be ready..."
    for i in {1..30}; do
        if curl -sf "http://127.0.0.1:8082/readyz" | grep -q '"status":"ready"'; then
            echo "Gateway ready!"
            return 0
        fi
        echo "  ... waiting (${i}/30)"
        sleep 1
    done
    echo "Gateway did not become ready in time" >&2
    kill ${GATEWAY_PID} 2>/dev/null || true
    exit 1
}

setup_tenant_and_app() {
    section "Setting up Tenant, Application, and Channel"

    HEADERS=(-H "X-Tenant-Id: ${TENANT_ID}" -H "X-Admin-Subject: ${ADMIN_SUBJECT}" -H "Content-Type: application/json")

    # Create tenant (might already exist from bootstrap)
    echo "Creating tenant '${TENANT_ID}'..."
    curl -sf -X POST "http://127.0.0.1:8081/admin/tenants" "${HEADERS[@]}" \
        -d "{\"id\":\"${TENANT_ID}\",\"name\":\"Load Test Tenant\"}" >/dev/null || true
    echo "  Tenant ready"

    # Create application
    echo "Creating application '${APP_NAME}'..."
    APP_RESPONSE=$(curl -sf -X POST "http://127.0.0.1:8081/admin/applications" "${HEADERS[@]}" \
        -d "{\"name\":\"${APP_NAME}\"}")
    APP_KEY=$(echo "${APP_RESPONSE}" | jq -r '.key')
    APP_SECRET=$(echo "${APP_RESPONSE}" | jq -r '.secret')
    echo "  App Key: ${APP_KEY}"
    echo "  App Secret: ${APP_SECRET} (save this - shown only once!)"

    # Configure quota
    echo "Setting quota for ${CONNECTIONS} connections..."
    curl -sf -X PUT "http://127.0.0.1:8081/admin/quotas/${TENANT_ID}" "${HEADERS[@]}" \
        -d "{\"max_connections\":$((CONNECTIONS * 2)),\"max_publish_qps\":$((PUBLISH_QPS * 2)),\"max_message_bytes\":4096,\"publish_failure_policy\":\"degrade\"}" >/dev/null
    echo "  Quota set"

    # Configure channel
    echo "Configuring channel '${CHANNEL_NAME}'..."
    curl -sf -X PATCH "http://127.0.0.1:8081/admin/channels/${CHANNEL_NAME}" "${HEADERS[@]}" \
        -d "{\"max_subscribers\":$((CONNECTIONS * 2)),\"coalescing_enabled\":true,\"merge_ratio_override\":20,\"receive_rate_target\":50}" >/dev/null
    echo "  Channel configured"
}

run_loadgen() {
    section "Running Load Generator"

    PUBLISH_URL="http://127.0.0.1:8080/publish"
    echo "Target: ${PUBLISH_URL}"
    echo "Connections: ${CONNECTIONS} | Publish QPS: ${PUBLISH_QPS} | Duration: ${DURATION}"
    echo "Channel: ${CHANNEL_NAME} | Channel Span: ${CHANNEL_SPAN} | Probes: ${PROBES}"
    echo "Report: ${REPORT_PATH}"
    echo ""

    go run ./cmd/loadgen \
        --publish-url "${PUBLISH_URL}" \
        --tenant "${TENANT_ID}" \
        --app-key "${APP_KEY}" \
        --secret "${APP_SECRET}" \
        --channel "${CHANNEL_NAME}" \
        --connections "${CONNECTIONS}" \
        --publish-qps "${PUBLISH_QPS}" \
        --duration "${DURATION}" \
        --ramp "${RAMP}" \
        --channel-span "${CHANNEL_SPAN}" \
        --probes "${PROBES}" \
        --report "${REPORT_PATH}"

    EXIT_CODE=$?
    echo ""
    echo "Loadgen exited with code: ${EXIT_CODE}"
    return ${EXIT_CODE}
}

stop_gateway() {
    if [[ -n "${GATEWAY_PID:-}" ]]; then
        section "Stopping Gateway (PID: ${GATEWAY_PID})"
        kill ${GATEWAY_PID} 2>/dev/null || true
        wait ${GATEWAY_PID} 2>/dev/null || true
        echo "Gateway stopped"
    fi
}

trap stop_gateway EXIT

# Main
echo "StarFlow Local Load Test Runner"
echo "==============================="

if ! command -v go &>/dev/null; then
    echo "Go not found in PATH" >&2
    exit 1
fi

if ! command -v jq &>/dev/null; then
    echo "jq not found - please install jq for JSON parsing" >&2
    exit 1
fi

start_gateway
setup_tenant_and_app
run_loadgen
EXIT_CODE=$?

if [[ ${EXIT_CODE} -eq 0 ]]; then
    echo ""
    echo "✅ Load test PASSED (conforms to all targets)"
else
    echo ""
    echo "❌ Load test FAILED (does not conform)"
fi

# Print report summary if exists
if [[ -f "${REPORT_PATH}" ]]; then
    echo ""
    section "Report Summary"
    cat "${REPORT_PATH}" | jq -r '
        "Duration: \(.DurationSec)s",
        "Target Connections: \(.TargetConns)",
        "Target QPS: \(.TargetQPS)",
        "Offered QPS: \(.OfferedQPS)",
        "Sent: \(.Sent) | Accepted: \(.Accepted) | Refused: \(.Refused) | Failed: \(.Failed)",
        "Accept Rate: \(.AcceptRate * 100 | floor / 100)%",
        "Latency p50/p95/p99 (publish): \(.LatencyP50Ms)/\(.LatencyP95Ms)/\(.LatencyP99Ms) ms",
        (if .Delivery then
            "Connections Ready: \(.Delivery.Ready)/\(.Delivery.TargetConns)",
            "Delivered: \(.Delivery.Delivered) | Bytes: \(.Delivery.BytesIn)",
            "Egress: \(.Delivery.EgressBytesPerSec | floor) B/s total, \(.Delivery.EgressPerViewerBps | floor) B/s per viewer",
            "Delivery Latency p50/p95/p99: \(.Delivery.DeliveryP50Ms)/\(.Delivery.DeliveryP95Ms)/\(.Delivery.DeliveryP99Ms) ms",
            "Gap Notices: \(.Delivery.GapNotices)",
            "Fanout Factor: \(.FanoutFactor)"
        else "" end),
        "Conforms: \(.Conforms)",
        (if .Failures and (.Failures | length > 0) then
            "Failures:", (.Failures[] | "  - \(.)")
        else "" end)
    '
fi

exit ${EXIT_CODE}