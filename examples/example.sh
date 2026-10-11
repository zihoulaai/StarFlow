#!/usr/bin/env bash
# StarFlow End-to-End Example (Bash)
# This script demonstrates the complete flow using curl and a simple Python subscriber

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKDIR"

GATEWAY_BIN="./bin/gateway"
DB_FILE="starflow_example.db"
SECRET_FILE=".example_secret"

DATA_PORT=8080
ADMIN_PORT=8081
READY_PORT=8082

cleanup() {
    log "Cleaning up..."
    if [[ -n "${GATEWAY_PID:-}" ]]; then
        kill "$GATEWAY_PID" 2>/dev/null || true
        wait "$GATEWAY_PID" 2>/dev/null || true
    fi
    if [[ -n "${SUBSCRIBER_PID:-}" ]]; then
        kill "$SUBSCRIBER_PID" 2>/dev/null || true
    fi
    rm -f "$DB_FILE" "$SECRET_FILE"
}
trap cleanup EXIT INT TERM

# Step 1: Build gateway
log "Building gateway..."
go build -mod=vendor -o "$GATEWAY_BIN" ./cmd/gateway
success "Gateway built"

# Step 2: Generate secret
log "Generating secret..."
openssl rand -hex 32 > "$SECRET_FILE"
SECRET=$(cat "$SECRET_FILE")

# Step 3: Start gateway
log "Starting gateway on ports ${DATA_PORT} (data), ${ADMIN_PORT} (admin), ${READY_PORT} (ready)..."
export GOMEMLIMIT_BYTES=$((512*1024*1024))
export CONTROL_DSN="${DB_FILE}?_pragma=foreign_keys(1)"
export SECRET_SOURCE=file
export SECRET_REF="$SECRET_FILE"
export BOOTSTRAP_TENANT=t1
export BOOTSTRAP_SUBJECT=ops
export LISTEN=":${DATA_PORT}"
export ADMIN_LISTEN=":${ADMIN_PORT}"
export READY_LISTEN=":${READY_PORT}"
export BUS_KIND=memory
export INSTANCE_ID=1

"$GATEWAY_BIN" > gateway.log 2>&1 &
GATEWAY_PID=$!

# Wait for gateway to be ready
log "Waiting for gateway to be ready..."
for i in {1..30}; do
    if curl -sf "http://localhost:${READY_PORT}/readyz" >/dev/null 2>&1; then
        success "Gateway is ready"
        break
    fi
    sleep 1
    if [[ $i -eq 30 ]]; then
        error "Gateway failed to start. Check gateway.log"
        cat gateway.log
        exit 1
    fi
done

# Step 4: Create tenant
log "Creating tenant 'demo'..."
TENANT_RESP=$(curl -s -X POST "http://localhost:${ADMIN_PORT}/admin/tenants" \
    -H "X-Tenant-Id: t1" -H "X-Admin-Subject: ops" \
    -H "Content-Type: application/json" -d '{"name":"demo"}')
echo "$TENANT_RESP"

# Step 5: Create application
log "Creating application 'web'..."
APP_RESP=$(curl -s -X POST "http://localhost:${ADMIN_PORT}/admin/applications" \
    -H "X-Tenant-Id: t1" -H "X-Admin-Subject: ops" \
    -H "Content-Type: application/json" \
    -d '{"name":"web","can_publish":true,"can_subscribe":true}')
echo "$APP_RESP"

APP_KEY=$(echo "$APP_RESP" | grep -o '"app_key":"[^"]*"' | cut -d'"' -f4)
APP_SECRET=$(echo "$APP_RESP" | grep -o '"secret":"[^"]*"' | cut -d'"' -f4)

if [[ -z "$APP_KEY" ]]; then
    error "Failed to create application"
    exit 1
fi
success "Application created: app_key=$APP_KEY"

# Step 6: Configure channel
log "Configuring channel 'room:1'..."
curl -s -X PATCH "http://localhost:${ADMIN_PORT}/admin/channels/?channel=room:1" \
    -H "X-Tenant-Id: t1" -H "X-Admin-Subject: ops" \
    -H "Content-Type: application/json" \
    -d '{"max_subscribers":1000}'

# Step 7: Start Python subscriber in background
log "Starting subscriber..."
cat > subscriber.py << 'EOF'
import asyncio
import json
import websockets
import sys

async def subscriber():
    app_key = sys.argv[1]
    secret = sys.argv[2]
    url = f"ws://localhost:{sys.argv[3]}/sub"
    
    async with websockets.connect(url, subprotocols=["push.v1"]) as ws:
        hello = {
            "op": "hello",
            "app_key": app_key,
            "token": secret,
            "channels": ["room:1"],
            "resume": False,
        }
        await ws.send(json.dumps(hello))
        print(f"[Subscriber] Connected to {url}")
        
        async for raw in ws:
            msg = json.loads(raw)
            op = msg.get("op")
            if op == "ready":
                print("[Subscriber] Authenticated")
            elif op == "subscribed":
                print(f"[Subscriber] Subscribed: {msg.get('channels', [])}")
            elif op == "message":
                print(f"[Subscriber] Received: {msg['body']} (id={msg['id']})")
            elif op == "gap":
                print(f"[Subscriber] Gap: dropped {msg['dropped']} messages")
            elif op == "error":
                print(f"[Subscriber] Error: {msg}")

if __name__ == "__main__":
    asyncio.run(subscriber())
EOF

python subscriber.py "$APP_KEY" "$APP_SECRET" "$DATA_PORT" &
SUBSCRIBER_PID=$!

# Wait for subscriber to connect
sleep 2

# Step 8: Publish messages
log "Publishing messages..."
for i in {0..5}; do
    body="Hello from StarFlow!"
    if [[ $i -gt 0 ]]; then
        body="Message #$i"
    fi
    RESP=$(curl -s -X POST "http://localhost:${DATA_PORT}/publish" \
        -H "X-App-Key: ${APP_KEY}" \
        -H "X-App-Secret: ${APP_SECRET}" \
        -H "Content-Type: application/json" \
        -d "{\"channel\":\"room:1\",\"body\":\"${body}\"}")
    echo "  Publish $i: $(echo "$RESP" | grep -o '"outcome":"[^"]*"' | cut -d'"' -f4)"
    sleep 0.1
done

# Wait for delivery
sleep 2

# Step 9: Show metrics
log "Fetching metrics..."
curl -s "http://localhost:${READY_PORT}/metrics" | head -30

success "End-to-end example completed!"
echo ""
echo "Received messages via subscriber (check output above)"
echo ""
echo "To publish more messages:"
echo "  curl -X POST http://localhost:${DATA_PORT}/publish \\"
echo "    -H 'X-App-Key: ${APP_KEY}' \\"
echo "    -H 'X-App-Secret: ${APP_SECRET}' \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"channel\":\"room:1\",\"body\":\"test\"}'"

# Keep running for manual testing
echo ""
echo "Gateway is still running. Press Ctrl+C to stop."
wait