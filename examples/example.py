#!/usr/bin/env python3
"""
StarFlow End-to-End Example (Python)

A pure Python example that demonstrates the complete flow without requiring Node.js.
Uses websockets to connect to the gateway.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import websockets

# Colors
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'

def log(msg): print(f"{BLUE}[INFO]{NC} {msg}")
def success(msg): print(f"{GREEN}[SUCCESS]{NC} {msg}")
def warn(msg): print(f"{YELLOW}[WARN]{NC} {msg}")
def error(msg): print(f"{RED}[ERROR]{NC} {msg}")

# Project root is parent of examples directory
ROOTDIR = Path(__file__).parent.parent
WORKDIR = ROOTDIR
GATEWAY_BIN = ROOTDIR / "bin" / "gateway"
DB_FILE = ROOTDIR / "starflow_example.db"
SECRET_FILE = ROOTDIR / ".example_secret"

DATA_PORT = 8080
ADMIN_PORT = 8081
READY_PORT = 8082

gateway_proc = None
received_messages = []


def cleanup():
    global gateway_proc
    log("Cleaning up...")
    if gateway_proc:
        gateway_proc.terminate()
        try:
            gateway_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            gateway_proc.kill()
        gateway_proc = None
    # Give time for DB connections to close
    time.sleep(0.5)
    for f in (DB_FILE, SECRET_FILE):
        try:
            f.unlink()
        except FileNotFoundError:
            pass
        except PermissionError:
            pass  # DB might still be closing


def run_cmd(cmd, check=True, capture=True):
    """Run a command and return stdout."""
    result = subprocess.run(cmd, shell=True, capture_output=capture, text=True, cwd=WORKDIR)
    if check and result.returncode != 0:
        error(f"Command failed: {cmd}")
        error(f"stdout: {result.stdout}")
        error(f"stderr: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()


async def http_request(url, method, data, headers):
    """Async HTTP request using thread pool."""
    loop = asyncio.get_event_loop()
    def _req():
        import urllib.request
        req = urllib.request.Request(url, data=json.dumps(data).encode(), headers=headers, method=method)
        print(f"  [HTTP] {method} {url} -> ", end="", flush=True)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                result = json.loads(resp.read().decode())
                print(f"{resp.status} {result}")
                return result
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"{e.code} {body}")
            raise
    return await loop.run_in_executor(None, _req)


async def http_post(url, data, headers):
    """Async HTTP POST using thread pool."""
    return await http_request(url, "POST", data, headers)


async def http_patch(url, data, headers):
    """Async HTTP PATCH using thread pool."""
    return await http_request(url, "PATCH", data, headers)


async def http_get(url):
    """Async HTTP GET using thread pool."""
    loop = asyncio.get_event_loop()
    def _get():
        import urllib.request
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode()
    return await loop.run_in_executor(None, _get)


async def subscriber_loop(app_key, secret, stop_event):
    """WebSocket subscriber that prints received messages."""
    url = f"ws://localhost:{DATA_PORT}/sub"
    log(f"[Subscriber] Connecting to {url}")

    try:
        async with websockets.connect(url, subprotocols=["push.v1"]) as ws:
            # Send hello frame
            hello = {
                "op": "hello",
                "app_key": app_key,
                "token": secret,
                "channels": ["room:1"],
                "resume": False,
            }
            await ws.send(json.dumps(hello))
            log("[Subscriber] Sent hello")

            async for raw in ws:
                if stop_event.is_set():
                    break
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                op = msg.get("op")
                if op == "ready":
                    session = msg.get("session")
                    log(f"[Subscriber] Authenticated, session={session}")
                elif op == "subscribed":
                    accepted = msg.get("channels", [])
                    rejected = msg.get("rejected", [])
                    log(f"[Subscriber] Subscribed: {accepted}")
                    if rejected:
                        for r in rejected:
                            warn(f"[Subscriber] Rejected: {r['channel']} - {r['reason']}")
                elif op == "message":
                    channel = msg.get("channel")
                    body = msg.get("body", "")
                    msg_id = msg.get("id")
                    time_ms = msg.get("time_ms")
                    priority = msg.get("priority", 0)
                    log(f"[Subscriber] Received on {channel}: {body}")
                    log(f"  ID: {msg_id}, Time: {time_ms}, Priority: {priority}")
                    received_messages.append({"channel": channel, "body": body, "id": msg_id})
                elif op == "gap":
                    dropped = msg.get("dropped", 0)
                    reason = msg.get("reason", 0)
                    warn(f"[Subscriber] Gap: dropped {dropped} messages (reason: {reason})")
                elif op == "error":
                    reason = msg.get("reason", "unknown")
                    error(f"[Subscriber] Server error: {reason} - {msg.get('message', '')}")
                elif op == "pong":
                    pass  # heartbeat response
                elif op == "drain":
                    log("[Subscriber] Received drain notice")
                else:
                    log(f"[Subscriber] Unknown op: {op}")
    except websockets.exceptions.ConnectionClosed as e:
        if not stop_event.is_set():
            warn(f"[Subscriber] Connection closed: {e}")
    except Exception as e:
        if not stop_event.is_set():
            error(f"[Subscriber] Error: {e}")


async def run_subscriber(app_key, secret, stop_event):
    """Run subscriber with reconnection logic."""
    while not stop_event.is_set():
        await subscriber_loop(app_key, secret, stop_event)
        if stop_event.is_set():
            break
        warn("[Subscriber] Reconnecting in 1s...")
        await asyncio.sleep(1)


async def publish_messages(app_key, secret, count=6):
    """Publish messages to the gateway."""
    log("Publishing messages...")
    headers = {
        "X-App-Key": app_key,
        "X-App-Secret": secret,
        "Content-Type": "application/json",
    }
    url = f"http://localhost:{DATA_PORT}/publish"
    
    for i in range(count):
        body = "Hello from StarFlow!" if i == 0 else f"Message #{i}"
        try:
            result = await http_post(url, {"channel": "room:1", "body": body}, headers)
            outcome = result.get("outcome", "unknown")
            if i == 0:
                if outcome == "accepted":
                    success(f"First message accepted")
                else:
                    warn(f"First message outcome: {outcome}")
            else:
                print(f"  Message #{i}: {outcome}")
        except Exception as e:
            error(f"Failed to publish message {i}: {e}")
        await asyncio.sleep(0.1)


async def fetch_metrics():
    """Fetch and display metrics."""
    try:
        metrics = await http_get(f"http://localhost:{READY_PORT}/metrics")
        log("Metrics:")
        print(metrics[:3000])
    except Exception as e:
        warn(f"Could not fetch metrics: {e}")


async def main():
    global gateway_proc, received_messages

    # Step 1: Build gateway
    log("Building gateway...")
    run_cmd("go build -mod=vendor -o bin/gateway ./cmd/gateway")
    success("Gateway built")

    # Step 2: Generate secret
    log("Generating secret...")
    secret = run_cmd("openssl rand -hex 32")
    SECRET_FILE.write_text(secret)

    # Step 3: Environment
    env = os.environ.copy()
    env.update({
        "GOMEMLIMIT_BYTES": str(512 * 1024 * 1024),
        "CONTROL_DSN": f"{DB_FILE}?_pragma=foreign_keys(1)",
        "SECRET_SOURCE": "file",
        "SECRET_REF": str(SECRET_FILE),
        "BOOTSTRAP_TENANT": "t1",
        "BOOTSTRAP_SUBJECT": "ops",
        "LISTEN": f":{DATA_PORT}",
        "ADMIN_LISTEN": f":{ADMIN_PORT}",
        "READY_LISTEN": f":{READY_PORT}",
        "BUS_KIND": "memory",
        "INSTANCE_ID": "1",
    })

    # Step 4: Start gateway
    log(f"Starting gateway on ports {DATA_PORT} (data), {ADMIN_PORT} (admin), {READY_PORT} (ready)...")
    gateway_proc = subprocess.Popen(
        [str(GATEWAY_BIN)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # Print gateway logs in background
    def print_logs():
        for line in gateway_proc.stdout:
            print(f"  [gateway] {line.rstrip()}")

    log_thread = threading.Thread(target=print_logs, daemon=True)
    log_thread.start()

    # Wait for ready
    log("Waiting for gateway to be ready...")
    for _ in range(30):
        try:
            await http_get(f"http://localhost:{READY_PORT}/readyz")
            success("Gateway is ready")
            break
        except Exception:
            pass
        await asyncio.sleep(1)
    else:
        error("Gateway failed to start")
        sys.exit(1)

    # Step 5: Create tenant
    log("Creating tenant 'demo'...")
    resp = await http_post(
        f"http://localhost:{ADMIN_PORT}/admin/tenants",
        {"name": "demo"},
        {"X-Tenant-Id": "t1", "X-Admin-Subject": "ops", "Content-Type": "application/json"},
    )
    print(resp)

    # Step 6: Create application
    log("Creating application 'web'...")
    resp = await http_post(
        f"http://localhost:{ADMIN_PORT}/admin/applications",
        {"name": "web", "can_publish": True, "can_subscribe": True},
        {"X-Tenant-Id": "t1", "X-Admin-Subject": "ops", "Content-Type": "application/json"},
    )
    print(resp)

    app_key = resp["application"]["app_key"]
    app_secret = resp["secret"]
    success(f"Application created: app_key={app_key}")

    # Step 7: Configure channel
    log("Configuring channel 'room:1'...")
    resp = await http_patch(
        f"http://localhost:{ADMIN_PORT}/admin/channels/?channel=room:1",
        {"max_subscribers": 1000},
        {"X-Tenant-Id": "t1", "X-Admin-Subject": "ops", "Content-Type": "application/json"},
    )
    print(resp)

    # Step 8: Start subscriber
    log("Starting subscriber...")
    stop_event = asyncio.Event()
    subscriber_task = asyncio.create_task(run_subscriber(app_key, app_secret, stop_event))

    # Wait for subscriber to connect
    await asyncio.sleep(2)

    # Step 9: Publish messages
    await publish_messages(app_key, app_secret, count=6)

    # Wait for delivery
    log("Waiting for delivery...")
    await asyncio.sleep(2)

    # Step 10: Show metrics
    await fetch_metrics()

    # Stop subscriber
    stop_event.set()
    try:
        await asyncio.wait_for(subscriber_task, timeout=5)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    success("End-to-end example completed!")
    print(f"\nReceived {len(received_messages)} messages")
    print("\nDemo complete. Gateway will be stopped.")
    print(f"You can run the example again to test more, or manually publish:")
    print(f"  curl -X POST http://localhost:{DATA_PORT}/publish \\")
    print(f"    -H 'X-App-Key: {app_key}' \\")
    print(f"    -H 'X-App-Secret: {app_secret}' \\")
    print(f"    -H 'Content-Type: application/json' \\")
    print(f"    -d '{{\"channel\":\"room:1\",\"body\":\"test\"}}'")

    # Stop subscriber
    stop_event.set()
    try:
        await asyncio.wait_for(subscriber_task, timeout=5)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()