#!/usr/bin/env bash
#
# toxiproxy-down.sh -- remove the "nats" proxy and stop the toxiproxy-server
# started by toxiproxy-up.sh (by its pid file).
#
# Does NOT touch the real nats-server -- stop that yourself:
#   kill "$(cat /tmp/nats-toxiproxy/nats-server.pid)"   # if you used one
#
# Usage:
#   ./Sources/Scenarios/fault/toxiproxy-down.sh

set -uo pipefail

TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:8474}"
export TOXIPROXY_URL

BASE_DIR="/tmp/nats-toxiproxy"
PID_FILE="${BASE_DIR}/toxiproxy.pid"

# Remove the proxy (best effort; server may already be gone).
toxiproxy-cli delete nats >/dev/null 2>&1 && echo "removed proxy 'nats'" || echo "proxy 'nats' not present"

if [ -f "$PID_FILE" ]; then
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.2
        done
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
        echo "stopped toxiproxy-server (pid ${pid})"
    else
        echo "toxiproxy-server (pid ${pid:-?}) not running"
    fi
    rm -f "$PID_FILE"
else
    echo "no pid file at ${PID_FILE}; toxiproxy-server not stopped by this script"
fi
