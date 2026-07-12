#!/usr/bin/env bash
#
# toxiproxy-up.sh -- start toxiproxy and put a proxy in front of a local
# nats-server so faults can be injected on the client<->server connection.
#
# Topology:
#
#   Scenarios client --> 127.0.0.1:${PROXY_PORT}  (toxiproxy "nats" proxy)
#                            |
#                            v
#                        127.0.0.1:${NATS_PORT}   (real nats-server -js)
#
# Point the client at the PROXY port, then use fault-*.sh (or raw
# `toxiproxy-cli toxic add ...`) to cut / slow / throttle the link while the
# `live-consume` scenario keeps publishing + consuming. See FAULTS.md.
#
# Env:
#   PROXY_PORT   toxiproxy listen port for clients (default 4666)
#   NATS_PORT    real nats-server client port to forward to (default 4300)
#   TOXIPROXY_URL  admin API base (default http://localhost:8474)
#
# Usage:
#   nats-server -js -p 4300 &                 # start the real server first
#   ./Sources/Scenarios/fault/toxiproxy-up.sh
#   NATS_URL=nats://127.0.0.1:4666 ./.build/debug/Scenarios live-consume

set -euo pipefail

PROXY_PORT="${PROXY_PORT:-4666}"
NATS_PORT="${NATS_PORT:-4300}"
TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:8474}"
export TOXIPROXY_URL

BASE_DIR="/tmp/nats-toxiproxy"
PID_FILE="${BASE_DIR}/toxiproxy.pid"
LOG_FILE="${BASE_DIR}/toxiproxy.log"

for tool in toxiproxy-server toxiproxy-cli curl jq; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "error: ${tool} not found on PATH" >&2
        exit 1
    fi
done

mkdir -p "$BASE_DIR"

# Start the toxiproxy admin server unless one is already answering.
if curl -sf "${TOXIPROXY_URL}/version" >/dev/null 2>&1; then
    echo "toxiproxy-server already running at ${TOXIPROXY_URL} (version $(curl -sf "${TOXIPROXY_URL}/version"))"
else
    echo "starting toxiproxy-server (admin ${TOXIPROXY_URL}) ..."
    toxiproxy-server >"$LOG_FILE" 2>&1 &
    echo "$!" >"$PID_FILE"
    for _ in $(seq 1 50); do
        if curl -sf "${TOXIPROXY_URL}/version" >/dev/null 2>&1; then
            break
        fi
        sleep 0.2
    done
    if ! curl -sf "${TOXIPROXY_URL}/version" >/dev/null 2>&1; then
        echo "error: toxiproxy admin API never came up; see ${LOG_FILE}" >&2
        exit 1
    fi
    echo "  toxiproxy-server up (pid $(cat "$PID_FILE"), version $(curl -sf "${TOXIPROXY_URL}/version"))"
fi

# (Re)create the "nats" proxy so a re-run always lands on a clean forward.
# NOTE: toxiproxy-cli (urfave/cli v2) stops parsing flags after the first
# positional arg, so the -l/-u flags MUST come BEFORE the proxy name.
toxiproxy-cli delete nats >/dev/null 2>&1 || true
toxiproxy-cli create -l "127.0.0.1:${PROXY_PORT}" -u "127.0.0.1:${NATS_PORT}" nats

echo ""
echo "proxy 'nats' READY: 127.0.0.1:${PROXY_PORT} --> 127.0.0.1:${NATS_PORT}"
echo "point the client at the PROXY:"
echo "  NATS_URL=nats://127.0.0.1:${PROXY_PORT}"
echo ""
echo "inject faults (see FAULTS.md):"
echo "  ./Sources/Scenarios/fault/fault-cut.sh       # full connection cut"
echo "  ./Sources/Scenarios/fault/fault-restore.sh   # restore link + clear toxics"
echo "  ./Sources/Scenarios/fault/fault-latency.sh   # add latency toxic"
echo "tear down:"
echo "  ./Sources/Scenarios/fault/toxiproxy-down.sh"
