#!/usr/bin/env bash
#
# cluster-down.sh -- stop the local 3-node JetStream cluster started by
# cluster-up.sh, killing each node by its pid file.
#
# Usage:
#   ./Sources/Scenarios/cluster/cluster-down.sh          # stop nodes, keep store
#   ./Sources/Scenarios/cluster/cluster-down.sh --wipe   # stop nodes AND rm store
#
# The pid files nats-server writes via -P are the source of truth; a .shpid
# fallback (the backgrounded shell PID) is used if the -P file is missing.

set -uo pipefail

BASE_DIR="/tmp/nats-cluster"
NODES=(n1 n2 n3)
WIPE=0

if [ "${1:-}" = "--wipe" ]; then
    WIPE=1
fi

stop_node() {
    local name="$1"
    local pid_file="${BASE_DIR}/${name}.pid"
    local shpid_file="${BASE_DIR}/${name}.shpid"
    local pid=""

    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file" 2>/dev/null || true)
    fi
    if [ -z "$pid" ] && [ -f "$shpid_file" ]; then
        pid=$(cat "$shpid_file" 2>/dev/null || true)
    fi

    if [ -z "$pid" ]; then
        echo "  ${name}: no pid file, skipping"
        return
    fi

    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        echo "  ${name}: sent TERM to pid ${pid}"
        # Give it a moment; escalate to KILL if still alive.
        for _ in 1 2 3 4 5; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.2
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
            echo "  ${name}: escalated to KILL for pid ${pid}"
        fi
    else
        echo "  ${name}: pid ${pid} not running"
    fi

    rm -f "$pid_file" "$shpid_file"
}

echo "stopping nats cluster ..."
for name in "${NODES[@]}"; do
    stop_node "$name"
done

if [ "$WIPE" -eq 1 ]; then
    rm -rf "$BASE_DIR"
    echo "wiped ${BASE_DIR}"
else
    echo "left store + logs under ${BASE_DIR} (pass --wipe to remove)"
fi
