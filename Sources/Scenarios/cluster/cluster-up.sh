#!/usr/bin/env bash
#
# cluster-up.sh -- start a local 3-node JetStream cluster of nats-server processes.
#
# No Docker required: three plain `nats-server` processes on ports 4222/4223/4224
# form a RAFT cluster named "JSC" so JetStream assets can be created with R3
# (three replicas) and exercise real leader election / failover.
#
# Each node gets its OWN store dir and server name; they share a cluster name and
# the full routes list so every node dials every other node. We wait until the
# routes are up AND a JetStream metadata (meta) leader has been elected before
# returning, then print the client seed URLs.
#
# Usage:
#   ./Sources/Scenarios/cluster/cluster-up.sh
#   ./Sources/Scenarios/cluster/cluster-down.sh   # tear down
#
# The scenario connects to all three seeds regardless of NATS_URL:
#   swift run Scenarios cluster

set -euo pipefail

BASE_DIR="/tmp/nats-cluster"
READY_TIMEOUT_SECONDS=30

# One node's configuration: name | client port | cluster (route-advertise) port.
NODES=(
    "n1 4222 6222"
    "n2 4223 6223"
    "n3 4224 6224"
)

# The full routes list every node advertises -- all three cluster ports.
ROUTES="nats://127.0.0.1:6222,nats://127.0.0.1:6223,nats://127.0.0.1:6224"
CLUSTER_NAME="JSC"

if ! command -v nats-server >/dev/null 2>&1; then
    echo "error: nats-server not found on PATH" >&2
    exit 1
fi

echo "nats-server: $(nats-server --version)"
mkdir -p "$BASE_DIR"

# True if a TCP port already accepts connections (node already listening).
port_open() {
    local port="$1"
    (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null && exec 3>&- && return 0
    return 1
}

# Refuse to stomp on an already-running cluster.
for node in "${NODES[@]}"; do
    read -r name client_port _cluster_port <<<"$node"
    if port_open "$client_port"; then
        echo "error: port ${client_port} (node ${name}) already in use." >&2
        echo "       run ./Sources/Scenarios/cluster/cluster-down.sh first." >&2
        exit 1
    fi
done

start_node() {
    local name="$1" client_port="$2" cluster_port="$3"
    local store_dir="${BASE_DIR}/${name}"
    local log_file="${BASE_DIR}/${name}.log"
    local pid_file="${BASE_DIR}/${name}.pid"

    mkdir -p "$store_dir"
    echo "starting ${name}: client=${client_port} cluster=${cluster_port} store=${store_dir}"

    nats-server \
        -js \
        -sd "$store_dir" \
        -server_name "$name" \
        -p "$client_port" \
        -cluster_name "$CLUSTER_NAME" \
        -cluster "nats://127.0.0.1:${cluster_port}" \
        -routes "$ROUTES" \
        -P "$pid_file" \
        >"$log_file" 2>&1 &

    # Record the shell-visible PID as a fallback; nats-server also writes -P.
    echo "$!" >"${BASE_DIR}/${name}.shpid"
}

for node in "${NODES[@]}"; do
    read -r name client_port cluster_port <<<"$node"
    start_node "$name" "$client_port" "$cluster_port"
done

echo "waiting for cluster to become ready (timeout ${READY_TIMEOUT_SECONDS}s) ..."

deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))

# 1) every client port must accept connections.
for node in "${NODES[@]}"; do
    read -r name client_port _cluster_port <<<"$node"
    while ! port_open "$client_port"; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "error: node ${name} (port ${client_port}) never came up; see ${BASE_DIR}/${name}.log" >&2
            exit 1
        fi
        sleep 0.2
    done
    echo "  node ${name} listening on ${client_port}"
done

# 2) a JetStream metadata leader must be elected (proves the RAFT meta-group has
#    quorum across the routes). The line appears in whichever node won the vote.
while true; do
    if grep -qi "JetStream cluster new metadata leader" "${BASE_DIR}"/n*.log 2>/dev/null; then
        leader_line=$(grep -hi "JetStream cluster new metadata leader" "${BASE_DIR}"/n*.log | tail -1)
        echo "  meta leader elected: ${leader_line##*] }"
        break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "error: JetStream meta leader was not elected within ${READY_TIMEOUT_SECONDS}s" >&2
        echo "       inspect logs: ${BASE_DIR}/n1.log ${BASE_DIR}/n2.log ${BASE_DIR}/n3.log" >&2
        exit 1
    fi
    sleep 0.3
done

echo ""
echo "cluster READY -- 3 nodes, cluster '${CLUSTER_NAME}', JetStream R3-capable"
echo "client seed URLs:"
echo "  nats://127.0.0.1:4222"
echo "  nats://127.0.0.1:4223"
echo "  nats://127.0.0.1:4224"
echo ""
echo "run the validation scenario (connects to all three seeds):"
echo "  swift run Scenarios cluster"
echo ""
echo "inspect with the nats CLI, e.g.:"
echo "  nats --server nats://127.0.0.1:4222 stream ls"
echo "  nats --server nats://127.0.0.1:4222 server report jetstream"
