#!/usr/bin/env bash
#
# fault-latency.sh -- add a downstream latency toxic to the "nats" proxy.
#
# The connection stays UP; every byte from server->client is delayed, so
# round-trips (publish acks, delivered messages, heartbeats) slow down but keep
# flowing. Demonstrates the client tolerates a slow link WITHOUT spuriously
# resetting the connection. Clear it with fault-restore.sh.
#
# Args (positional, optional):
#   $1  latency in ms  (default 800)
#   $2  jitter in ms   (default 200)
#
# Usage:
#   ./Sources/Scenarios/fault/fault-latency.sh            # 800ms +/- 200ms
#   ./Sources/Scenarios/fault/fault-latency.sh 1500 500

set -euo pipefail

TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:8474}"
export TOXIPROXY_URL

LATENCY="${1:-800}"
JITTER="${2:-200}"

toxiproxy-cli toxic add nats -t latency -n latency_down \
    -a latency="$LATENCY" -a jitter="$JITTER"
echo "added latency toxic 'latency_down': ${LATENCY}ms +/- ${JITTER}ms (downstream)"
echo "clear with: toxiproxy-cli toxic remove -n latency_down nats   (or fault-restore.sh)"
