#!/usr/bin/env bash
#
# fault-bandwidth.sh -- throttle the "nats" proxy's downstream bandwidth.
#
# Caps server->client throughput to RATE KB/s. Combined with a publish burst
# this creates real backpressure: delivery slows / lags but the connection
# survives (no crash). Clear it with fault-restore.sh.
#
# Args (positional, optional):
#   $1  rate in KB/s  (default 10)
#
# Usage:
#   ./Sources/Scenarios/fault/fault-bandwidth.sh        # 10 KB/s
#   ./Sources/Scenarios/fault/fault-bandwidth.sh 4

set -euo pipefail

TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:8474}"
export TOXIPROXY_URL

RATE="${1:-10}"

toxiproxy-cli toxic add nats -t bandwidth -n bandwidth_down -a rate="$RATE"
echo "added bandwidth toxic 'bandwidth_down': ${RATE} KB/s (downstream)"
echo "clear with: toxiproxy-cli toxic remove -n bandwidth_down nats   (or fault-restore.sh)"
