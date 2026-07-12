#!/usr/bin/env bash
#
# fault-cut.sh -- full connection cut: DISABLE the "nats" proxy.
#
# Disabling drops the live TCP connection immediately AND refuses new connects,
# so the client observes a disconnect and enters its reconnect loop, retrying
# (and failing) until fault-restore.sh re-enables the proxy. This is the
# headline resilience test -- on restore, an ordered consumer re-creates itself
# and delivery resumes contiguously (no gap, no dup). See FAULTS.md.
#
# (Alternative black-hole cut that keeps the socket half-open instead of
# dropping it: `toxiproxy-cli toxic add nats -t timeout -a timeout=0` -- the
# client only notices via its own PING/PONG stall. Documented in FAULTS.md.)
#
# Usage:
#   ./Sources/Scenarios/fault/fault-cut.sh
#   ./Sources/Scenarios/fault/fault-restore.sh   # bring it back

set -euo pipefail

TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:8474}"

curl -sf -X POST "${TOXIPROXY_URL}/proxies/nats" -d '{"enabled":false}' >/dev/null
echo "proxy 'nats' DISABLED -- live connection dropped, new connects refused"
echo "restore with: ./Sources/Scenarios/fault/fault-restore.sh"
