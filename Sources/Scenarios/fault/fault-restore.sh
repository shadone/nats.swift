#!/usr/bin/env bash
#
# fault-restore.sh -- undo any injected fault: remove ALL toxics from the "nats"
# proxy and re-ENABLE it. Traffic flows cleanly again; the client reconnects.
#
# Usage:
#   ./Sources/Scenarios/fault/fault-restore.sh

set -euo pipefail

TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:8474}"
export TOXIPROXY_URL

# Remove every toxic currently attached (latency, bandwidth, timeout, ...).
toxics=$(curl -sf "${TOXIPROXY_URL}/proxies/nats/toxics" | jq -r '.[].name')
for name in $toxics; do
    toxiproxy-cli toxic remove -n "$name" nats >/dev/null 2>&1 \
        && echo "removed toxic '${name}'" || true
done

curl -sf -X POST "${TOXIPROXY_URL}/proxies/nats" -d '{"enabled":true}' >/dev/null
echo "proxy 'nats' ENABLED + toxics cleared -- traffic restored"
