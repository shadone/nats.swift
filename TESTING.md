# Testing & reliability

This fork treats reliability as a first-class concern. This document is the map of how the client
is tested — what runs on every push, what runs nightly, and how to reproduce each layer locally.

## Layers

| Layer | What it covers | Where |
| --- | --- | --- |
| **Unit** | Pure logic with no server: `$JS.ACK` parsing, config JSON round-trips, the FIFO buffer, and a **property/model test** of the ordered-consumer no-loss/no-dup invariant over 400 random gap/reset schedules. Fast and deterministic. | `Tests/**/Unit` |
| **Integration** | Every feature against a real `nats-server`: KV, ObjectStore, pull/push/ordered consumers, TTL, async publish, Services, reconnect resilience. | `Tests/**/Integration` |
| **Chaos** | Mid-stream consumer resets, concurrent KV CAS, heartbeat-under-load, slow-consumer overflow, shared-handle stress. | `Tests/**/Integration/*Chaos*`, `*Concurrency*`, `*Stress*` |
| **Interop** | Bidirectional `nats` CLI round-trips (KV / ObjectStore / Services) so the wire format matches the ecosystem. | integration tests that shell out to `nats` |
| **Stress harness** | A release-mode load harness (`PerfBench`) looped over the delivery-race-prone consumer scenarios. This is the **reliable reproducer** for lifetime/delivery races the single-shot debug suite cannot see. | `Sources/PerfBench` |
| **Scenarios** | Runnable real-world programs: KV watch, object transfer, work queue, Services, async publish, a long-lived `live-consume` soak, 3-node cluster failover, slow consumer. | `Sources/Scenarios` |

## What runs on every push (`ci` workflow)

Linux-first, on cheap `swift:<ver>` containers:

- **Swift version matrix** — the library **builds** on the `6.0` floor (matching the README's "Swift 6.0+"), and the **full suite** runs on `6.1` and `6.2`. (Swift 6.0's XCTest rejects async test methods over a `self`-capture Sendable rule that 6.1 relaxed — a toolchain quirk, not a library limit.)
- **Release + consumer stress** — a release build, `swift test -c release`, and 20 looped iterations of the ordered/push/pull consumer scenarios. A delivery/lifetime regression surfaces here as a stall even when the debug suite is green.
- **macOS build + test** — the only place the Apple **CryptoKit** ObjectStore path is exercised, plus `nats`-CLI interop.
- **iOS build**, **swift-format lint**, **DocC build**.

## What runs nightly (`nightly` workflow)

Heavier suites, too slow to block a push:

- **Soak** — a 5-minute `live-consume` run (long-lived ordered consumer + steady publisher) asserting contiguous, no-loss/no-dup delivery, plus the self-contained scenarios.
- **3-node cluster failover** — a real local RAFT cluster (no Docker); kills the stream leader and asserts the client keeps delivering contiguously through the re-election.
- **Fault injection** — `toxiproxy` in front of the server; cuts the link mid-stream and restores it, asserting the consumer rides out the outage (best-effort while the toolchain stabilizes).
- **ThreadSanitizer** — the full suite under TSan (informational until the timing-bounded tests are tuned for the ~2-5x slowdown).

## Running locally

```bash
# Unit + integration suite (needs nats-server on PATH)
swift test

# A single test or module
swift test --filter OrderedConsumeTests
swift test --filter OrderedConsumerUnitTests/testCursorNoLossNoDupUnderRandomGapsAndResets

# Release + the consumer stress loop (the reliable reproducer for delivery races)
nats-server -js -p 4222 &
swift build -c release --product PerfBench
for i in $(seq 1 20); do
  ./.build/release/PerfBench --scenario orderedConsume,pushConsume,pullConsume,pushConsumeHB --msgs 3000 || break
done

# Benchmarks (see PERF.md)
./.build/release/PerfBench --scenario all

# A soak / scenario run
swift build -c release --product Scenarios
nats-server -js -p 4222 &
SCEN_DURATION=300 ./.build/release/Scenarios live-consume

# 3-node cluster failover (no Docker)
./Sources/Scenarios/cluster/cluster-up.sh
./.build/release/Scenarios cluster
./Sources/Scenarios/cluster/cluster-down.sh --wipe

# Fault injection (needs toxiproxy-server / toxiproxy-cli / jq on PATH) — see FAULTS.md
```

## Gotchas

- **Release vs debug matters.** Some lifetime/delivery races only manifest in a release build (ARC
  releases at last-use), and the in-process test scheduler tends to hide them. When verifying an
  ordered-consumer lifetime fix, loop the **release** `PerfBench`, not only `swift test`.
- **Clear stale processes before a full run.** A machine sleep mid-`swift test` can leave a wedged
  `xctest` and orphaned servers that block later runs: `pkill -9 -f xctest; pkill -9 -f "nats-server -p -1"`.
- **Linux prerequisites.** `libsodium-dev` (for `nkeys.swift` → `swift-sodium`) and `curl` (the
  `nats` installers) are not in the base `swift` image; the CI installs them.
