# Changelog

All notable changes to this fork are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this fork versions independently of
upstream [`nats-io/nats.swift`](https://github.com/nats-io/nats.swift) (forked at `v0.4.0`).

## [Unreleased]

The first fork release: a first-class JetStream surface, cross-platform support, and a real
reliability posture on top of upstream's Core-NATS-only client.

### Added

- **JetStream KeyValue** — buckets, get/put/create/update/delete/purge with optimistic
  concurrency, status, hang-safe `watch`/`watchAll`/`keys`/`history`/`purgeDeletes`, per-key TTL.
- **JetStream ObjectStore** — chunked and streaming put/get with SHA-256 digest verification,
  `getInfo`/`delete`/`updateMeta`/`links`/`seal`/`status`, `watch`/`list`.
- **Ordered push consumer** — reset-transparent delivery with flow control and idle heartbeats;
  no message loss or duplication across a reset (a data-sequence gap, missed heartbeat, 409, or
  reconnect).
- **Push consumers** — ephemeral, durable, and queue/deliver-group.
- **Modern consumer API** — `consume`/`messages`/`next` across pull, push, and ordered consumers.
- **Async batched publish** — `publishAsync` with a bounded in-flight window and backpressure.
- **Per-message and per-key TTL** (NATS 2.11+), wire-identical to `nats.go`.
- **Service (micro) API** — the `Services` module: endpoints/groups, `$SRV` discovery, per-endpoint stats.
- **Connection ergonomics** — in-memory credentials (no temp file), `ignoreDiscoveredServers()`,
  `waitForConnected()`, `state`/`isConnected`, `unlimitedReconnects()`.
- **Linux support** — builds and passes the full suite on Linux (upstream CI is macOS/iOS only).
- **Swift 6 language mode** across the package, with strict concurrency enforced as errors.
- **Reliability infrastructure** — a release-mode consumer-stress CI gate; a property/model test of
  the ordered no-loss/no-dup invariant; and a nightly soak, 3-node cluster failover, fault-injection
  (toxiproxy), and ThreadSanitizer suite. See [TESTING.md](./TESTING.md).

### Changed

- Slow-consumer overflow now surfaces a `.error` event (was a silent drop) over an amortized-O(1)
  buffer (was an O(n²) drain), with a tunable `subscriptionCapacity(_:)`.
- The package requires a Swift 6.0+ toolchain to build; running the test suite requires 6.1+.

### Fixed

- **JetStream CAS-publish acks** — a failed `PubAck` (wrong expected-last-sequence, msg-id dedup)
  was mis-decoded as a *successful* ack, so every JetStream publish CAS failure was silently
  swallowed library-wide. Now detected and surfaced.
- **Ordered-consumer delivery stall** — a caller holding only the returned consume/messages context
  (dropping the consumer handle) could race the consumer's deallocation, whose `deinit` deleted the
  server consumer and silently stopped delivery (~50% under load). The context now pins the consumer
  for its lifetime.
- **Swift 6.0/6.1 build** — `Response<T>` and its error types are now explicitly `Sendable`, so the
  library compiles on the Swift 6.0/6.1 toolchains (not only 6.2's region isolation).
- Numerous correctness fixes across the transport, reset engine, parser, auth, TTL, ObjectStore
  streaming, `publishAsync` lifetime, and subscription reader path (three adversarial review sweeps).
