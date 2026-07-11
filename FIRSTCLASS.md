# Making nats.swift a first-class, general-purpose NATS client

This document records an experiment: taking the community
[`nats.swift`](https://github.com/nats-io/nats.swift) client (at `v0.4.0`+, tag
`a9031f1`) and closing the gaps that kept it from being a first-class,
production-grade, general-purpose NATS client on par with `nats.go` and
`async-nats` (Rust). The work lives on branch `firstclass-kv`; nothing here is
deployed.

It began from a concrete need — a real Swift service had to
hand-roll ~330 lines of KV over the JetStream wire protocol, write a temp
credentials file, and work around a hang-on-disconnect landmine — and grew into
the general goal: build the missing first-class surface the way the reference
clients build it, as a real contribution to the library.

## What was delivered

Fifteen reviewed, tested, committed features, then a performance + chaos hardening pass. Every commit builds clean, passes
against a real `nats-server` (v2.12.7), is `swift format`-clean, and was
reviewed by an independent pass (correctness + Swift-6 concurrency) with
findings fixed before commit. The final suite is **287 tests, 0 failures**.

| Commit | Feature |
| --- | --- |
| `1cb59f1` | **In-memory credentials** + **`ignoreDiscoveredServers()`** |
| `96df86c` | **KeyValue** foundation (get/put/create/update/delete/purge/status) **+ an upstream CAS bug fix** |
| `0d1a443` | **Ordered push consumer** — the JetStream reset engine nats.swift lacked |
| `7bd3e08` | **KV watch/keys/history/purgeDeletes** on the ordered consumer (hang-safe) |
| `440e678` | **Public `consume`/`messages`/`next` API** (pull, push, ordered) |
| `4380f93` | **Strict-concurrency hardening** of the new surface |
| `b06d740` | **Connection ergonomics** — `waitForConnected`, `state`, `unlimitedReconnects` |
| `392c734` | **ObjectStore core** — chunked put/get with SHA-256 digest |
| `f62af34` | **ObjectStore** watch/list/updateMeta/links/seal/status |
| `a7e251f` | **Service (micro) API** — a new `Services` module |
| `f155008` | **Swift-6 keystone** — the public API surface is now `Sendable` |
| `a5a32ec` | **Swift 6 language mode** adopted library-wide (`swiftLanguageModes: [.v6]`) |
| `ddacd92` | **Per-message TTL + KV per-key TTL** (NATS 2.11+) |
| `60ec898` | **ObjectStore streaming put/get** |
| `c5f8be1` | **Durable push consumers + queue/deliver groups** |
| `76a1f79` | **PerfBench** performance harness (full-surface benchmarks) |
| `3ad7aae` | **Chaos/correctness suite** — consumer resets + KV CAS under concurrency |
| `d3bc2e3` | **Push-delivery hot-path fix** — ~7× push-with-heartbeat throughput |
| `ccc7f89` | this report + README (perf record in `PERF.md`) |

### The keystone: an ordered push consumer

nats.swift had **no push or ordered consumers** — only single-shot pull
`fetch`. That is the deepest structural gap, and the reason KV watch could only
be a poll loop. The reference clients (nats.go, async-nats, nats.js, nats.py)
all build KV/Object watch on an **ordered push consumer**. So rather than
reimplement a poll-loop workaround inside the library, this work added the real
thing, mirroring the canonical nats.go algorithm (`js.go`'s `checkOrderedMsgs` /
`resetOrderedConsumer`, cross-checked against `jetstream/ordered.go`):

- a push delivery loop (deliver-subject inbox, flow-control replies, in-loop
  idle-heartbeat detection, 100/409 control handling);
- a reset engine that recreates the consumer from the last delivered stream
  sequence, with the **no-loss/no-dup invariant** enforced at a single point
  (the restart cursor advances only for a message actually yielded — confirmed
  by review, proven by a test that deletes the consumer mid-stream and asserts
  contiguous `[1…10]` delivery across the reset);
- a Swift-6 concurrency model: one `actor` for state + one pump `Task` +
  `AsyncThrowingStream`, with best-effort `deinit` teardown and a bounded
  consumer-delete so a dropped handle can't leak.

**Everything else rides this one engine:** KV watch, ObjectStore watch, and the
public `consume`/`messages`/`next` API — one engine, not four.

### The two stores (KV + ObjectStore)

Both are wire-compatible with the whole NATS ecosystem, proven by **bidirectional
`nats` CLI interop tests** (write with Swift, read with the CLI and vice-versa):

- **KeyValue** — `KV_<bucket>` streams, `$KV.<bucket>.<key>` subjects,
  `KV-Operation` tombstones, optimistic concurrency, and hang-safe
  `watch`/`watchAll`/`keys`/`history`/`purgeDeletes`.
- **ObjectStore** — `OBJ_<bucket>` streams, `$O.<bucket>.C/M` chunk/meta
  subjects, **padded** URL-safe base64 name encoding, `SHA-256=<base64url>`
  digests verified on every get, `Nats-Rollup` meta supersede; chunked put/get
  (128 KiB default), getInfo/delete/updateMeta/links/seal/status, and
  `watch`/`list` on the ordered consumer (a 1 MiB / 1024-chunk object and a
  corrupt-digest negative test are in the suite).

### A general-purpose public consumer API and the Service API

- `MessageConsuming` (`consume(handler:) -> ConsumeContext`, `messages() ->
  MessagesContext` as a native `AsyncSequence`, `next(timeout:)`) across the
  pull `Consumer`, a public `PushConsumer`, and the public `OrderedConsumer`,
  with `OrderedConsumerConfig` hiding the wire-fields the reset algorithm owns.
  `next(timeout:)` is lossless. One shared mailbox engine per kind.
- **Service (micro) API** — a new pure-Core-NATS `Services` module mirroring
  nats.go's micro: an actor `Service` with endpoints/groups, auto request/reply,
  and the `$SRV.PING/INFO/STATS` discovery protocol (control subjects
  plain-subscribed so every instance answers; endpoints queue-grouped so
  requests load-balance), per-endpoint stats, and `Nats-Service-Error` headers —
  wire-verified against the `nats micro` CLI.

### Connection ergonomics and Swift-6 Sendable

- `waitForConnected()`, a public `state`/`isConnected` accessor, and
  `unlimitedReconnects()` — closing the exact out-of-band workarounds a real service needed.
- The whole package now builds under **Swift 6 language mode**
  (`swift-tools-version:6.0`, `swiftLanguageModes: [.v6]`) with strict
  concurrency enforced as errors. `NatsClient`/`JetStreamContext`/
  `JetStreamMessage` are `Sendable`; the transport core turned out to be
  effectively clean once the keystone landed, and the remaining migration was a
  small, behavior-preserving set of fixes (explicit `Sendable` capture lists on
  two JetStream task-group closures, an `NSLock`-guarded output pump in the
  `NatsServer` test helper, `@unchecked Sendable` on the two watchers which are
  iterated and stopped from different tasks, and a `#file → #filePath` fix for
  v6's `ConciseMagicFile`). Debug and release build clean; all tests pass.

### Hardening: performance and chaos testing

Before recommending this for production, the keystone was measured and stress-
tested, not just feature-completed:

- **A `PerfBench` harness** benchmarks the whole surface (core pub/sub, req/reply
  latency, JetStream publish, KV, ObjectStore, and pull/push/ordered/heartbeat
  consumers) against a real `nats-server`. The full baseline and methodology are
  in `PERF.md`.
- **It surfaced a real hot-path bug.** Push delivery with an idle heartbeat ran
  at ~1/5 the throughput of push without one, because `PushDelivery.race()` built
  a two-child task group on *every* message; the ordered consumer, which forces
  heartbeat on, could never avoid it. A non-suspending `tryNext()` poll now takes
  an already-buffered message directly and only enters the task group when the
  inbox is idle (where missed-heartbeat detection belongs). Result: push-with-
  heartbeat throughput up ~7× (the recommended production config), with no change
  to semantics — verified by adversarial review and the chaos suite.
- **A chaos/correctness suite** drives the ordered consumer through repeated
  mid-stream resets and live consume-while-publishing (asserting contiguous, no-
  loss/no-dup delivery), a heartbeat push consumer under load, and concurrent
  optimistic KV CAS (a regression guard for the `AckFuture` bug below). All green
  and non-flaky across repeated runs.

## A bug fixed for everyone, not just KV

Implementing KV's optimistic concurrency surfaced a latent bug in the base
library: `AckFuture.wait()` mis-decoded a failed publish ack. A
wrong-last-sequence `PubAck` comes back as `{"error":{…,"err_code":10071},
"seq":0}` — it carries `seq` but no `type`, so it was greedily decoded as a
*successful* `Ack(seq: 0)`. **Every** JetStream publish CAS failure
(`Nats-Expected-Last-Subject-Sequence`, message-ID dedup guards) was therefore
silently swallowed across the whole library, not only in KV. The fix detects the
error object before decoding the ack. This is the kind of thing "make it
first-class" actually entails, and it is a strong argument for upstreaming
rather than forking.

## What it took

Each feature followed the same loop: study the canonical nats.go
implementation, design against nats.swift's existing primitives, implement,
review independently (a dedicated pass for the ordered-consumer correctness and
the concurrency model), fix the findings, then commit. The ordered consumer was
the long pole; its review caught (and the fix closed) a `stop()`-during-
`recreate()` leak race and a first-creation fail-fast gap before either shipped.
Swift's single `.build` directory forced the implementation work to run
sequentially, which set the pace more than the difficulty did.

### By the numbers

- **23 commits**: the KV store, the ordered/push consumer engine, the public
  consumer API, the ObjectStore (incl. streaming IO), the `Services` module,
  per-message + KV TTL, durable/queue-group push consumers, the Swift 6 language-
  mode adoption, and the performance/chaos hardening (harness, suite, hot-path
  fix).
- **287 tests, 0 failures** against a real `nats-server`, including
  bidirectional `nats`-CLI interop for KV/ObjectStore/Services, deterministic
  reset/recovery tests (delete the consumer mid-watch → resume with no gap or
  dup), and leak tests each shown to fail without their fix.

## What remains for full "first-class general-purpose"

Everything first-class is now in: the two stores, the ordered/push consumers
(including **durable and queue/deliver-group** push consumers), the modern
consumer API, the Service API, full Swift 6 language mode, **per-message + KV
per-key TTL** (NATS 2.11+), and **ObjectStore streaming put/get**. What's left is
niche parity/throughput work, not a first-class blocker:

1. **A pull-based ordered consumer** — nats.go v2 also offers ordered delivery on
   a pull consumer; the push-based ordered consumer here already backs KV/Object
   watch and the public consume/messages/next API.
2. **Overlapping-pull throughput optimization** for the pull `consume`/`messages`
   path — the current sequential batch loop is correct, just not maximally
   pipelined.

## Recommendation: upstream, don't fork

This work is deliberately shaped as a contribution, not a private fork: it
matches the library's style and the nats.go wire/semantic conventions, is
additive and backward-compatible, and includes a genuine bug fix that benefits
every user. The pragmatic path is to **propose it upstream to
`nats-io/nats.swift`**, in reviewable slices:

1. The `AckFuture` CAS fix first — a small, clear, standalone bug fix.
2. In-memory credentials, `ignoreDiscoveredServers`, and the connection
   ergonomics — small, isolated.
3. The ordered push consumer + the public consumer API — the substantial one;
   coordinate with the maintainers (who list KV/ObjectStore/Service as roadmap
   and may have in-flight work).
4. KeyValue, then ObjectStore, then the Service module on top.
5. The Sendable keystone, then the full language-mode migration.
6. The additive polish (per-message + KV TTL, ObjectStore streaming IO,
   durable/queue-group push consumers) — each is independent and backward-
   compatible, so it can land in any order after the surface it extends.

A maintained private fork is the fallback if upstreaming stalls, but it is a
standing maintenance cost and should be a last resort. Either way, the four
the gaps that motivated this are closed and then some: the Swift service could
drop its hand-rolled KV, its temp-credentials file, and its connection/reconnect
workarounds — and now also has ObjectStore and a Service framework — and use the
library directly.
