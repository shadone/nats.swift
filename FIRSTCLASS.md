# Making nats.swift a first-class, general-purpose NATS client

This document records an experiment: taking the community
[`nats.swift`](https://github.com/nats-io/nats.swift) client (at `v0.4.0`+, tag
`a9031f1`) and closing the gaps that kept it from being a first-class,
production-grade, general-purpose NATS client on par with `nats.go` and
`async-nats` (Rust). The work lives on branch `firstclass-kv`; nothing here is
deployed.

It began from a concrete need — a real Swift service had to
hand-roll ~330 lines of KV over the JetStream wire protocol, write a temp
credentials file, and work around a hang-on-disconnect landmine — but the goal
became the general one: build the missing pieces the way the reference clients
build them, as a real contribution to the library.

## What was delivered

Seven reviewed, tested, committed features. Every commit builds clean, passes
against a real `nats-server` (v2.12.7), is `swift format`-clean, and was
reviewed by an independent pass (correctness + Swift-6 concurrency) with
findings fixed before commit.

| Commit | Feature |
| --- | --- |
| `1cb59f1` | **In-memory credentials** (`credentials(_ contents:)`) and **`ignoreDiscoveredServers()`** |
| `96df86c` | **KeyValue store** — foundation (get/put/create/update/delete/purge/status, config↔stream mapping) **+ a real upstream bug fix** |
| `0d1a443` | **Ordered push consumer** — the JetStream reset engine nats.swift lacked |
| `7bd3e08` | **KV watch/keys/history/purgeDeletes** on the ordered consumer (hang-safe, recovery-tested) |
| `440e678` | **Public `consume`/`messages`/`next` consumer API** across pull, push and ordered consumers |
| `4380f93` | **Strict-concurrency hardening** of the new surface (compiler-verified) |
| `b06d740` | **Connection ergonomics** — `waitForConnected`, `state`/`isConnected`, `unlimitedReconnects` |

### The gaps that motivated this — all closed

1. **KeyValue** was entirely absent. Now a full KV store, wire-compatible with
   every other NATS client (`KV_<bucket>` streams, `$KV.<bucket>.<key>`
   subjects, `KV-Operation` tombstones), proven by bidirectional `nats` CLI
   interop tests.
2. **In-memory credentials** — the client took a file URL only; now an inline
   `.creds` string is parsed from memory (no 0600 temp file).
3. **`ignoreDiscoveredServers`** — the client ingested gossiped `connect_urls`
   with no opt-out; now a single, verified guard suppresses them for a
   single-LB (e.g. Cloud Run) topology.
4. **Watch hang-safety** — the previous poll-based workaround could hang
   forever on a mid-request disconnect. KV watch now rides the ordered
   consumer's reset engine, which recreates from the last delivered sequence on
   any gap, missed heartbeat, or consumer/leadership loss.

### The keystone: an ordered push consumer

nats.swift had **no push or ordered consumers** — only single-shot pull
`fetch`. That is the deepest structural gap, and the reason KV watch could only
be a poll loop. The reference clients (nats.go, async-nats, nats.js, nats.py)
all build KV watch on an **ordered push consumer**. So rather than reimplement
the poll-loop workaround inside the library, this work added the real thing,
mirroring the canonical nats.go algorithm (`js.go`'s `checkOrderedMsgs` /
`resetOrderedConsumer`, cross-checked against `jetstream/ordered.go`):

- a push delivery loop (deliver-subject inbox, flow-control replies,
  in-loop idle-heartbeat detection, 100/409 control handling);
- a reset engine that recreates the consumer from the last delivered stream
  sequence, with the **no-loss/no-dup invariant** enforced at a single point
  (the restart cursor advances only for a message actually yielded to the
  caller — independently confirmed by review, and proven by a test that deletes
  the consumer mid-stream and asserts contiguous `[1…10]` delivery across the
  reset);
- a Swift-6 concurrency model: one `actor` for state + one pump `Task` +
  `AsyncThrowingStream`, with best-effort `deinit` teardown and a bounded
  consumer-delete so a dropped handle can't leak.

On top of it sit KV watch **and** the public `consume`/`messages`/`next` API —
one engine, not two.

### A general-purpose public consumer API

`MessageConsuming` (`consume(handler:) -> ConsumeContext`, `messages() ->
MessagesContext` as a native `AsyncSequence`, `next(timeout:)`) is conformed to
by the pull `Consumer`, a public `PushConsumer`, and the public
`OrderedConsumer`. `OrderedConsumerConfig` exposes only user-safe fields and
hides the push wire-fields the reset algorithm owns, so callers cannot set what
would break it. `next(timeout:)` is lossless (a timeout wakes the waiter with
`nil` without discarding a concurrently-arriving message).

## A bug fixed for everyone, not just KV

Implementing KV's optimistic concurrency surfaced a latent bug in the base
library: `AckFuture.wait()` mis-decoded a failed publish ack. A
wrong-last-sequence `PubAck` comes back as `{"error":{…,"err_code":10071},
"seq":0}` — it carries `seq` but no `type`, so it was greedily decoded as a
*successful* `Ack(seq: 0)`. Every JetStream publish CAS failure
(`Nats-Expected-Last-Subject-Sequence`, message-ID dedup guards) was therefore
**silently swallowed** across the whole library, not only in KV. The fix
detects the error object before decoding the ack. This is the kind of thing
"make it first-class" actually entails, and it is a strong argument for
upstreaming rather than forking.

## What it took

Each feature followed the same loop: study the canonical nats.go
implementation, design against nats.swift's existing primitives, implement,
review independently (a dedicated pass for the ordered-consumer correctness and
the concurrency model), fix the findings, then commit. The ordered consumer was
the long pole — the reset algorithm's no-loss/no-dup invariant and the Swift-6
concurrency model needed the most care, and the review caught (and the fix
closed) a `stop()`-during-`recreate()` leak race and a first-creation
fail-fast gap before either shipped.

Swift's single `.build` directory forced the Swift implementation work to run
sequentially (unlike a Go module's per-package builds), which set the pace more
than the difficulty did.

### By the numbers

- **7 feature commits**, 43 files, **+6,455 / -24** lines.
- **18 new source files** (~3,741 net Sources LOC): the KV store, the ordered/
  push consumer engine, the public consumer API, and the connection ergonomics.
- **12 new test files.** The suite runs against a real `nats-server` and
  includes bidirectional `nats`-CLI interop, deterministic reset/recovery tests
  (delete the consumer mid-watch → resume with no gap or dup), and leak tests
  that were each shown to fail without their fix.
- Strict-concurrency warnings on the new code: **37 → 7** (the 7 residual are
  all blocked on the pre-existing non-`Sendable` `NatsClient`/`JetStreamContext`
  — see the roadmap). Library-wide: 211 → 29.

## What remains for full "first-class general-purpose"

The foundation now exists (an ordered push consumer + the modern consumer API),
which makes the rest tractable and mostly additive:

1. **ObjectStore** — the second high-level store (chunked put/get, digests, a
   meta stream, `watch`/`list`/`delete`/`info`). Builds on the same ordered
   consumer for its watch. Estimated ~800–1,200 LOC + tests; the largest
   remaining item, but no new primitives are required.
2. **Service (micro) API** — the request/reply services framework (endpoints,
   stats, discovery). Independent of the above; a separate mid-sized feature.
3. **Durable push consumers + queue/deliver groups**, and a **pull-based
   ordered consumer** — the consumer fields are wired (`deliverGroup` exists),
   but the v1 surface deliberately deferred these (they need redelivery/ack
   semantics KV/ordered never exercise). The **overlapping-pull optimization**
   for pull `consume`/`messages` is likewise deferred (the v1 batch loop is
   correct, just not throughput-optimal).
4. **Full Swift-6 language mode** — an estimated 2–4 focused engineer-weeks,
   gated almost entirely on making the core `NatsClient` `Sendable` (the
   keystone that dissolves the 7 residual JetStream warnings), then
   `JetStreamContext`, then the `ConnectionHandler` NIO state and event system.
   The new JetStream code is already ~95% strict-clean and falls out for free
   once core is `Sendable`.
5. **Per-message TTL** (NATS 2.11+) in `StreamConfig` — small; needed only for
   KV per-key TTL.

## Recommendation: upstream, don't fork

This work is deliberately shaped as a contribution, not a private fork: it
matches the library's style and the nats.go wire/semantic conventions, is
additive and backward-compatible, and includes a genuine bug fix that benefits
every user. The pragmatic path is to **propose it upstream to
`nats-io/nats.swift`**, in reviewable slices:

1. The `AckFuture` CAS fix first — a small, clear, standalone bug fix.
2. In-memory credentials and `ignoreDiscoveredServers` — small, isolated.
3. The ordered push consumer + the public consumer API — the substantial one;
   coordinate with the maintainers, who list KV/ObjectStore/Service as roadmap
   and may have opinions or in-flight work.
4. KeyValue on top.

A maintained private fork is the fallback if upstreaming stalls, but it is a
standing maintenance cost (as with the existing `nats.js` fork) and should be a
last resort. Either way, the gaps that motivated this are now closed: the Swift
service could drop its hand-rolled KV, its temp-credentials file, and
its connection-state and reconnect workarounds and use the library directly.
