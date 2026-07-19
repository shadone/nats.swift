# Performance

Benchmarks for the nats.swift client, measured with the `PerfBench` harness
(`Sources/PerfBench/`) against a real `nats-server`. This is the fork's honest
performance record for the added JetStream surface — not a marketing claim.

## Running

```bash
nats-server -js -p 4222 &                 # JetStream enabled
swift build -c release --product PerfBench
./.build/release/PerfBench --scenario all # or a comma list; --json for machine output
```

Options: `--url` (`$NATS_URL` honored), `--scenario all|<ids>`, `--msgs`,
`--size`, `--obj-size`, `--reqs`, `--json`.

## Methodology

- Each scenario warms up (a tenth of the run, capped at 10k) and then times ONLY
  the hot loop with a monotonic clock (`DispatchTime.uptimeNanoseconds`);
  connection, stream/bucket creation and warmup are excluded.
- Publish scenarios include one trailing `flush()` (a round-trip) in the timed
  window so the rate reflects delivered throughput, not just enqueue speed.
- Streams/KV/Object buckets are uniquely named per run and torn down afterward.
- Latency scenarios report nearest-rank percentiles over per-op samples.
- Consumer scenarios time first-delivery → N-th-delivery.

## Baseline

`nats-server 2.12.7`, Apple Swift 6.2.4, macOS arm64, release build, 16-byte
payloads, warm steady state; representative figures over two full passes. Treat the
consumer rows as ranges: throughput there swings run-to-run with how much backlog
sits in the deliver-inbox buffer at read time.

| Scenario | Metric |
| --- | --- |
| corePub | ~1.15M msgs/s (~18.5 MB/s) |
| corePubSub | ~450k msgs/s |
| reqReply | p50 ~100 µs, p90 ~126 µs, p99 ~155 µs |
| jsPublish (sync, ack-awaited) | ~14k msgs/s |
| jsPublishAsync (batched window) | ~125k msgs/s |
| jsPublishAsyncConc (concurrent producers) | ~110k msgs/s |
| kvPutGet | put ~13k / get ~12k ops/s |
| objPutGet (32 MiB) | put ~470 MB/s, get ~1.4–1.5 GB/s |
| pullConsume (ack-none) | ~150–210k msgs/s |
| pushConsume (no heartbeat) | ~155–370k msgs/s |
| pushConsumeHB (heartbeat + flow control) | ~245–270k msgs/s |
| orderedConsume | ~160k msgs/s |

Round-trip-bound scenarios (jsPublish, kvPutGet, reqReply) run one to two orders of
magnitude below fire-and-forget publish because each awaits a server ack.
`objPutGet` get exceeds put because put pays chunking + SHA-256 digest. The four
consumer scenarios now land in the same order of magnitude — the ordered consumer
is no longer the outlier it was (see below).

## Ordered-consumer delivery-stall fix

`orderedConsume` used to stall outright on ~50% of runs — 0 delivery, hang to the
120s scenario timeout. Root cause was a lifetime race, not throughput: the ordered
engine's pump holds only `[weak self]` and `OrderedMessageSource` references the
consumer weakly (to avoid a retain cycle), so nothing in the returned
`ConsumeContext → stream → source` chain kept the `OrderedConsumer` alive. A caller
that drops the consumer handle and keeps only the context — exactly what this harness
does — raced the pump's weak-self load against ARC releasing the consumer. Consumer
loses → its `deinit` deletes the ephemeral server consumer and finishes the stream →
silent stall. (Confirmed server-side: during a stall the stream held all messages but
the consumer was already gone.)

Fix: the caller-owned context now strongly pins the consumer for its lifetime
(`owner: self`), matching push/pull consumers, while the internal source ref stays
weak so the no-leak guarantee holds. Verified at 20/20 harness runs (was ~50% stall).
A unit test can't reliably reproduce the race — the in-process test scheduler wins it
for the pump every time — so the release harness loop is the regression check.

## Optimizations

Three per-message hot-path fixes preceded the stall fix, each measured and guarded by
the chaos suite. Absolute numbers have risen further on this hardware since (see the
Baseline); the ratios are what each fix bought.

### 1. Per-message task group in push delivery

`PushDelivery.race()` built a two-child `withThrowingTaskGroup` (reader + a
`2×idleHeartbeat` sleeper) on EVERY message when idle heartbeat was enabled — and
the ordered consumer hardcodes heartbeat on, so the keystone always paid it. A
non-suspending `SubscriptionIterator.tryNext()` poll now takes an already-buffered
message directly, entering the task group only when the deliver inbox is idle —
where missed-heartbeat detection legitimately belongs (the server suppresses
heartbeats during active delivery, so skipping the timeout while data flows is
correct; the timeout re-arms the moment traffic stops). ~7–10× on the
heartbeat-enabled push path.

### 2. Per-message task group in the pull fetch loop

`FetchResult.FetchIterator.nextWithTimeout` had the identical anti-pattern — a
per-message task group whenever `idleHeartbeat` was set, which the continuous
consume loop always sets. The same `tryNext()` fast path applies. ~2.3×.

### 3. Full metadata parse in the ordered accept-check

`OrderedConsumer.handleData` built a full `MessageMetadata` (~5 String
allocations) plus a throwaway `JetStreamMessage` per message to read three fields.
`parseAckFields` extracts the serial, stream seq and consumer seq directly from
the `$JS.ACK` reply subject as `Substring`s — no per-message heap allocation —
and is byte-for-byte equivalent to the old parser (same token indices, numeric
validation and ignore-on-failure behavior). ~2×.

## Known ceilings (future work)

- **Async publish (~125k msgs/s)** — `publishAsync` (batched, bounded window) is
  bounded by the send/ack pipeline and the in-flight window (4000), NOT by the
  publisher's serialization mechanism: a head-to-head A/B of an actor-based vs a
  single-lock publisher showed the lock version ~10% faster sequentially, ~17%
  faster with concurrent producers, and far more consistent (CV ~4% vs ~27%) — a real
  but modest win that confirmed the bottleneck is the pipeline, not the actor hop.
  The lock version is what ships. Raising the window or pipelining the connection
  write path would move throughput more than any change to the publisher's locking.
- **Ordered consumer double mailbox** — the public `consume`/`messages`/`next` path
  funnels the reset engine's `natsMessages` `AsyncThrowingStream` through a *second*
  `MessageStream` mailbox (two async queues vs one for push/pull). It no longer shows
  up as a throughput outlier at these message sizes, but the extra hop is real;
  collapsing it would touch the unified-mailbox delivery keystone, so it stays
  deferred as higher-risk / lower-value (KV/Object watch, the ordered engine's primary
  use, reads `natsMessages` directly — the faster single-queue path).
- **Overlapping pulls** — the pull loop still fetches the next batch only once the
  current one drains, leaving a round-trip gap between batches. With the per-message
  cost gone this is a minor remaining factor; an overlapping-pull scheme (top up
  before drain, as nats.go does) would trim it further.
