# Performance

Benchmarks for the nats.swift client, measured with the `PerfBench` harness
(`Sources/PerfBench/`) against a real `nats-server`. This is the performance
record for the `firstclass-kv` hardening work; it is not a marketing claim.

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
payloads, warm steady state. Representative figures — throughput of the consumer
scenarios varies run-to-run with how much backlog sits in the deliver-inbox
buffer at read time.

| Scenario | Metric |
| --- | --- |
| corePub | ~1.2M msgs/s (~20 MB/s) |
| corePubSub | ~390k msgs/s |
| reqReply | p50 ~100 µs, p99 ~160 µs |
| jsPublish (ack-awaited) | ~14.7k msgs/s |
| kvPutGet | put ~13.9k / get ~12.4k ops/s |
| objPutGet (32 MiB) | put ~460 MB/s, get ~1.4 GB/s |
| pullConsume (ack-none) | ~170–190k msgs/s |
| pushConsume (no heartbeat) | ~230–250k msgs/s |
| pushConsumeHB (heartbeat + flow control) | ~160–230k msgs/s |
| orderedConsume | ~42–58k msgs/s |

Round-trip-bound scenarios (jsPublish, kvPutGet, reqReply) are one to two orders
of magnitude below fire-and-forget publish because each awaits a server ack.
`objPutGet` get exceeds put because put pays chunking + SHA-256 digest.

## Optimizations

Three per-message hot-path fixes, each measured and guarded by the chaos suite:

### 1. Per-message task group in push delivery

`PushDelivery.race()` built a two-child `withThrowingTaskGroup` (reader + a
`2×idleHeartbeat` sleeper) on EVERY message when idle heartbeat was enabled — and
the ordered consumer hardcodes heartbeat on, so the keystone always paid it. A
non-suspending `SubscriptionIterator.tryNext()` poll now takes an already-buffered
message directly, entering the task group only when the deliver inbox is idle —
where missed-heartbeat detection legitimately belongs (the server suppresses
heartbeats during active delivery, so skipping the timeout while data flows is
correct; the timeout re-arms the moment traffic stops).

### 2. Per-message task group in the pull fetch loop

`FetchResult.FetchIterator.nextWithTimeout` had the identical anti-pattern — a
per-message task group whenever `idleHeartbeat` was set, which the continuous
consume loop always sets. The same `tryNext()` fast path applies.

### 3. Full metadata parse in the ordered accept-check

`OrderedConsumer.handleData` built a full `MessageMetadata` (~5 String
allocations) plus a throwaway `JetStreamMessage` per message to read three fields.
`parseAckFields` extracts the serial, stream seq and consumer seq directly from
the `$JS.ACK` reply subject as `Substring`s — no per-message heap allocation —
and is byte-for-byte equivalent to the old parser (same token indices, numeric
validation and ignore-on-failure behavior).

| Scenario | Before | After |
| --- | --- | --- |
| pushConsumeHB | ~23.5k msgs/s | ~160–230k msgs/s (~7–10×) |
| pullConsume | ~83k msgs/s | ~170–190k msgs/s (~2.3×) |
| orderedConsume | ~24k msgs/s | ~42–58k msgs/s (~2×) |
| pushConsume (no heartbeat) | ~126k msgs/s | ~230–250k msgs/s (no regression) |

## Known ceilings (future work)

- **orderedConsume ~42–58k msgs/s** — still the slowest consumer. With the
  task-group and parse costs removed, the remaining gap to the push consumers is
  the ordered engine's delivery plumbing: the public `consume`/`messages`/`next`
  path funnels the reset engine's `natsMessages` `AsyncThrowingStream` through a
  *second* `MessageStream` mailbox (two async queues vs one for push/pull).
  Collapsing that would touch the unified mailbox design and the keystone's
  delivery path, so it is deferred as higher-risk / lower-value: KV/Object watch
  (the ordered engine's primary use) reads `natsMessages` directly — the faster
  single-queue path — and is inherently low-throughput.
- **Overlapping pulls** — the pull loop still fetches the next batch only once the
  current one drains, leaving a round-trip gap between batches. With the
  per-message cost gone this is a minor remaining factor; an overlapping-pull
  scheme (top up before drain, as nats.go does) would trim it further.
