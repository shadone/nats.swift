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
| pullConsume (ack-none) | ~83k msgs/s |
| pushConsume (no heartbeat) | ~120k msgs/s |
| pushConsumeHB (heartbeat + flow control) | ~90–240k msgs/s |
| orderedConsume | ~24k msgs/s |

Round-trip-bound scenarios (jsPublish, kvPutGet, reqReply) are one to two orders
of magnitude below fire-and-forget publish because each awaits a server ack.
`objPutGet` get exceeds put because put pays chunking + SHA-256 digest.

## Optimization: push delivery hot path

`PushDelivery.race()` built a two-child `withThrowingTaskGroup` (reader + a
`2×idleHeartbeat` sleeper) on EVERY message when idle heartbeat is enabled. That
capped push-with-heartbeat delivery and — because the ordered consumer hardcodes
heartbeat on — forced the keystone into it. A non-suspending
`SubscriptionIterator.tryNext()` poll now takes an already-buffered message
directly, entering the task group only when the deliver inbox is actually empty
(idle) — where missed-heartbeat detection legitimately belongs. Since the server
suppresses heartbeats during active delivery, skipping the timeout while data
flows is correct; the buffer drains and the timeout re-arms the moment traffic
stops.

| Scenario | Before | After |
| --- | --- | --- |
| pushConsumeHB | ~23.5k msgs/s | ~90–240k msgs/s (~7×) |
| orderedConsume | ~17.8k msgs/s | ~24k msgs/s (~1.4×) |
| pushConsume (no heartbeat) | ~126k msgs/s | unchanged (no regression) |

## Known ceilings (future work)

- **orderedConsume ~24k msgs/s** — now the slowest consumer. With the task-group
  cost removed, the ceiling is the ordered consumer's per-message bookkeeping
  (ack-subject metadata parse + cursor tracking + the extra actor /
  `AsyncThrowingStream` hop the reset wrapper interposes). Reducing it touches the
  no-loss/no-dup cursor logic, so it is higher-risk; the ordered consumer backs
  low-throughput KV/Object watch, so its absolute rate matters less in practice.
- **pullConsume ~83k msgs/s** — the sequential fetch-batch loop leaves a
  round-trip gap between batches. An overlapping-pull scheme (issue the next pull
  before the current batch drains, as nats.go does) would close much of the gap
  to the push consumers.
