# Scenarios — semi-manual real-world verification of nats.swift

Runnable demonstration programs for the first-class nats.swift client
(`firstclass-kv` branch). Each one drives a real feature against a real
`nats-server` and prints clear, timestamped, observable lines to stdout so a
human can follow along and cross-check with the `nats` CLI. These are NOT unit
tests: you run them, watch them, and — for the long-lived ones — inject faults
(restart or `kill -9` the server) and watch the client recover.

Every scenario reads the server URL from `NATS_URL` (default
`nats://localhost:4222`) and quiets the client's own logging to `.warning`.

## Prerequisites

- A running JetStream server:

  ```
  nats-server -js -p 4222
  ```

- The `nats` CLI (https://github.com/nats-io/natscli) for the interop
  cross-checks and fault injection. Point it at the same server with
  `--server nats://localhost:4222` (or `export NATS_URL=...`).

## Running

Build once, then run a scenario by name:

```
swift build
swift run Scenarios <name>
```

Or run the built binary directly (faster, no rebuild check):

```
.build/debug/Scenarios <name>
```

Release build (recommended for `async-publish` throughput and `live-consume`):

```
swift run -c release Scenarios <name>
```

Override the server:

```
NATS_URL=nats://localhost:4222 swift run Scenarios <name>
```

`<name>` is one of: `kv-watch`, `object-transfer`, `work-queue`, `service`,
`async-publish`, `live-consume`, `cluster`, `slow-consumer`. Running
with no name (or an unknown one) prints the usage list.

The two long-lived scenarios (`service`, `live-consume`) run until you press
Ctrl-C. For a bounded, scripted run set `SCEN_DURATION=<seconds>` and they exit
cleanly after that long (used by the verification steps below).

Two companion runbooks drive scenarios against harsher or more specific
conditions: `CLUSTER.md` (R3 cluster + failover) and **`FAULTS.md`** (toxiproxy/
lame-duck fault injection; uses `live-consume` and `slow-consumer` plus the
scripts in `fault/`).

---

## kv-watch

Purpose: KeyValue store + live watch. A background `watchAll()` prints the
initial snapshot, the `nil` end-of-initial marker, then every live mutation as
the foreground puts / updates / deletes / creates keys and reads them back.

Run:

```
swift run Scenarios kv-watch
```

Expected output (representative):

```
[hh:mm:ss.SSS] [kv] bucket scenarios_kv ready
[hh:mm:ss.SSS] [watch] end of initial values
[hh:mm:ss.SSS] [kv] PUT config.timeout rev=1
[hh:mm:ss.SSS] [kv] UPDATE config.timeout rev=3 (expected rev1)
[hh:mm:ss.SSS] [kv] DELETE config.retries
[hh:mm:ss.SSS] [kv] CREATE config.mode rev=5
[hh:mm:ss.SSS] [watch] observed config.timeout = "45" @rev3 op=PUT
[hh:mm:ss.SSS] [watch] observed config.retries = "" @rev4 op=DEL
[hh:mm:ss.SSS] [kv] GET config.timeout = "45" @rev3
[hh:mm:ss.SSS] [kv] GET config.retries = nil (deleted)
[hh:mm:ss.SSS] [kv] status: bucket=scenarios_kv values=3 history=1 bytes=330
[hh:mm:ss.SSS] [kv] DONE
```

CLI cross-check (run while, or after, the scenario — the bucket persists until
the next run deletes it):

```
nats kv ls
nats kv get scenarios_kv config.timeout
nats kv watch scenarios_kv
nats kv history scenarios_kv config.timeout
```

---

## object-transfer

Purpose: ObjectStore round-trip. A small object (buffered `put` + `getBytes`,
exact-bytes assertion) and an 8 MiB object (`put` + streamed `getStream`, size
assertion). Completing the stream WITHOUT throwing is itself the SHA-256 digest
+ size verification — `ObjectStreamReader` verifies at the end of iteration.

Run:

```
swift run Scenarios object-transfer
```

Expected output (representative):

```
[hh:mm:ss.SSS] [obj] bucket scenarios_obj ready
[hh:mm:ss.SSS] [obj] small.txt put size=50 chunks=1 digest=SHA-256=Fz...4M=
[hh:mm:ss.SSS] [obj] small.txt round-trip PASS (50 bytes)
[hh:mm:ss.SSS] [obj] large.bin put size=8388608 chunks=64 digest=SHA-256=0e...kU=
[hh:mm:ss.SSS] [obj] large.bin streamed 8388608 bytes in 64 chunks
[hh:mm:ss.SSS] [obj] large.bin size+digest PASS
[hh:mm:ss.SSS] [obj] DONE (PASS)
```

CLI cross-check (the scenario deletes its bucket at the end, so run these while
it is still executing, or comment out the final delete):

```
nats object ls scenarios_obj
nats object info scenarios_obj large.bin
```

---

## work-queue

Purpose: durable consumer resume on a workqueue-retention stream. Publishes 20
jobs, consumes + acks the first 10 through a durable pull consumer, `stop()`s,
then starts a NEW consume on the SAME durable to drain the rest. Each job is
processed exactly once overall. A short `ackWait` (2s) lets the jobs left unacked
in phase 1 redeliver in phase 2.

Run:

```
swift run Scenarios work-queue
```

Expected output (representative):

```
[hh:mm:ss.SSS] [wq] stream SCEN_WQ ready (workqueue retention)
[hh:mm:ss.SSS] [wq] published 20 jobs
[hh:mm:ss.SSS] [wq] phase1 delivered job-1 (acked #1)
...
[hh:mm:ss.SSS] [wq] phase1 delivered job-10 (acked #10)
[hh:mm:ss.SSS] [wq] phase1 stopped after acking 10; unique so far=10
[hh:mm:ss.SSS] [wq] phase2 delivered job-11 (delivery #1)
...
[hh:mm:ss.SSS] [wq] phase2 delivered job-20 (delivery #10)
[hh:mm:ss.SSS] [wq] phase1 acked=10 phase2 deliveries=10 unique jobs=20/20
[hh:mm:ss.SSS] [wq] DONE (PASS: durable resumed, all 20 delivered)
```

Note: phase 2 starts delivering ~2s after phase 1 stops — that is the `ackWait`
elapsing so the never-acked jobs 11-20 become redeliverable.

CLI cross-check (run before the scenario deletes the stream, or from a second
terminal mid-run):

```
nats stream info SCEN_WQ
nats consumer info SCEN_WQ worker
```

Try a fault (stop mid-stream): the scenario already demonstrates a mid-stream
stop and durable resume. To see it manually, run the scenario and in another
terminal watch the consumer's `num_ack_pending` climb and fall:

```
watch -n0.5 'nats consumer info SCEN_WQ worker | grep -i pending'
```

---

## service

Purpose: a NATS micro Service. Registers a `calc` service with an `add`
endpoint (on subject `calc.add`) that adds two integers from a JSON request,
then keeps running so you can call it and inspect discovery with the CLI.

Run (leave it running; Ctrl-C to stop):

```
swift run Scenarios service
```

Bounded run (exits after 20s):

```
SCEN_DURATION=20 swift run Scenarios service
```

Expected output (representative):

```
[hh:mm:ss.SSS] [service] service 'calc' v1.0.0 running with endpoint 'add' (subject calc.add)
[hh:mm:ss.SSS] [service] call it:   nats req calc.add '{"a":2,"b":3}'
[hh:mm:ss.SSS] [service] add(2, 3) -> 5
[hh:mm:ss.SSS] [service] add(40, 2) -> 42
[hh:mm:ss.SSS] [service] bad request "not-json": dataCorrupted(...)
[hh:mm:ss.SSS] [service] DONE
```

CLI cross-check (while the service is running):

```
nats req calc.add '{"a":2,"b":3}'      # -> {"sum":5}
nats req calc.add '{"a":40,"b":2}'     # -> {"sum":42}
nats req calc.add 'not-json'           # -> Nats-Service-Error: ... code 400
nats micro ping
nats micro info calc
nats micro stats calc
```

Note: the endpoint listens on the subject `calc.add` because the scenario passes
`subject: "calc.add"` to `addEndpoint`. Without an explicit subject, a nats.swift
endpoint listens on its bare NAME (`add`) — the service name is not a subject
prefix (this matches nats.go).

---

## async-publish

Purpose: the batched async publisher end to end. Fires 20,000 `publishAsync`
back to back through the bounded in-flight window, drains with
`publishAsyncComplete`, then confirms the window emptied
(`publishAsyncPending() == 0`) and the stream stored exactly N messages.

Run (release build recommended):

```
swift run -c release Scenarios async-publish
```

Expected output (representative):

```
[hh:mm:ss.SSS] [ap] stream SCEN_AP ready
[hh:mm:ss.SSS] [ap] firing 20000 async publishes ...
[hh:mm:ss.SSS] [ap] all fired; draining acks (publishAsyncComplete) ...
[hh:mm:ss.SSS] [ap] published 20000 in 0.46s = 43801 msgs/sec, pending=0
[hh:mm:ss.SSS] [ap] stream reports 20000 messages (expected 20000)
[hh:mm:ss.SSS] [ap] DONE (PASS)
```

(The rate is machine-dependent; a debug build is several times slower than
release. What matters for correctness is `pending=0` and `stream reports 20000`.)

CLI cross-check (run before the scenario deletes the stream, or from a second
terminal mid-run):

```
nats stream info SCEN_AP
```

---

## live-consume

Purpose: the long-lived fault-injection harness. An ordered consumer prints each
delivered stream sequence while a background task publishes one message every
500ms. Leave it running and restart / `kill -9` the server in another terminal
to watch delivery resume CONTIGUOUSLY (no gap, no duplicate) across the
reconnect — the ordered consumer's no-loss / no-dup recovery.

Run (leave it running; Ctrl-C to stop):

```
swift run -c release Scenarios live-consume
```

Bounded run (exits after 30s):

```
SCEN_DURATION=30 swift run Scenarios live-consume
```

Expected output (steady state):

```
[hh:mm:ss.SSS] [live] stream SCEN_LIVE ready
[hh:mm:ss.SSS] [live] delivered seq=1 "tick-1"
[hh:mm:ss.SSS] [live] delivered seq=2 "tick-2"
...
[hh:mm:ss.SSS] [live] heartbeat: last delivered seq=6
```

Each delivered stream `seq` increases by exactly 1. A discontinuity would print
a trailing `<-- GAP` marker; you should never see one.

CLI cross-check (in another terminal):

```
nats stream info SCEN_LIVE
nats consumer ls SCEN_LIVE
```

Try a fault (the whole point of this scenario):

1. Start it and let it deliver a dozen messages.
2. In another terminal, restart the server with the SAME store directory so the
   stream and its messages survive:

   ```
   # graceful restart (Ctrl-C the server, then start it again), or a hard kill:
   kill -9 $(pgrep -f 'nats-server -js'); nats-server -js -p 4222 &
   ```

   (If you started the server with `-sd <dir>`, restart it with the same `-sd
   <dir>` so JetStream state persists.)
3. Watch the harness: during the outage the `heartbeat` line keeps printing with
   an unchanged `last delivered seq`, publishes fail silently, then delivery
   resumes with the NEXT contiguous `seq` (no gap, no duplicate). The payload
   `tick-N` number may skip (the ticks whose publish failed during the outage
   are lost), but the stream `seq` stays contiguous — that is the invariant.

Observed across a hard `kill -9` + restart mid-run (seq stays contiguous
10 -> 11 while the payload skips tick-11):

```
[..08.537] [live] delivered seq=10 "tick-10"
[..10.139] [live] heartbeat: last delivered seq=10
[..13.324] [live] heartbeat: last delivered seq=10
[..14.625] [live] delivered seq=11 "tick-12"
[..15.158] [live] delivered seq=12 "tick-13"
```
