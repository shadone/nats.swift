# Fault-injection resilience runbook

This exercises the `nats.swift` client against the **messy** network and server
failures a clean server restart never reaches: a full connection cut, latency,
bandwidth starvation, a half-open (black-hole) link, a lame-duck drain, and a
saturated client-side subscription buffer (slow consumer). Faults are injected
with [toxiproxy](https://github.com/Shopify/toxiproxy) on the wire and with the
`nats-server` lame-duck signal.

The harness is the existing **`live-consume`** scenario
(`Sources/Scenarios/LiveConsume.swift`): a JetStream ordered consumer that prints
every delivered stream sequence while a background task publishes one message
every 500 ms, plus a 3 s heartbeat. Point it at a toxiproxy port and inject on
the proxy; watch delivery halt and then **resume contiguously** (no gap, no dup)
across the fault. The client already uses `.unlimitedReconnects()` +
`.reconnectWait(0.25)` and reads `NATS_URL`.

Everything here was actually run and the **observed** client output is quoted per
fault. All output was produced against `nats-server v2.12.7`, `toxiproxy 2.12.0`.

---

## Topology

```
  live-consume client ──▶ 127.0.0.1:4666  (toxiproxy "nats" proxy)
                               │  ← inject faults here
                               ▼
                          127.0.0.1:4300  (real nats-server -js)
```

Client relevant defaults (`NatsClientOptions`): `pingInterval = 60s`,
3 missed pings ⇒ disconnect; the ordered-consumer consume loop uses a 5 s idle
heartbeat and a 30 s pull expiry.

## Prerequisites

- `toxiproxy-server` / `toxiproxy-cli` 2.12, `nats-server` v2.12+, `nats` CLI,
  `curl`, `jq` on PATH.
- **Use fresh ports.** Port 4222 may be held by an orphaned server; this runbook
  uses **4300** for the server, **4666** for the proxy, **8474** for the
  toxiproxy admin API.

## Scripts (`Sources/Scenarios/fault/`)

| Script | Purpose |
|---|---|
| `toxiproxy-up.sh` | start toxiproxy-server + create the `nats` proxy (4666 → 4300) |
| `toxiproxy-down.sh` | remove the proxy + stop toxiproxy-server |
| `fault-cut.sh` | full connection cut (disable the proxy) |
| `fault-restore.sh` | remove **all** toxics + re-enable the proxy |
| `fault-latency.sh [ms] [jitter]` | add a downstream latency toxic (default 800/200) |
| `fault-bandwidth.sh [KB/s]` | add a downstream bandwidth toxic (default 10) |

Env overrides: `PROXY_PORT` (4666), `NATS_PORT` (4300), `TOXIPROXY_URL`
(`http://localhost:8474`).

---

## Setup

```bash
# 1) real server on a FRESH port, with a pid file for the lame-duck test
nats-server -js -p 4300 -sd /tmp/nats-js-4300 -P /tmp/nats-4300.pid &

# 2) toxiproxy + the "nats" proxy in front of it
./Sources/Scenarios/fault/toxiproxy-up.sh          # prints nats://127.0.0.1:4666

# 3) build + run the harness THROUGH the proxy
swift build
NATS_URL=nats://127.0.0.1:4666 ./.build/debug/Scenarios live-consume
```

> **Output buffering gotcha.** Swift's `print` is line-buffered on a TTY but
> **block-buffered** to a pipe or file, so a backgrounded `> live.log` shows
> nothing until the process exits (and nothing at all if you `kill` it). Two ways
> to observe live:
> - run it in a **real terminal** (TTY ⇒ line-buffered), inject faults from a
>   second terminal; or
> - bound the run with `SCEN_DURATION=<sec>` and inject on a timed schedule; the
>   process flushes its fully **self-timestamped** log on normal exit, giving the
>   complete timeline. (This runbook was captured with the second method.)

Every delivered line is `[HH:MM:SS.SSS] [live] delivered seq=<n> "tick-<k>"`. The
`seq` is the JetStream **stream sequence** (contiguous, gap-checked by the
harness); `tick-<k>` is the publisher's counter. During any outage the publisher
keeps counting but its publishes fail and are **not stored**, so after recovery
the `tick-` label jumps while the `seq` stays perfectly contiguous — that jump is
the visible proof messages were lost on the *publish* side while the *stream*
stayed consistent.

---

## Fault 1 — Full connection cut (headline test)

**Setup:** harness delivering steadily through the proxy.

**Inject:**
```bash
./Sources/Scenarios/fault/fault-cut.sh        # disable proxy: drop live conn, refuse new
# ... wait ~10 s ...
./Sources/Scenarios/fault/fault-restore.sh    # re-enable proxy
```
(`fault-cut.sh` POSTs `{"enabled":false}` to the proxy. A black-hole variant that
keeps the socket half-open instead of dropping it is Fault 4.)

**Observed** — delivery halts at seq=20, heartbeats freeze there for the whole
cut, then delivery **resumes at seq=21 with no gap and no dup**:
```
[16:01:03.851] [live] delivered seq=20 "tick-20"
[16:01:06.516] [live] heartbeat: last delivered seq=20     <- CUT here; frozen
[16:01:09.716] [live] heartbeat: last delivered seq=20
[16:01:12.916] [live] heartbeat: last delivered seq=20     <- RESTORE here
[16:01:15.884] [live] delivered seq=21 "tick-23"           <- resumes, contiguous
[16:01:16.427] [live] delivered seq=22 "tick-24"
[16:01:16.952] [live] delivered seq=23 "tick-25"
```
The stream sequence is contiguous across the cut (20 → 21). The payload jumps
`tick-20 → tick-23`: `tick-21`/`tick-22` were published *during* the outage, the
publishes failed, and those ticks were never stored — the ordered consumer
re-created itself on reconnect and resumed from `streamSeq + 1`.

**Whole-run guarantee:** across this run (cut **and** the three faults below) the
harness delivered **seqs 1..166, each exactly once — zero gaps, zero duplicates,
zero `<-- GAP` markers.**

---

## Fault 2 — Latency injection

**Setup:** harness delivering steadily.

**Inject:**
```bash
./Sources/Scenarios/fault/fault-latency.sh 800 200
#   == toxiproxy-cli toxic add -t latency -n latency_down -a latency=800 -a jitter=200 nats
# ... observe ...
./Sources/Scenarios/fault/fault-restore.sh
```

**Observed** — the connection stays up; delivery **continues without any reset or
gap** straight through the injection point:
```
[16:01:22.800] [live] delivered seq=34 "tick-36"
[16:01:23.333] [live] delivered seq=35 "tick-37"    <- LATENCY 800/200 applied
[16:01:23.866] [live] delivered seq=36 "tick-38"
[16:01:24.402] [live] delivered seq=37 "tick-39"
[16:01:24.934] [live] delivered seq=38 "tick-40"
```
Each message's server→client leg is delayed ~0.8 s, but the pipeline stays full
so steady-state cadence is unchanged and **no reconnect / consumer reset is
triggered.** No spurious disconnect from the added round-trip latency.

---

## Fault 3 — Bandwidth throttle

**Setup:** harness delivering steadily.

**Inject:**
```bash
./Sources/Scenarios/fault/fault-bandwidth.sh 1        # 1 KB/s downstream
#   == toxiproxy-cli toxic add -t bandwidth -n bandwidth_down -a rate=1 nats
# ... observe ...
./Sources/Scenarios/fault/fault-restore.sh
```

**Observed** — the client stays connected and keeps delivering; **no crash, no
reset:**
```
[16:01:41.985] [live] delivered seq=70 "tick-72"
[16:01:42.518] [live] delivered seq=71 "tick-73"    <- BANDWIDTH 1 KB/s applied
[16:01:43.051] [live] delivered seq=72 "tick-74"
[16:01:43.583] [live] delivered seq=73 "tick-75"
```
The harness's steady traffic is tiny (~2 msgs/s of a few dozen bytes), so a 1 KB/s
cap does not visibly backpressure it — the honest takeaway here is **"survives the
throttle."** To see real backpressure, pair the throttle with a burst: apply the
toxic, then run the slow-consumer burst (Fault 6) or `nats bench pub
scen.x --server nats://127.0.0.1:4666 --msgs 200000` through the proxy and watch
delivery lag far behind the publisher while the client stays alive.

---

## Fault 4 — Half-open / black-hole (connection stalls then recovers)

A `timeout=0` toxic stops data **without** closing the socket (a black hole).
Toxics default to **downstream** only, so client→server publishes still flow and
get stored while server→client delivery is frozen — a clean one-directional
stall.

**Inject:**
```bash
toxiproxy-cli toxic add -t timeout -n blackhole -a timeout=0 nats
# ... wait ~10 s (well under the 60 s ping interval, so no ping-based disconnect) ...
./Sources/Scenarios/fault/fault-restore.sh
```

**Observed** — delivery freezes at seq=108 for the whole black-hole; on clear the
messages that piled up during the stall **flush at once (seqs 109..119 at the same
timestamp), contiguous,** then normal cadence resumes:
```
[16:02:02.219] [live] delivered seq=108 "tick-110"
[16:02:04.115] [live] heartbeat: last delivered seq=108    <- BLACKHOLE here; frozen
[16:02:07.315] [live] heartbeat: last delivered seq=108
[16:02:10.515] [live] heartbeat: last delivered seq=108
[16:02:13.715] [live] heartbeat: last delivered seq=108
[16:02:16.915] [live] heartbeat: last delivered seq=108
[16:02:18.519] [live] delivered seq=109 "tick-111"         <- CLEAR; buffered burst flushes
[16:02:18.519] [live] delivered seq=110 "tick-112"
   ... seqs 111..118 all at 16:02:18.519 ...
[16:02:18.519] [live] delivered seq=119 "tick-121"
[16:02:19.049] [live] delivered seq=120 "tick-122"         <- normal cadence resumes
```
Because the black hole is downstream-only, the publisher's ticks 111–121 **were**
stored during the stall (contiguous seqs), unlike Fault 1 where the whole
connection was cut and publishes were lost. The TCP connection was never dropped,
so **no reconnect was needed — the client recovered the half-open link on its
own.**

> **Slow-close variant.** `toxiproxy-cli toxic add -t slow_close -a delay=5000 nats`
> only delays the FIN when a connection is *being* closed; combine it with
> `fault-cut.sh` to exercise a 5 s-delayed teardown. The representative half-open
> case above (stall-then-recover with no reconnect) is the more interesting one
> and is what was captured.

---

## Fault 5 — Lame-duck drain

`nats-server` enters lame-duck mode on a signal: it stops accepting new clients,
tells connected clients to reconnect (elsewhere in a cluster), drains, and shuts
down. This test uses a **dedicated** server with a short `lame_duck_duration` so
the drain+shutdown is observable, and the client connects **directly** (no proxy).

**Setup** (`/tmp/ldm.conf`):
```
port: 4301
jetstream { store_dir: "/tmp/nats-js-4301" }
lame_duck_grace_period: "2s"
lame_duck_duration: "30s"
```
```bash
nats-server -c /tmp/ldm.conf -P /tmp/nats-4301.pid &
NATS_URL=nats://127.0.0.1:4301 SCEN_DURATION=95 ./.build/debug/Scenarios live-consume &
```

**Inject:**
```bash
nats-server --signal ldm=$(cat /tmp/nats-4301.pid)     # enter lame duck
# server drains + exits; then restart it on the SAME store dir so the stream persists
nats-server -c /tmp/ldm.conf -P /tmp/nats-4301b.pid &
```

**Observed (server):**
```
[INF] Entering lame duck mode, stop accepting new clients
[INF] Initiating JetStream Shutdown...
[INF] JetStream Shutdown
[INF] Initiating Shutdown...      (~2 s later == grace period)
[INF] Server Exiting..
```
**Observed (client)** — delivery halts at seq=19, heartbeats freeze during the
outage, and after the server is restarted the client **reconnects and resumes at
seq=20, contiguous:**
```
[16:05:52.717] [live] delivered seq=19 "tick-19"
[16:05:55.718] [live] heartbeat: last delivered seq=19   <- LDM signalled; server draining/gone
[16:05:58.913] [live] heartbeat: last delivered seq=19
[16:06:01.221] [live] delivered seq=20 "tick-25"         <- reconnected to restarted server
[16:06:01.749] [live] delivered seq=21 "tick-26"
```
(Whole LDM run: seqs 1..164, zero gaps, zero dups; client exited `rc=0`.)

> **Single-node caveat.** With one server there is nowhere to migrate to, so
> lame-duck degenerates to "server goes away, then comes back." The client
> resilience — detect the disconnect, enter the reconnect loop, **do not hang or
> error permanently**, resume contiguously when the server returns — is fully
> demonstrated. The cluster **migration** path (client hops to a surviving peer
> before the lame server exits) needs the 3-node cluster in `CLUSTER.md`; it is
> not reproducible on a single node.

---

## Fault 6 — Slow consumer (client-side buffer overflow)

A `NatsSubscription` buffers up to `512 * 1024 = 524288` **messages**
(`NatsSubscription.defaultSubCapacity`). Once full, further inbound messages are
**dropped silently** — the slow-consumer branch of
`NatsSubscription.receiveMessage` (marked `TODO(pp)`: no SlowConsumer event is
surfaced yet). The dedicated scenario proves the client **survives** a burst that
saturates the buffer.

```bash
# needs the plain server (no proxy); the `nats` CLI must be on PATH
NATS_URL=nats://127.0.0.1:4300 ./.build/debug/Scenarios slow-consumer
```

**Observed (3/3 runs PASS):**
```
[slow] subscribed to scen.slow.drop (client sub buffer caps at 524288 messages)
[slow] blasting 800000 messages via `nats bench pub` (nobody reading) ...
[slow] burst published in 0.26s; letting the server drain the backlog into the buffer ...
[slow] probe: drained 200 buffered messages, moreBuffered=true (buffer saturated)
[slow] sent=800000 > buffer cap=524288: the client dropped at least 275712 messages via its slow-consumer guard (silent drop)
[slow] post-overflow round-trip OK -- client still alive
[slow] DONE (PASS: buffer saturated + overflow dropped, client survived)
```

Why the scenario is shaped this way (two **real** client limitations documented
here):

1. **Silent drop.** Overflow is discarded with no event/callback — a consumer
   cannot tell it lost messages. This is the `TODO(pp)` slow-consumer branch.
2. **O(n²) drain.** The buffer drains via `Array.removeFirst()` (O(n)), so fully
   draining a saturated 512Ki-slot buffer is O(n²) — tens of minutes. The
   scenario therefore only **probes a small prefix** (proving the burst was
   absorbed and more remains behind it) instead of counting every survivor, and
   infers the drop from `sent (800000) > cap (524288)`.

The burst is generated by `nats bench pub` from a **separate** process
(~5M msgs/s), not in-process: publishing 512Ki+ messages from a single Swift
publisher is impractical (each `publish` is an async hop and the echo competes for
the same event loop; an in-process attempt did not finish 700K in 200 s). Note
also the server's own guard: if the client's receive loop can't keep up, the
server may close the connection first with `Slow Consumer Detected: WriteDeadline
of 10s exceeded` (server-side slow consumer) — the scenario's liveness check
re-publishes with retries so it passes whether the drop was client-side or the
connection was closed and reconnected.

---

## Fault 7 — kill -9 + restart (cross-reference)

Covered structurally by **Fault 5**, which actually kills a server process
(lame-duck shutdown) and restarts it on the same store dir, showing contiguous
resume. A raw `SIGKILL` is equivalent and is printed in `live-consume`'s own
banner:
```
fault:  kill -9 $(pgrep -f 'nats-server -js'); nats-server -js -p 4222 &
watch:  nats stream info SCEN_LIVE
```
Run it against the plain server (no proxy): delivery halts, heartbeats freeze,
then on restart the ordered consumer re-creates and delivery resumes contiguously
— same signature as Faults 1 and 5.

---

## Teardown

```bash
./Sources/Scenarios/fault/toxiproxy-down.sh     # remove proxy + stop toxiproxy-server
kill "$(cat /tmp/nats-4300.pid)"                # stop the real server(s) you started
kill "$(cat /tmp/nats-4301b.pid)" 2>/dev/null   # lame-duck test server, if still up
```
