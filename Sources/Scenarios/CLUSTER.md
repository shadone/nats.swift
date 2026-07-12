# R3 cluster validation runbook

This exercises the `nats.swift` client against a **replicated (R3)** clustered
JetStream with real RAFT leader election and failover -- behaviour the single-node
test suite never reaches. It brings up three `nats-server` processes (or three
containers), then runs the `cluster` scenario, which creates an R3 KV bucket and
an R3 stream, publishes, consumes with a durable consumer, and **asserts the
reported leader + 2 replica peers** to prove three-way replication end to end.

- Scenario source: `Sources/Scenarios/Cluster.swift`
- Cluster scripts: `Sources/Scenarios/cluster/`

## 1. Start the cluster (no Docker)

Three plain `nats-server` processes on ports `4222/4223/4224` form a RAFT cluster
named `JSC`. Each node gets its own store dir and server name; they share the
cluster name and the full routes list. Requires `nats-server` (v2.12+) on `PATH`.

```bash
./Sources/Scenarios/cluster/cluster-up.sh
```

The script waits until all three client ports accept connections **and** a
JetStream metadata leader has been elected, then prints the client seed URLs:

```
  node n1 listening on 4222
  node n2 listening on 4223
  node n3 listening on 4224
  meta leader elected: JetStream cluster new metadata leader: n2/JSC

cluster READY -- 3 nodes, cluster 'JSC', JetStream R3-capable
client seed URLs:
  nats://127.0.0.1:4222
  nats://127.0.0.1:4223
  nats://127.0.0.1:4224
```

Store dirs, logs (`n1.log`/`n2.log`/`n3.log`) and pid files (`n1.pid` etc.) live
under `/tmp/nats-cluster`.

### Docker alternative

The shell scripts are the no-Docker path. If you prefer containers, a compose
file with three `nats:2.12` services (same cluster name, routes, and `-js`) is
provided:

```bash
docker compose -f Sources/Scenarios/cluster/docker-compose.yml up -d
swift run Scenarios cluster
docker compose -f Sources/Scenarios/cluster/docker-compose.yml down -v
```

## 2. Run the validation scenario

The scenario connects to **all three seeds** (so it survives a node loss) and
always uses every seed regardless of `NATS_URL`. Override the seed list with
`NATS_CLUSTER_URLS` (comma-separated) for a differently-addressed cluster.

```bash
swift build
./.build/debug/Scenarios cluster
# or: swift run Scenarios cluster
```

### Expected output

```
[..] [cluster] seeds: nats://127.0.0.1:4222, nats://127.0.0.1:4223, nats://127.0.0.1:4224
[..] [cluster] connected
[..] [cluster] KV bucket scen_cluster_kv created (replicas=3)
[..] [cluster] KV put region="eu" rev=1
[..] [cluster] KV get region="eu" @rev1
[..] [cluster] KV status: bucket=scen_cluster_kv values=3 history=1 bytes=173
[..] [cluster] KV backing stream KV_scen_cluster_kv: replicas=3 cluster=JSC leader=n2 peers=3 replicas=[n1(current=true, lag=0), n3(current=true, lag=0)]
[..] [cluster] stream SCEN_CLUSTER created (subjects=scen.cluster.>, replicas=3)
[..] [cluster] published 20 messages to scen.cluster.msg
[..] [cluster] stream info: messages=20 replicas=3
[..] [cluster] stream cluster=JSC leader=n2 peers=3 replicas=[n1(current=true, lag=0), n3(current=true, lag=0)]
[..] [cluster] ASSERT stream leader present: n2 -- OK
[..] [cluster] ASSERT stream has 3 peers (leader + 2 replicas) -- OK
[..] [cluster] ASSERT all replica peers current (caught up to leader) -- OK
[..] [cluster] ASSERT consumed+acked all 20 messages -- OK
[..] [cluster] DONE (PASS): R3 KV + stream + durable consumer verified on the 3-node cluster
```

`peers=3` (leader + two `current` replica peers) is the proof of R3. Exit code is
`0` on PASS, `1` if any assertion fails.

The scenario **self-cleans**: it deletes `scen_cluster_kv` and `SCEN_CLUSTER` on
exit so it is idempotent and re-runnable. That is why the failover drill below
uses a dedicated, persistent stream you inspect between steps.

## 3. Failover test (kill the leader)

This is the real payload: prove a new leader is elected when the current one dies
and the client keeps working. Node -> pid-file map:

| server name | client port | pid file                  |
| ----------- | ----------- | ------------------------- |
| n1          | 4222        | `/tmp/nats-cluster/n1.pid` |
| n2          | 4223        | `/tmp/nats-cluster/n2.pid` |
| n3          | 4224        | `/tmp/nats-cluster/n3.pid` |

### 3a. Create a persistent R3 stream and find its leader

```bash
S=nats://127.0.0.1:4222
nats --server $S stream add FAILOVER_TEST --subjects "failover.>" --replicas 3 --defaults
for i in 1 2 3 4 5; do nats --server $S pub failover.msg "pre-$i"; done
nats --server $S stream info FAILOVER_TEST | grep -iA6 "Cluster Information"
```

Read the `Leader:` line, e.g. `Leader: n3`.

### 3b. Kill the leader's process

Map the leader server name to its pid file and kill it:

```bash
# leader was n3 in this example:
kill "$(cat /tmp/nats-cluster/n3.pid)"
```

### 3c. Confirm a NEW leader was elected

Query a **surviving** seed (not the one you just killed):

```bash
nats --server nats://127.0.0.1:4222 stream info FAILOVER_TEST | grep -iA6 "Cluster Information"
```

The `Leader:` line now names a different node (e.g. `n1`); the killed node shows
`OFFLINE, outdated`. The quorum (2 of 3) is intact, so the stream stays writable:

```bash
for i in 6 7 8 9 10; do nats --server nats://127.0.0.1:4222 pub failover.msg "post-$i"; done
nats --server nats://127.0.0.1:4222 stream info FAILOVER_TEST | grep -iE "Messages:|Leader:"
# -> Messages: 10  (5 pre + 5 post-failover)
```

### 3d. Confirm the client still works against the degraded cluster

Run `live-consume` pointed at a surviving seed while a node is down. The Swift
client connects, publishes steady traffic, and an ordered consumer delivers every
sequence contiguously (no gap) through JetStream on the 2-node quorum:

```bash
NATS_URL="nats://127.0.0.1:4222" SCEN_DURATION=6 ./.build/debug/Scenarios live-consume
```

> Note: re-running `cluster` while a node is down will fail to *create* a fresh R3
> stream -- placing 3 replicas needs 3 live nodes. That is a JetStream placement
> constraint, not a client bug. Re-run `cluster` only after the killed node
> rejoins (3d below), or use `live-consume` (above) to prove the client during the
> degraded window.

### 3e. Restart the node and watch it re-sync

```bash
# restart n3 with the SAME args cluster-up.sh used:
ROUTES="nats://127.0.0.1:6222,nats://127.0.0.1:6223,nats://127.0.0.1:6224"
nats-server -js -sd /tmp/nats-cluster/n3 -server_name n3 -p 4224 \
  -cluster_name JSC -cluster nats://127.0.0.1:6224 -routes "$ROUTES" \
  -P /tmp/nats-cluster/n3.pid >/tmp/nats-cluster/n3.log 2>&1 &

sleep 4
nats --server nats://127.0.0.1:4222 stream info FAILOVER_TEST | grep -iA6 "Cluster Information"
```

The rejoined node flips from `OFFLINE, outdated` back to `current` once it catches
up. A full re-run of the `cluster` scenario now PASSES again with all three peers
`current`:

```bash
nats --server nats://127.0.0.1:4222 stream rm FAILOVER_TEST -f
./.build/debug/Scenarios cluster        # DONE (PASS)
```

## 4. Tear down

```bash
./Sources/Scenarios/cluster/cluster-down.sh          # stop nodes, keep store + logs
./Sources/Scenarios/cluster/cluster-down.sh --wipe   # stop nodes AND rm /tmp/nats-cluster
```
