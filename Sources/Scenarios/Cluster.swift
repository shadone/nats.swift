// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation
import JetStream
import Nats

/// Raised when one or more R3 cluster assertions failed, so `main` reports a
/// non-zero exit for the `cluster` scenario.
private struct ClusterAssertionFailure: Error, CustomStringConvertible {
    let failures: [String]
    var description: String { "cluster assertions failed: \(failures.joined(separator: "; "))" }
}

/// The three client seed URLs of the local 3-node cluster started by
/// `cluster/cluster-up.sh`. Overridable via `NATS_CLUSTER_URLS` (comma-separated)
/// for a differently-addressed cluster; `NATS_URL` is intentionally ignored here
/// because this scenario always wants every seed so it survives a node loss.
private func clusterSeedURLs() -> [URL] {
    let defaults = "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"
    let raw = ProcessInfo.processInfo.environment["NATS_CLUSTER_URLS"] ?? defaults
    return raw.split(separator: ",").compactMap {
        URL(string: $0.trimmingCharacters(in: .whitespaces))
    }
}

/// Renders a `ClusterInfo` (leader + non-leader replica peers) as one line:
/// the total peer count is `leader + replicas`, and each replica shows whether it
/// is `current` (caught up to the leader) and its lag.
private func describeCluster(_ cluster: ClusterInfo?) -> String {
    guard let cluster else { return "cluster=<none> (not a clustered asset?)" }
    let leader = cluster.leader ?? "<none>"
    let replicas = cluster.replicas ?? []
    let peerCount = (cluster.leader != nil ? 1 : 0) + replicas.count
    let peerDetail =
        replicas
        .map { "\($0.name)(current=\($0.current), lag=\($0.lag ?? 0))" }
        .joined(separator: ", ")
    return
        "cluster=\(cluster.name ?? "?") leader=\(leader) peers=\(peerCount) "
        + "replicas=[\(peerDetail)]"
}

/// R3 cluster validation: exercises KV, streams, and a durable consumer against a
/// REPLICATED (three-replica) clustered JetStream and PROVES replication end to
/// end by inspecting the reported leader + replica peers. Run it against the local
/// 3-node cluster from `cluster/cluster-up.sh`; see `CLUSTER.md` for the failover
/// drill (kill the leader, watch a new one get elected, re-run).
func runCluster() async throws {
    let seeds = clusterSeedURLs()
    out("cluster", "seeds: \(seeds.map(\.absoluteString).joined(separator: ", "))")
    let client = NatsClientOptions().urls(seeds).reconnectWait(0.25).unlimitedReconnects().build()
    try await client.connect()
    out("cluster", "connected")

    let js = JetStreamContext(client: client)
    var failures: [String] = []

    // --- R3 KeyValue bucket -------------------------------------------------
    let bucketName = "scen_cluster_kv"
    _ = try? await js.deleteKeyValue(bucket: bucketName)
    var kvConfig = KeyValueConfig(bucket: bucketName)
    kvConfig.replicas = 3
    let kv = try await js.createKeyValue(cfg: kvConfig)
    out("cluster", "KV bucket \(bucketName) created (replicas=3)")

    let kvPairs = ["region": "eu", "tier": "gold", "shard": "07"]
    for (key, value) in kvPairs.sorted(by: { $0.key < $1.key }) {
        let rev = try await kv.put(key, Data(value.utf8))
        out("cluster", "KV put \(key)=\"\(value)\" rev=\(rev)")
    }
    for key in kvPairs.keys.sorted() {
        guard let entry = try await kv.get(key) else {
            failures.append("KV get \(key) returned nil")
            continue
        }
        let value = String(decoding: entry.value, as: UTF8.self)
        out("cluster", "KV get \(key)=\"\(value)\" @rev\(entry.revision)")
    }

    let kvStatus = try await kv.status()
    out(
        "cluster",
        "KV status: bucket=\(kvStatus.bucket) values=\(kvStatus.values) "
            + "history=\(kvStatus.history) bytes=\(kvStatus.bytes)")

    // The KV backing stream is KV_<bucket>; its cluster info proves R3 for the KV.
    if let kvStream = try await js.getStream(name: "KV_\(bucketName)") {
        let info = try await kvStream.info()
        out(
            "cluster",
            "KV backing stream KV_\(bucketName): replicas=\(info.config.replicas) "
                + describeCluster(info.cluster))
        if info.config.replicas != 3 {
            failures.append("KV backing stream replicas=\(info.config.replicas), expected 3")
        }
        if info.cluster?.leader == nil {
            failures.append("KV backing stream has no cluster leader")
        }
    } else {
        failures.append("KV backing stream KV_\(bucketName) not found")
    }

    // --- R3 stream ----------------------------------------------------------
    let streamName = "SCEN_CLUSTER"
    let messageCount = 20
    _ = try? await js.deleteStream(name: streamName)
    let stream = try await js.createStream(
        cfg: StreamConfig(name: streamName, subjects: ["scen.cluster.>"], replicas: 3))
    out("cluster", "stream \(streamName) created (subjects=scen.cluster.>, replicas=3)")

    for i in 1...messageCount {
        _ = try await js.publish("scen.cluster.msg", message: Data("msg-\(i)".utf8)).wait()
    }
    out("cluster", "published \(messageCount) messages to scen.cluster.msg")

    // Fetch fresh server-side info and PROVE R3 replication.
    let info = try await stream.info()
    out("cluster", "stream info: messages=\(info.state.messages) replicas=\(info.config.replicas)")
    out("cluster", "stream \(describeCluster(info.cluster))")

    if info.config.replicas != 3 {
        failures.append("stream config.replicas=\(info.config.replicas), expected 3")
    }
    if let leader = info.cluster?.leader, !leader.isEmpty {
        out("cluster", "ASSERT stream leader present: \(leader) -- OK")
    } else {
        failures.append("stream reports no cluster leader")
    }
    // A healthy R3 stream reports the leader plus two non-leader replica peers.
    let replicaPeers = info.cluster?.replicas?.count ?? 0
    let totalPeers = (info.cluster?.leader != nil ? 1 : 0) + replicaPeers
    if totalPeers == 3 {
        out("cluster", "ASSERT stream has 3 peers (leader + \(replicaPeers) replicas) -- OK")
    } else {
        failures.append("stream reports \(totalPeers) peers (leader + replicas), expected 3")
    }
    let allCurrent = info.cluster?.replicas?.allSatisfy { $0.current } ?? false
    if allCurrent && replicaPeers > 0 {
        out("cluster", "ASSERT all replica peers current (caught up to leader) -- OK")
    } else {
        out("cluster", "note: not all replica peers report current yet (may still be catching up)")
    }
    if info.state.messages != UInt64(messageCount) {
        failures.append("stream stored \(info.state.messages) messages, expected \(messageCount)")
    }

    // --- durable consumer on the R3 stream ----------------------------------
    let consumer = try await js.createConsumer(
        stream: streamName,
        cfg: ConsumerConfig(
            name: "cluster-worker", ackPolicy: .explicit, ackWait: NanoTimeInterval(30)))
    out("cluster", "durable consumer 'cluster-worker' created")

    if let consumerInfo = try? await consumer.info() {
        out(
            "cluster",
            "consumer \(describeCluster(consumerInfo.cluster)) numPending=\(consumerInfo.numPending)"
        )
    }

    let acked = Locked<Int>(0)
    let context = try consumer.consume { message in
        let acknowledged = acked.withLock { count -> Int in
            count += 1
            return count
        }
        let text = message.payload.map { String(decoding: $0, as: UTF8.self) } ?? ""
        out("cluster", "consumed \(text) (#\(acknowledged))")
        Task { try? await message.ack() }
    }

    do {
        try await waitUntil(deadlineSeconds: 30) { acked.get() >= messageCount }
    } catch {
        failures.append("consumer only acked \(acked.get())/\(messageCount) before timeout")
    }
    try await Task.sleep(nanoseconds: 300_000_000)
    context.stop()

    let consumedCount = acked.get()
    if consumedCount == messageCount {
        out("cluster", "ASSERT consumed+acked all \(messageCount) messages -- OK")
    } else {
        failures.append("consumed \(consumedCount) messages, expected \(messageCount)")
    }

    // --- teardown + verdict -------------------------------------------------
    _ = try? await js.deleteStream(name: streamName)
    _ = try? await js.deleteKeyValue(bucket: bucketName)
    try? await client.close()

    out("cluster", "failover drill: kill the stream leader, then re-run -- see CLUSTER.md")
    if failures.isEmpty {
        out(
            "cluster",
            "DONE (PASS): R3 KV + stream + durable consumer verified on the 3-node cluster")
    } else {
        for failure in failures {
            out("cluster", "FAILURE: \(failure)")
        }
        out("cluster", "DONE (FAIL): \(failures.count) assertion(s) failed")
        throw ClusterAssertionFailure(failures: failures)
    }
}
