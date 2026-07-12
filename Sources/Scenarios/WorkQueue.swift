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

/// Parses the job number from a `job-<n>` payload.
private func jobNumber(_ message: JetStreamMessage) -> Int {
    guard let payload = message.payload,
        let text = String(data: payload, encoding: .utf8),
        let number = Int(text.dropFirst("job-".count))
    else {
        return -1
    }
    return number
}

/// Work-queue durable resume: publish 20 jobs into a workqueue-retention stream,
/// consume + ack the first 10 through a durable pull consumer, stop, then resume a
/// NEW consume on the SAME durable to drain the rest. A short `ackWait` lets the
/// jobs left unacked in phase 1 redeliver in phase 2, so every job is processed
/// exactly once overall.
func runWorkQueue() async throws {
    let client = try await connect()
    let js = JetStreamContext(client: client)
    let streamName = "SCEN_WQ"
    let total = 20

    _ = try? await js.deleteStream(name: streamName)
    _ = try await js.createStream(
        cfg: StreamConfig(name: streamName, subjects: ["scen.wq.>"], retention: .workqueue))
    out("wq", "stream \(streamName) ready (workqueue retention)")

    for i in 1...total {
        _ = try await js.publish("scen.wq.job", message: Data("job-\(i)".utf8)).wait()
    }
    out("wq", "published \(total) jobs")

    let consumer = try await js.createConsumer(
        stream: streamName,
        cfg: ConsumerConfig(name: "worker", ackPolicy: .explicit, ackWait: NanoTimeInterval(2)))
    out("wq", "durable pull consumer 'worker' created (ackWait=2s)")

    let seen = Locked<Set<Int>>([])

    // Phase 1: ack only the first 10 jobs, then stop. The jobs beyond the budget
    // are delivered in the batch but left unacked, so the durable redelivers them.
    let phase1Acked = Locked<Int>(0)
    let context1 = try consumer.consume { message in
        let number = jobNumber(message)
        let ackedCount: Int? = phase1Acked.withLock { count in
            guard count < 10 else { return nil }
            count += 1
            return count
        }
        guard let ackedCount else { return }
        seen.withLock { _ = $0.insert(number) }
        out("wq", "phase1 delivered job-\(number) (acked #\(ackedCount))")
        Task { try? await message.ack() }
    }

    try await waitUntil(deadlineSeconds: 15) { phase1Acked.get() >= 10 }
    try await Task.sleep(nanoseconds: 300_000_000)
    context1.stop()
    out("wq", "phase1 stopped after acking \(phase1Acked.get()); unique so far=\(seen.get().count)")

    // Phase 2: resume on the SAME durable; ack everything until all 20 are seen.
    let phase2Count = Locked<Int>(0)
    let context2 = try consumer.consume { message in
        let number = jobNumber(message)
        let inserted = seen.withLock { $0.insert(number).inserted }
        let deliveries = phase2Count.withLock { count in
            count += 1
            return count
        }
        let suffix = inserted ? ")" : ", already-seen)"
        out("wq", "phase2 delivered job-\(number) (delivery #\(deliveries)" + suffix)
        Task { try? await message.ack() }
    }

    try await waitUntil(deadlineSeconds: 30) { seen.get().count >= total }
    try await Task.sleep(nanoseconds: 300_000_000)
    context2.stop()

    let unique = seen.get().count
    out(
        "wq",
        "phase1 acked=\(phase1Acked.get()) phase2 deliveries=\(phase2Count.get()) "
            + "unique jobs=\(unique)/\(total)")
    out(
        "wq",
        "cross-check: nats consumer info \(streamName) worker  |  "
            + "nats stream info \(streamName)")

    _ = try? await js.deleteStream(name: streamName)
    try? await client.close()
    let verdict =
        unique == total
        ? "DONE (PASS: durable resumed, all \(total) delivered)" : "DONE (FAIL)"
    out("wq", verdict)
}
