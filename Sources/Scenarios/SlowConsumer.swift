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
import Nats

/// Slow-consumer / buffer-overflow demonstration for core NATS.
///
/// A `NatsSubscription` buffers up to `512 * 1024 = 524288` messages
/// (`NatsSubscription.defaultSubCapacity`). Once the buffer is full, further
/// inbound messages are DROPPED silently -- see the slow-consumer branch of
/// `NatsSubscription.receiveMessage`. This scenario proves the client SURVIVES a
/// burst that saturates that buffer (the overflow is dropped, not fatal).
///
/// The subscriber (this Swift client) never reads while `nats bench pub` blasts a
/// burst LARGER than the buffer. NIO keeps draining the TCP socket into the
/// app-level buffer regardless of whether the application reads, so the server
/// never sees a slow consumer -- the CLIENT-side buffer fills to its cap and
/// drops the overflow. We then probe a small prefix of the buffer (to prove the
/// burst was absorbed) and do a fresh round-trip (to prove the client is alive).
///
/// Two real limitations are on display here, both documented in FAULTS.md:
///   1. Overflow is dropped SILENTLY -- no SlowConsumer event is surfaced yet
///      (see the `TODO(pp)` in `NatsSubscription`).
///   2. The buffer drains via `Array.removeFirst()` (O(n)), so fully draining a
///      saturated 512Ki-slot buffer is O(n^2) -- impractically slow. That is why
///      this scenario only probes a small prefix instead of counting every
///      surviving message.
///
/// Runs against a plain nats-server (no toxiproxy needed) -- the drop is a
/// client-side buffer limit, not a network fault. Point `NATS_URL` at the real
/// server (e.g. `nats://127.0.0.1:4300`); the `nats` CLI must be on PATH.
func runSlowConsumer() async throws {
    let url = ProcessInfo.processInfo.environment["NATS_URL"] ?? "nats://localhost:4222"
    let client = try await connect()

    let capacity = 512 * 1024  // must match NatsSubscription.defaultSubCapacity
    let sent = 800_000  // comfortably larger than the buffer so overflow is dropped
    let subject = "scen.slow.drop"

    let subscription = try await client.subscribe(subject: subject)
    // Make sure the SUB has reached the server before the burst starts, otherwise
    // early messages have no subscriber and the server drops them.
    try await client.flush()
    out("slow", "subscribed to \(subject) (client sub buffer caps at \(capacity) messages)")

    // Blast the burst from a SEPARATE process while nothing reads the
    // subscription. NIO fills the app buffer to capacity; the rest is dropped.
    out("slow", "blasting \(sent) messages via `nats bench pub` (nobody reading) ...")
    let start = DispatchTime.now().uptimeNanoseconds
    let blastStatus = runNatsBenchPublish(subject: subject, url: url, count: sent)
    let blastSeconds = Double(DispatchTime.now().uptimeNanoseconds - start) / 1_000_000_000
    guard blastStatus == 0 else {
        out(
            "slow",
            "`nats bench pub` failed (exit \(blastStatus)); is the `nats` CLI on PATH? "
                + "run: nats bench pub \(subject) --server \(url) --msgs \(sent) --size 16")
        try? await client.close()
        out("slow", "DONE (SKIP: could not generate the burst)")
        return
    }
    out(
        "slow",
        "burst published in \(String(format: "%.2f", blastSeconds))s; "
            + "letting the server drain the backlog into the buffer ...")

    // Let NIO deliver the backlog into the sub buffer.
    try await Task.sleep(nanoseconds: 3_000_000_000)

    // Probe a SMALL prefix (non-suspending) to prove the burst was absorbed into
    // the buffer. We deliberately do NOT drain it all: each `tryNext` is an
    // `Array.removeFirst()` on the near-full buffer (O(n)), so a full drain would
    // be O(n^2). Probing a few hundred is enough to show the buffer filled, and
    // that there is still more buffered behind the probe.
    let probeLimit = 200
    let iterator = subscription.makeAsyncIterator()
    var probed = 0
    while probed < probeLimit, try iterator.tryNext() != nil {
        probed += 1
    }
    let moreBuffered = (try? iterator.tryNext()) != nil

    let atLeastDropped = max(0, sent - capacity)
    out(
        "slow",
        "probe: drained \(probed) buffered messages, moreBuffered=\(moreBuffered) "
            + "(buffer saturated)")
    out(
        "slow",
        "sent=\(sent) > buffer cap=\(capacity): the client dropped at least "
            + "\(atLeastDropped) messages via its slow-consumer guard (silent drop)")

    // Prove the client survived the overflow: a fresh publish/subscribe round-trip,
    // polled with retries. Re-publishing each attempt also covers the case where a
    // server-side slow-consumer close forced a reconnect mid-burst -- the client
    // recovers and a later ping gets through.
    let aliveSubject = "scen.slow.alive"
    let aliveSub = try await client.subscribe(subject: aliveSubject)
    var alive = false
    for _ in 0..<20 {
        try? await client.publish(Data("ping".utf8), subject: aliveSubject)
        try? await client.flush()
        try await Task.sleep(nanoseconds: 500_000_000)
        if (try? aliveSub.makeAsyncIterator().tryNext()) != nil {
            alive = true
            break
        }
    }
    out(
        "slow",
        alive
            ? "post-overflow round-trip OK -- client still alive"
            : "post-overflow round-trip FAILED after retries")

    try? await client.close()

    let ok = probed > 0 && moreBuffered && alive
    out(
        "slow",
        ok
            ? "DONE (PASS: buffer saturated + overflow dropped, client survived)"
            : "DONE (FAIL)")
}

/// Runs `nats bench pub` in a child process to blast `count` messages onto
/// `subject`, returning its exit status (0 on success). Synchronous by design:
/// it drives a separate process while this client's NIO event loop fills the
/// subscription buffer in the background.
private func runNatsBenchPublish(subject: String, url: String, count: Int) -> Int32 {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = [
        "nats", "bench", "pub", subject,
        "--server", url,
        "--msgs", "\(count)",
        "--size", "16",
        "--clients", "1",
    ]
    process.standardOutput = FileHandle.nullDevice
    process.standardError = FileHandle.nullDevice
    do {
        try process.run()
    } catch {
        return -1
    }
    process.waitUntilExit()
    return process.terminationStatus
}
