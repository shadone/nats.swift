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

import NatsServer
import XCTest

@testable import JetStream
@testable import Nats

/// Chaos / correctness tests for the ordered consumer's delivery path — the missed-heartbeat reset
/// and steady-state hot path that an upcoming `PushDelivery.race()` refactor touches. Every reset
/// must resume from `streamSeq + 1` and lose or duplicate nothing.
final class OrderedConsumerChaosTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// Repeatedly deletes the active consumer out from under the wrapper while it drains a fully
    /// pre-published stream. Because all N messages exist up front, every recreate resumes from
    /// `streamSeq + 1`; across 8 forced resets the sequences seen must remain contiguous, strictly
    /// increasing — no gap, no duplicate.
    func testContiguousDeliveryUnderRepeatedResets() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let total = 1500
        let payload = "hello".data(using: .utf8)!
        for _ in 1...total {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }

        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test", deliverPolicy: .all,
            namePrefix: "ord", idleHeartbeat: 0.1)
        try await oc.start()

        // Watchdog: if delivery stalls (broken build), finish the stream so reads unblock and fail.
        let watchdog = watchdog(for: oc, after: 40)
        defer { watchdog.cancel() }

        // One bounded chaos task: delete the active consumer exactly 8 times, then return so
        // delivery can drain the remainder without further interference. The 50ms cadence is spaced
        // wider than a 409-driven recreate (so progress can't be starved) yet tight enough that all
        // eight deletes land while delivery is still live, forcing eight real mid-stream resets.
        let chaos = Task {
            for _ in 0..<8 {
                try? await Task.sleep(nanoseconds: 50_000_000)
                if let name = await oc.currentConsumerName() {
                    try? await ctx.deleteConsumer(stream: "test", name: name)
                }
            }
        }
        defer { chaos.cancel() }

        var iterator = oc.natsMessages.makeAsyncIterator()
        var seqs: [UInt64] = []
        for _ in 0..<total {
            let next = try await iterator.next()
            let msg = try unwrap(next, "expected message")
            seqs.append(try streamSequence(of: msg, client: client))
        }

        chaos.cancel()
        await oc.stop()

        XCTAssertEqual(
            seqs, Array(1...UInt64(total)),
            "stream sequences must stay contiguous across repeated resets — no gap, no duplicate")
    }

    /// Starts an ordered consumer on an EMPTY stream, then grows the stream live while reading. This
    /// is steady-state delivery on the hot path the refactor optimizes: N messages published
    /// concurrently must all arrive in contiguous stream order.
    func testLiveConsumeWhilePublishing() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test", deliverPolicy: .all,
            namePrefix: "ord", idleHeartbeat: 0.1)
        try await oc.start()

        let watchdog = watchdog(for: oc, after: 40)
        defer { watchdog.cancel() }

        let total = 1000
        let publisher = Task {
            let payload = "hello".data(using: .utf8)!
            for _ in 1...total {
                _ = try await ctx.publish("foo.A", message: payload).wait()
            }
        }
        defer { publisher.cancel() }

        var iterator = oc.natsMessages.makeAsyncIterator()
        var seqs: [UInt64] = []
        for _ in 0..<total {
            let next = try await iterator.next()
            let msg = try unwrap(next, "expected message")
            seqs.append(try streamSequence(of: msg, client: client))
        }

        try await publisher.value
        await oc.stop()

        XCTAssertEqual(
            seqs, Array(1...UInt64(total)),
            "all \(total) live-published messages must arrive in contiguous stream order")
    }

    // MARK: - Helpers

    private func connect() async throws -> NatsClient {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return client
    }

    private func streamSequence(of msg: NatsMessage, client: NatsClient) throws -> UInt64 {
        try JetStreamMessage(message: msg, client: client).metadata().streamSequence
    }

    /// Finishes the ordered consumer's stream after `seconds` so a stalled read fails instead of
    /// hanging the whole test process.
    private func watchdog(for oc: OrderedConsumer, after seconds: TimeInterval) -> Task<Void, Never>
    {
        Task {
            try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            await oc.stop()
        }
    }

    private func unwrap<T>(
        _ value: T?, _ message: String, file: StaticString = #filePath, line: UInt = #line
    ) throws -> T {
        guard let value else {
            XCTFail(message, file: file, line: line)
            throw CancellationError()
        }
        return value
    }
}
