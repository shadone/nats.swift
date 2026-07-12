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

/// Integration tests for the pull consumer's unified consume / messages / next surface.
final class PullConsumeTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func setup() async throws -> (NatsClient, JetStreamContext, Consumer) {
        let client = try await ConsumeTestSupport.connect(natsServer)
        let ctx = JetStreamContext(client: client)
        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        let consumer = try await ctx.createConsumer(
            stream: "test", cfg: ConsumerConfig(name: "pull", ackPolicy: .explicit))
        return (client, ctx, consumer)
    }

    /// A `Consumer` handle is `Sendable`: a single handle can be captured by multiple concurrent
    /// tasks. This exercises the lock-guarded `info` under concurrency — before the handle became
    /// Sendable this capture would not compile under Swift 6, and the mutable `info` var refreshed by
    /// concurrent `info()` calls would be a data race.
    func testConsumerHandleSharedAcrossTasks() async throws {
        let (client, _, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await withThrowingTaskGroup(of: String.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    var name = ""
                    for _ in 0..<10 {
                        name = try await consumer.info().name
                    }
                    return name
                }
            }
            for try await name in group {
                XCTAssertEqual(name, "pull", "shared handle must return consistent info under load")
            }
        }
    }

    /// consume(handler:) receives every published message; stop() then closes cleanly.
    func testConsumeReceivesAllMessages() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let collector = MessageCollector()
        let cc = try consumer.consume { message in
            collector.record(message)
            Task { try? await message.ack() }
        }

        try await ConsumeTestSupport.waitUntil { collector.count >= 20 }
        cc.stop()
        await cc.waitUntilClosed()

        XCTAssertEqual(collector.count, 20)
    }

    /// stop() halts delivery: messages published after stop are not delivered.
    func testConsumeStopHaltsDelivery() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)

        let collector = MessageCollector()
        let cc = try consumer.consume { message in
            collector.record(message)
            Task { try? await message.ack() }
        }
        try await ConsumeTestSupport.waitUntil { collector.count >= 10 }

        cc.stop()
        await cc.waitUntilClosed()
        let afterStop = collector.count

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)
        try await Task.sleep(nanoseconds: 500_000_000)

        XCTAssertEqual(collector.count, afterStop, "no delivery after stop()")
    }

    /// drain() closes cleanly after the already-delivered messages are processed.
    func testConsumeDrainClosesCleanly() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let collector = MessageCollector()
        let cc = try consumer.consume { message in
            collector.record(message)
            Task { try? await message.ack() }
        }
        try await ConsumeTestSupport.waitUntil { collector.count >= 20 }

        cc.drain()
        await cc.waitUntilClosed()

        XCTAssertGreaterThanOrEqual(collector.count, 20)
    }

    /// messages() iterates published messages in stream order; breaking the loop stops cleanly.
    func testMessagesIterateInOrder() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let messages = try consumer.messages()
        var seqs: [UInt64] = []
        for try await message in messages {
            try await message.ack()
            seqs.append(try message.metadata().streamSequence)
            if seqs.count == 20 {
                break
            }
        }
        messages.stop()

        XCTAssertEqual(seqs, Array(1...20), "messages delivered in contiguous stream order")
    }

    /// next(timeout:) returns the next message, and nil when none arrive within the timeout.
    func testNextReturnsMessageThenTimesOut() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 1)

        let first = try await consumer.next(timeout: 5)
        let message = try XCTUnwrap(first, "expected a message within the timeout")
        try await message.ack()
        XCTAssertEqual(message.payload, "msg-1".data(using: .utf8))

        let none = try await consumer.next(timeout: 1)
        XCTAssertNil(none, "next must return nil when no message arrives within the timeout")
    }

    /// fetchNoWait returns only the currently-available messages, without blocking for the batch.
    func testFetchNoWaitReturnsAvailable() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 5)

        // Ask for more than are available; fetchNoWait must not wait for the shortfall.
        var count = 0
        let batch = try await consumer.fetchNoWait(batch: 100)
        for try await message in batch {
            try await message.ack()
            count += 1
        }
        XCTAssertEqual(count, 5, "fetchNoWait returns only the available messages")
    }

    /// fetchBytes retrieves messages up to a byte budget and stops within it.
    func testFetchBytesReturnsMessages() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)

        // A budget comfortably above one message's on-wire size (payload + JS headers/subject).
        var count = 0
        let batch = try await consumer.fetchBytes(maxBytes: 4096, expires: 2)
        for try await message in batch {
            try await message.ack()
            count += 1
        }
        XCTAssertGreaterThan(count, 0, "fetchBytes returns at least one message")
        XCTAssertLessThanOrEqual(count, 10)
    }

    /// Dropping a ConsumeContext WITHOUT calling stop() must halt delivery: the context's best-effort
    /// deinit cancels the loop task and tears the pull loop down, so messages published afterwards are
    /// not delivered. This exercises the deinit backstop rather than the explicit stop() contract.
    func testDroppingConsumeContextWithoutStopHaltsDelivery() async throws {
        let (client, ctx, consumer) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)

        let collector = MessageCollector()
        try await Self.consumeTenThenDropContext(consumer, collector: collector)
        let afterDrop = collector.count
        XCTAssertGreaterThanOrEqual(afterDrop, 10)

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)
        try await Task.sleep(nanoseconds: 500_000_000)

        XCTAssertEqual(
            collector.count, afterDrop,
            "no delivery after the ConsumeContext was dropped without stop()")
    }

    /// Consumes the first 10 messages, keeping the ConsumeContext alive only until they arrive, then
    /// releases it at return — no stop() is ever called.
    private static func consumeTenThenDropContext(
        _ consumer: Consumer, collector: MessageCollector
    ) async throws {
        let cc = try consumer.consume { message in
            collector.record(message)
            Task { try? await message.ack() }
        }
        try await ConsumeTestSupport.waitUntil { collector.count >= 10 }
        // Keep `cc` alive until the messages have arrived; the optimizer must not release it early.
        withExtendedLifetime(cc) {}
    }
}
