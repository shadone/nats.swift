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

/// Integration tests for the public push consumer's unified consume / messages / next surface.
final class PushConsumeTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func setup() async throws -> (NatsClient, JetStreamContext) {
        let client = try await ConsumeTestSupport.connect(natsServer)
        let ctx = JetStreamContext(client: client)
        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        return (client, ctx)
    }

    /// consume(handler:) on a server-created push consumer receives every published message.
    func testConsumeReceivesAllMessages() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let push = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(name: "push", ackPolicy: .none))

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let collector = MessageCollector()
        let cc = try push.consume { collector.record($0) }

        try await ConsumeTestSupport.waitUntil { collector.count >= 20 }
        cc.stop()
        await cc.waitUntilClosed()

        XCTAssertEqual(collector.count, 20)
    }

    /// stop() halts delivery: messages published after stop are not delivered.
    func testConsumeStopHaltsDelivery() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let push = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(name: "push", ackPolicy: .none))

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)

        let collector = MessageCollector()
        let cc = try push.consume { collector.record($0) }
        try await ConsumeTestSupport.waitUntil { collector.count >= 10 }

        cc.stop()
        await cc.waitUntilClosed()
        let afterStop = collector.count

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)
        try await Task.sleep(nanoseconds: 500_000_000)

        XCTAssertEqual(collector.count, afterStop, "no delivery after stop()")
    }

    /// drain() processes messages already handed to the callback, then closes cleanly.
    func testConsumeDrainClosesCleanly() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let push = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(name: "push", ackPolicy: .none))

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let collector = MessageCollector()
        let cc = try push.consume { collector.record($0) }
        try await ConsumeTestSupport.waitUntil { collector.count >= 20 }

        cc.drain()
        await cc.waitUntilClosed()

        XCTAssertGreaterThanOrEqual(collector.count, 20)
    }

    /// messages() iterates published messages in stream order; breaking the loop stops cleanly.
    func testMessagesIterateInOrder() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let push = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(name: "push", ackPolicy: .none))

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let messages = try push.messages()
        var seqs: [UInt64] = []
        for try await message in messages {
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
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let push = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(name: "push", ackPolicy: .none))

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 1)

        let first = try await push.next(timeout: 5)
        let message = try XCTUnwrap(first, "expected a message within the timeout")
        XCTAssertEqual(message.payload, "msg-1".data(using: .utf8))

        let none = try await push.next(timeout: 1)
        XCTAssertNil(none, "next must return nil when no message arrives within the timeout")
    }

    /// Both factories round-trip: create a durable push consumer, then bind it by name and consume.
    func testPushConsumerFactoryRoundTrip() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let created = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(durable: "pushdur", ackPolicy: .none))
        XCTAssertEqual(created.cachedInfo.name, "pushdur")

        let bound = try await ctx.pushConsumer(stream: "test", name: "pushdur")
        XCTAssertEqual(bound.cachedInfo.name, "pushdur")

        let collector = MessageCollector()
        let cc = try bound.consume { collector.record($0) }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)
        try await ConsumeTestSupport.waitUntil { collector.count >= 10 }
        cc.stop()
        await cc.waitUntilClosed()

        XCTAssertGreaterThanOrEqual(collector.count, 10)
    }

    /// A PushConsumer created but NEVER consumed must not leak. `createPushConsumer` eagerly
    /// subscribes and creates the ephemeral server consumer; dropping the handle without ever calling
    /// consume()/messages()/next() must still tear that down, via PushConsumer's best-effort deinit.
    func testUnconsumedPushConsumerIsTornDownWhenDropped() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        var push: PushConsumer? = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(ackPolicy: .none))
        let name = push!.cachedInfo.name

        let existed = await ConsumeTestSupport.consumerExists(ctx, stream: "test", name: name)
        XCTAssertTrue(existed, "the ephemeral consumer should exist right after creation")

        // Drop the only reference without consuming or stopping: the deinit backstop must reap it.
        push = nil

        try await ConsumeTestSupport.waitUntilConsumerGone(ctx, stream: "test", name: name)
    }

    /// Breaking a messages() loop and then dropping the consumer (without stop()) must not leak. For
    /// a push consumer the delivery stream is shared and owned by the consumer, so teardown happens
    /// once the consumer, context and iterator are all released — through MessageStream's backstop
    /// deinit (PushConsumer's own deinit defers to it once a stream exists).
    func testMessagesLoopBrokenThenConsumerDroppedIsTornDown() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        var push: PushConsumer? = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(ackPolicy: .none))
        let name = push!.cachedInfo.name

        // Iterate a few, then break — releasing the context and iterator but not the consumer.
        try await Self.consumeAFewViaMessages(push!)
        let stillPresent = await ConsumeTestSupport.consumerExists(ctx, stream: "test", name: name)
        XCTAssertTrue(stillPresent, "the consumer still owns the shared stream after a broken loop")

        // Now drop the consumer itself, still without stop(): the shared stream's deinit must reap it.
        push = nil

        try await ConsumeTestSupport.waitUntilConsumerGone(ctx, stream: "test", name: name)
    }

    /// Iterates a few messages via messages(), breaks the loop, and lets the context and iterator go
    /// out of scope at return — the passed-in consumer stays alive in the caller.
    private static func consumeAFewViaMessages(_ push: PushConsumer) async throws {
        let messages = try push.messages()
        var seen = 0
        for try await _ in messages {
            seen += 1
            if seen >= 5 {
                break
            }
        }
    }
}
