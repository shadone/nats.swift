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

/// Integration tests for the ordered consumer's unified consume / messages / next surface.
final class OrderedConsumeTests: XCTestCase {

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

    /// consume(handler:) receives every published message; stop() then closes cleanly.
    func testConsumeReceivesAllMessages() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let oc = try await ctx.orderedConsumer(stream: "test", cfg: OrderedConsumerConfig())
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let collector = MessageCollector()
        let cc = try oc.consume { collector.record($0) }

        try await ConsumeTestSupport.waitUntil { collector.count >= 20 }
        cc.stop()
        await cc.waitUntilClosed()

        XCTAssertEqual(collector.sequences, Array(1...20))
    }

    /// stop() halts delivery: messages published after stop are not delivered.
    func testConsumeStopHaltsDelivery() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let oc = try await ctx.orderedConsumer(stream: "test", cfg: OrderedConsumerConfig())
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)

        let collector = MessageCollector()
        let cc = try oc.consume { collector.record($0) }
        try await ConsumeTestSupport.waitUntil { collector.count >= 10 }

        cc.stop()
        await cc.waitUntilClosed()
        let afterStop = collector.count

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)
        try await Task.sleep(nanoseconds: 500_000_000)

        XCTAssertEqual(collector.count, afterStop, "no delivery after stop()")
    }

    /// messages() iterates published messages in stream order; breaking the loop stops cleanly.
    func testMessagesIterateInOrder() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let oc = try await ctx.orderedConsumer(stream: "test", cfg: OrderedConsumerConfig())
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 20)

        let messages = try oc.messages()
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

        let oc = try await ctx.orderedConsumer(stream: "test", cfg: OrderedConsumerConfig())
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 1)

        let first = try await oc.next(timeout: 5)
        let message = try XCTUnwrap(first, "expected a message within the timeout")
        XCTAssertEqual(message.payload, "msg-1".data(using: .utf8))

        let none = try await oc.next(timeout: 1)
        XCTAssertNil(none, "next must return nil when no message arrives within the timeout")
    }

    /// consume() survives a mid-consume deletion of the underlying consumer: the ordered engine
    /// recreates from `streamSeq + 1` and delivery resumes with no gap and no duplicate.
    func testConsumeSurvivesConsumerDeletion() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        // Direct construction with a short heartbeat so the missed-heartbeat reset fires quickly;
        // the public consume() surface is still what is exercised.
        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test", config: OrderedConsumerConfig(), idleHeartbeat: 0.2)
        try await oc.start()

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 5)

        let collector = MessageCollector()
        let cc = try oc.consume { collector.record($0) }

        // Wait for the first 5, then delete the active consumer out from under the engine.
        try await ConsumeTestSupport.waitUntil { collector.count >= 5 }
        let name = oc.cachedInfo.name
        try await ctx.deleteConsumer(stream: "test", name: name)

        // Publish 5 more; the recreated consumer resumes from stream seq 6.
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 5)

        try await ConsumeTestSupport.waitUntil(20) { collector.count >= 10 }
        cc.stop()
        await cc.waitUntilClosed()

        XCTAssertEqual(
            collector.sequences, Array(1...10),
            "stream sequences must be contiguous across the reset — no gap, no duplicate")
    }

    /// The factory round-trips: orderedConsumer creates and consumes.
    func testOrderedConsumerFactoryRoundTrip() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let oc = try await ctx.orderedConsumer(
            stream: "test", cfg: OrderedConsumerConfig(deliverPolicy: .all))
        XCTAssertFalse(oc.cachedInfo.name.isEmpty, "a consumer must have been created")

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)

        let collector = MessageCollector()
        let cc = try oc.consume { collector.record($0) }
        try await ConsumeTestSupport.waitUntil { collector.count >= 10 }
        cc.stop()
        await cc.waitUntilClosed()

        XCTAssertEqual(collector.sequences, Array(1...10))
    }
}
