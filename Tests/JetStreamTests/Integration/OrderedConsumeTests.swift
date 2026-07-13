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

    /// Regression: when a finite `maxResetAttempts` is exhausted, the delivery stream must FAIL
    /// (surface the error) rather than finish cleanly. Previously the pump caught the recreate
    /// error and finished the stream normally, so the caller silently stopped receiving messages
    /// with no signal that delivery ended prematurely.
    func testExhaustedResetAttemptsFailStreamNotSilently() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        // One reset attempt, fast heartbeat so the reset fires quickly once we break the stream.
        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test",
            config: OrderedConsumerConfig(maxResetAttempts: 1), idleHeartbeat: 0.2)
        try await oc.start()
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 3)

        let msgs = try oc.messages()
        let iterate = Task { () -> Error? in
            do {
                for try await _ in msgs {}
                return nil  // stream finished cleanly (the bug)
            } catch {
                return error  // stream failed (the fix)
            }
        }

        // Let the first messages arrive, then destroy the whole stream: the recreate's
        // createOrUpdateConsumer can never succeed, so the single reset attempt is exhausted.
        try await Task.sleep(nanoseconds: 500_000_000)
        try await ctx.deleteStream(name: "test")

        // Join, bounded so a hang fails the test instead of wedging the suite.
        let outcome: Error?? = await withTaskGroup(of: Error??.self) { group in
            group.addTask { await iterate.value }
            group.addTask {
                try? await Task.sleep(nanoseconds: 15_000_000_000)
                return Optional<Error>.none  // timed out: treated as a non-throw (test fails)
            }
            let first = await group.next() ?? nil
            group.cancelAll()
            return first
        }
        iterate.cancel()

        XCTAssertNotNil(
            outcome ?? nil,
            "delivery stream must FAIL on exhausted reset attempts, not finish silently")
    }

    /// Regression: an ordered consumer used through the public surface must deallocate after stop().
    /// The shared MessageStream's source held a STRONG back-reference to the consumer, forming a
    /// retain cycle (consumer -> shared -> stream -> source -> consumer). So even after stop() let
    /// the reset pump exit (releasing its own strong self), the source cycle kept the consumer --
    /// and its subscription, tasks, and connection-event listener -- alive for the process lifetime.
    /// With the back-reference now weak, stop()+drop deallocates it and its listener is removed.
    func testOrderedConsumerDeallocatesAfterStop() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 3)

        // A Sendable box for the weak reference: Swift 6 forbids capturing a mutable `weak var`
        // local in the concurrently-executing poll closures below.
        final class WeakRef: @unchecked Sendable { weak var oc: OrderedConsumer? }
        let ref = WeakRef()

        let baseline = client.totalEventHandlerCount()
        do {
            let oc = try await ctx.orderedConsumer(stream: "test", cfg: OrderedConsumerConfig())
            ref.oc = oc
            let collector = MessageCollector()
            // consume(...) creates the shared MessageStream + source (the object that used to form
            // the retain cycle).
            let cc = try oc.consume { collector.record($0) }
            try await ConsumeTestSupport.waitUntil { collector.count >= 3 }
            XCTAssertGreaterThan(
                client.totalEventHandlerCount(), baseline,
                "the consumer should have registered a connection-event listener while active")
            cc.stop()
            await cc.waitUntilClosed()
        }
        // The retain cycle is broken, so once stop() releases the pump's strong self and external
        // refs drop, the consumer deallocates.
        try await ConsumeTestSupport.waitUntil(10) { ref.oc == nil }
        XCTAssertNil(ref.oc, "ordered consumer leaked after stop() (reference cycle not broken)")
        // Teardown removed the connection-event listener, so the count returns to baseline.
        try await ConsumeTestSupport.waitUntil(5) { client.totalEventHandlerCount() <= baseline }
        XCTAssertLessThanOrEqual(
            client.totalEventHandlerCount(), baseline,
            "ordered consumer leaked its connection-event listener")
    }
}
