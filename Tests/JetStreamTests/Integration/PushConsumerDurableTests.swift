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
import NatsServer
import XCTest

@testable import JetStream
@testable import Nats

/// Integration tests for DURABLE push consumers and QUEUE/DELIVER-GROUP load balancing.
final class PushConsumerDurableTests: XCTestCase {

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

    /// A DURABLE push consumer persists server-side across a stop(), and rebinding it resumes from
    /// where it left off. Contrast: an ephemeral push consumer is deleted on stop() (below).
    func testDurablePushConsumerPersistsAndResumesAfterStop() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        // A DURABLE push consumer with explicit ack so its ack floor advances as we ack.
        let durable = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(durable: "dpush", ackPolicy: .explicit))
        XCTAssertEqual(durable.cachedInfo.name, "dpush")

        // Consume and ack the first 10, then stop the handle.
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)
        let firstSeqs = try await Self.consumeAndAck(durable, count: 10)
        XCTAssertEqual(firstSeqs, Array(1...10))

        // The DURABLE must STILL EXIST server-side after stop() (only ephemerals are reaped).
        let stillExists = await ConsumeTestSupport.consumerExists(
            ctx, stream: "test", name: "dpush")
        XCTAssertTrue(stillExists, "a durable push consumer must persist after stop()")

        // Wait for the deliver-subject subscription to be torn down before rebinding.
        try await Self.waitUntilNotPushBound(ctx, stream: "test", name: "dpush")

        // Publish 10 more, rebind the durable by name, and continue: it resumes from seq 11.
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 10)
        let bound = try await ctx.pushConsumer(stream: "test", name: "dpush")
        let secondSeqs = try await Self.consumeAndAck(bound, count: 10)
        XCTAssertEqual(
            secondSeqs, Array(11...20), "durable resumed from its persisted (acked) position")
    }

    /// An EPHEMERAL push consumer (no durable name) is deleted server-side on stop().
    func testEphemeralPushConsumerDeletedOnStop() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let ephemeral = try await ctx.createPushConsumer(
            stream: "test", cfg: ConsumerConfig(ackPolicy: .none))
        let name = ephemeral.cachedInfo.name

        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 5)
        let collector = MessageCollector()
        let cc = try ephemeral.consume { collector.record($0) }
        try await ConsumeTestSupport.waitUntil { collector.count >= 5 }

        cc.stop()
        await cc.waitUntilClosed()

        try await ConsumeTestSupport.waitUntilConsumerGone(ctx, stream: "test", name: name)
    }

    /// Two instances binding the SAME durable + deliver group load-balance delivery: each message
    /// reaches exactly one instance, together they cover all messages, and both get a share.
    func testQueueGroupLoadBalancesAcrossTwoInstances() async throws {
        let (client1, ctx1) = try await setup()
        defer { Task { try? await client1.close() } }

        // A second, independent connection to the same server.
        let client2 = try await ConsumeTestSupport.connectAdditional(natsServer)
        defer { Task { try? await client2.close() } }
        let ctx2 = JetStreamContext(client: client2)

        let total = 50

        // Instance 1 creates the durable queue-group consumer (and subscribes with the queue group).
        let cfg = ConsumerConfig.durablePushQueueGroup(durable: "qpush", deliverGroup: "workers")
        let instance1 = try await ctx1.createPushConsumer(stream: "test", cfg: cfg)
        XCTAssertEqual(instance1.cachedInfo.config.deliverGroup, "workers")

        // Instance 2 binds the same durable (subscribes with the same queue group).
        let instance2 = try await ctx2.pushConsumer(stream: "test", name: "qpush")

        let c1 = MessageCollector()
        let c2 = MessageCollector()
        let cc1 = try instance1.consume { m in
            c1.record(m)
            Task { try? await m.ack() }
        }
        let cc2 = try instance2.consume { m in
            c2.record(m)
            Task { try? await m.ack() }
        }

        try await ConsumeTestSupport.publish(ctx1, subject: "foo.a", count: total)
        try await ConsumeTestSupport.waitUntil { c1.count + c2.count >= total }
        // Settle briefly to surface any accidental duplicate delivery.
        try await Task.sleep(nanoseconds: 300_000_000)

        cc1.stop()
        cc2.stop()
        await cc1.waitUntilClosed()
        await cc2.waitUntilClosed()

        let seqs1 = Set(c1.sequences)
        let seqs2 = Set(c2.sequences)
        XCTAssertEqual(
            c1.count + c2.count, total, "every message delivered exactly once across the group")
        XCTAssertEqual(seqs1.count, c1.count, "instance 1 saw no duplicates")
        XCTAssertEqual(seqs2.count, c2.count, "instance 2 saw no duplicates")
        XCTAssertTrue(seqs1.isDisjoint(with: seqs2), "no message delivered to both instances")
        XCTAssertEqual(
            seqs1.union(seqs2), Set((1...UInt64(total))), "the group covered all messages")
        XCTAssertFalse(seqs1.isEmpty, "instance 1 got a share of the work")
        XCTAssertFalse(seqs2.isEmpty, "instance 2 got a share of the work")
    }

    /// The `durablePushQueueGroup` convenience produces a durable, queue-group, push config, and the
    /// server round-trips the durable name and deliver group into the created consumer.
    func testDurablePushQueueGroupConvenienceRoundTrips() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        let cfg = ConsumerConfig.durablePushQueueGroup(durable: "qcfg", deliverGroup: "grp")
        XCTAssertEqual(cfg.durable, "qcfg")
        XCTAssertEqual(cfg.deliverGroup, "grp")
        XCTAssertEqual(cfg.ackPolicy, .explicit)

        let push = try await ctx.createPushConsumer(stream: "test", cfg: cfg)
        XCTAssertEqual(push.cachedInfo.name, "qcfg")
        XCTAssertEqual(push.cachedInfo.config.durable, "qcfg")
        XCTAssertEqual(push.cachedInfo.config.deliverGroup, "grp")
        XCTAssertNotNil(push.cachedInfo.config.deliverSubject)
    }

    /// nats-CLI interop: `nats consumer info <stream> <durable>` reports the durable and its deliver
    /// group. Skipped when the `nats` CLI is not on PATH.
    func testNatsCliShowsDurableWithDeliverGroup() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        _ = try await ctx.createPushConsumer(
            stream: "test",
            cfg: ConsumerConfig.durablePushQueueGroup(durable: "clidur", deliverGroup: "cligrp"))

        guard
            let result = Self.runNats([
                "-s", natsServer.clientURL, "consumer", "info", "test", "clidur", "--timeout=5s",
            ])
        else {
            throw XCTSkip("nats CLI not available on PATH")
        }
        XCTAssertEqual(result.exit, 0, "nats consumer info failed: \(result.output)")
        XCTAssertTrue(result.output.contains("clidur"), "info should name the durable")
        XCTAssertTrue(result.output.contains("cligrp"), "info should show the deliver group")
    }

    /// Regression: a genuinely stalled push consumer -- the deliver subject goes silent, no message
    /// AND no idle heartbeat, while the TCP connection stays healthy -- must SURFACE the missed
    /// heartbeat, not loop/hang forever. `.missedHeartbeat` used to be swallowed alongside
    /// `.idleHeartbeat` (`case .idleHeartbeat, .missedHeartbeat: continue`). Deleting the consumer
    /// out from under the push binding produces exactly this silence (verified: no advisory reaches
    /// the deliver subject, heartbeats just stop).
    func testPushConsumerSurfacesMissedHeartbeatOnStall() async throws {
        let (client, ctx) = try await setup()
        defer { Task { try? await client.close() } }

        // Push consumer with a short idle heartbeat so a stall is detected quickly.
        let pc = try await ctx.createPushConsumer(
            stream: "test",
            cfg: ConsumerConfig(
                durable: "hbpush", ackPolicy: .none, idleHeartbeat: NanoTimeInterval(0.3)))
        let name = pc.cachedInfo.name

        // Actively iterate: deliver + read one message.
        try await ConsumeTestSupport.publish(ctx, subject: "foo.a", count: 1)
        _ = try await pc.next(timeout: 5)

        // Delete the consumer: the deliver subject goes silent (no heartbeat, no advisory).
        try await ctx.deleteConsumer(stream: "test", name: name)

        // The next read must THROW (missed heartbeat) within ~2x idleHeartbeat, not hang/end.
        do {
            _ = try await pc.next(timeout: 10)
            XCTFail("a stalled push consumer must surface the stall, not hang or end silently")
        } catch JetStreamError.FetchError.noHeartbeatReceived {
            // expected: the genuine stall (deliver subject silence) is surfaced
        } catch JetStreamError.FetchError.consumerDeleted {
            // also acceptable: a server that delivers a Consumer Deleted status surfaces it too
        }
    }

    // MARK: - Helpers

    /// Consumes exactly `count` messages via `messages()`, acking each inline, then stops the stream.
    /// Returns the stream sequences in delivery order.
    private static func consumeAndAck(_ push: PushConsumer, count: Int) async throws -> [UInt64] {
        let messages = try push.messages()
        var seqs: [UInt64] = []
        for try await message in messages {
            seqs.append(try message.metadata().streamSequence)
            try await message.ack()
            if seqs.count >= count {
                break
            }
        }
        messages.stop()
        return seqs
    }

    /// Polls until the consumer's deliver-subject subscription is gone (`pushBound == false`), so a
    /// rebind starts from a clean single-subscriber state.
    private static func waitUntilNotPushBound(
        _ ctx: JetStreamContext,
        stream: String,
        name: String,
        timeout: TimeInterval = 10,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if let consumer = try await ctx.getConsumer(stream: stream, name: name),
                consumer.info.pushBound != true
            {
                return
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        XCTFail("consumer \(name) still push-bound after \(timeout)s", file: file, line: line)
    }

    /// Runs the `nats` CLI via `env` so it resolves from PATH. Returns `nil` when the CLI is absent
    /// (exit 127 or a launch failure), letting callers `XCTSkip`.
    private static func runNats(_ arguments: [String]) -> (exit: Int32, output: String)? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats"] + arguments
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = pipe
        do {
            try process.run()
        } catch {
            return nil
        }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        if process.terminationStatus == 127 {
            return nil
        }
        return (process.terminationStatus, String(data: data, encoding: .utf8) ?? "")
    }
}
