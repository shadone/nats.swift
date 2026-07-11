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

final class OrderedConsumerTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// Mid-stream the consumer is deleted out from under the wrapper. The deliver inbox goes silent,
    /// the missed-heartbeat race fires, and the wrapper recreates the consumer from `streamSeq + 1`.
    /// The stream sequences seen across the reset must be CONTIGUOUS, STRICTLY INCREASING — no gap,
    /// no duplicate.
    func testMissedHeartbeatResetResumesContiguously() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        _ = stream

        let payload = "hello".data(using: .utf8)!
        for _ in 1...5 {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }

        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test", deliverPolicy: .all,
            namePrefix: "ord", idleHeartbeat: 0.2)
        await oc.start()

        // Watchdog: if delivery stalls (broken build), finish the stream so reads unblock and fail.
        let watchdog = watchdog(for: oc, after: 20)
        defer { watchdog.cancel() }

        var iterator = oc.messages.makeAsyncIterator()
        var seqs: [UInt64] = []

        // Read the first 5 messages (stream seqs 1...5).
        for _ in 0..<5 {
            let next = try await iterator.next()
            let msg = try unwrap(next, "expected message")
            seqs.append(try streamSequence(of: msg, client: client))
        }

        // Force a reset by deleting the active consumer out from under the wrapper.
        let currentName = await oc.currentConsumerName()
        let name = try XCTUnwrap(currentName, "consumer must be active")
        try await ctx.deleteConsumer(stream: "test", name: name)

        // Publish 5 more (stream seqs 6...10). The recreated consumer resumes from seq 6.
        for _ in 1...5 {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }

        for _ in 0..<5 {
            let next = try await iterator.next()
            let msg = try unwrap(next, "expected message")
            seqs.append(try streamSequence(of: msg, client: client))
        }

        await oc.stop()

        XCTAssertEqual(
            seqs, Array(1...10),
            "stream sequences must be contiguous and strictly increasing across the reset")
    }

    /// Bulk-publish N messages and assert every one is delivered, in order, exactly once.
    func testInOrderNoLossUnderLoad() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let total = 200
        let payload = "hello".data(using: .utf8)!
        for _ in 1...total {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }

        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test", deliverPolicy: .all,
            namePrefix: "ord", idleHeartbeat: 0.1)
        await oc.start()

        let watchdog = watchdog(for: oc, after: 30)
        defer { watchdog.cancel() }

        var iterator = oc.messages.makeAsyncIterator()
        var seqs: [UInt64] = []
        for _ in 0..<total {
            let next = try await iterator.next()
            let msg = try unwrap(next, "expected message")
            seqs.append(try streamSequence(of: msg, client: client))
        }

        await oc.stop()

        XCTAssertEqual(
            seqs, Array(1...UInt64(total)),
            "all \(total) messages must be delivered in order exactly once")
    }

    /// Regression for the stop()/recreate leak race: `stop()` can complete (clearing state) while
    /// an in-flight `recreate()` is suspended at `subscribe`/`createOrUpdateConsumer`. Before the
    /// fix, the resumed recreate committed a brand-new subscription and ephemeral server consumer
    /// that nothing tore down — leaking a server consumer until `inactiveThreshold` (~5m).
    ///
    /// A fully deterministic single-shot race is impractical (the leak only manifests when `stop()`
    /// lands inside the recreate's await window), so this drives start()+immediate-stop() in a tight
    /// loop with fresh, uniquely-named consumers and asserts the stream's consumer count returns to
    /// baseline. With the fix, every generation is either never committed or torn down by the
    /// `closed` re-check, so the count drains to zero; without it, orphaned consumers accumulate and
    /// linger past this window.
    func testStopDuringCreateDoesNotLeakConsumers() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        // A few messages so the create path has real work (widening the in-flight window).
        let payload = "hello".data(using: .utf8)!
        for _ in 1...3 {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }

        // Each iteration uses the default unique (nuid) name prefix, so a leaked generation shows
        // up as a distinct ephemeral consumer rather than overwriting the previous one. The pump's
        // `recreate()` suspends across `subscribe` then `createOrUpdateConsumer` (a server
        // round-trip); to actually land `stop()` inside that window we sweep a small post-start
        // delay across iterations (0 up to ~6ms in 0.1ms steps) rather than calling stop()
        // instantly, which would always beat the pump to the first `closed` check.
        for i in 0..<60 {
            let oc = OrderedConsumer(
                ctx: ctx, streamName: "test", deliverPolicy: .all, idleHeartbeat: 0.2)
            await oc.start()
            try await Task.sleep(nanoseconds: UInt64(i) * 100_000)
            await oc.stop()
        }

        // Fire-and-forget deletes and the closed-recheck teardown are async, so poll to baseline.
        try await pollUntilNoConsumers(ctx: ctx, stream: "test", timeout: 20)
    }

    /// A hard client close with no reconnect must TERMINATE the ordered consumer: the `messages`
    /// stream finishes by throwing, rather than the pump suppressing the missed-heartbeat reset
    /// forever and wedging the watcher. Waits for the ephemeral consumer to be established first so
    /// the close races the reading loop, not the initial create.
    func testConnectionCloseTerminatesMessagesStream() async throws {
        let client = try await connect()
        let ctx = JetStreamContext(client: client)

        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test", deliverPolicy: .all, idleHeartbeat: 0.2)
        await oc.start()

        let watchdog = watchdog(for: oc, after: 20)
        defer { watchdog.cancel() }

        try await waitForConsumer(oc: oc, timeout: 10)
        try await client.close()

        var iterator = oc.messages.makeAsyncIterator()
        do {
            // No messages were published; the stream must finish by THROWING, not return nil.
            while try await iterator.next() != nil {}
            XCTFail("messages stream must finish by throwing after the connection closes")
        } catch {
            // Expected: the .closed event terminated the stream with an error.
        }
    }

    // MARK: - Helpers

    /// Counts the consumers currently registered on `stream`.
    private func consumerCount(ctx: JetStreamContext, stream: String) async throws -> Int {
        var count = 0
        let names = await ctx.consumerNames(stream: stream)
        for try await _ in names {
            count += 1
        }
        return count
    }

    /// Polls until `stream` has zero consumers, failing after `timeout` seconds.
    private func pollUntilNoConsumers(
        ctx: JetStreamContext, stream: String, timeout: TimeInterval
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var last = -1
        while Date() < deadline {
            last = try await consumerCount(ctx: ctx, stream: stream)
            if last == 0 {
                return
            }
            try await Task.sleep(nanoseconds: 200_000_000)
        }
        XCTFail("expected all ordered consumers torn down; \(last) still present after \(timeout)s")
    }

    /// Polls until the ordered consumer has an active server-side consumer, failing after `timeout`.
    private func waitForConsumer(oc: OrderedConsumer, timeout: TimeInterval) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await oc.currentConsumerName() != nil {
                return
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        XCTFail("ordered consumer was never established within \(timeout)s")
    }

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
