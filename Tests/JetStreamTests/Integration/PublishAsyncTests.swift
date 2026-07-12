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

import Nats
import NatsServer
import XCTest

@testable import JetStream

/// Integration tests for batched async JetStream publishing (`publishAsync`) against a real
/// nats-server: one shared ack subscription, a bounded in-flight window, and re-awaitable futures.
final class PublishAsyncTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func connectedContext() async throws -> (NatsClient, JetStreamContext) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return (client, JetStreamContext(client: client))
    }

    /// Batched publish of N messages, flushed via `publishAsyncComplete()`; the window drains to zero
    /// and the stream ends up with exactly N messages. Every future resolves to an `Ack`, and the set
    /// of assigned sequences is exactly `1...N` (no gaps, no duplicates).
    func testBatchedPublishCompletesAndSequencesMatch() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "ASYNC_PUB", subjects: ["async.pub.>"]))

        let n = 500
        var futures: [PubAckFuture] = []
        futures.reserveCapacity(n)
        for i in 1...n {
            let future = try await ctx.publishAsync(
                "async.pub.msg", message: Data("msg-\(i)".utf8))
            futures.append(future)
        }

        try await ctx.publishAsyncComplete()

        let pending = await ctx.publishAsyncPending()
        XCTAssertEqual(pending, 0, "window should be fully drained after complete()")

        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, UInt64(n), "stream should hold exactly N messages")

        var seqs = Set<UInt64>()
        for future in futures {
            let ack = try await future.wait()
            XCTAssertEqual(ack.stream, "ASYNC_PUB")
            seqs.insert(ack.seq)
        }
        XCTAssertEqual(seqs, Set(1...UInt64(n)), "assigned sequences should be exactly 1...N")
    }

    /// A resolved `PubAckFuture` is re-awaitable: awaiting the same future twice yields the same ack.
    func testFutureIsReAwaitable() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        _ = try await ctx.createStream(
            cfg: StreamConfig(name: "ASYNC_REAWAIT", subjects: ["async.reawait.>"]))

        let future = try await ctx.publishAsync(
            "async.reawait.a", message: Data("only".utf8))
        try await ctx.publishAsyncComplete()

        let first = try await future.wait()
        let second = try await future.wait()
        XCTAssertEqual(first.seq, second.seq)
        XCTAssertEqual(first.stream, second.stream)
        XCTAssertEqual(first.seq, 1)
    }

    /// Backpressure smoke test: a deliberately tiny in-flight window (4) forces the stall path many
    /// times over 200 publishes. It must still complete without deadlock and land every message.
    func testBackpressureSmallWindowCompletes() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "ASYNC_BP", subjects: ["async.bp.>"]))

        // Construct the publisher directly with a small window (internal API via @testable import).
        let pub = JetStreamPublishAsync(client: client, timeout: 5, maxPending: 4)
        defer { Task { await pub.shutdown() } }

        let n = 200
        var futures: [PubAckFuture] = []
        futures.reserveCapacity(n)
        for i in 1...n {
            let future = try await pub.publishAsync(
                "async.bp.msg", message: Data("m-\(i)".utf8))
            futures.append(future)
        }

        try await pub.complete(timeout: 30)

        let pending = await pub.pending()
        XCTAssertEqual(pending, 0)

        var seqs = Set<UInt64>()
        for future in futures {
            seqs.insert(try await future.wait().seq)
        }
        XCTAssertEqual(seqs, Set(1...UInt64(n)))

        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, UInt64(n))
    }

    /// An async publish to a subject no stream is listening on surfaces as a thrown error from
    /// `wait()` (proving failures propagate through the box, not just successes).
    func testPublishToNonexistentStreamSurfacesError() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        // No stream is created for this subject.
        let future = try await ctx.publishAsync(
            "async.no.stream", message: Data("orphan".utf8))

        do {
            _ = try await future.wait()
            XCTFail("expected streamNotFound to be thrown from wait()")
        } catch JetStreamError.PublishError.streamNotFound {
            // expected: the ack came back as a no-responders reply
        }

        // The failed publish still drains the window.
        let pending = await ctx.publishAsyncPending()
        XCTAssertEqual(pending, 0)
    }

    /// Two tasks awaiting the SAME not-yet-resolved future must BOTH resolve — the fan-out box must not
    /// leak one of the parked continuations.
    func testConcurrentDoubleAwaitResolvesBoth() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }
        _ = try await ctx.createStream(
            cfg: StreamConfig(name: "ASYNC_DA", subjects: ["async.da.>"]))

        let future = try await ctx.publishAsync("async.da.a", message: Data("x".utf8))
        let completed = await runWithTimeout(10) {
            await withTaskGroup(of: UInt64?.self) { group in
                group.addTask { try? await future.wait().seq }
                group.addTask { try? await future.wait().seq }
                var resolved = 0
                for await seq in group where seq != nil { resolved += 1 }
                XCTAssertEqual(resolved, 2, "both concurrent awaiters must resolve")
            }
        }
        XCTAssertTrue(completed, "concurrent double-await must not hang")
    }

    /// Closing the connection while publishers are parked in backpressure must WAKE them all (the pump
    /// subscription ends → every stalled publisher is resumed). Without the fix they hang forever.
    func testCloseWhileStalledUnblocksPublishers() async throws {
        let (client, ctx) = try await connectedContext()
        _ = try await ctx.createStream(
            cfg: StreamConfig(name: "ASYNC_CS", subjects: ["async.cs.>"]))

        // Window of 1 → nearly every concurrent publish parks in the backpressure loop.
        let pub = JetStreamPublishAsync(client: client, timeout: 5, maxPending: 1)
        let completed = await runWithTimeout(15) {
            await withTaskGroup(of: Void.self) { group in
                for i in 1...50 {
                    group.addTask {
                        _ = try? await pub.publishAsync("async.cs.m", message: Data("m-\(i)".utf8))
                    }
                }
                try? await Task.sleep(nanoseconds: 300_000_000)  // let them stall
                try? await client.close()  // ends the pump sub → must wake every parked publisher
                await group.waitForAll()
            }
        }
        XCTAssertTrue(completed, "closing must unblock all backpressure-stalled publishers")
        await pub.shutdown()
    }

    /// `publishAsyncComplete(timeout:)` throws `timeout` when an in-flight ack never resolves.
    ///
    /// A plain subscriber on the target subject gives it a responder — suppressing the server's
    /// no-responders 503 — but never sends a JetStream ack, so the publish's box stays pending
    /// indefinitely and `complete()` reliably times out (no race with fast draining).
    func testCompleteTimesOutWhilePending() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let sink = try await client.subscribe(subject: "async.ct.stuck")
        defer { Task { try? await sink.unsubscribe() } }

        let future = try await ctx.publishAsync("async.ct.stuck", message: Data("stuck".utf8))
        let pending = await ctx.publishAsyncPending()
        XCTAssertEqual(pending, 1, "the publish should be stuck in flight (no ack, no 503)")

        do {
            try await ctx.publishAsyncComplete(timeout: 0.3)
            XCTFail("complete() should time out while an ack is stuck")
        } catch JetStreamError.RequestError.timeout {
            // expected
        }
        _ = future  // still unresolved; cleaned up on shutdown/close
    }

    /// Runs `body` against a timeout; returns true if it finished, false if it hung past `seconds`.
    private func runWithTimeout(
        _ seconds: TimeInterval, _ body: @escaping @Sendable () async -> Void
    ) async -> Bool {
        await withTaskGroup(of: Bool.self) { group in
            group.addTask {
                await body()
                return true
            }
            group.addTask {
                try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
                return false
            }
            let finished = await group.next() ?? false
            group.cancelAll()
            return finished
        }
    }
}
