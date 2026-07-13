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
import NatsServer
import XCTest

/// Regression tests for the core subscription reader lifecycle: concurrent readers, close() while a
/// reader is suspended, and `delivered` accounting under a cancelled wait with `unsubscribe(after:)`.
final class SubscriptionLifecycleTests: XCTestCase {

    nonisolated(unsafe) static let allTests = [
        ("testConcurrentReadersBothResumed", testConcurrentReadersBothResumed),
        ("testCloseResumesSuspendedReader", testCloseResumesSuspendedReader),
        (
            "testUnsubscribeAfterCancelDoesNotDropMessage",
            testUnsubscribeAfterCancelDoesNotDropMessage
        ),
        ("testAlreadyCancelledReaderDoesNotHang", testAlreadyCancelledReaderDoesNotHang),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func connect() async throws -> NatsClient {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return client
    }

    /// Runs `body`, failing (rather than hanging the whole suite) if it does not finish within
    /// `seconds`. Returns whether it finished in time.
    private func completesWithin(
        _ seconds: UInt64, _ body: @escaping @Sendable () async -> Void
    )
        async -> Bool
    {
        await withTaskGroup(of: Bool.self) { group in
            group.addTask {
                await body()
                return true
            }
            group.addTask {
                try? await Task.sleep(nanoseconds: seconds * 1_000_000_000)
                return false
            }
            let first = await group.next() ?? false
            group.cancelAll()
            return first
        }
    }

    /// Regression: two readers suspended on ONE subscription must BOTH be resumed. A single stored
    /// continuation let the second reader overwrite (and permanently orphan) the first's; the FIFO
    /// waiter queue resumes them in order.
    func testConcurrentReadersBothResumed() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let sub = try await client.subscribe(subject: "foo")

        let r1 = Task { () -> NatsMessage? in
            var it = sub.makeAsyncIterator()
            return try await it.next()
        }
        let r2 = Task { () -> NatsMessage? in
            var it = sub.makeAsyncIterator()
            return try await it.next()
        }
        // Let both suspend, then deliver two messages.
        try await Task.sleep(nanoseconds: 300_000_000)
        try await client.publish("a".data(using: .utf8)!, subject: "foo")
        try await client.publish("b".data(using: .utf8)!, subject: "foo")

        let bothResumed = await completesWithin(5) {
            _ = try? await r1.value
            _ = try? await r2.value
        }
        r1.cancel()
        r2.cancel()
        XCTAssertTrue(
            bothResumed, "both concurrent readers must be resumed; one was orphaned and hung")
    }

    /// Regression: `client.close()` must resume a reader suspended in `next()` with `nil`
    /// (end-of-stream). `performClose()` never completed open subscriptions, so a pending reader
    /// hung forever -- a shutdown deadlock if the app awaited that reader.
    func testCloseResumesSuspendedReader() async throws {
        let client = try await connect()
        let sub = try await client.subscribe(subject: "foo")

        let observedNil = NIOLockBox()
        let reader = Task {
            var it = sub.makeAsyncIterator()
            let msg = try? await it.next()
            observedNil.set(msg == nil)
        }
        // Let the reader suspend (nothing published), then close.
        try await Task.sleep(nanoseconds: 300_000_000)
        try await client.close()

        let returned = await completesWithin(5) { _ = await reader.value }
        reader.cancel()
        XCTAssertTrue(returned, "close() left a suspended reader hung instead of ending it")
        XCTAssertTrue(
            observedNil.get(), "the reader should observe end-of-stream (nil) after close")
    }

    /// Regression: a cancelled wait must not consume an `unsubscribe(after:)` delivery slot. The
    /// count was incremented BEFORE a message was in hand, so a cancelled suspended read inflated it
    /// and the local auto-unsubscribe fired one message early, silently dropping a buffered message.
    func testUnsubscribeAfterCancelDoesNotDropMessage() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let sub = try await client.subscribe(subject: "foo")
        try await sub.unsubscribe(after: 3)

        // Start a reader with nothing published -> it suspends. Cancel it: the phantom-increment path.
        let waiter = Task { () -> NatsMessage? in
            var it = sub.makeAsyncIterator()
            return try await it.next()
        }
        try await Task.sleep(nanoseconds: 300_000_000)
        waiter.cancel()
        _ = try? await waiter.value  // let the cancellation settle

        // Publish exactly 3 (the server-side UNSUB max). All 3 must still be deliverable.
        for payload in ["m0", "m1", "m2"] {
            try await client.publish(payload.data(using: .utf8)!, subject: "foo")
        }

        var received: [String] = []
        let deadline = Date().addingTimeInterval(5)
        var it = sub.makeAsyncIterator()
        while received.count < 3, Date() < deadline {
            guard let msg = try await it.next() else {
                break  // auto-unsubscribed -- if before 3, that is the bug
            }
            received.append(String(data: msg.payload ?? Data(), encoding: .utf8) ?? "")
        }
        XCTAssertEqual(
            received, ["m0", "m1", "m2"],
            "a cancelled wait dropped a message via delivered miscount")
    }

    /// Regression: a reader that calls next() when its task is ALREADY cancelled must return, not
    /// hang. `withTaskCancellationHandler` fires onCancel synchronously before the operation runs in
    /// that case, so it finds no parked waiter -- the operation must itself observe the cancellation
    /// rather than parking a waiter nothing will ever wake.
    func testAlreadyCancelledReaderDoesNotHang() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let sub = try await client.subscribe(subject: "foo")

        let reader = Task { () -> Bool in
            // Only start reading once cancelled, forcing the cancelled-before-entry path.
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 5_000_000)
            }
            var it = sub.makeAsyncIterator()
            let msg = try? await it.next()
            return msg == nil
        }
        try await Task.sleep(nanoseconds: 200_000_000)
        reader.cancel()

        let returned = await completesWithin(5) { _ = await reader.value }
        XCTAssertTrue(returned, "an already-cancelled reader hung in next() instead of returning")
    }
}

/// A tiny Sendable boolean box for observing a value set inside a detached task.
private final class NIOLockBox: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false
    func set(_ v: Bool) {
        lock.lock()
        value = v
        lock.unlock()
    }
    func get() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}
