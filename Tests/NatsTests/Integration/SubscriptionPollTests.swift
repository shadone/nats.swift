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

/// Tests for the non-suspending `SubscriptionIterator.tryNext()` poll that backs the JetStream push
/// delivery fast path: it must return buffered messages in FIFO order, report "nothing ready" as
/// `nil` (never blocking, never falsely ending the stream), and share one ordered buffer with the
/// async `next()`.
final class SubscriptionPollTests: XCTestCase {

    nonisolated(unsafe) static let allTests = [
        ("testTryNextDrainsBufferThenReportsEmpty", testTryNextDrainsBufferThenReportsEmpty),
        ("testTryNextSharesFIFOBufferWithAsyncNext", testTryNextSharesFIFOBufferWithAsyncNext),
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

    /// Polls `iter` until it yields a message (or fails after `timeout`) — bridges the async delivery
    /// of published messages into the non-blocking poll without a fixed sleep.
    private func pollNext(
        _ iter: NatsSubscription.SubscriptionIterator, timeout: TimeInterval = 5,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws -> NatsMessage {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if let message = try iter.tryNext() {
                return message
            }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        XCTFail("no message became available within \(timeout)s", file: file, line: line)
        throw CancellationError()
    }

    /// tryNext returns each buffered message in order, then `nil` (would-block, NOT end-of-stream)
    /// once drained; a later publish refills the buffer and tryNext resumes.
    func testTryNextDrainsBufferThenReportsEmpty() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }

        let sub = try await client.subscribe(subject: "poll.a")
        let iter = sub.makeAsyncIterator()

        for i in 1...3 {
            try await client.publish("m\(i)".data(using: .utf8)!, subject: "poll.a")
        }
        try await client.flush()

        var payloads: [String] = []
        for _ in 0..<3 {
            let msg = try await pollNext(iter)
            payloads.append(String(data: msg.payload ?? Data(), encoding: .utf8) ?? "")
        }
        XCTAssertEqual(
            payloads, ["m1", "m2", "m3"], "buffered messages must poll out in FIFO order")

        // Drained: the poll reports empty (would-block) rather than blocking or ending the stream.
        XCTAssertNil(try iter.tryNext(), "an empty inbox must poll as nil, not end-of-stream")

        // Refill and confirm the poll resumes.
        try await client.publish("m4".data(using: .utf8)!, subject: "poll.a")
        try await client.flush()
        let fourth = try await pollNext(iter)
        XCTAssertEqual(String(data: fourth.payload ?? Data(), encoding: .utf8), "m4")
    }

    /// tryNext and the async next() draw from the SAME ordered buffer: polling the first messages
    /// then awaiting the rest yields all four in contiguous publish order. Using the retry-poll for
    /// the first two (rather than assuming instant buffering) keeps it deterministic; the awaited
    /// reads then pull the remainder from that same FIFO.
    func testTryNextSharesFIFOBufferWithAsyncNext() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }

        let sub = try await client.subscribe(subject: "poll.b")
        let iter = sub.makeAsyncIterator()

        for i in 1...4 {
            try await client.publish("m\(i)".data(using: .utf8)!, subject: "poll.b")
        }
        try await client.flush()

        let first = try await pollNext(iter)
        let second = try await pollNext(iter)
        let thirdRead = try await iter.next()
        let third = try XCTUnwrap(thirdRead, "expected m3 from the shared buffer")
        let fourthRead = try await iter.next()
        let fourth = try XCTUnwrap(fourthRead, "expected m4 from the shared buffer")

        let payloads = [first, second, third, fourth].map {
            String(data: $0.payload ?? Data(), encoding: .utf8) ?? ""
        }
        XCTAssertEqual(
            payloads, ["m1", "m2", "m3", "m4"], "poll and await must share one FIFO order")
    }
}
