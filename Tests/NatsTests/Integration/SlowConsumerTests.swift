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
import Logging
import NIOConcurrencyHelpers
import Nats
import NatsServer
import XCTest

/// Exercises the client-side slow-consumer path: a subscription with a small
/// ``NatsClientOptions/subscriptionCapacity(_:)`` that overflows when nothing reads it.
/// The overflow must surface a ``NatsError/SubscriptionError/slowConsumer(subject:)`` via the
/// `.error` event (once per episode), drop the excess, and leave the subscription usable.
class SlowConsumerTests: XCTestCase {

    nonisolated(unsafe) static let allTests = [
        (
            "testSlowConsumerEventFiresAndSubscriptionSurvives",
            testSlowConsumerEventFiresAndSubscriptionSurvives
        ),
        (
            "testSlowConsumerReFiresAfterRecovery",
            testSlowConsumerReFiresAfterRecovery
        ),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// Polls `condition` until it holds or `timeout` elapses. Returns whether it held.
    /// Non-suspending / bounded so the test can never hang.
    private func waitUntil(
        _ timeout: TimeInterval = 10, _ condition: @Sendable () -> Bool
    ) async throws -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if condition() { return true }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        return condition()
    }

    func testSlowConsumerEventFiresAndSubscriptionSurvives() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let capacity = 16
        let burst = 200
        let subject = "slow.consumer.subject"

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .subscriptionCapacity(capacity)
            .build()

        // Collect the subjects of every slow-consumer error the client surfaces. The `.error`
        // handler runs on the connection's I/O thread, so guard the collection with a lock.
        let slowConsumerSubjects = NIOLockedValueBox<[String]>([])
        client.on(.error) { event in
            guard case .error(let err) = event,
                let subErr = err as? NatsError.SubscriptionError,
                case .slowConsumer(let subj) = subErr
            else { return }
            slowConsumerSubjects.withLockedValue { $0.append(subj) }
        }

        try await client.connect()

        // Subscribe but DO NOT read. Flush so the SUB reaches the server before the burst,
        // otherwise the server drops messages that arrive before the interest is registered.
        let sub = try await client.subscribe(subject: subject)
        try await client.flush()

        // Blast a burst far larger than the buffer. With nobody reading, the first `capacity`
        // messages are buffered (payloads "0"..."capacity-1", in order) and the rest are dropped.
        for i in 0..<burst {
            try await client.publish(Data("\(i)".utf8), subject: subject)
        }
        try await client.flush()

        // The event fires once message capacity+1 overflows the buffer. Poll (bounded) for it.
        let fired = try await waitUntil(10) {
            !slowConsumerSubjects.withLockedValue { $0 }.isEmpty
        }
        XCTAssertTrue(fired, "slow-consumer error should have fired on buffer overflow")

        // Let any (there should be none) further events settle, then assert the shape: exactly
        // one event for THIS subject — a single slow episode fires exactly once, not per drop.
        try await Task.sleep(nanoseconds: 300_000_000)
        let captured = slowConsumerSubjects.withLockedValue { $0 }
        XCTAssertEqual(
            captured, [subject], "exactly one slow-consumer event, for the right subject")

        // The subscription must have SURVIVED: draining it yields the first `capacity` messages
        // in FIFO order (the overflow was dropped, not the whole subscription).
        let iter = sub.makeAsyncIterator()
        var drained: [Data] = []
        while let message = try iter.tryNext() {
            drained.append(message.payload ?? Data())
        }
        let expected = (0..<capacity).map { Data("\($0)".utf8) }
        XCTAssertEqual(drained, expected, "buffer holds the first `capacity` messages, in order")

        // Liveness: a fresh publish/subscribe round-trip still works — no crash, no hang.
        let aliveSubject = "slow.consumer.alive"
        let aliveSub = try await client.subscribe(subject: aliveSubject)
        try await client.flush()
        try await client.publish(Data("ping".utf8), subject: aliveSubject)
        try await client.flush()

        let aliveIter = aliveSub.makeAsyncIterator()
        var aliveMsg: NatsMessage?
        let aliveDeadline = Date().addingTimeInterval(5)
        while Date() < aliveDeadline {
            if let message = try aliveIter.tryNext() {
                aliveMsg = message
                break
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTAssertEqual(
            aliveMsg?.payload, Data("ping".utf8),
            "client must still deliver messages after the slow-consumer episode")

        try await client.close()
    }

    /// The slow-consumer edge flag must RESET once the backlog drains, so a second, independent
    /// overflow episode fires a fresh event. Uses `capacity == 1` — the case where the low-water mark
    /// is 0 and the flag would get permanently stuck without the empty-buffer reset.
    func testSlowConsumerReFiresAfterRecovery() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let subject = "slow.consumer.reepisode"
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .subscriptionCapacity(1)
            .build()

        let fireCount = NIOLockedValueBox<Int>(0)
        client.on(.error) { event in
            guard case .error(let err) = event,
                let subErr = err as? NatsError.SubscriptionError,
                case .slowConsumer(let subj) = subErr, subj == subject
            else { return }
            fireCount.withLockedValue { $0 += 1 }
        }

        try await client.connect()
        let sub = try await client.subscribe(subject: subject)
        try await client.flush()

        func overflowEpisode() async throws {
            for i in 0..<20 {
                try await client.publish(Data("\(i)".utf8), subject: subject)
            }
            try await client.flush()
        }

        // Episode 1: overflow → fires once.
        try await overflowEpisode()
        let firstFired = try await waitUntil(10) { fireCount.withLockedValue { $0 } >= 1 }
        XCTAssertTrue(firstFired, "first slow-consumer episode should fire")

        // Drain the buffer fully so the edge flag resets (buffer empties).
        let iter = sub.makeAsyncIterator()
        while try iter.tryNext() != nil {}

        // Episode 2: a fresh overflow must fire AGAIN (== 2, not stuck at 1).
        try await overflowEpisode()
        let secondFired = try await waitUntil(10) { fireCount.withLockedValue { $0 } >= 2 }
        XCTAssertTrue(
            secondFired,
            "a second slow-consumer episode must fire after recovery (edge flag must not stick)")

        try await client.close()
    }
}
