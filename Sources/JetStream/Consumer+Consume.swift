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

/// Pull-consumer ``MessageConsuming`` conformance.
///
/// Each continuous-consumption call (``consume(_:onError:)`` or ``messages()``) starts its OWN,
/// independent sequential pull-batch loop: it issues successive `fetch` requests and refills when the
/// current batch drains, reusing ``FetchResult``'s idle-heartbeat handling. Unlike the push and
/// ordered consumers — which share a single server-driven delivery pump across all three methods —
/// every pull `consume`/`messages` call is a separate pull-request cycle, mirroring nats.go, where
/// concurrent pull subscriptions on one consumer are allowed and independent.
///
/// Running several such loops (or a loop plus ``next(timeout:)``) concurrently on the SAME consumer
/// is therefore supported, but they all pull from the same server-side consumer and so COMPETE for
/// its messages — each message goes to exactly one loop, non-deterministically. Per the
/// ``MessageConsuming`` contract, consume a given consumer through one method at a time unless you
/// deliberately want that fan-out. ``next(timeout:)`` is a distinct one-shot `fetch(batch: 1)`, not a
/// view onto any running loop.
///
/// The overlapping-pull / pending-threshold optimization that nats.go performs within a single loop
/// is intentionally NOT implemented in v1; a correct batch loop is the goal.
extension Consumer: MessageConsuming {

    /// Default batch size for the continuous pull loop.
    private static let defaultConsumeBatch = 100

    /// Default per-request expiry for the continuous pull loop.
    private static let defaultConsumeExpires: TimeInterval = 30

    /// Default idle heartbeat for the continuous pull loop.
    private static let defaultConsumeHeartbeat: TimeInterval = 5

    /// The ``ConsumerInfo`` cached on this consumer. Refreshed by ``info()``.
    public var cachedInfo: ConsumerInfo {
        info
    }

    /// Continuously receives messages via a sequential pull-batch loop.
    ///
    /// This starts a fresh, independent pull loop; it does NOT share state with ``messages()`` or a
    /// prior ``consume(_:onError:)``. Multiple concurrent loops on the same consumer compete for its
    /// messages (see the type-level note) — typically run one at a time.
    @discardableResult
    public func consume(
        _ handler: @escaping MessageHandler, onError: ConsumeErrorHandler? = nil
    ) throws -> ConsumeContext {
        let stream = makePullStream(
            batch: Consumer.defaultConsumeBatch,
            expires: Consumer.defaultConsumeExpires,
            idleHeartbeat: Consumer.defaultConsumeHeartbeat)
        return JetStreamConsumeContext(stream: stream, handler: handler, onError: onError)
    }

    /// Iterates messages via a sequential pull-batch loop.
    ///
    /// Like ``consume(_:onError:)``, each call starts its own independent pull loop; concurrent loops
    /// on the same consumer compete for its messages. Breaking the `for await` loop and releasing the
    /// returned context tears the loop down (see ``MessagesContext``); ``MessagesContext/stop()``/``MessagesContext/drain()`` do so
    /// eagerly.
    public func messages() throws -> any MessagesContext {
        let stream = makePullStream(
            batch: Consumer.defaultConsumeBatch,
            expires: Consumer.defaultConsumeExpires,
            idleHeartbeat: Consumer.defaultConsumeHeartbeat)
        return JetStreamMessagesContext(stream: stream)
    }

    /// Retrieves the next message via a single `fetch(batch: 1, expires: timeout)` (nats.go `Next`).
    ///
    /// - Parameter timeout: the pull-request expiry.
    /// - Returns: the next message, or `nil` if none arrived within `timeout`.
    public func next(timeout: TimeInterval = 30) async throws -> JetStreamMessage? {
        let result = try await fetch(batch: 1, expires: timeout)
        var iterator = result.makeAsyncIterator()
        return try await iterator.next()
    }

    /// Retrieves up to `batch` messages that are CURRENTLY available, without waiting for new ones.
    ///
    /// Unlike ``fetch(batch:expires:idleHeartbeat:)``, this returns as soon as the server has
    /// reported what it has, even if fewer than `batch` messages are available.
    ///
    /// - Parameter batch: maximum number of messages to retrieve.
    /// - Returns: a ``FetchResult`` iterating the immediately available messages.
    public func fetchNoWait(batch: Int) async throws -> FetchResult {
        let request = PullRequest(
            batch: batch, expires: NanoTimeInterval(30), noWait: true)
        return try await sendPull(request, batch: batch, idleHeartbeat: nil)
    }

    /// Retrieves messages up to a total of `maxBytes`, sending a single request.
    ///
    /// - Parameters:
    ///   - maxBytes: maximum total bytes to retrieve.
    ///   - expires: timeout of the pull request.
    ///   - idleHeartbeat: interval at which the server should send heartbeats when idle.
    /// - Returns: a ``FetchResult`` iterating the retrieved messages.
    public func fetchBytes(
        maxBytes: Int, expires: TimeInterval = 30, idleHeartbeat: TimeInterval? = nil
    ) async throws -> FetchResult {
        let heartbeat = idleHeartbeat.map { NanoTimeInterval($0) }
        let request = PullRequest(
            batch: maxBytes, expires: NanoTimeInterval(expires), maxBytes: maxBytes,
            heartbeat: heartbeat)
        return try await sendPull(request, batch: maxBytes, idleHeartbeat: idleHeartbeat)
    }

    /// Issues a raw pull request and wraps the reply subscription in a ``FetchResult``.
    private func sendPull(
        _ request: PullRequest, batch: Int, idleHeartbeat: TimeInterval?
    ) async throws -> FetchResult {
        let subject = ctx.apiSubject("CONSUMER.MSG.NEXT.\(info.stream).\(info.name)")
        let inbox = ctx.client.newInbox()
        let sub = try await ctx.client.subscribe(subject: inbox)
        try await ctx.client.publish(
            JSONEncoder().encode(request), subject: subject, reply: inbox)
        return FetchResult(ctx: ctx, sub: sub, idleHeartbeat: idleHeartbeat, batch: batch)
    }

    /// Builds a ``MessageStream`` backed by a fresh sequential pull-batch loop.
    private func makePullStream(
        batch: Int, expires: TimeInterval, idleHeartbeat: TimeInterval?
    ) -> MessageStream {
        let source = PullMessageSource(
            consumer: self, batch: batch, expires: expires, idleHeartbeat: idleHeartbeat)
        return MessageStream(source: source)
    }
}

/// A sequential pull-batch ``MessageSource``: fetch a batch, yield each message, refill on drain.
private final class PullMessageSource: MessageSource, @unchecked Sendable {
    private let consumer: Consumer
    private let batch: Int
    private let expires: TimeInterval
    private let idleHeartbeat: TimeInterval?

    private let stateLock = NSLock()
    private var closed = false
    private var current: FetchResult?

    // Accessed only by the single pump task iterating `next()`.
    private var iterator: FetchResult.FetchIterator?

    init(consumer: Consumer, batch: Int, expires: TimeInterval, idleHeartbeat: TimeInterval?) {
        self.consumer = consumer
        self.batch = batch
        self.expires = expires
        self.idleHeartbeat = idleHeartbeat
    }

    func next() async throws -> JetStreamMessage? {
        while true {
            if isClosed() {
                return nil
            }
            if iterator == nil {
                let result = try await consumer.fetch(
                    batch: batch, expires: expires, idleHeartbeat: idleHeartbeat)
                if isClosed() {
                    await result.cancel()
                    return nil
                }
                setCurrent(result)
                iterator = result.makeAsyncIterator()
            }

            var it = iterator!
            let message = try await it.next()
            iterator = it

            if let message {
                return message
            }
            // Batch drained (expiry / no-more-messages) — refill on the next loop.
            iterator = nil
            setCurrent(nil)
        }
    }

    func teardown() async {
        let result = stateLock.withLockScoped { () -> FetchResult? in
            closed = true
            let result = current
            current = nil
            return result
        }
        await result?.cancel()
    }

    private func isClosed() -> Bool {
        stateLock.lock()
        defer { stateLock.unlock() }
        return closed
    }

    private func setCurrent(_ result: FetchResult?) {
        stateLock.lock()
        current = result
        stateLock.unlock()
    }
}
