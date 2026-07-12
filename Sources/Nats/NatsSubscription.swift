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
import NIOConcurrencyHelpers
import NIOCore

/// An `AsyncSequence` of inbound ``NatsMessage`` for a single subscription.
///
/// Inbound messages are buffered up to `capacity` (see
/// ``NatsClientOptions/subscriptionCapacity(_:)``). When the buffer is full the client is a
/// "slow consumer": further messages are dropped and a
/// ``NatsError/SubscriptionError/slowConsumer(subject:)`` is surfaced via the connection's
/// `.error` event (once per slow episode). The subscription itself keeps working — the error is
/// never delivered into the sequence — and resumes buffering once the consumer catches up.
public final class NatsSubscription: AsyncSequence, Sendable {
    public typealias Element = NatsMessage
    public typealias AsyncIterator = SubscriptionIterator

    public let subject: String
    public let queue: String?
    private let _max = NIOLockedValueBox<UInt64?>(nil)
    internal var max: UInt64? {
        get { _max.withLockedValue { $0 } }
        set { _max.withLockedValue { $0 = newValue } }
    }

    internal var delivered: UInt64 {
        state.withLockedValue { $0.delivered }
    }
    internal let sid: UInt64

    private struct State: Sendable {
        var buffer = FIFOBuffer<Result<NatsMessage, NatsError.SubscriptionError>>()
        var closed = false
        var delivered: UInt64 = 0
        var continuation:
            CheckedContinuation<Result<NatsMessage, NatsError.SubscriptionError>?, Never>? = nil
        // Edge flag: set on the first overflow drop of a slow episode so the slow-consumer
        // event fires exactly once per episode; cleared once the backlog drains below the
        // low-water mark (see `slowConsumerLowWaterMark`).
        var slowConsumer = false
    }

    private let state = NIOLockedValueBox(State())
    private let capacity: UInt64
    private let conn: ConnectionHandler

    /// Backlog level at which the per-episode slow-consumer edge flag is cleared, so a later
    /// episode surfaces a fresh event. Half of `capacity` avoids flapping around the boundary.
    private var slowConsumerLowWaterMark: Int { Int(capacity / 2) }

    internal static let defaultSubCapacity: UInt64 = 512 * 1024

    convenience init(sid: UInt64, subject: String, queue: String?, conn: ConnectionHandler) throws {
        try self.init(
            sid: sid, subject: subject, queue: queue, capacity: NatsSubscription.defaultSubCapacity,
            conn: conn)
    }

    init(
        sid: UInt64, subject: String, queue: String?, capacity: UInt64, conn: ConnectionHandler
    ) throws {
        if !NatsSubscription.validSubject(subject) {
            throw NatsError.SubscriptionError.invalidSubject
        }
        if let queue, !NatsSubscription.validQueue(queue) {
            throw NatsError.SubscriptionError.invalidQueue
        }
        self.sid = sid
        self.subject = subject
        self.queue = queue
        self.capacity = capacity
        self.conn = conn
    }

    public func makeAsyncIterator() -> SubscriptionIterator {
        return SubscriptionIterator(subscription: self)
    }

    /// Non-suspending poll: returns an already-buffered message without ever waiting, or `nil` when
    /// nothing is ready RIGHT NOW. A `nil` here means "would block" — it is NOT end-of-stream (call
    /// `nextMessage()` to await and to observe close). A buffered error is thrown, matching
    /// `nextMessage()`. Used by JetStream push delivery to skip its heartbeat-timeout task group when
    /// the deliver inbox already has a message in hand. `package`-visible: an internal optimization
    /// primitive, not end-user API.
    package func tryNextMessage() throws -> NatsMessage? {
        enum Outcome {
            case message(NatsMessage)
            case failure(NatsError.SubscriptionError)
            case empty
        }
        // Mirror `nextMessage`'s bookkeeping (delivered++ on a consumed slot), but only when a
        // message is actually taken — an empty poll consumes nothing, so the caller's fallback
        // `nextMessage()` still accounts for exactly one delivery. (Because `tryNext` bumps
        // `delivered` only on a taken message while `nextMessage` bumps it per call, mixing the two
        // on a subscription that set an `unsubscribe(max:)` limit could shift the auto-unsubscribe
        // point; the JetStream deliver inbox sets no max, so this is inert here.)
        let outcome: Outcome = state.withLockedValue { state in
            // Closed always wins, even over buffered content — matches `nextMessage`, which returns
            // `nil` on a closed subscription before inspecting the buffer. Reporting `.empty` lets
            // the caller's async fallback observe the end-of-stream `nil`.
            if state.closed {
                return .empty
            }
            guard let first = state.buffer.popFirst() else {
                return .empty
            }
            state.delivered += 1
            // Reset the slow-consumer edge flag once the backlog drains below the low-water mark (or
            // fully empties), so a later slow episode fires a fresh event. The `isEmpty` clause also
            // covers `capacity == 1`, where the low-water mark is 0 and would otherwise never reset.
            if state.slowConsumer
                && (state.buffer.isEmpty || state.buffer.count < slowConsumerLowWaterMark)
            {
                state.slowConsumer = false
            }
            switch first {
            case .success(let message): return .message(message)
            case .failure(let error): return .failure(error)
            }
        }

        switch outcome {
        case .empty:
            return nil
        case .failure(let error):
            removeIfAtMax()
            throw error
        case .message(let message):
            removeIfAtMax()
            return message
        }
    }

    /// Auto-unsubscribe once `delivered` reaches an `unsubscribe(max:)` limit — the same check
    /// `nextMessage()` performs after taking a message. Inert for push delivery (no max is set).
    private func removeIfAtMax() {
        let delivered = state.withLockedValue { $0.delivered }
        if let max, delivered >= max {
            conn.removeSub(sub: self)
        }
    }

    /// Delivers an inbound message: resumes a waiting reader, or buffers it, or (on overflow) drops it.
    ///
    /// Returns the subject a slow-consumer `.error` event should be fired for (once per overflow
    /// episode), or `nil` when none is due. The event is RETURNED rather than fired here so the caller
    /// can fire it AFTER releasing the connection's `subscriptions` lock — a user `.error` handler runs
    /// synchronously, and must never run while that lock is held (it would stall concurrent
    /// subscribe/unsubscribe). Resuming the reader continuation here is fine: `resume` only schedules.
    func receiveMessage(_ message: NatsMessage) -> String? {
        // Decide everything under the lock, then act (resume) OUTSIDE it: never call a continuation
        // while holding `state.withLockedValue`.
        enum PostLockAction {
            case resume(
                CheckedContinuation<Result<NatsMessage, NatsError.SubscriptionError>?, Never>)
            case fireSlowConsumer
            case none
        }

        let action: PostLockAction = state.withLockedValue { state in
            if let continuation = state.continuation {
                // Only append to buffer if no continuation is available
                state.continuation = nil
                return .resume(continuation)
            } else if state.buffer.count < capacity {
                state.buffer.append(.success(message))
                return .none
            } else {
                // Slow consumer: buffer is full. Drop the message (don't block the read loop,
                // don't grow unbounded), but surface a slow-consumer event ONCE per episode.
                if state.slowConsumer {
                    return .none
                }
                state.slowConsumer = true
                return .fireSlowConsumer
            }
        }

        switch action {
        case .resume(let continuation):
            continuation.resume(returning: .success(message))
            return nil
        case .fireSlowConsumer:
            return subject
        case .none:
            return nil
        }
    }

    func receiveError(_ error: NatsError.SubscriptionError) {
        let continuationToResume:
            CheckedContinuation<Result<NatsMessage, NatsError.SubscriptionError>?, Never>? =
                state.withLockedValue { state in
                    if let continuation = state.continuation {
                        state.continuation = nil
                        return continuation
                    } else {
                        state.buffer.append(.failure(error))
                        return nil
                    }
                }

        continuationToResume?.resume(returning: .failure(error))
    }

    internal func complete() {
        let continuationToResume:
            CheckedContinuation<Result<NatsMessage, NatsError.SubscriptionError>?, Never>? =
                state.withLockedValue { state in
                    state.closed = true
                    let cont = state.continuation
                    state.continuation = nil
                    return cont
                }

        continuationToResume?.resume(returning: nil)
    }

    // AsyncIterator implementation
    public final class SubscriptionIterator: AsyncIteratorProtocol, Sendable {
        private let subscription: NatsSubscription

        init(subscription: NatsSubscription) {
            self.subscription = subscription
        }

        public func next() async throws -> Element? {
            try await subscription.nextMessage()
        }

        /// Non-suspending poll; see ``NatsSubscription/tryNextMessage()``. `nil` means "nothing ready
        /// now", not end-of-stream.
        package func tryNext() throws -> Element? {
            try subscription.tryNextMessage()
        }
    }

    private func nextMessage() async throws -> Element? {
        // Use withTaskCancellationHandler to prevent continuation leaks and hangs
        // if the parent Task is cancelled while awaiting a message.
        let result: Result<Element, NatsError.SubscriptionError>? =
            await withTaskCancellationHandler {
                await withCheckedContinuation { continuation in
                    enum Action {
                        case resume(Result<Element, NatsError.SubscriptionError>?)
                        case suspend
                    }

                    let action: Action = state.withLockedValue { state in
                        if state.closed {
                            return .resume(nil)
                        }

                        // delivered tracks "slots consumed", not "messages returned".
                        // It is incremented here — before the message is in hand.
                        state.delivered += 1

                        if let message = state.buffer.popFirst() {
                            // Reset the slow-consumer edge flag once the backlog drains below the
                            // low-water mark (or fully empties, which also covers `capacity == 1`),
                            // so a later slow episode fires a fresh event.
                            if state.slowConsumer
                                && (state.buffer.isEmpty
                                    || state.buffer.count < slowConsumerLowWaterMark)
                            {
                                state.slowConsumer = false
                            }
                            return .resume(message)
                        } else {
                            state.continuation = continuation
                            return .suspend
                        }
                    }

                    // Resume outside the lock
                    if case .resume(let val) = action {
                        continuation.resume(returning: val)
                    }
                }
            } onCancel: {
                // If the iteration is cancelled, wake up the suspension point and clean up
                let continuationToResume:
                    CheckedContinuation<Result<NatsMessage, NatsError.SubscriptionError>?, Never>? =
                        state.withLockedValue { state in
                            let cont = state.continuation
                            state.continuation = nil
                            return cont
                        }
                continuationToResume?.resume(returning: nil)
            }

        let delivered = state.withLockedValue { $0.delivered }
        if let max, delivered >= max {
            conn.removeSub(sub: self)
        }

        switch result {
        case .success(let msg):
            return msg
        case .failure(let error):
            throw error
        default:
            return nil
        }
    }

    /// Unsubscribes from subscription.
    ///
    /// - Parameter after: If set, unsubscribe will be performed after reaching given number of messages.
    ///   If it already reached or surpassed the passed value, it will immediately stop.
    ///
    /// > **Throws:**
    /// > - ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    /// > - ``NatsError/SubscriptionError/subscriptionClosed`` if the subscription is already closed
    public func unsubscribe(after: UInt64? = nil) async throws {
        logger.info("unsubscribe from subject \(subject)")
        if case .closed = self.conn.currentState {
            throw NatsError.ClientError.connectionClosed
        }
        let isClosed = state.withLockedValue { $0.closed }
        if isClosed {
            throw NatsError.SubscriptionError.subscriptionClosed
        }
        return try await self.conn.unsubscribe(sub: self, max: after)
    }

    // validateSubject will do a basic subject validation.
    // Spaces are not allowed and all tokens should be > 0 in length.
    private static func validSubject(_ subj: String) -> Bool {
        let whitespaceCharacterSet = CharacterSet.whitespacesAndNewlines
        if subj.rangeOfCharacter(from: whitespaceCharacterSet) != nil {
            return false
        }
        let tokens = subj.split(separator: ".")
        for token in tokens {
            if token.isEmpty {
                return false
            }
        }
        return true
    }

    // validQueue will check a queue name for whitespaces.
    private static func validQueue(_ queue: String) -> Bool {
        let whitespaceCharacterSet = CharacterSet.whitespacesAndNewlines
        return queue.rangeOfCharacter(from: whitespaceCharacterSet) == nil
    }
}
