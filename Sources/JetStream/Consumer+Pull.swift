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
import Nuid

/// Extension to ``Consumer`` adding pull consumer capabilities.
extension Consumer {

    /// Retrieves up to a provided number of messages from a stream.
    /// This method will send a single request and deliver requested messages unless time out is met earlier.
    ///
    ///  - Parameters:
    ///   - batch: maximum number of messages to be retrieved
    ///   - expires: timeout of a pull request
    ///   - idleHeartbeat: interval in which server should send heartbeat messages (if no user messages are available).
    ///
    ///  - Returns: ``FetchResult`` which implements `AsyncSequence` allowing iteration over messages.
    ///
    ///  - Throws:
    ///   - ``JetStreamError/FetchError`` if there was an error while fetching messages
    public func fetch(
        batch: Int, expires: TimeInterval = 30, idleHeartbeat: TimeInterval? = nil
    ) async throws -> FetchResult {
        var request: PullRequest
        if let idleHeartbeat {
            request = PullRequest(
                batch: batch, expires: NanoTimeInterval(expires),
                heartbeat: NanoTimeInterval(idleHeartbeat))
        } else {
            request = PullRequest(batch: batch, expires: NanoTimeInterval(expires))
        }

        let subject = ctx.apiSubject("CONSUMER.MSG.NEXT.\(info.stream).\(info.name)")
        let inbox = ctx.client.newInbox()
        let sub = try await ctx.client.subscribe(subject: inbox)
        try await self.ctx.client.publish(
            JSONEncoder().encode(request), subject: subject, reply: inbox)
        return FetchResult(ctx: ctx, sub: sub, idleHeartbeat: idleHeartbeat, batch: batch)
    }
}

/// Used to iterate over results of ``Consumer/fetch(batch:expires:idleHeartbeat:)``
public class FetchResult: AsyncSequence {
    public typealias Element = JetStreamMessage
    public typealias AsyncIterator = FetchIterator

    private let ctx: JetStreamContext
    private let sub: NatsSubscription
    private let idleHeartbeat: TimeInterval?
    private let batch: Int

    init(ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?, batch: Int) {
        self.ctx = ctx
        self.sub = sub
        self.idleHeartbeat = idleHeartbeat
        self.batch = batch
    }

    public func makeAsyncIterator() -> FetchIterator {
        return FetchIterator(
            ctx: ctx,
            sub: self.sub, idleHeartbeat: self.idleHeartbeat, remainingMessages: self.batch)
    }

    /// Tears down the underlying reply subscription. Used to promptly stop a continuous pull loop.
    internal func cancel() async {
        try? await sub.unsubscribe()
    }

    public struct FetchIterator: AsyncIteratorProtocol {
        private let ctx: JetStreamContext
        private let sub: NatsSubscription
        private let idleHeartbeat: TimeInterval?
        private var remainingMessages: Int
        private var subIterator: NatsSubscription.AsyncIterator
        private var didUnsub = false

        init(
            ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?,
            remainingMessages: Int
        ) {
            self.ctx = ctx
            self.sub = sub
            self.idleHeartbeat = idleHeartbeat
            self.remainingMessages = remainingMessages
            self.subIterator = sub.makeAsyncIterator()
        }

        /// Unsubscribes at most once, tolerating an already-closed subscription. `unsubscribe()`
        /// throws on an already-closed sub, which happens two ways here: the eager batch-end
        /// unsubscribe double-firing with the trailing exhausted-batch path, and drain()/teardown()
        /// closing the sub out from under an in-flight fetch. Both should end quietly, not surface a
        /// spurious `subscriptionClosed`.
        private mutating func unsubscribeOnce() async throws {
            if didUnsub {
                return
            }
            didUnsub = true
            do {
                try await sub.unsubscribe()
            } catch NatsError.SubscriptionError.subscriptionClosed {
                // Already closed (e.g. torn down by drain()/teardown()); nothing to do.
            }
        }

        public mutating func next() async throws -> JetStreamMessage? {
            if remainingMessages <= 0 {
                try await unsubscribeOnce()
                return nil
            }

            while true {
                let message: NatsMessage?

                if let idleHeartbeat = idleHeartbeat {
                    let timeout = idleHeartbeat * 2
                    do {
                        message = try await nextWithTimeout(timeout, subIterator)
                    } catch {
                        // A missed heartbeat (or any error) ends this fetch: clean up the reply-inbox
                        // subscription, then surface the error. nextWithTimeout no longer unsubscribes
                        // itself, so cleanup routes through the once-guard here.
                        try? await unsubscribeOnce()
                        throw error
                    }
                } else {
                    message = try await subIterator.next()
                }

                guard let message else {
                    // the subscription has ended
                    try await unsubscribeOnce()
                    return nil
                }

                let status = message.status ?? .ok

                switch status {
                case .timeout:
                    try await unsubscribeOnce()
                    return nil
                case .idleHeartbeat:
                    // in case of idle heartbeat error, we want to
                    // wait for next message on subscription
                    continue
                case .notFound:
                    try await unsubscribeOnce()
                    return nil
                case .ok:
                    remainingMessages -= 1
                    let jsMessage = JetStreamMessage(message: message, client: ctx.client)
                    if remainingMessages <= 0 {
                        // Last message of the batch: unsubscribe now. The post-switch cleanup used to
                        // live below but was unreachable (every arm returns/throws/continues), so the
                        // reply-inbox subscription leaked until a SUBSEQUENT next() call -- which for
                        // batch=1 (every Consumer.next(timeout:)) never comes.
                        try await unsubscribeOnce()
                    }
                    return jsMessage
                case .badRequest:
                    try await unsubscribeOnce()
                    throw JetStreamError.FetchError.badRequest
                case .noResponders:
                    try await unsubscribeOnce()
                    throw JetStreamError.FetchError.noResponders
                case .requestTerminated:
                    try await unsubscribeOnce()
                    guard let description = message.description else {
                        throw JetStreamError.FetchError.invalidResponse
                    }

                    let descLower = description.lowercased()
                    if descLower.contains("message size exceeds maxbytes") {
                        return nil
                    } else if descLower.contains("leadership changed") {
                        throw JetStreamError.FetchError.leadershipChanged
                    } else if descLower.contains("consumer deleted") {
                        throw JetStreamError.FetchError.consumerDeleted
                    } else if descLower.contains("consumer is push based") {
                        throw JetStreamError.FetchError.consumerIsPush
                    } else {
                        // Any other 409 reason (newer servers add more, e.g. exceeded MaxWaiting /
                        // MaxRequestBatch): surface it. Falling through here instead would return to a
                        // read on the already-unsubscribed subscription and mask the real reason as
                        // `subscriptionClosed`.
                        throw JetStreamError.FetchError.unknownStatus(status, message.description)
                    }
                default:
                    // Unsubscribe before surfacing, matching every other terminal branch; otherwise
                    // an unrecognized status leaks the reply-inbox subscription.
                    try await unsubscribeOnce()
                    throw JetStreamError.FetchError.unknownStatus(status, message.description)
                }
            }
        }

        func nextWithTimeout(
            _ timeout: TimeInterval, _ subIterator: NatsSubscription.AsyncIterator
        ) async throws -> NatsMessage? {
            // Fast path: an already-buffered message is returned WITHOUT building the per-message
            // task group (the same optimization as `PushDelivery.race()`). Under a full pull batch the
            // reply inbox is rarely empty, so the group + its two child tasks are skipped on the hot
            // path. The heartbeat timeout is only needed when the inbox is idle — the empty case that
            // falls through to the group below.
            if let ready = try subIterator.tryNext() {
                return ready
            }
            return try await withThrowingTaskGroup(of: PullRaceOutcome.self) { group in
                group.addTask {
                    // `.ended` (a nil from the iterator) means the subscription was torn down --
                    // e.g. drain()/teardown() unsubscribing it -- NOT a heartbeat timeout.
                    if let msg = try await subIterator.next() {
                        return .message(msg)
                    }
                    return .ended
                }
                // Capture the already-Sendable `sub` by value so the child task holds an
                // immutable Sendable copy of the reference rather than implicitly capturing
                // `self` (the `FetchIterator` region still in use by the current task).
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                    return .heartbeatTimedOut
                }
                defer {
                    group.cancelAll()
                }
                for try await outcome in group {
                    switch outcome {
                    case .message(let msg):
                        return msg
                    case .ended:
                        // The subscription ended (torn down). Report end-of-stream, not a timeout,
                        // so a drain() mid-wait finishes cleanly instead of surfacing a spurious
                        // noHeartbeatReceived. The outer loop's `guard let message` then unsubscribes
                        // (idempotent) and returns nil.
                        return nil
                    case .heartbeatTimedOut:
                        // No traffic within 2x idleHeartbeat. Surface the missed heartbeat; the
                        // caller (next()) cleans up the reply-inbox subscription via its once-guard.
                        throw JetStreamError.FetchError.noHeartbeatReceived
                    }
                }
                // Unreachable: the group always yields at least one outcome.
                return nil
            }
        }
    }
}

/// Outcome of racing the reply-inbox read against the idle-heartbeat timer in `nextWithTimeout`.
/// Distinguishing `.ended` (subscription torn down) from `.heartbeatTimedOut` (no traffic) is what
/// lets a drain()/teardown() finish cleanly instead of surfacing a spurious missed heartbeat.
private enum PullRaceOutcome: Sendable {
    case message(NatsMessage)
    case ended
    case heartbeatTimedOut
}

internal struct PullRequest: Codable {
    let batch: Int
    let expires: NanoTimeInterval
    let maxBytes: Int?
    let noWait: Bool?
    let heartbeat: NanoTimeInterval?

    internal init(
        batch: Int, expires: NanoTimeInterval, maxBytes: Int? = nil, noWait: Bool? = nil,
        heartbeat: NanoTimeInterval? = nil
    ) {
        self.batch = batch
        self.expires = expires
        self.maxBytes = maxBytes
        self.noWait = noWait
        self.heartbeat = heartbeat
    }

    enum CodingKeys: String, CodingKey {
        case batch
        case expires
        case maxBytes = "max_bytes"
        case noWait = "no_wait"
        case heartbeat = "idle_heartbeat"
    }
}
