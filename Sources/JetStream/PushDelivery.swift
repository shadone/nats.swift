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

/// Delivery mechanism for a JetStream push consumer.
///
/// Given a push consumer (created with a deliver subject, flow control and an idle heartbeat), a
/// ``PushDelivery`` drives the deliver-inbox subscription and classifies every incoming
/// ``NatsMessage`` into a ``PushDelivery/Event``. Control traffic that can be resolved without
/// higher-level state — flow-control replies and consumer-stalled replies — is handled inline;
/// everything the ordered wrapper needs to make a decision (data, heartbeats, terminal 409s and a
/// missed heartbeat) is surfaced as an event.
///
/// The classification contract mirrors nats.go's `isJSControlMessage` and the new
/// `jetstream/push.go` status handling:
/// - DATA ⇔ `status == nil` → ``PushDelivery/Event/message(_:)``.
/// - CONTROL ⇔ `status == .idleHeartbeat` (100) with an empty body; branch on `description`
///   (case-insensitive), not on the code:
///   - `"Flow…"` → publish an empty message to the reply subject, then continue.
///   - `"Idle…"` → if a `Nats-Consumer-Stalled` header is present, publish an empty message to that
///     subject, then surface ``PushDelivery/Event/idleHeartbeat(_:)`` so the wrapper can run the
///     `Nats-Last-Consumer` sequence-mismatch check.
///   - any other control body → consumed as a heartbeat (resets the deadline), no event.
/// - 409 ⇔ `status == .requestTerminated`: `"Consumer Deleted"` →
///   ``PushDelivery/Event/consumerDeleted``, `"Leadership Change"` →
///   ``PushDelivery/Event/leadershipChanged``.
///
/// Missed heartbeats are folded into ``PushDelivery/next()``: each `await iterator.next()` is raced
/// against `Task.sleep(2 × idleHeartbeat)` (the same idiom as `Consumer+Pull`'s `nextWithTimeout`).
/// If the sleep wins, ``PushDelivery/Event/missedHeartbeat`` is surfaced. There is no separate timer
/// task.
internal final class PushDelivery {
    /// An event surfaced from the deliver-inbox subscription for the ordered wrapper to act on.
    internal enum Event {
        /// A data message that should be handed to the wrapper's accept-check.
        case message(NatsMessage)
        /// An idle heartbeat. The wrapper runs the `Nats-Last-Consumer` sequence-mismatch check.
        case idleHeartbeat(NatsMessage)
        /// A 409 `Consumer Deleted` status. Terminal for the current consumer.
        case consumerDeleted
        /// A 409 `Leadership Change` status.
        case leadershipChanged
        /// No message (nor control) arrived within `2 × idleHeartbeat`.
        case missedHeartbeat
        /// The underlying subscription ended (unsubscribe or connection teardown).
        case closed
    }

    private let client: NatsClient
    private let iterator: NatsSubscription.AsyncIterator
    private let idleHeartbeatSeconds: TimeInterval

    /// A data message the previous ``race()`` dequeued in a dead heat with its timeout, held over to
    /// be returned by the next ``race()`` instead of being dropped. See ``race()``. Only ever touched
    /// by the single pump task that drives this delivery, so it needs no synchronization.
    private var pendingMessage: NatsMessage?

    internal init(
        client: NatsClient, subscription: NatsSubscription, idleHeartbeatSeconds: TimeInterval
    ) {
        self.client = client
        self.iterator = subscription.makeAsyncIterator()
        self.idleHeartbeatSeconds = idleHeartbeatSeconds
    }

    /// Reads and classifies the next actionable event from the deliver inbox.
    ///
    /// Flow-control and generic control messages are handled inline and never returned; the loop
    /// continues until a data message, heartbeat, terminal status, missed heartbeat or subscription
    /// end is reached.
    internal func next() async throws -> Event {
        while true {
            switch try await race() {
            case .timedOut:
                return .missedHeartbeat
            case .ended:
                return .closed
            case .message(let msg):
                if let event = await handle(msg) {
                    return event
                }
            }
        }
    }

    /// Classifies a single message, performing any inline side effects (flow-control reply,
    /// consumer-stalled reply). Returns the event to surface, or `nil` when the message was fully
    /// handled inline and the caller should continue reading.
    ///
    /// Exposed at `internal` access so the flow-control reply behaviour can be driven
    /// deterministically from tests without depending on server timing.
    internal func handle(_ msg: NatsMessage) async -> Event? {
        // DATA ⇔ no status header (mirrors isJSControlMessage checking statusHdr != "100").
        guard let status = msg.status else {
            return .message(msg)
        }

        // CONTROL ⇔ status 100 with an empty body; branch on the description, not the code.
        if status == .idleHeartbeat {
            let description = (msg.description ?? "").lowercased()
            if description.hasPrefix("flow") {
                // Flow control: reply on dequeue. Because control is FIFO-interleaved with data on
                // one iterator, replying here already guarantees all preceding data was delivered.
                if let reply = msg.replySubject, !reply.isEmpty {
                    try? await client.publish(Data(), subject: reply)
                }
                return nil
            }
            if description.hasPrefix("idle") {
                // A stalled consumer asks the client to nudge it by replying to the carried subject.
                if let stalled = msg.headers?.get(.natsConsumerStalled)?.description,
                    !stalled.isEmpty
                {
                    try? await client.publish(Data(), subject: stalled)
                }
                return .idleHeartbeat(msg)
            }
            // Any other control body: consumed as a heartbeat (the race already reset the deadline).
            return nil
        }

        // 409 terminal statuses.
        if status == .requestTerminated {
            let description = (msg.description ?? "").lowercased()
            if description.contains("consumer deleted") {
                return .consumerDeleted
            }
            if description.contains("leadership") {
                return .leadershipChanged
            }
            // Any other 409 (e.g. server shutdown) is treated as a reason to recreate.
            return .leadershipChanged
        }

        // Any other status is ignored; keep reading.
        return nil
    }

    private enum RaceOutcome {
        case message(NatsMessage)
        case ended
        case timedOut
    }

    /// Awaits the next message, racing it against a `2 × idleHeartbeat` sleep so a silent deliver
    /// inbox surfaces as ``RaceOutcome/timedOut``. When the heartbeat interval is not set the read
    /// is unbounded.
    ///
    /// The task group cancels the losing child when the race resolves. If the timeout wins, the
    /// reader child may nonetheless have ALREADY dequeued a message in a dead heat: the subscription
    /// iterator returns a message it has in hand even once cancelled, and only a still-suspended read
    /// returns `nil` on cancellation. To avoid dropping that message, when the timeout wins the reader
    /// is cancelled (so a suspended read unblocks with `nil`) and then drained: any message it had in
    /// hand is stashed in ``pendingMessage`` and returned by the next call rather than lost.
    ///
    /// This closes the rare loss a raw ``PushConsumer`` would otherwise see under that exact
    /// coincidence. For the ordered consumer it is moot either way — a missed heartbeat recreates the
    /// consumer from `streamSeq + 1` and refetches the message — so the fix neither helps nor harms
    /// the ordered path.
    private func race() async throws -> RaceOutcome {
        if let pending = pendingMessage {
            pendingMessage = nil
            return .message(pending)
        }

        // Fast path: if the deliver inbox already has a message buffered, take it WITHOUT building
        // the heartbeat-timeout task group. Under real throughput the inbox is rarely empty, so the
        // group (and its two per-message child tasks) is skipped on the hot path. This is a pure
        // optimization: the message is the same one `iterator.next()` would return, in the same FIFO
        // order, and it is still classified by `handle()` (flow-control replies included) exactly as
        // before. A missed heartbeat can only occur while the inbox is idle — i.e. the empty case
        // handled below — so heartbeat detection and the dead-heat stash are untouched.
        if let ready = try iterator.tryNext() {
            return .message(ready)
        }

        if idleHeartbeatSeconds <= 0 {
            if let msg = try await iterator.next() {
                return .message(msg)
            }
            return .ended
        }

        let timeoutNanos = UInt64(2 * idleHeartbeatSeconds * 1_000_000_000)
        let iterator = self.iterator
        return try await withThrowingTaskGroup(of: RaceOutcome.self) { group in
            group.addTask {
                if let msg = try await iterator.next() {
                    return .message(msg)
                }
                return .ended
            }
            group.addTask {
                try await Task.sleep(nanoseconds: timeoutNanos)
                return .timedOut
            }
            // The first child to finish wins.
            guard let outcome = try await group.next() else {
                group.cancelAll()
                return .ended
            }
            // Cancel the loser. When the timeout won this unblocks a still-suspended reader (it
            // returns `nil`); a reader that already had a message in hand still yields it, which the
            // drain below preserves.
            group.cancelAll()
            if case .timedOut = outcome {
                while let remaining = try? await group.next() {
                    if case .message(let msg) = remaining {
                        pendingMessage = msg
                    }
                }
            }
            return outcome
        }
    }
}
