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

/// A callback invoked for every message delivered by ``MessageConsuming/consume(_:onError:)``.
public typealias MessageHandler = @Sendable (JetStreamMessage) -> Void

/// A callback invoked when continuous consumption encounters an error.
public typealias ConsumeErrorHandler = @Sendable (Error) -> Void

/// The unified, first-class consumption surface shared by pull, push and ordered JetStream
/// consumers.
///
/// `MessageConsuming` offers three ways to receive messages, mirroring nats.go's `Consumer` /
/// `PushConsumer` interfaces:
/// - ``consume(_:onError:)`` for continuous, callback-driven delivery.
/// - ``messages()`` for `AsyncSequence` iteration.
/// - ``next(timeout:)`` for a single, timeout-bounded message.
///
/// Consume a given consumer through one of them at a time. For push and ordered consumers all three
/// share a single server-driven delivery pump; for a pull consumer each ``consume(_:onError:)`` /
/// ``messages()`` call is an independent pull loop and concurrent loops compete for the consumer's
/// messages (see `Consumer`'s conformance for the details).
public protocol MessageConsuming {
    /// Continuously receives messages, invoking `handler` for each.
    ///
    /// Delivery runs on a background task until the returned ``ConsumeContext`` is stopped or
    /// drained. Terminal delivery errors are reported to `onError` (if provided).
    ///
    /// - Parameters:
    ///   - handler: invoked for every delivered message.
    ///   - onError: invoked once with a terminal error, if delivery fails.
    /// - Returns: a ``ConsumeContext`` used to stop or drain delivery.
    @discardableResult
    func consume(
        _ handler: @escaping MessageHandler, onError: ConsumeErrorHandler?
    ) throws -> ConsumeContext

    /// Returns an `AsyncSequence` of delivered messages.
    ///
    /// Iterating the returned ``MessagesContext`` yields messages in delivery order. Calling
    /// ``MessagesContext/stop()`` / ``MessagesContext/drain()`` is the primary way to tear the
    /// iteration down; breaking out of the loop and releasing the context (and any iterators it
    /// produced) also tears it down as a best-effort backstop, via `deinit`.
    func messages() throws -> any MessagesContext

    /// Retrieves the next single message, waiting at most `timeout` seconds.
    ///
    /// - Parameter timeout: the maximum time to wait for a message.
    /// - Returns: the next message, or `nil` if none arrived within `timeout`.
    func next(timeout: TimeInterval) async throws -> JetStreamMessage?

    /// Fetches current ``ConsumerInfo`` from the server, refreshing ``cachedInfo``.
    func info() async throws -> ConsumerInfo

    /// The ``ConsumerInfo`` currently cached on this consumer. Performs no network request.
    var cachedInfo: ConsumerInfo { get }
}

extension MessageConsuming {
    /// Continuous, callback-driven delivery without an error handler.
    @discardableResult
    public func consume(_ handler: @escaping MessageHandler) throws -> ConsumeContext {
        try consume(handler, onError: nil)
    }

    /// Default single-message retrieval expressed through ``messages()``.
    ///
    /// Each pull/push/ordered consumer overrides this with a more direct implementation, so this
    /// default is currently dead code; it exists only so any FUTURE ``MessageConsuming`` type gets
    /// `next(timeout:)` for free.
    ///
    /// - Note: Unlike ``MessageStream/next(timeout:)`` — which is lossless because a timeout only
    ///   wakes the mailbox waiter without consuming a message — this default races the iterator's
    ///   `next()` against a `Task.sleep` in a task group and cancels the loser. If the sleep wins in a
    ///   dead heat with a just-delivered message, that message can be dropped (the same structural
    ///   race as ``PushDelivery/race()``). A future conformer that relies on this default rather than
    ///   overriding it inherits that race; prefer routing through a ``MessageStream`` (as the built-in
    ///   conformers do) if losslessness matters.
    public func next(timeout: TimeInterval = 30) async throws -> JetStreamMessage? {
        let context = try messages()
        defer { context.stop() }
        let iterator = context.makeAsyncIterator()
        return try await withThrowingTaskGroup(of: JetStreamMessage?.self) { group in
            group.addTask {
                // Take a local mutable copy inside the task rather than capturing the outer `var`:
                // `JetStreamMessageIterator` is a `Sendable` value type, so the sending closure gets
                // its own copy and the read stays confined to this child task.
                var iterator = iterator
                return try await iterator.next()
            }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                return nil
            }
            defer { group.cancelAll() }
            return try await group.next() ?? nil
        }
    }
}

/// Controls a continuous ``MessageConsuming/consume(_:onError:)`` delivery.
public protocol ConsumeContext: Sendable {
    /// Stops delivery immediately: unsubscribes and discards any buffered messages. No further
    /// messages are handed to the callback after this call.
    func stop()

    /// Stops delivery gracefully: unsubscribes, but processes any already-buffered messages through
    /// the callback before closing.
    func drain()

    /// Completes once delivery is fully stopped or drained and no more messages will be processed.
    func waitUntilClosed() async
}

/// A concrete `AsyncIteratorProtocol` iterator produced by every ``MessagesContext``.
///
/// Constraining ``MessagesContext``'s iterator to this single concrete type keeps
/// `any MessagesContext` usable directly in a `for try await` loop.
public struct JetStreamMessageIterator: AsyncIteratorProtocol, Sendable {
    private let pull: @Sendable () async throws -> JetStreamMessage?

    internal init(pull: @escaping @Sendable () async throws -> JetStreamMessage?) {
        self.pull = pull
    }

    public mutating func next() async throws -> JetStreamMessage? {
        try await pull()
    }
}

/// An `AsyncSequence` view over a ``MessageConsuming/messages()`` delivery.
public protocol MessagesContext: AsyncSequence, Sendable
where Element == JetStreamMessage, AsyncIterator == JetStreamMessageIterator {
    /// Stops iteration immediately: unsubscribes and discards any buffered messages.
    func stop()

    /// Stops iteration gracefully: unsubscribes, but keeps already-buffered messages available on
    /// subsequent iterations before the sequence ends.
    func drain()
}
