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

/// Nonisolated, lock-guarded state shared between the ``OrderedConsumer`` actor and its nonisolated
/// ``MessageConsuming`` methods: the latest ``ConsumerInfo`` and the single delivery
/// ``MessageStream`` every consumption method drives.
final class OrderedSharedState: @unchecked Sendable {
    private let lock = NSLock()
    private var info: ConsumerInfo?
    private var stream: MessageStream?

    /// Records the ``ConsumerInfo`` captured at each successful (re)create.
    func setInfo(_ info: ConsumerInfo) {
        lock.lock()
        self.info = info
        lock.unlock()
    }

    /// The latest recorded ``ConsumerInfo``, or `nil` before the first create.
    func currentInfo() -> ConsumerInfo? {
        lock.lock()
        defer { lock.unlock() }
        return info
    }

    /// Returns the shared delivery stream, creating it once via `make` on first access.
    func sharedStream(_ make: () -> MessageStream) -> MessageStream {
        lock.lock()
        defer { lock.unlock() }
        if let stream {
            return stream
        }
        let created = make()
        stream = created
        return created
    }
}

/// Ordered-consumer ``MessageConsuming`` conformance.
///
/// All three consumption methods wrap the ordered engine's single, reset-transparent delivery
/// stream (``OrderedConsumer/natsMessages``). Because that stream is single-consumer, consume an
/// ordered consumer through one of them at a time; the underlying reset/resume behavior (no gap,
/// no duplicate across a mid-consume consumer deletion) is preserved unchanged.
///
/// > Pull-based ordered consumers are not implemented in v1; the ordered consumer is push-based.
extension OrderedConsumer: MessageConsuming {

    /// The ``ConsumerInfo`` captured at the last successful (re)create.
    ///
    /// - Precondition: the consumer has been started. The public
    ///   ``JetStreamContext/orderedConsumer(stream:cfg:)`` factory always calls `start()` (and awaits
    ///   the first successful create) before returning, so any ordered consumer obtained through the
    ///   public API satisfies this. The precondition can only fail when the actor is constructed
    ///   directly (an internal/test path) and `cachedInfo` is read before `start()` has completed —
    ///   a programmer error, not a runtime condition, hence the hard failure rather than a synthetic
    ///   empty value that would mask the misuse.
    public nonisolated var cachedInfo: ConsumerInfo {
        guard let info = shared.currentInfo() else {
            preconditionFailure(
                "OrderedConsumer.cachedInfo read before start() completed; obtain the consumer via "
                    + "JetStreamContext.orderedConsumer(stream:cfg:), which starts it first")
        }
        return info
    }

    /// Fetches current ``ConsumerInfo`` for the active underlying consumer, refreshing the cache.
    public func info() async throws -> ConsumerInfo {
        guard let name = currentName else {
            if let cached = shared.currentInfo() {
                return cached
            }
            throw JetStreamError.OrderedConsumerError.closed
        }
        let subj = "CONSUMER.INFO.\(streamName).\(name)"
        let resp: Response<ConsumerInfo> = try await ctx.request(subj)
        switch resp {
        case .success(let info):
            shared.setInfo(info)
            return info
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    /// Continuously receives ordered messages, invoking `handler` for each.
    @discardableResult
    public nonisolated func consume(
        _ handler: @escaping MessageHandler, onError: ConsumeErrorHandler? = nil
    ) throws -> ConsumeContext {
        JetStreamConsumeContext(stream: orderedStream(), handler: handler, onError: onError)
    }

    /// Iterates ordered messages as an `AsyncSequence`.
    public nonisolated func messages() throws -> any MessagesContext {
        JetStreamMessagesContext(stream: orderedStream())
    }

    /// Retrieves the next ordered message, waiting at most `timeout` seconds.
    public nonisolated func next(timeout: TimeInterval = 30) async throws -> JetStreamMessage? {
        try await orderedStream().next(timeout: timeout)
    }

    /// The single delivery stream over the ordered engine, created once and shared.
    private nonisolated func orderedStream() -> MessageStream {
        shared.sharedStream {
            MessageStream(source: OrderedMessageSource(consumer: self, client: ctx.client))
        }
    }
}

/// A ``MessageSource`` over the ordered engine's reset-transparent `natsMessages` stream.
private final class OrderedMessageSource: MessageSource, @unchecked Sendable {
    private let consumer: OrderedConsumer
    private let client: NatsClient
    private var iterator: AsyncThrowingStream<NatsMessage, Error>.Iterator

    init(consumer: OrderedConsumer, client: NatsClient) {
        self.consumer = consumer
        self.client = client
        self.iterator = consumer.natsMessages.makeAsyncIterator()
    }

    func next() async throws -> JetStreamMessage? {
        var it = iterator
        let message = try await it.next()
        iterator = it
        guard let message else {
            return nil
        }
        return JetStreamMessage(message: message, client: client)
    }

    func teardown() async {
        await consumer.stop()
    }
}
