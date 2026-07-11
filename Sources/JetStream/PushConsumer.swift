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

/// A server-created (or bound) JetStream push consumer.
///
/// A push consumer is server-driven: the server delivers messages to a deliver subject, which this
/// type subscribes to. Consume it continuously with ``consume(_:onError:)``, iterate it with
/// ``messages()``, or pull one message at a time with ``next(timeout:)``. Unlike a pull consumer,
/// a push consumer has no `fetch` — delivery is entirely server-driven.
///
/// Create one with ``JetStreamContext/createPushConsumer(stream:cfg:)`` or bind an existing one with
/// ``JetStreamContext/pushConsumer(stream:name:)``.
///
/// > All three consumption methods share the SAME deliver-subject subscription; consume a push
/// > consumer through one of them at a time.
///
/// Durable push consumers and queue/deliver groups are supported: create with a
/// ``ConsumerConfig/durable`` name to persist across restarts, and with a
/// ``ConsumerConfig/deliverGroup`` to load-balance delivery across instances (see
/// ``JetStreamContext/createPushConsumer(stream:cfg:)``). `StopAfter` is not implemented in v1.
public final class PushConsumer: MessageConsuming, @unchecked Sendable {
    private let ctx: JetStreamContext
    private let deliverSubject: String
    private let subscription: NatsSubscription
    private let idleHeartbeatSeconds: TimeInterval
    private let ownsConsumer: Bool

    private let stateLock = NSLock()
    private var cached: ConsumerInfo
    private var sharedStream: MessageStream?

    internal init(
        ctx: JetStreamContext,
        info: ConsumerInfo,
        deliverSubject: String,
        subscription: NatsSubscription,
        idleHeartbeatSeconds: TimeInterval,
        ownsConsumer: Bool
    ) {
        self.ctx = ctx
        self.cached = info
        self.deliverSubject = deliverSubject
        self.subscription = subscription
        self.idleHeartbeatSeconds = idleHeartbeatSeconds
        self.ownsConsumer = ownsConsumer
    }

    /// The ``ConsumerInfo`` cached on this consumer. Refreshed by ``info()``.
    public var cachedInfo: ConsumerInfo {
        stateLock.lock()
        defer { stateLock.unlock() }
        return cached
    }

    /// Fetches current ``ConsumerInfo`` from the server, refreshing ``cachedInfo``.
    public func info() async throws -> ConsumerInfo {
        let current = cachedInfo
        let subj = "CONSUMER.INFO.\(current.stream).\(current.name)"
        let resp: Response<ConsumerInfo> = try await ctx.request(subj)
        switch resp {
        case .success(let info):
            stateLock.withLockScoped { cached = info }
            return info
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    /// Continuously receives server-pushed messages, invoking `handler` for each.
    @discardableResult
    public func consume(
        _ handler: @escaping MessageHandler, onError: ConsumeErrorHandler? = nil
    ) throws -> ConsumeContext {
        JetStreamConsumeContext(stream: stream(), handler: handler, onError: onError)
    }

    /// Iterates server-pushed messages as an `AsyncSequence`.
    public func messages() throws -> any MessagesContext {
        JetStreamMessagesContext(stream: stream())
    }

    /// Retrieves the next server-pushed message, waiting at most `timeout` seconds.
    public func next(timeout: TimeInterval = 30) async throws -> JetStreamMessage? {
        try await stream().next(timeout: timeout)
    }

    deinit {
        // Best-effort backstop, mirroring ``OrderedConsumer``'s `deinit`. `createPushConsumer`
        // subscribes to the deliver subject and creates the (possibly ephemeral) server consumer
        // eagerly, before returning this handle — so a caller that only inspects `info()`, or that
        // discards the handle after an error, would otherwise leak the subscription and the ephemeral
        // consumer. If any consumption method already ran, the shared delivery stream exists and its
        // own teardown (via `stop()`/`drain()` or ``MessageStream``'s `deinit`) owns unsubscribing and
        // deleting the consumer, so we skip here to avoid a double teardown. A bound consumer
        // (`ownsConsumer == false`) is only unsubscribed, never deleted — it was not created here.
        stateLock.lock()
        let neverConsumed = sharedStream == nil
        let owns = ownsConsumer
        let streamName = cached.stream
        let consumerName = cached.name
        stateLock.unlock()
        guard neverConsumed else {
            return
        }
        let sub = subscription
        let ctx = self.ctx
        Task {
            try? await sub.unsubscribe()
            if owns {
                try? await ctx.deleteConsumer(stream: streamName, name: consumerName)
            }
        }
    }

    /// The single, lazily-created delivery stream shared by all consumption methods.
    private func stream() -> MessageStream {
        stateLock.lock()
        defer { stateLock.unlock() }
        if let sharedStream {
            return sharedStream
        }
        let source = PushMessageSource(
            ctx: ctx,
            subscription: subscription,
            idleHeartbeatSeconds: idleHeartbeatSeconds,
            streamName: cached.stream,
            consumerName: cached.name,
            ownsConsumer: ownsConsumer)
        let stream = MessageStream(source: source)
        sharedStream = stream
        return stream
    }
}

/// A ``MessageSource`` over a push consumer's deliver-subject subscription, driven by
/// ``PushDelivery`` for flow-control and heartbeat handling.
private final class PushMessageSource: MessageSource, @unchecked Sendable {
    private let ctx: JetStreamContext
    private let subscription: NatsSubscription
    private let delivery: PushDelivery
    private let streamName: String
    private let consumerName: String
    private let ownsConsumer: Bool

    private let stateLock = NSLock()
    private var closed = false

    init(
        ctx: JetStreamContext,
        subscription: NatsSubscription,
        idleHeartbeatSeconds: TimeInterval,
        streamName: String,
        consumerName: String,
        ownsConsumer: Bool
    ) {
        self.ctx = ctx
        self.subscription = subscription
        self.delivery = PushDelivery(
            client: ctx.client, subscription: subscription,
            idleHeartbeatSeconds: idleHeartbeatSeconds)
        self.streamName = streamName
        self.consumerName = consumerName
        self.ownsConsumer = ownsConsumer
    }

    func next() async throws -> JetStreamMessage? {
        while true {
            if isClosed() {
                return nil
            }
            let event = try await delivery.next()
            switch event {
            case .message(let msg):
                return JetStreamMessage(message: msg, client: ctx.client)
            case .idleHeartbeat, .missedHeartbeat:
                continue
            case .consumerDeleted:
                throw JetStreamError.FetchError.consumerDeleted
            case .leadershipChanged:
                throw JetStreamError.FetchError.leadershipChanged
            case .closed:
                return nil
            }
        }
    }

    func teardown() async {
        let alreadyClosed = stateLock.withLockScoped { () -> Bool in
            if closed {
                return true
            }
            closed = true
            return false
        }
        if alreadyClosed {
            return
        }

        try? await subscription.unsubscribe()
        if ownsConsumer {
            try? await ctx.deleteConsumer(stream: streamName, name: consumerName)
        }
    }

    private func isClosed() -> Bool {
        stateLock.lock()
        defer { stateLock.unlock() }
        return closed
    }
}
