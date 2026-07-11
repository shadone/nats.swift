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

extension JetStreamContext {

    /// Creates a server-side push consumer and returns a ``PushConsumer`` bound to its deliver
    /// subject.
    ///
    /// If `cfg` does not set a ``ConsumerConfig/deliverSubject``, a fresh inbox is generated and
    /// used (making the consumer push-based). The deliver-subject subscription is established BEFORE
    /// the consumer is created, so no delivery is missed.
    ///
    /// ## Durable vs ephemeral
    /// Setting ``ConsumerConfig/durable`` creates a DURABLE consumer that persists server-side across
    /// client restarts; it is NOT deleted when the returned handle is stopped/drained (rebind it
    /// later with ``pushConsumer(stream:name:)`` to resume). An ephemeral consumer (no `durable`) is
    /// deleted when the returned consumer is stopped/drained. This mirrors nats.go, where a consumer
    /// is durable when `Config.Durable != ""`.
    ///
    /// ## Queue / deliver group
    /// Setting ``ConsumerConfig/deliverGroup`` makes this a queue push consumer: the deliver-subject
    /// subscription joins that queue group, so multiple instances binding the same consumer and
    /// deliver group LOAD-BALANCE delivery (each message goes to exactly one member). This mirrors
    /// nats.go's `jetstream/push.go`, which calls `QueueSubscribe(DeliverSubject, DeliverGroup, …)`
    /// when `Config.DeliverGroup != ""` and `Subscribe(…)` otherwise. Pair a deliver group with a
    /// `durable` name so the shared consumer survives across instances and restarts.
    ///
    /// - Parameters:
    ///   - stream: name of the stream the consumer is created on.
    ///   - cfg: consumer configuration. `deliverSubject` is filled in if absent.
    /// - Returns: a ``PushConsumer`` ready to consume.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError``: if the consumer could not be created.
    /// > - ``JetStreamError/RequestError``: if the request fails (e.g. JetStream not enabled).
    public func createPushConsumer(stream: String, cfg: ConsumerConfig) async throws -> PushConsumer
    {
        try Stream.validate(name: stream)

        var config = cfg
        let deliver = config.deliverSubject ?? client.newInbox()
        config.deliverSubject = deliver

        let sub = try await subscribePush(subject: deliver, deliverGroup: config.deliverGroup)
        do {
            let consumer = try await upsertConsumer(stream: stream, cfg: config)
            let heartbeat = config.idleHeartbeat?.value ?? 0
            return PushConsumer(
                ctx: self,
                info: consumer.info,
                deliverSubject: deliver,
                subscription: sub,
                idleHeartbeatSeconds: heartbeat,
                ownsConsumer: config.durable == nil)
        } catch {
            try? await sub.unsubscribe()
            throw error
        }
    }

    /// Binds an existing push consumer by name and returns a ``PushConsumer`` for it.
    ///
    /// Reads the consumer's ``ConsumerInfo`` and subscribes to its deliver subject. If the consumer
    /// has a ``ConsumerConfig/deliverGroup`` (a queue push consumer), the subscription joins that
    /// queue group so this instance load-balances delivery with any other instance bound to the same
    /// consumer. A bound consumer is never deleted on teardown (only unsubscribed) — binding does not
    /// own it — so binding a durable is the way to resume consuming it after a restart.
    ///
    /// - Parameters:
    ///   - stream: name of the stream the consumer lives on.
    ///   - name: the consumer name.
    /// - Returns: a ``PushConsumer`` bound to the existing consumer's deliver subject.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/PushConsumerError/consumerNotFound(_:)``: if the consumer does not exist.
    /// > - ``JetStreamError/PushConsumerError/notPushConsumer(_:)``: if it has no deliver subject.
    public func pushConsumer(stream: String, name: String) async throws -> PushConsumer {
        guard let consumer = try await getConsumer(stream: stream, name: name) else {
            throw JetStreamError.PushConsumerError.consumerNotFound(name)
        }
        let info = consumer.info
        guard let deliver = info.config.deliverSubject else {
            throw JetStreamError.PushConsumerError.notPushConsumer(name)
        }

        let sub = try await subscribePush(subject: deliver, deliverGroup: info.config.deliverGroup)
        let heartbeat = info.config.idleHeartbeat?.value ?? 0
        return PushConsumer(
            ctx: self,
            info: info,
            deliverSubject: deliver,
            subscription: sub,
            idleHeartbeatSeconds: heartbeat,
            ownsConsumer: false)
    }

    /// Subscribes to a push consumer's deliver subject, joining `deliverGroup` as a queue group when
    /// one is set. Mirrors nats.go's `Subscribe` vs `QueueSubscribe` branch in `jetstream/push.go`:
    /// an empty or absent group subscribes plainly; a non-empty group load-balances delivery across
    /// all subscribers sharing that group on the deliver subject.
    private func subscribePush(
        subject: String, deliverGroup: String?
    ) async throws
        -> NatsSubscription
    {
        let queue = deliverGroup.flatMap { $0.isEmpty ? nil : $0 }
        return try await client.subscribe(subject: subject, queue: queue)
    }
}

extension ConsumerConfig {

    /// Builds a configuration for a durable, load-balanced push consumer.
    ///
    /// A durable consumer (persists server-side across client restarts) with a queue/deliver group,
    /// so multiple client instances that create or bind this consumer with the same `durable` name
    /// and `deliverGroup` share delivery — each message goes to exactly one instance. Defaults to
    /// explicit acknowledgement, which a load-balanced work queue normally wants.
    ///
    /// - Parameters:
    ///   - durable: the durable name; also identifies the consumer for rebinding.
    ///   - deliverGroup: the queue/deliver group members load-balance over.
    ///   - deliverSubject: the deliver subject; a fresh inbox is generated when `nil`.
    ///   - deliverPolicy: where in the stream to start delivering (defaults to ``DeliverPolicy/all``).
    ///   - ackPolicy: the acknowledgement policy (defaults to ``AckPolicy/explicit``).
    ///   - idleHeartbeat: optional idle-heartbeat interval.
    ///   - flowControl: optional flow control (requires `idleHeartbeat`).
    public static func durablePushQueueGroup(
        durable: String,
        deliverGroup: String,
        deliverSubject: String? = nil,
        deliverPolicy: DeliverPolicy = .all,
        ackPolicy: AckPolicy = .explicit,
        idleHeartbeat: NanoTimeInterval? = nil,
        flowControl: Bool? = nil
    ) -> ConsumerConfig {
        ConsumerConfig(
            durable: durable,
            deliverPolicy: deliverPolicy,
            ackPolicy: ackPolicy,
            deliverSubject: deliverSubject,
            deliverGroup: deliverGroup,
            flowControl: flowControl,
            idleHeartbeat: idleHeartbeat)
    }
}
