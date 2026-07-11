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

extension JetStreamContext {

    /// Creates a server-side push consumer and returns a ``PushConsumer`` bound to its deliver
    /// subject.
    ///
    /// If `cfg` does not set a ``ConsumerConfig/deliverSubject``, a fresh inbox is generated and
    /// used (making the consumer push-based). The deliver-subject subscription is established BEFORE
    /// the consumer is created, so no delivery is missed. An ephemeral consumer (no
    /// ``ConsumerConfig/durable``) is deleted when the returned consumer is stopped/drained.
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

        let sub = try await client.subscribe(subject: deliver)
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

        let sub = try await client.subscribe(subject: deliver)
        let heartbeat = info.config.idleHeartbeat?.value ?? 0
        return PushConsumer(
            ctx: self,
            info: info,
            deliverSubject: deliver,
            subscription: sub,
            idleHeartbeatSeconds: heartbeat,
            ownsConsumer: false)
    }
}
