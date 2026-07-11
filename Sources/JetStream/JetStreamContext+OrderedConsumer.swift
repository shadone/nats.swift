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

    /// Creates an ordered consumer over a stream (nats.go `js.OrderedConsumer`).
    ///
    /// The consumer is an ephemeral, memory-backed push consumer that transparently recreates itself
    /// on any delivery interruption and resumes from exactly where it left off — no acks, no
    /// redeliveries, no gaps, no duplicates. The library owns the push wire fields; only
    /// ``OrderedConsumerConfig`` is caller-supplied.
    ///
    /// The first underlying consumer is created before this method returns, so a missing stream, an
    /// invalid filter, or a permission error is thrown here rather than surfacing later during
    /// consumption.
    ///
    /// - Parameters:
    ///   - stream: name of the stream to consume from.
    ///   - cfg: the ordered-consumer configuration.
    /// - Returns: a started ``OrderedConsumer`` ready to consume.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError`` / ``JetStreamError/RequestError`` if the initial consumer
    /// >   could not be created.
    public func orderedConsumer(
        stream: String, cfg: OrderedConsumerConfig
    ) async throws -> OrderedConsumer {
        try Stream.validate(name: stream)
        let consumer = OrderedConsumer(ctx: self, streamName: stream, config: cfg)
        try await consumer.start()
        return consumer
    }
}
