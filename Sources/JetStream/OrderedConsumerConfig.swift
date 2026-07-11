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

/// Configuration for an ``OrderedConsumer``.
///
/// This is the ONLY configuration a caller supplies. The push wire fields an ordered consumer
/// depends on — deliver subject, flow control, idle heartbeat, `ackPolicy = .none` and memory
/// storage — are set by the library and are deliberately NOT exposed here: the transparent-reset
/// algorithm relies on them, so letting a caller change them would break the no-loss / no-dup
/// guarantee.
public struct OrderedConsumerConfig: Sendable {
    /// Filter messages by these subjects. Empty/`nil` matches everything the stream carries.
    /// A single entry maps to a single-subject filter; multiple entries require nats-server 2.10+.
    public var filterSubjects: [String]?

    /// From which point to start delivering messages. Defaults to ``DeliverPolicy/all``.
    public var deliverPolicy: DeliverPolicy

    /// Start sequence, applied only when ``deliverPolicy`` is ``DeliverPolicy/byStartSequence``.
    public var optStartSeq: UInt64?

    /// Start time (RFC3339), applied only when ``deliverPolicy`` is ``DeliverPolicy/byStartTime``.
    public var optStartTime: String?

    /// The rate at which messages are replayed. Defaults to ``ReplayPolicy/instant``.
    public var replayPolicy: ReplayPolicy

    /// How long the server keeps the underlying consumer alive while inactive. Defaults to 5m.
    public var inactiveThreshold: NanoTimeInterval?

    /// Deliver only message headers (no payload). Defaults to `false`.
    public var headersOnly: Bool

    /// Maximum attempts to recreate the consumer in a single recovery cycle. `-1` (the default)
    /// means unlimited.
    public var maxResetAttempts: Int

    /// Application-defined metadata for the underlying consumer. Requires nats-server 2.10+.
    public var metadata: [String: String]?

    /// Optional custom prefix for generated consumer names (`{prefix}_{serial}`). A unique id is
    /// used when `nil`.
    public var namePrefix: String?

    /// Creates an ordered-consumer configuration.
    public init(
        filterSubjects: [String]? = nil,
        deliverPolicy: DeliverPolicy = .all,
        optStartSeq: UInt64? = nil,
        optStartTime: String? = nil,
        replayPolicy: ReplayPolicy = .instant,
        inactiveThreshold: NanoTimeInterval? = nil,
        headersOnly: Bool = false,
        maxResetAttempts: Int = -1,
        metadata: [String: String]? = nil,
        namePrefix: String? = nil
    ) {
        self.filterSubjects = filterSubjects
        self.deliverPolicy = deliverPolicy
        self.optStartSeq = optStartSeq
        self.optStartTime = optStartTime
        self.replayPolicy = replayPolicy
        self.inactiveThreshold = inactiveThreshold
        self.headersOnly = headersOnly
        self.maxResetAttempts = maxResetAttempts
        self.metadata = metadata
        self.namePrefix = namePrefix
    }
}
