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

/// Pure mapping and parsing helpers for the KeyValue wire conventions, factored
/// out of ``KeyValue`` so they are unit-testable without a running server.
///
/// The wire conventions are the interop contract shared with every other NATS
/// KeyValue client: a bucket `<name>` is backed by stream `KV_<name>`, a key
/// `<key>` maps to subject `$KV.<name>.<key>`, and the entry operation is carried
/// in the `KV-Operation` header (`PUT` / `DEL` / `PURGE`; absent means `PUT`).
enum KeyValueCoding {

    /// The server-enforced maximum number of historical values kept per key.
    static let maxHistory: UInt8 = 64

    // MARK: - Names and subjects

    static func streamName(forBucket bucket: String) -> String {
        "KV_\(bucket)"
    }

    static func subjectPrefix(forBucket bucket: String) -> String {
        "$KV.\(bucket)."
    }

    static func subject(forBucket bucket: String, key: String) -> String {
        "\(subjectPrefix(forBucket: bucket))\(key)"
    }

    static func allKeysFilterSubject(forBucket bucket: String) -> String {
        "\(subjectPrefix(forBucket: bucket))>"
    }

    /// Extracts the key from a fully qualified `$KV.<bucket>.<key>` subject,
    /// or nil if the subject does not belong to the bucket.
    static func key(fromSubject subject: String, bucket: String) -> String? {
        let prefix = subjectPrefix(forBucket: bucket)
        guard subject.hasPrefix(prefix) else { return nil }
        let key = String(subject.dropFirst(prefix.count))
        return key.isEmpty ? nil : key
    }

    // MARK: - Validation

    /// Whether a bucket name is valid (`^[a-zA-Z0-9_-]+$`).
    static func isValidBucketName(_ bucket: String) -> Bool {
        guard !bucket.isEmpty else { return false }
        return bucket.range(of: "^[a-zA-Z0-9_-]+$", options: .regularExpression) != nil
    }

    static func validateBucketName(_ bucket: String) throws {
        guard isValidBucketName(bucket) else {
            throw JetStreamError.KeyValueError.invalidBucketName(bucket)
        }
    }

    /// Whether a key is valid: non-empty, no leading or trailing `.`, and made
    /// up only of `-`, `/`, `_`, `=`, `.` and alphanumerics.
    static func isValidKey(_ key: String) -> Bool {
        guard !key.isEmpty else { return false }
        if key.hasPrefix(".") || key.hasSuffix(".") { return false }
        return key.range(of: "^[-/_=.a-zA-Z0-9]+$", options: .regularExpression) != nil
    }

    static func validateKey(_ key: String) throws {
        guard isValidKey(key) else {
            throw JetStreamError.KeyValueError.invalidKey(key)
        }
    }

    // MARK: - Config mapping

    /// Maps a ``KeyValueConfig`` to the backing ``StreamConfig``, matching the
    /// layout produced by nats.go / async-nats / the `nats` CLI.
    ///
    /// Key invariants (getting any of these wrong silently breaks the bucket):
    /// - `allowRollup = true` — required for `purge` (`Nats-Rollup: sub`).
    /// - `allowDirect = true` — enables read-after-write `DIRECT.GET`.
    /// - `denyDelete = true` — KV never deletes individual stream messages.
    /// - `maxMsgsPerSubject = history` — bounds retained revisions per key.
    /// - `discard = new` — reject writes once limits are reached.
    static func streamConfig(from cfg: KeyValueConfig) throws -> StreamConfig {
        try validateBucketName(cfg.bucket)

        if cfg.history > maxHistory {
            throw JetStreamError.KeyValueError.historyTooLarge
        }
        let history: Int64 = cfg.history > 0 ? Int64(cfg.history) : 1
        let replicas = cfg.replicas > 0 ? cfg.replicas : 1
        let maxBytes: Int64 = cfg.maxBytes == 0 ? -1 : cfg.maxBytes
        let maxValueSize: Int32 = cfg.maxValueSize == 0 ? -1 : cfg.maxValueSize
        let compression: StoreCompression = cfg.compression ? .s2 : .none

        // The server defaults the duplicate window to 2 minutes; it must not
        // exceed max age, so cap it to the TTL when the TTL is shorter.
        let twoMinutes = 2.0 * 60.0
        var duplicateWindow = twoMinutes
        if let ttl = cfg.ttl, ttl.value > 0, ttl.value < twoMinutes {
            duplicateWindow = ttl.value
        }

        // Subjects are only set when the bucket owns its data. Mirror and source
        // buckets derive their subjects from the upstream stream(s).
        let ownsData = cfg.mirror == nil && (cfg.sources?.isEmpty ?? true)
        let subjects: [String]? = ownsData ? [allKeysFilterSubject(forBucket: cfg.bucket)] : nil

        return StreamConfig(
            name: streamName(forBucket: cfg.bucket),
            description: cfg.description,
            subjects: subjects,
            maxConsumers: -1,
            maxBytes: maxBytes,
            discard: .new,
            maxAge: cfg.ttl ?? NanoTimeInterval(0),
            maxMsgsPerSubject: history,
            maxMsgSize: maxValueSize,
            storage: cfg.storage,
            replicas: replicas,
            duplicates: NanoTimeInterval(duplicateWindow),
            placement: cfg.placement,
            mirror: cfg.mirror,
            sources: cfg.sources,
            denyDelete: true,
            allowRollup: true,
            compression: compression,
            rePublish: cfg.republish,
            allowDirect: true,
            mirrorDirect: cfg.mirror != nil,
            metadata: cfg.metadata)
    }

    // MARK: - Entry decode

    /// The operation carried in the `KV-Operation` header. Absent or unknown
    /// values decode to ``KeyValueOperation/put``.
    static func operation(from headers: NatsHeaderMap?) -> KeyValueOperation {
        guard let value = headers?.get(.kvOperation) else { return .put }
        return KeyValueOperation(rawValue: value.description) ?? .put
    }

    /// Decodes a fetched ``StreamMessage`` into a ``KeyValueEntry``.
    ///
    /// The entry carries its true operation; collapsing tombstones to nil is the
    /// caller's responsibility (see ``KeyValue/get(_:)``).
    static func entry(
        from message: StreamMessage, bucket: String, key: String
    ) -> KeyValueEntry {
        let entryKey = self.key(fromSubject: message.subject, bucket: bucket) ?? key
        return KeyValueEntry(
            bucket: bucket,
            key: entryKey,
            value: message.payload,
            revision: message.sequence,
            created: message.time,
            delta: 0,
            operation: operation(from: message.headers))
    }

    /// Decodes a live push-consumer delivery into a ``KeyValueEntry``.
    ///
    /// Unlike the point-read decode above, the revision, timestamp and `delta`
    /// come from the message's JetStream ``MessageMetadata`` (parsed from the
    /// `$JS.ACK` reply subject): the metadata's `pending` count is carried into
    /// `delta`, which the watcher uses to detect the end of initial values.
    static func entry(
        from message: NatsMessage, metadata: MessageMetadata, bucket: String
    ) -> KeyValueEntry {
        let entryKey = key(fromSubject: message.subject, bucket: bucket) ?? message.subject
        return KeyValueEntry(
            bucket: bucket,
            key: entryKey,
            value: message.payload ?? Data(),
            revision: metadata.streamSequence,
            created: rfc3339(fromAckTimestamp: metadata.timestamp),
            delta: metadata.pending,
            operation: operation(from: message.headers))
    }

    // MARK: - Timestamps

    /// A shared RFC3339 formatter with fractional seconds. Immutable after
    /// configuration, so it is safe to read concurrently.
    ///
    /// `nonisolated(unsafe)` because `ISO8601DateFormatter` is not `Sendable`, yet this instance is
    /// only mutated inside the initializer closure and thereafter used read-only. Foundation's
    /// date formatters are documented to be thread-safe for formatting/parsing once configured, so
    /// the concurrent reads here (`string(from:)` / `date(from:)`) are race-free.
    nonisolated(unsafe) private static let rfc3339Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    /// Formats a JetStream `$JS.ACK` timestamp token (Unix nanoseconds, as a
    /// decimal string) as an RFC3339 string, matching the `created` format used
    /// by point reads. Returns the raw token if it is not an integer.
    static func rfc3339(fromAckTimestamp token: String) -> String {
        guard let nanos = UInt64(token) else { return token }
        let date = Date(timeIntervalSince1970: Double(nanos) / 1_000_000_000)
        return rfc3339Formatter.string(from: date)
    }

    /// Parses an RFC3339 `created` timestamp back into a `Date`, or nil when the
    /// string is not RFC3339.
    static func date(fromRFC3339 string: String) -> Date? {
        rfc3339Formatter.date(from: string)
    }

    // MARK: - Publish headers

    /// Builds the `Nats-Expected-Last-Subject-Sequence` header value used to
    /// gate a compare-and-set write on the key's current revision.
    static func expectedLastSubjectSequenceValue(_ revision: UInt64) -> NatsHeaderValue {
        NatsHeaderValue(String(revision))
    }
}
