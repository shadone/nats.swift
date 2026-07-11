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

/// A handle to a KeyValue bucket, bound to a backing JetStream stream.
///
/// Obtain a `KeyValue` from a ``JetStreamContext`` via
/// ``JetStreamContext/keyValue(bucket:)``,
/// ``JetStreamContext/createKeyValue(cfg:)`` or
/// ``JetStreamContext/createOrUpdateKeyValue(cfg:)``.
///
/// This foundation exposes value access (`get`), writes (`put`, `create`,
/// `update`), tombstones (`delete`, `purge`) and `status`. Consumer-based
/// operations (`watch`, `watchAll`, `keys`, `history`, `purgeDeletes`) are
/// provided separately.
public final class KeyValue {

    /// The name of the bucket.
    public let bucket: String

    internal let ctx: JetStreamContext
    internal let stream: Stream

    /// Whether the backing stream allows `DIRECT.GET`, cached from the stream
    /// config at bind time (this value is immutable after stream creation).
    private let allowDirect: Bool

    internal init(ctx: JetStreamContext, bucket: String, stream: Stream) {
        self.ctx = ctx
        self.bucket = bucket
        self.stream = stream
        self.allowDirect = stream.info.config.allowDirect
    }

    // MARK: - Reads

    /// Returns the latest live entry for the key, or nil when the key is absent
    /// or its latest entry is a `delete`/`purge` tombstone.
    ///
    /// - Parameter key: the key to fetch.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func get(_ key: String) async throws -> KeyValueEntry? {
        try KeyValueCoding.validateKey(key)
        guard let entry = try await latestEntry(forKey: key), entry.operation == .put else {
            return nil
        }
        return entry
    }

    /// Returns the entry for the key at a specific revision, or nil when no such
    /// message exists, the message belongs to a different key, or the entry is a
    /// `delete`/`purge` tombstone.
    ///
    /// - Parameters:
    ///   - key: the key the revision is expected to belong to.
    ///   - revision: the backing stream sequence of the entry.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func get(_ key: String, revision: UInt64) async throws -> KeyValueEntry? {
        try KeyValueCoding.validateKey(key)
        let message: StreamMessage?
        if allowDirect {
            message = try await stream.getMessageDirect(sequence: revision)
        } else {
            message = try await stream.getMessage(sequence: revision)
        }
        guard let message else { return nil }
        // The revision must belong to the requested key.
        guard message.subject == KeyValueCoding.subject(forBucket: bucket, key: key) else {
            return nil
        }
        let entry = KeyValueCoding.entry(from: message, bucket: bucket, key: key)
        return entry.operation == .put ? entry : nil
    }

    // MARK: - Writes

    /// Puts a value for the key, creating it or overwriting the current value.
    ///
    /// - Parameters:
    ///   - key: the key to write.
    ///   - value: the value bytes.
    ///
    /// - Returns: the revision (backing stream sequence) of the written entry.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/PublishError`` if the publish is not acknowledged.
    public func put(_ key: String, _ value: Data) async throws -> UInt64 {
        try KeyValueCoding.validateKey(key)
        return try await publish(key: key, value: value, headers: nil)
    }

    /// Creates a value for the key only if the key does not currently hold a
    /// live value. A key whose latest entry is a `delete`/`purge` tombstone can
    /// be re-created.
    ///
    /// - Parameters:
    ///   - key: the key to create.
    ///   - value: the value bytes.
    ///
    /// - Returns: the revision of the created entry.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/KeyValueError/keyExists(_:)`` if the key already holds
    /// >   a live value.
    public func create(_ key: String, _ value: Data) async throws -> UInt64 {
        try KeyValueCoding.validateKey(key)
        do {
            return try await updateUnchecked(key: key, value: value, revision: 0)
        } catch JetStreamError.KeyValueError.wrongLastRevision {
            // The key exists. If its latest entry is a tombstone we may re-create
            // over it using that revision as the expected last sequence.
            if let last = try await latestEntry(forKey: key), last.operation != .put {
                return try await updateUnchecked(key: key, value: value, revision: last.revision)
            }
            throw JetStreamError.KeyValueError.keyExists(key)
        }
    }

    /// Updates the value for the key only if its current revision matches.
    ///
    /// - Parameters:
    ///   - key: the key to update.
    ///   - value: the new value bytes.
    ///   - revision: the revision the key is expected to currently hold.
    ///
    /// - Returns: the revision of the updated entry.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/KeyValueError/wrongLastRevision`` if the key's current
    /// >   revision does not match `revision`.
    public func update(_ key: String, _ value: Data, revision: UInt64) async throws -> UInt64 {
        try KeyValueCoding.validateKey(key)
        return try await updateUnchecked(key: key, value: value, revision: revision)
    }

    /// Places a `delete` tombstone on the key, marking it as deleted while
    /// retaining history.
    ///
    /// - Parameters:
    ///   - key: the key to delete.
    ///   - lastRevision: when set, the delete only succeeds if the key's current
    ///     revision matches.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/KeyValueError/wrongLastRevision`` if `lastRevision` is
    /// >   set and does not match the key's current revision.
    public func delete(_ key: String, lastRevision: UInt64? = nil) async throws {
        try KeyValueCoding.validateKey(key)
        var headers = NatsHeaderMap()
        headers[.kvOperation] = NatsHeaderValue(KeyValueOperation.delete.rawValue)
        if let lastRevision {
            headers[.expectedLastSubjectSequence] =
                KeyValueCoding.expectedLastSubjectSequenceValue(lastRevision)
        }
        _ = try await publishTombstone(key: key, headers: headers)
    }

    /// Purges the key: places a `purge` tombstone and rolls up the subject,
    /// removing all prior history for the key.
    ///
    /// - Parameters:
    ///   - key: the key to purge.
    ///   - lastRevision: when set, the purge only succeeds if the key's current
    ///     revision matches.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/KeyValueError/wrongLastRevision`` if `lastRevision` is
    /// >   set and does not match the key's current revision.
    public func purge(_ key: String, lastRevision: UInt64? = nil) async throws {
        try KeyValueCoding.validateKey(key)
        var headers = NatsHeaderMap()
        headers[.kvOperation] = NatsHeaderValue(KeyValueOperation.purge.rawValue)
        headers[.natsRollup] = NatsHeaderValue("sub")
        if let lastRevision {
            headers[.expectedLastSubjectSequence] =
                KeyValueCoding.expectedLastSubjectSequenceValue(lastRevision)
        }
        _ = try await publishTombstone(key: key, headers: headers)
    }

    // MARK: - Status

    /// Retrieves the current status of the bucket, refreshing the backing
    /// stream info.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func status() async throws -> KeyValueStatus {
        let info = try await stream.info()
        return KeyValueStatus(bucket: bucket, streamInfo: info)
    }

    // MARK: - Internals

    /// Fetches the latest entry for the key without collapsing tombstones, or
    /// nil when the key has never been written.
    private func latestEntry(forKey key: String) async throws -> KeyValueEntry? {
        let subject = KeyValueCoding.subject(forBucket: bucket, key: key)
        let message: StreamMessage?
        if allowDirect {
            message = try await stream.getMessageDirect(lastForSubject: subject)
        } else {
            message = try await stream.getMessage(lastForSubject: subject)
        }
        guard let message else { return nil }
        return KeyValueCoding.entry(from: message, bucket: bucket, key: key)
    }

    /// Publishes a value with an expected-last-subject-sequence guard, mapping a
    /// compare-and-set failure to ``JetStreamError/KeyValueError/wrongLastRevision``.
    private func updateUnchecked(key: String, value: Data, revision: UInt64) async throws -> UInt64
    {
        var headers = NatsHeaderMap()
        headers[.expectedLastSubjectSequence] =
            KeyValueCoding.expectedLastSubjectSequenceValue(revision)
        do {
            return try await publish(key: key, value: value, headers: headers)
        } catch JetStreamError.PublishError.streamWrongLastSequence {
            throw JetStreamError.KeyValueError.wrongLastRevision
        }
    }

    private func publish(key: String, value: Data, headers: NatsHeaderMap?) async throws -> UInt64 {
        let subject = KeyValueCoding.subject(forBucket: bucket, key: key)
        let ack = try await ctx.publish(subject, message: value, headers: headers).wait()
        return ack.seq
    }

    /// Publishes a headers-only tombstone message, mapping a compare-and-set
    /// failure to ``JetStreamError/KeyValueError/wrongLastRevision``.
    private func publishTombstone(key: String, headers: NatsHeaderMap) async throws -> UInt64 {
        do {
            return try await publish(key: key, value: Data(), headers: headers)
        } catch JetStreamError.PublishError.streamWrongLastSequence {
            throw JetStreamError.KeyValueError.wrongLastRevision
        }
    }
}
