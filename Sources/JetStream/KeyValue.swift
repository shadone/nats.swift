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
/// This exposes value access (`get`), writes (`put`, `create`, `update`),
/// tombstones (`delete`, `purge`), `status`, and the consumer-based operations
/// `watch`, `watchAll`, `keys`, `history` and `purgeDeletes`.
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
        try await createEntry(key: key, value: value, ttl: nil)
    }

    /// Creates a value for the key with a per-key time-to-live, only if the key
    /// does not currently hold a live value. The entry is automatically removed
    /// after `ttl` elapses.
    ///
    /// Per-key TTLs only take effect when the bucket was created with
    /// ``KeyValueConfig/limitMarkerTTL`` set; otherwise the TTL is silently
    /// ignored by the server. As in nats.go, a TTL can only be attached on
    /// create, not on `put` or `update`.
    ///
    /// - Parameters:
    ///   - key: the key to create.
    ///   - value: the value bytes.
    ///   - ttl: the lifetime of the entry before it is removed.
    ///
    /// - Returns: the revision of the created entry.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidKey(_:)`` if the key is invalid.
    /// > - ``JetStreamError/KeyValueError/keyExists(_:)`` if the key already holds
    /// >   a live value.
    public func create(_ key: String, _ value: Data, ttl: NanoTimeInterval) async throws -> UInt64 {
        try await createEntry(key: key, value: value, ttl: ttl)
    }

    private func createEntry(
        key: String, value: Data, ttl: NanoTimeInterval?
    ) async throws -> UInt64 {
        try KeyValueCoding.validateKey(key)
        do {
            return try await updateUnchecked(key: key, value: value, revision: 0, ttl: ttl)
        } catch JetStreamError.KeyValueError.wrongLastRevision {
            // The key exists. If its latest entry is a tombstone we may re-create
            // over it using that revision as the expected last sequence.
            if let last = try await latestEntry(forKey: key), last.operation != .put {
                return try await updateUnchecked(
                    key: key, value: value, revision: last.revision, ttl: ttl)
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

    // MARK: - Watch

    /// Watches keys matching a filter, which may contain the `*` and `>`
    /// wildcards. The returned ``KeyValueWatcher`` first delivers the current
    /// value of each matched key, then a `nil` marker, then live updates.
    ///
    /// - Parameters:
    ///   - keys: the key or wildcard filter to watch, appended to the bucket
    ///     subject prefix (`$KV.<bucket>.<keys>`). Not validated as a key, so
    ///     wildcards are permitted.
    ///   - opts: the watch options.
    ///
    /// - Returns: a ``KeyValueWatcher``. Call ``KeyValueWatcher/stop()`` when done.
    public func watch(
        _ keys: String, opts: KeyValueWatchOptions = .init()
    ) async throws -> KeyValueWatcher {
        let watcher = KeyValueWatcher(
            ctx: ctx,
            streamName: KeyValueCoding.streamName(forBucket: bucket),
            bucket: bucket,
            filterSubject: KeyValueCoding.subject(forBucket: bucket, key: keys),
            opts: opts)
        try await watcher.start()
        return watcher
    }

    /// Watches every key in the bucket. Equivalent to ``watch(_:opts:)`` with the
    /// `>` filter.
    ///
    /// - Parameter opts: the watch options.
    ///
    /// - Returns: a ``KeyValueWatcher``. Call ``KeyValueWatcher/stop()`` when done.
    public func watchAll(opts: KeyValueWatchOptions = .init()) async throws -> KeyValueWatcher {
        let watcher = KeyValueWatcher(
            ctx: ctx,
            streamName: KeyValueCoding.streamName(forBucket: bucket),
            bucket: bucket,
            filterSubject: KeyValueCoding.allKeysFilterSubject(forBucket: bucket),
            opts: opts)
        try await watcher.start()
        return watcher
    }

    /// Returns the sorted, de-duplicated list of live keys in the bucket,
    /// excluding keys whose latest entry is a `delete`/`purge` tombstone.
    ///
    /// - Returns: the live keys, sorted ascending.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/noKeysFound`` if the bucket holds no
    /// >   live keys.
    public func keys() async throws -> [String] {
        var opts = KeyValueWatchOptions()
        opts.ignoreDeletes = true
        opts.metaOnly = true
        let watcher = try await watchAll(opts: opts)
        let entries = try await collectInitialValues(from: watcher)

        var result: [String] = []
        for key in entries.map({ $0.key }).sorted() where result.last != key {
            result.append(key)
        }
        if result.isEmpty {
            throw JetStreamError.KeyValueError.noKeysFound
        }
        return result
    }

    /// Returns every historical entry for a key, oldest first, including
    /// `delete`/`purge` tombstones.
    ///
    /// - Parameter key: the key to fetch history for.
    ///
    /// - Returns: the historical entries in revision order.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/keyNotFound`` if the key has no history.
    public func history(_ key: String) async throws -> [KeyValueEntry] {
        var opts = KeyValueWatchOptions()
        opts.includeHistory = true
        let watcher = try await watch(key, opts: opts)
        let entries = try await collectInitialValues(from: watcher)
        if entries.isEmpty {
            throw JetStreamError.KeyValueError.keyNotFound
        }
        return entries
    }

    /// Removes `delete`/`purge` tombstone markers from the bucket.
    ///
    /// Collects the current state of every key first (stopping the watcher), then
    /// purges the subject of each key whose latest entry is a tombstone. Purging
    /// happens only after the watcher is stopped: a live watcher would keep
    /// bumping the consumer's `numPending` and prevent the end-of-initial marker.
    ///
    /// - Parameter olderThan: when set, only tombstones older than this age are
    ///   fully removed; younger tombstones keep their marker (their prior history
    ///   is still rolled up). An explicit `0` — like any negative value — removes
    ///   every delete/purge marker regardless of age. When `nil`, a default of 30
    ///   minutes is used.
    public func purgeDeletes(olderThan: TimeInterval? = nil) async throws {
        let watcher = try await watchAll()
        let entries = try await collectInitialValues(from: watcher)

        let threshold = olderThan ?? (30 * 60)
        let limit: Date? = threshold > 0 ? Date().addingTimeInterval(-threshold) : nil

        for entry in entries where entry.operation == .delete || entry.operation == .purge {
            let subject = KeyValueCoding.subject(forBucket: bucket, key: entry.key)
            if let limit, let created = KeyValueCoding.date(fromRFC3339: entry.created),
                created > limit
            {
                // Marker is younger than the limit: keep it, roll up its history.
                _ = try await stream.purge(keep: 1, subject: subject)
            } else {
                _ = try await stream.purge(subject: subject)
            }
        }
    }

    // MARK: - Internals

    /// Drains a watcher's initial values up to (and excluding) the `nil`
    /// end-of-initial-values marker, then stops the watcher. Serves `keys`,
    /// `history` and `purgeDeletes`, all of which want only the initial snapshot.
    private func collectInitialValues(
        from watcher: KeyValueWatcher
    ) async throws -> [KeyValueEntry] {
        var entries: [KeyValueEntry] = []
        do {
            for try await entry in watcher {
                guard let entry else {
                    break  // nil marker: all initial values received.
                }
                entries.append(entry)
            }
        } catch {
            await watcher.stop()
            throw error
        }
        await watcher.stop()
        return entries
    }

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
    private func updateUnchecked(
        key: String, value: Data, revision: UInt64, ttl: NanoTimeInterval? = nil
    ) async throws -> UInt64 {
        var headers = NatsHeaderMap()
        headers[.expectedLastSubjectSequence] =
            KeyValueCoding.expectedLastSubjectSequenceValue(revision)
        do {
            return try await publish(key: key, value: value, headers: headers, ttl: ttl)
        } catch JetStreamError.PublishError.streamWrongLastSequence {
            throw JetStreamError.KeyValueError.wrongLastRevision
        }
    }

    private func publish(
        key: String, value: Data, headers: NatsHeaderMap?, ttl: NanoTimeInterval? = nil
    ) async throws -> UInt64 {
        let subject = KeyValueCoding.subject(forBucket: bucket, key: key)
        let ack = try await ctx.publish(subject, message: value, headers: headers, msgTTL: ttl)
            .wait()
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
