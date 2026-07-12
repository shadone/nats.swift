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
import Nuid

#if canImport(CryptoKit)
    import CryptoKit
#else
    import Crypto
#endif

/// The result of getting an object: its info and assembled contents.
public struct ObjectResult {
    /// The object's metadata and instance information.
    public let info: ObjectInfo

    /// The object's assembled contents.
    public let data: Data
}

/// A handle to an object store bucket, bound to a backing JetStream stream.
///
/// Obtain an `ObjectStore` from a ``JetStreamContext`` via
/// ``JetStreamContext/objectStore(bucket:)``,
/// ``JetStreamContext/createObjectStore(cfg:)`` or
/// ``JetStreamContext/createOrUpdateObjectStore(cfg:)``.
///
/// This exposes the chunked object round-trip: `put`, `get`/`getBytes`, `getInfo` and
/// `delete`. Objects larger than a chunk are split into `$O.<bucket>.C.<nuid>` chunk
/// messages and reassembled on read; their SHA-256 digest is verified against the stored
/// meta on every `get`.
///
/// An `ObjectStore` handle is `Sendable`: its stored state is all immutable and its backing
/// ``Stream`` is thread-safe, so a single handle can be shared across concurrent tasks.
public final class ObjectStore: Sendable {

    /// The name of the bucket.
    public let bucket: String

    internal let ctx: JetStreamContext
    internal let stream: Stream

    /// Whether the backing stream allows `DIRECT.GET`, cached from the stream config at
    /// bind time (this value is immutable after stream creation).
    private let allowDirect: Bool

    internal init(ctx: JetStreamContext, bucket: String, stream: Stream) {
        self.ctx = ctx
        self.bucket = bucket
        self.stream = stream
        self.allowDirect = stream.info.config.allowDirect
    }

    // MARK: - Put

    /// Puts an object into the store, creating it or overwriting an existing object with
    /// the same name. The object name is taken from `meta.name`.
    ///
    /// - Parameters:
    ///   - meta: the object metadata. `meta.name` must be non-empty and must not carry a
    ///     link.
    ///   - data: the object contents.
    ///
    /// - Returns: the ``ObjectInfo`` describing the stored object (its `modTime` is the
    ///   caller-facing put time, not the server timestamp stored on the wire).
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/badObjectMeta`` if the name is empty.
    /// > - ``JetStreamError/ObjectStoreError/linkNotAllowed`` if the meta carries a link.
    /// > - ``JetStreamError/PublishError`` if a chunk or the meta is not acknowledged.
    @discardableResult
    public func put(_ meta: ObjectMeta, data: Data) async throws -> ObjectInfo {
        // Delegate to the shared streaming core (``ObjectStoreStreaming.swift``) by feeding
        // the whole buffer as a single-element async sequence. The core re-chunks it into
        // the same fixed-size windows this method used to emit inline, so the wire form
        // (chunk subjects, chunk count, digest and rolled-up meta) is byte-identical.
        try await putStreaming(meta, source: SingleValueAsyncSequence(value: data))
    }

    /// Puts a byte buffer into the store under the given name.
    ///
    /// - Parameters:
    ///   - name: the object name.
    ///   - data: the object contents.
    ///
    /// - Returns: the ``ObjectInfo`` describing the stored object.
    @discardableResult
    public func put(_ name: String, data: Data) async throws -> ObjectInfo {
        try await put(ObjectMeta(name: name), data: data)
    }

    // MARK: - Get

    /// Gets an object from the store, returning its info and assembled contents.
    ///
    /// - Parameters:
    ///   - name: the object name.
    ///   - showDeleted: when `true`, a deleted object is returned instead of throwing.
    ///
    /// - Returns: an ``ObjectResult`` with the object's info and contents.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/objectNotFound`` if the object does not exist.
    /// > - ``JetStreamError/ObjectStoreError/digestMismatch`` if the reassembled contents
    /// >   do not match the stored digest or size.
    /// > - ``JetStreamError/ObjectStoreError/notImplemented(_:)`` if the object is a link
    /// >   (link resolution is not yet supported).
    public func get(_ name: String, showDeleted: Bool = false) async throws -> ObjectResult {
        try await read(name: name, showDeleted: showDeleted)
    }

    /// Gets an object from the store and returns its contents as a byte buffer.
    ///
    /// - Parameters:
    ///   - name: the object name.
    ///   - showDeleted: when `true`, a deleted object is returned instead of throwing.
    ///
    /// - Returns: the object's contents.
    ///
    /// > **Throws:** see ``get(_:showDeleted:)``.
    public func getBytes(_ name: String, showDeleted: Bool = false) async throws -> Data {
        try await read(name: name, showDeleted: showDeleted).data
    }

    // MARK: - Info

    /// Retrieves the current information for an object.
    ///
    /// The `modTime` is derived from the meta message's server timestamp: the value
    /// stored inside the meta JSON is always the zero time, so the true modification time
    /// is only available on read.
    ///
    /// - Parameters:
    ///   - name: the object name.
    ///   - showDeleted: when `true`, a deleted object's info is returned instead of
    ///     throwing.
    ///
    /// - Returns: the ``ObjectInfo``.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/nameRequired`` if the name is empty.
    /// > - ``JetStreamError/ObjectStoreError/objectNotFound`` if the object does not exist
    /// >   (or is deleted and `showDeleted` is `false`).
    /// > - ``JetStreamError/ObjectStoreError/bucketNotFound(_:)`` if the backing stream
    /// >   is gone.
    /// > - ``JetStreamError/ObjectStoreError/badObjectMeta`` if the stored meta is
    /// >   malformed or carries no nuid.
    public func getInfo(_ name: String, showDeleted: Bool = false) async throws -> ObjectInfo {
        guard !name.isEmpty else {
            throw JetStreamError.ObjectStoreError.nameRequired
        }
        let metaSubj = ObjectStoreCoding.metaSubject(forBucket: bucket, name: name)

        let message: StreamMessage?
        do {
            if allowDirect {
                message = try await stream.getMessageDirect(lastForSubject: metaSubj)
            } else {
                message = try await stream.getMessage(lastForSubject: metaSubj)
            }
        } catch JetStreamError.StreamError.streamNotFound {
            throw JetStreamError.ObjectStoreError.bucketNotFound(bucket)
        }

        guard let message else {
            throw JetStreamError.ObjectStoreError.objectNotFound
        }
        guard var info = try? ObjectStoreCoding.decodeInfo(message.payload), !info.nuid.isEmpty
        else {
            throw JetStreamError.ObjectStoreError.badObjectMeta
        }
        if !showDeleted && info.deleted {
            throw JetStreamError.ObjectStoreError.objectNotFound
        }
        // The stored mtime is the zero time; the real one is the message timestamp.
        info.modTime = message.time
        return info
    }

    // MARK: - Delete

    /// Deletes an object: marks it deleted (rolling up its meta) and purges its chunks.
    /// Deleting a non-existent object throws; deleting an already-deleted object is a
    /// no-op that succeeds.
    ///
    /// - Parameter name: the object name.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/objectNotFound`` if the object does not exist.
    /// > - ``JetStreamError/ObjectStoreError/badObjectMeta`` if the stored meta carries no
    /// >   nuid.
    public func delete(_ name: String) async throws {
        var info = try await getInfo(name, showDeleted: true)
        guard !info.nuid.isEmpty else {
            throw JetStreamError.ObjectStoreError.badObjectMeta
        }

        // Place a delete marker and clear the instance fields.
        info.deleted = true
        info.size = 0
        info.chunks = 0
        info.digest = nil

        try await publishMeta(info)

        let chunkSubj = ObjectStoreCoding.chunkSubject(forBucket: bucket, nuid: info.nuid)
        _ = try await stream.purge(subject: chunkSubj)
    }

    // MARK: - Watch

    /// Watches every object in the bucket. The returned ``ObjectStoreWatcher`` first
    /// delivers the current ``ObjectInfo`` of each object, then a `nil` marker, then live
    /// updates.
    ///
    /// - Parameter opts: the watch options.
    ///
    /// - Returns: an ``ObjectStoreWatcher``. Call ``ObjectStoreWatcher/stop()`` when done.
    ///
    /// > **Throws:** the underlying create error if the backing stream is missing or the
    /// > consumer cannot be created (fail-fast, mirroring nats.go).
    public func watch(
        opts: ObjectStoreWatchOptions = .init()
    ) async throws -> ObjectStoreWatcher {
        // Pass the stream NAME string (never the `Stream` instance) so the long-lived
        // watcher pump cannot race a concurrent `Stream.info` mutation, exactly as KV does.
        let watcher = ObjectStoreWatcher(
            ctx: ctx,
            streamName: ObjectStoreCoding.streamName(forBucket: bucket),
            filterSubject: ObjectStoreCoding.allMetaSubject(forBucket: bucket),
            opts: opts)
        try await watcher.start()
        return watcher
    }

    // MARK: - List

    /// Lists information about the objects in the store.
    ///
    /// - Parameter showDeleted: when `true`, objects marked as deleted are included.
    ///
    /// - Returns: the objects' ``ObjectInfo``.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/noObjectsFound`` if the store holds no
    /// >   matching objects.
    public func list(showDeleted: Bool = false) async throws -> [ObjectInfo] {
        var opts = ObjectStoreWatchOptions()
        opts.ignoreDeletes = !showDeleted
        let watcher = try await watch(opts: opts)

        var objects: [ObjectInfo] = []
        do {
            for try await entry in watcher {
                guard let entry else {
                    break  // nil marker: all initial values received.
                }
                objects.append(entry)
            }
        } catch {
            await watcher.stop()
            throw error
        }
        await watcher.stop()

        if objects.isEmpty {
            throw JetStreamError.ObjectStoreError.noObjectsFound
        }
        return objects
    }

    // MARK: - UpdateMeta

    /// Updates the mutable metadata of an object: its name, description, headers and
    /// user metadata. The instance fields (`nuid`, `size`, `chunks`, `digest`) and the
    /// options (`link`, `chunkSize`) are never changed — those are managed internally.
    ///
    /// Renaming an object (`meta.name != name`) moves its meta to the new name and purges
    /// the old meta subject so the old name disappears.
    ///
    /// - Parameters:
    ///   - name: the current name of the object.
    ///   - meta: the new metadata. `meta.name` may differ from `name` to rename.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/updateMetaDeleted`` if the object does not
    /// >   exist or is deleted.
    /// > - ``JetStreamError/ObjectStoreError/objectAlreadyExists`` if renaming onto a name
    /// >   already held by a live object.
    public func updateMeta(_ name: String, meta: ObjectMeta) async throws {
        let info: ObjectInfo
        do {
            info = try await getInfo(name, showDeleted: true)
        } catch JetStreamError.ObjectStoreError.objectNotFound {
            throw JetStreamError.ObjectStoreError.updateMetaDeleted
        }
        if info.deleted {
            throw JetStreamError.ObjectStoreError.updateMetaDeleted
        }

        // If renaming, the target name must not already be held by a live object.
        if meta.name != name {
            if let existing = try await infoOrNil(meta.name), !existing.deleted {
                throw JetStreamError.ObjectStoreError.objectAlreadyExists
            }
        }

        // Overwrite ONLY the mutable meta fields; keep options/nuid/size/chunks/digest.
        var updated = info
        updated.name = meta.name
        updated.description = meta.description
        updated.headers = meta.headers
        updated.metadata = meta.metadata
        try await publishMeta(updated)

        // On rename the meta now lives under the new name; purge the old meta subject so
        // the old name no longer resolves.
        if meta.name != name {
            let oldMetaSubj = ObjectStoreCoding.metaSubject(forBucket: bucket, name: name)
            _ = try await stream.purge(subject: oldMetaSubj)
        }
    }

    // MARK: - Links

    /// Adds a link object that points to another object.
    ///
    /// - Parameters:
    ///   - name: the name of the new link object.
    ///   - object: the object being linked to.
    ///
    /// - Returns: the ``ObjectInfo`` of the created link.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/nameRequired`` if `name` is empty.
    /// > - ``JetStreamError/ObjectStoreError/objectRequired`` if the target object has an
    /// >   empty name.
    /// > - ``JetStreamError/ObjectStoreError/noLinkToDeleted`` if the target is deleted.
    /// > - ``JetStreamError/ObjectStoreError/noLinkToLink`` if the target is itself a link.
    /// > - ``JetStreamError/ObjectStoreError/objectAlreadyExists`` if `name` already holds
    /// >   a non-link object (an existing link may be overwritten).
    @discardableResult
    public func addLink(_ name: String, to object: ObjectInfo) async throws -> ObjectInfo {
        guard !name.isEmpty else {
            throw JetStreamError.ObjectStoreError.nameRequired
        }
        guard !object.name.isEmpty else {
            throw JetStreamError.ObjectStoreError.objectRequired
        }
        if object.deleted {
            throw JetStreamError.ObjectStoreError.noLinkToDeleted
        }
        if object.isLink {
            throw JetStreamError.ObjectStoreError.noLinkToLink
        }
        try await assertNameNotHeldByObject(name)

        let link = ObjectLink(bucket: object.bucket, name: object.name)
        return try await publishLink(name: name, link: link)
    }

    /// Adds a link object that points to a whole other object store (like a directory).
    ///
    /// - Parameters:
    ///   - name: the name of the new link object.
    ///   - store: the object store being linked to.
    ///
    /// - Returns: the ``ObjectInfo`` of the created link.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/nameRequired`` if `name` is empty.
    /// > - ``JetStreamError/ObjectStoreError/objectAlreadyExists`` if `name` already holds
    /// >   a non-link object (an existing link may be overwritten).
    @discardableResult
    public func addBucketLink(_ name: String, to store: ObjectStore) async throws -> ObjectInfo {
        guard !name.isEmpty else {
            throw JetStreamError.ObjectStoreError.nameRequired
        }
        try await assertNameNotHeldByObject(name)

        let link = ObjectLink(bucket: store.bucket, name: nil)
        return try await publishLink(name: name, link: link)
    }

    // MARK: - Seal / Status

    /// Seals the object store: the backing stream is marked sealed and no further
    /// modifications (puts, deletes, meta updates) are allowed.
    ///
    /// Only `sealed` is set; the server enforces a sealed stream as fully immutable, so
    /// there is no need to also deny delete/purge.
    public func seal() async throws {
        let info = try await stream.info()
        var cfg = info.config
        cfg.sealed = true
        _ = try await ctx.updateStream(cfg: cfg)
    }

    /// Retrieves the current status and configuration of the bucket, refreshing the
    /// backing stream info.
    ///
    /// - Returns: the ``ObjectStoreStatus``.
    public func status() async throws -> ObjectStoreStatus {
        let info = try await stream.info()
        return ObjectStoreStatus(bucket: bucket, streamInfo: info)
    }

    // MARK: - Internals

    /// Fetches the meta for `name` including deleted objects, returning nil when absent.
    private func infoOrNil(_ name: String) async throws -> ObjectInfo? {
        do {
            return try await getInfo(name, showDeleted: true)
        } catch JetStreamError.ObjectStoreError.objectNotFound {
            return nil
        }
    }

    /// The shared overwrite guard for the link operations: a non-link object already at
    /// `name` blocks the link; an existing link at `name` may be overwritten (object.go:
    /// 1023-1030 / 1063-1070).
    private func assertNameNotHeldByObject(_ name: String) async throws {
        if let existing = try await infoOrNil(name), !existing.isLink {
            throw JetStreamError.ObjectStoreError.objectAlreadyExists
        }
    }

    /// Publishes a link's meta (no chunks) under `name` and returns its info with a
    /// caller-facing `modTime` (the value stored on the wire is the zero time).
    private func publishLink(name: String, link: ObjectLink) async throws -> ObjectInfo {
        let meta = ObjectMeta(name: name, options: ObjectMetaOptions(link: link))
        let info = ObjectInfo(
            meta: meta, bucket: bucket, nuid: nextNuid(), size: 0, chunks: 0, digest: nil,
            modTime: ObjectStoreCoding.zeroTime)
        try await publishMeta(info)
        var result = info
        result.modTime = ObjectStoreCoding.nowTimestamp()
        return result
    }

    /// Fetches the info, then reassembles and verifies the object's chunks.
    private func read(name: String, showDeleted: Bool) async throws -> ObjectResult {
        let info = try await getInfo(name, showDeleted: showDeleted)
        if info.isLink {
            throw JetStreamError.ObjectStoreError.notImplemented(
                "reading object links is not yet supported")
        }
        if info.size == 0 {
            return ObjectResult(info: info, data: Data())
        }

        let chunkSubj = ObjectStoreCoding.chunkSubject(forBucket: bucket, nuid: info.nuid)
        // Reuse the ordered consumer engine (as the KV watcher does) to stream the chunks
        // in stream order with no-loss / no-dup recovery.
        let consumer = OrderedConsumer(
            ctx: ctx,
            streamName: ObjectStoreCoding.streamName(forBucket: bucket),
            filterSubject: chunkSubj,
            deliverPolicy: .all)
        try await consumer.start()

        // A non-empty object whose chunk subject has no pending messages — e.g. the object was
        // deleted or overwritten after `getInfo` resolved — would never deliver a chunk, so the read
        // loop below (which ends only on an in-band `pending == 0`) would hang forever. Fail fast on
        // the empty set. (`size == 0` was handled above, so zero pending here means the chunks are
        // gone.)
        if await consumer.initialPending() == 0 {
            await consumer.stop()
            throw JetStreamError.ObjectStoreError.objectNotFound
        }

        var assembled = Data()
        var hasher = SHA256()
        do {
            for try await message in consumer.natsMessages {
                let payload = message.payload ?? Data()
                assembled.append(payload)
                hasher.update(data: payload)
                // Stop once the server reports no more pending chunks for this object.
                if let meta = try? JetStreamMessage(message: message, client: ctx.client)
                    .metadata(), meta.pending == 0
                {
                    break
                }
            }
        } catch {
            await consumer.stop()
            throw error
        }
        await consumer.stop()

        // Verify integrity: the reassembled bytes must match the stored digest and size.
        guard let digestString = info.digest else {
            throw JetStreamError.ObjectStoreError.digestMismatch
        }
        let expected = try ObjectStoreCoding.decodeDigest(digestString)
        guard Data(hasher.finalize()) == expected, UInt64(assembled.count) == info.size else {
            throw JetStreamError.ObjectStoreError.digestMismatch
        }
        return ObjectResult(info: info, data: assembled)
    }

    /// Publishes an object's meta as a rollup message on its meta subject, storing the
    /// zero time as the modification time (the wire convention).
    ///
    /// `internal` (not `private`) so the streaming put core in ``ObjectStoreStreaming.swift``
    /// can share this exact rollup-publish path.
    internal func publishMeta(_ info: ObjectInfo) async throws {
        var toStore = info
        toStore.modTime = ObjectStoreCoding.zeroTime
        let data = try ObjectStoreCoding.encodeInfo(toStore)
        let metaSubj = ObjectStoreCoding.metaSubject(forBucket: bucket, name: info.name)
        var headers = NatsHeaderMap()
        headers[.natsRollup] = NatsHeaderValue("sub")
        _ = try await ctx.publish(metaSubj, message: data, headers: headers).wait()
    }
}
