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

import CryptoKit
import Foundation
import Nats
import Nuid

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
public final class ObjectStore {

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
        guard !meta.name.isEmpty else {
            throw JetStreamError.ObjectStoreError.badObjectMeta
        }
        if meta.link != nil {
            throw JetStreamError.ObjectStoreError.linkNotAllowed
        }

        // Resolve the chunk size, substituting the default for both nil AND an
        // explicit 0 (nats.go object.go:647-648) -- a 0 would make the chunk loop
        // below never advance. Normalize it back onto the meta so the persisted
        // options always carry max_chunk_size, matching nats.go's wire form.
        let requestedChunkSize = meta.chunkSize ?? 0
        let resolvedChunkSize =
            requestedChunkSize == 0 ? ObjectStoreCoding.defaultChunkSize : requestedChunkSize
        let chunkSize = Int(resolvedChunkSize)
        var normalizedMeta = meta
        var normalizedOptions = meta.options ?? ObjectMetaOptions()
        normalizedOptions.maxChunkSize = resolvedChunkSize
        normalizedMeta.options = normalizedOptions
        let newNuid = nextNuid()
        let chunkSubj = ObjectStoreCoding.chunkSubject(forBucket: bucket, nuid: newNuid)

        // Capture existing info so its chunks can be purged after a successful put. Not
        // found is fine; any other error is a problem.
        let existing: ObjectInfo?
        do {
            existing = try await getInfo(meta.name, showDeleted: true)
        } catch JetStreamError.ObjectStoreError.objectNotFound {
            existing = nil
        }

        // Stream the chunks, tracking the running digest, chunk count and total size.
        var sent: UInt32 = 0
        var total: UInt64 = 0
        var hasher = SHA256()
        var start = data.startIndex
        while start < data.endIndex {
            let end =
                data.index(start, offsetBy: chunkSize, limitedBy: data.endIndex)
                ?? data.endIndex
            let window = data.subdata(in: start..<end)
            do {
                _ = try await ctx.publish(chunkSubj, message: window).wait()
            } catch {
                _ = try? await stream.purge(subject: chunkSubj)
                throw error
            }
            hasher.update(data: window)
            sent += 1
            total += UInt64(window.count)
            start = end
        }

        let digest = ObjectStoreCoding.digest(fromBytes: Data(hasher.finalize()))
        let info = ObjectInfo(
            meta: normalizedMeta, bucket: bucket, nuid: newNuid, size: total, chunks: sent,
            digest: digest, modTime: ObjectStoreCoding.zeroTime)

        // Publish the meta (rolling up any previous meta for this name).
        do {
            try await publishMeta(info)
        } catch {
            _ = try? await stream.purge(subject: chunkSubj)
            throw error
        }

        // Purge the chunks of the superseded object, if any.
        if let existing, !existing.deleted {
            let oldChunkSubj = ObjectStoreCoding.chunkSubject(
                forBucket: bucket, nuid: existing.nuid)
            _ = try? await stream.purge(subject: oldChunkSubj)
        }

        var result = info
        result.modTime = ObjectStoreCoding.nowTimestamp()
        return result
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

    // MARK: - Internals

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
    private func publishMeta(_ info: ObjectInfo) async throws {
        var toStore = info
        toStore.modTime = ObjectStoreCoding.zeroTime
        let data = try ObjectStoreCoding.encodeInfo(toStore)
        let metaSubj = ObjectStoreCoding.metaSubject(forBucket: bucket, name: info.name)
        var headers = NatsHeaderMap()
        headers[.natsRollup] = NatsHeaderValue("sub")
        _ = try await ctx.publish(metaSubj, message: data, headers: headers).wait()
    }
}
