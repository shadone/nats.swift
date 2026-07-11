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

// Streaming object-store I/O: put an object from an `AsyncSequence<Data>` without ever
// buffering the whole object in memory, and get an object as an `AsyncSequence` of its
// chunk payloads. Both reuse the exact chunk/meta/digest wire conventions of the
// `Data`-based ``ObjectStore/put(_:data:)`` / ``ObjectStore/get(_:showDeleted:)`` -- this is
// purely additive; the wire format is unchanged.

extension ObjectStore {

    // MARK: - Streaming Put

    /// Streams an object into the store from an asynchronous sequence of `Data`, without
    /// buffering the whole object in memory. The object name is taken from `meta.name`.
    ///
    /// The `source` may yield `Data` values of any size: they are re-chunked into the
    /// store's fixed chunk size (`meta.chunkSize`, or the 128 KiB default) exactly as the
    /// buffered ``put(_:data:)`` does, so a streamed object is byte-for-byte identical on
    /// the wire to the same bytes put as one `Data` (same chunk subjects, same chunk count
    /// `ceil(total / chunkSize)`, same SHA-256 digest, same rolled-up meta).
    ///
    /// On any error -- a failed chunk publish, a `source` that throws mid-stream, or a
    /// failed meta publish -- the partially uploaded chunks are purged before the error is
    /// rethrown, leaving no orphaned chunks behind.
    ///
    /// - Parameters:
    ///   - meta: the object metadata. `meta.name` must be non-empty and must not carry a
    ///     link.
    ///   - source: an asynchronous sequence of arbitrarily-sized `Data` making up the
    ///     object's contents, in order.
    ///
    /// - Returns: the ``ObjectInfo`` describing the stored object (its `modTime` is the
    ///   caller-facing put time, not the server timestamp stored on the wire).
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/badObjectMeta`` if the name is empty.
    /// > - ``JetStreamError/ObjectStoreError/linkNotAllowed`` if the meta carries a link.
    /// > - ``JetStreamError/PublishError`` if a chunk or the meta is not acknowledged.
    /// > - any error thrown by `source`.
    @discardableResult
    public func put<S: AsyncSequence & Sendable>(
        _ meta: ObjectMeta, source: S
    ) async throws -> ObjectInfo where S.Element == Data {
        try await putStreaming(meta, source: source)
    }

    /// Streams an object into the store under the given name from an asynchronous sequence
    /// of `Data`. Convenience over ``put(_:source:)`` for the common no-extra-metadata case.
    ///
    /// - Parameters:
    ///   - name: the object name.
    ///   - source: an asynchronous sequence of arbitrarily-sized `Data` making up the
    ///     object's contents, in order.
    ///
    /// - Returns: the ``ObjectInfo`` describing the stored object.
    @discardableResult
    public func put<S: AsyncSequence & Sendable>(
        _ name: String, source: S
    ) async throws -> ObjectInfo where S.Element == Data {
        try await putStreaming(ObjectMeta(name: name), source: source)
    }

    /// The single put implementation shared by the `Data`-based ``put(_:data:)`` and the
    /// streaming ``put(_:source:)``.
    ///
    /// It performs the same validation, chunk-size resolution, existing-object lookup,
    /// chunk publishing, digest/size accounting, meta rollup and superseded-chunk purge as
    /// the original inline `Data` loop -- the only difference is that the bytes arrive from
    /// an `AsyncSequence` and are re-chunked through a rolling carry buffer rather than
    /// being sliced out of one contiguous `Data`. The resulting chunks, digest and meta are
    /// identical for identical input bytes.
    internal func putStreaming<S: AsyncSequence & Sendable>(
        _ meta: ObjectMeta, source: S
    ) async throws -> ObjectInfo where S.Element == Data {
        guard !meta.name.isEmpty else {
            throw JetStreamError.ObjectStoreError.badObjectMeta
        }
        if meta.link != nil {
            throw JetStreamError.ObjectStoreError.linkNotAllowed
        }

        // Resolve the chunk size, substituting the default for both nil AND an explicit 0
        // (nats.go object.go:647-648) -- a 0 would make the chunk loop below never advance.
        // Normalize it back onto the meta so the persisted options always carry
        // max_chunk_size, matching nats.go's wire form.
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

        // Running digest, chunk count and total size, updated as each window is published.
        var sent: UInt32 = 0
        var total: UInt64 = 0
        var hasher = SHA256()

        // Publishes one chunk window (never larger than `chunkSize`) and folds it into the
        // running digest/count/total. On a publish failure the partial upload is purged
        // before the error propagates.
        func publishChunk(_ window: Data) async throws {
            do {
                _ = try await ctx.publish(chunkSubj, message: window).wait()
            } catch {
                _ = try? await stream.purge(subject: chunkSubj)
                throw error
            }
            hasher.update(data: window)
            sent += 1
            total += UInt64(window.count)
        }

        // Re-chunk the incoming Data stream into fixed `chunkSize` windows. `carry` holds
        // the sub-chunk remainder between source elements (always strictly < chunkSize);
        // full windows are emitted as they fill, and the final short remainder is flushed
        // at end. This yields exactly ceil(total / chunkSize) chunks, matching the buffered
        // put. A `source` that throws is treated like a publish failure: purge the partial.
        do {
            var carry = Data()
            for try await piece in source {
                let work: Data
                if carry.isEmpty {
                    work = piece
                } else {
                    carry.append(piece)
                    work = carry
                    carry = Data()
                }
                let count = work.count
                var offset = 0
                while count - offset >= chunkSize {
                    let start = work.index(work.startIndex, offsetBy: offset)
                    let end = work.index(start, offsetBy: chunkSize)
                    try await publishChunk(work.subdata(in: start..<end))
                    offset += chunkSize
                }
                if offset < count {
                    let start = work.index(work.startIndex, offsetBy: offset)
                    carry = work.subdata(in: start..<work.endIndex)
                }
            }
            if !carry.isEmpty {
                try await publishChunk(carry)
            }
        } catch {
            _ = try? await stream.purge(subject: chunkSubj)
            throw error
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

    // MARK: - Streaming Get

    /// Gets an object from the store as a stream of its chunk payloads, without assembling
    /// the whole object into one `Data`.
    ///
    /// The returned ``ObjectStreamReader`` carries the object's ``ObjectInfo`` and is an
    /// `AsyncSequence` of `Data`: iterating it yields each chunk's payload in stream order.
    /// When the last chunk has been delivered the reader verifies the running SHA-256 digest
    /// and total size against the stored meta and, on a mismatch, throws
    /// ``JetStreamError/ObjectStoreError/digestMismatch`` at the *end* of iteration (the
    /// caller receives every chunk it yielded, then the throw). A zero-size object yields
    /// nothing and verifies trivially.
    ///
    /// The reader owns an ephemeral ordered consumer over the object's `$O.<bucket>.C.<nuid>`
    /// chunk subject. It is torn down automatically when iteration completes, throws, or is
    /// abandoned early (breaking out of the loop cancels the stream, which stops the
    /// consumer), so no server-side consumer leaks.
    ///
    /// - Parameters:
    ///   - name: the object name.
    ///   - showDeleted: when `true`, a deleted object is streamed instead of throwing.
    ///
    /// - Returns: an ``ObjectStreamReader`` over the object's chunks.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/objectNotFound`` if the object does not exist.
    /// > - ``JetStreamError/ObjectStoreError/notImplemented(_:)`` if the object is a link
    /// >   (link resolution is not yet supported), matching ``get(_:showDeleted:)``.
    /// > - the underlying create error if the backing consumer cannot be created.
    public func getStream(
        _ name: String, showDeleted: Bool = false
    ) async throws -> ObjectStreamReader {
        let info = try await getInfo(name, showDeleted: showDeleted)
        if info.isLink {
            throw JetStreamError.ObjectStoreError.notImplemented(
                "reading object links is not yet supported")
        }
        let reader = ObjectStreamReader(ctx: ctx, bucket: bucket, info: info)
        try await reader.start()
        return reader
    }
}

/// A streaming reader over the chunks of a stored object.
///
/// An `ObjectStreamReader` is an `AsyncSequence` of `Data`: each element is one chunk's
/// payload, delivered in stream order. Its ``info`` exposes the object's metadata and
/// instance information (size, chunk count, digest). The reader runs a running SHA-256 over
/// the chunks and, after the final chunk, verifies the digest and total size against
/// ``info``; on a mismatch it finishes iteration by throwing
/// ``JetStreamError/ObjectStoreError/digestMismatch``.
///
/// This is the streaming analogue of ``ObjectStore/get(_:showDeleted:)``: it consumes the
/// object's `$O.<bucket>.C.<nuid>` chunk subject through an ``OrderedConsumer`` and so
/// inherits its no-loss / no-dup recovery. The ordered consumer is torn down when iteration
/// completes, throws, or the caller stops early -- the reader wires the stream's
/// `onTermination` to stop the consumer, so an early `break` never leaks a server-side
/// consumer. Obtain a reader from ``ObjectStore/getStream(_:showDeleted:)``.
///
/// > Note: the reader is single-pass; iterate it once.
// `@unchecked Sendable`: every stored property is an immutable Sendable value except
// `pumpTask`, whose access is serialized by `stateLock` (mirrors ``ObjectStoreWatcher``).
// A reader is iterated and torn down from different tasks, so it must cross concurrency
// domains.
public final class ObjectStreamReader: AsyncSequence, @unchecked Sendable {
    public typealias Element = Data

    /// The object's metadata and instance information (size, chunk count, digest, ...).
    public let info: ObjectInfo

    /// The ordered consumer over the object's chunk subject, or `nil` for a zero-size
    /// object (which needs no chunk delivery).
    private let consumer: OrderedConsumer?
    private let stream: AsyncThrowingStream<Data, Error>
    private let continuation: AsyncThrowingStream<Data, Error>.Continuation

    // Immutable value copies captured for the pump (never `self`).
    private let client: NatsClient
    private let expectedDigest: Data?
    private let expectedSize: UInt64

    /// The pump task, spawned by ``start()`` only after the initial consumer creation
    /// succeeds. Guarded by `stateLock` because ``start()`` and the termination teardown
    /// may run on different tasks.
    private let stateLock = NSLock()
    private var pumpTask: Task<Void, Never>?

    /// Builds a reader for `info` (already resolved and known not to be a link).
    ///
    /// - Parameters:
    ///   - ctx: the JetStream context.
    ///   - bucket: the object-store bucket name.
    ///   - info: the resolved object info to stream.
    internal init(ctx: JetStreamContext, bucket: String, info: ObjectInfo) {
        self.info = info
        self.client = ctx.client
        self.expectedSize = info.size
        // Decode the stored digest once; a missing/invalid digest fails verification.
        self.expectedDigest = info.digest.flatMap { try? ObjectStoreCoding.decodeDigest($0) }

        let (stream, continuation) = AsyncThrowingStream.makeStream(
            of: Data.self, throwing: Error.self)
        self.stream = stream
        self.continuation = continuation

        if info.size == 0 {
            self.consumer = nil
        } else {
            let chunkSubj = ObjectStoreCoding.chunkSubject(forBucket: bucket, nuid: info.nuid)
            self.consumer = OrderedConsumer(
                ctx: ctx,
                streamName: ObjectStoreCoding.streamName(forBucket: bucket),
                filterSubject: chunkSubj,
                deliverPolicy: .all)
        }

        // Tear the ordered consumer down whenever the stream terminates: normal completion,
        // a thrown error, an explicit `stop()`, or the reader being deallocated (its `deinit`
        // finishes the stream). Note breaking a `for await` loop does NOT itself terminate the
        // stream -- teardown happens once the reader value is released or `stop()` is called;
        // a caller that stops early and RETAINS the reader should call `stop()` for
        // deterministic teardown. `consumer` is captured by value so it is stopped even after
        // `self` is gone; `self` is weak to avoid a retain cycle through the continuation.
        continuation.onTermination = { [weak self, consumer] _ in
            self?.cancelPump()
            if let consumer {
                Task { await consumer.stop() }
            }
        }
    }

    public func makeAsyncIterator() -> AsyncThrowingStream<Data, Error>.Iterator {
        stream.makeAsyncIterator()
    }

    /// Stops the reader deterministically: cancels the delivery pump, finishes the stream,
    /// and tears down the server-side ordered consumer (awaited, unlike the fire-and-forget
    /// path on natural completion). Call this if you stop iterating early while holding a
    /// reference to the reader; otherwise dropping the reader tears down on `deinit`.
    /// Idempotent.
    public func stop() async {
        cancelPump()
        continuation.finish()
        await consumer?.stop()
    }

    /// Performs the FAIL-FAST initial consumer creation, then starts the delivery pump.
    /// ``ObjectStore/getStream(_:showDeleted:)`` awaits this so a missing stream or a
    /// consumer-creation failure is THROWN there rather than surfacing only once iteration
    /// begins. A zero-size object needs no consumer: its stream finishes immediately.
    internal func start() async throws {
        guard let consumer else {
            // Empty object: no chunks to deliver, digest/size verify trivially.
            continuation.finish()
            return
        }
        do {
            try await consumer.start()
        } catch {
            continuation.finish(throwing: error)
            throw error
        }
        let task = Task { [consumer, continuation, client, expectedDigest, expectedSize] in
            await ObjectStreamReader.pump(
                consumer: consumer,
                continuation: continuation,
                client: client,
                expectedDigest: expectedDigest,
                expectedSize: expectedSize)
        }
        stateLock.withLockScoped { pumpTask = task }
    }

    /// Cancels the pump task if one is running. Idempotent; safe from the termination
    /// handler and from `deinit`.
    private func cancelPump() {
        let task = stateLock.withLockScoped { () -> Task<Void, Never>? in
            let existing = pumpTask
            pumpTask = nil
            return existing
        }
        task?.cancel()
    }

    deinit {
        // Best-effort backstop for a reader dropped without being fully iterated. Cancel
        // the pump and finish the stream; finishing triggers `onTermination`, which stops
        // the ordered consumer (fire-and-forget, since `deinit` cannot `await`). Failing
        // that, the server reaps the ephemeral consumer after its `inactiveThreshold`.
        cancelPump()
        continuation.finish()
    }

    // MARK: - Pump

    /// Drains the ordered consumer's chunk deliveries: yields each payload, folds it into
    /// the running digest and size, and stops once the server reports no more pending
    /// chunks. After the last chunk it verifies the digest and total size against the stored
    /// meta, finishing the stream normally on a match or by throwing
    /// ``JetStreamError/ObjectStoreError/digestMismatch`` on a mismatch. Runs as a single
    /// detached task, so its accounting stays local and race-free.
    private static func pump(
        consumer: OrderedConsumer,
        continuation: AsyncThrowingStream<Data, Error>.Continuation,
        client: NatsClient,
        expectedDigest: Data?,
        expectedSize: UInt64
    ) async {
        var hasher = SHA256()
        var total: UInt64 = 0
        do {
            for try await message in consumer.natsMessages {
                let payload = message.payload ?? Data()
                hasher.update(data: payload)
                total += UInt64(payload.count)
                continuation.yield(payload)
                // Stop once the server reports no more pending chunks for this object.
                if let meta = try? JetStreamMessage(message: message, client: client).metadata(),
                    meta.pending == 0
                {
                    break
                }
            }
        } catch {
            continuation.finish(throwing: error)
            return
        }

        // Integrity check at the end of the stream: the reassembled bytes must match the
        // stored digest and size (mirrors ``ObjectStore``'s buffered read verification).
        guard let expectedDigest, Data(hasher.finalize()) == expectedDigest, total == expectedSize
        else {
            continuation.finish(throwing: JetStreamError.ObjectStoreError.digestMismatch)
            return
        }
        continuation.finish()
    }
}

/// A minimal single-element `AsyncSequence`, used to feed the buffered ``ObjectStore/put(_:data:)``
/// through the shared streaming put core without changing its chunking behaviour.
struct SingleValueAsyncSequence<Element: Sendable>: AsyncSequence, Sendable {
    let value: Element

    struct AsyncIterator: AsyncIteratorProtocol {
        var value: Element?

        mutating func next() async -> Element? {
            defer { value = nil }
            return value
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(value: value)
    }
}
