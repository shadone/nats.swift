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

/// Options controlling how an ``ObjectStoreWatcher`` delivers object updates.
///
/// Unlike ``KeyValueWatchOptions`` there is no `metaOnly` or `resumeFromRevision`: an
/// object's meta message body *is* the ``ObjectInfo`` payload, so a headers-only consumer
/// could not decode it, and object watch exposes no resume knob (matching nats.go).
public struct ObjectStoreWatchOptions: Sendable {
    /// Deliver every historical revision of each object's meta before switching to the
    /// live tail, rather than only the latest meta per object.
    public var includeHistory: Bool = false

    /// Suppress objects carrying a delete marker (`deleted == true`). They are still
    /// counted toward the end-of-initial-values marker, just never yielded.
    public var ignoreDeletes: Bool = false

    /// Skip all initial values and deliver only updates made after the watch starts. The
    /// end-of-initial-values marker is delivered immediately.
    public var updatesOnly: Bool = false

    /// Creates watch options with the default settings (latest meta per object, deletes
    /// included, all initial values delivered).
    public init() {}
}

/// A live view over the objects in an object-store bucket.
///
/// An `ObjectStoreWatcher` is an `AsyncSequence` of `ObjectInfo?`. It first delivers the
/// current ``ObjectInfo`` of each object (subject to the configured
/// ``ObjectStoreWatchOptions``), then a single `nil` element — the END-OF-INITIAL-VALUES
/// marker — after which every element is a live update. The marker mirrors nats.go's
/// `<-nil>` sentinel and is delivered exactly once. On an empty bucket (or `updatesOnly`)
/// the marker is delivered immediately, before any entry.
///
/// This is the object-store analogue of ``KeyValueWatcher``: it consumes the bucket's
/// `$O.<bucket>.M.>` meta subjects through an ``OrderedConsumer`` and decodes each
/// message body as an ``ObjectInfo``, so it inherits the ordered consumer's no-loss /
/// no-dup recovery. If the underlying ephemeral consumer is lost the watcher transparently
/// resumes the live tail with no gap and no duplicate.
///
/// Always call ``stop()`` when finished to tear down the server-side consumer. The
/// `deinit` is only a best-effort backstop for a dropped watcher; it does not replace an
/// explicit ``stop()``.
// `@unchecked Sendable`: every stored property is an immutable Sendable value
// except `pumpTask`, whose access is serialized by `stateLock` (mirrors
// ``PushConsumer``). A watcher is iterated and stopped from different tasks
// (e.g. a watchdog calling ``stop()``), so it must cross concurrency domains.
public final class ObjectStoreWatcher: AsyncSequence, @unchecked Sendable {
    public typealias Element = ObjectInfo?

    private let consumer: OrderedConsumer
    private let stream: AsyncThrowingStream<ObjectInfo?, Error>
    private let continuation: AsyncThrowingStream<ObjectInfo?, Error>.Continuation

    // Immutable value copies captured for the pump (never `self`).
    private let client: NatsClient
    private let ignoreDeletes: Bool
    private let updatesOnly: Bool

    /// The pump task, spawned by ``start()`` only after the initial consumer creation
    /// succeeds. `nil` before ``start()`` and after a failed one. Guarded by
    /// `stateLock` because ``start()``/``stop()`` may run on different tasks.
    private let stateLock = NSLock()
    private var pumpTask: Task<Void, Never>?

    /// Creates a watcher over `filterSubject` (`$O.<bucket>.M.>`) on the object-store
    /// backing stream.
    ///
    /// The watcher captures only immutable value copies (the stream NAME and the filter
    /// subject) and performs all server interaction via the ``JetStreamContext`` and the
    /// ``OrderedConsumer``. It never touches a foreground ``ObjectStore`` or ``Stream``
    /// object, so it cannot race a concurrent `Stream.info` mutation.
    ///
    /// - Parameters:
    ///   - ctx: the JetStream context.
    ///   - streamName: the name of the object-store backing stream (`OBJ_<bucket>`).
    ///   - filterSubject: the `$O.<bucket>.M.>` subject to watch.
    ///   - opts: the watch options.
    ///   - idleHeartbeat: the ordered consumer idle heartbeat (also the
    ///     recovery-detection interval).
    internal init(
        ctx: JetStreamContext,
        streamName: String,
        filterSubject: String,
        opts: ObjectStoreWatchOptions,
        idleHeartbeat: TimeInterval = 5
    ) {
        // Deliver-policy mapping (object.go:1333-1338): latest meta per object by
        // default; every historical revision with includeHistory; only new updates with
        // updatesOnly (which wins over includeHistory).
        var deliverPolicy: DeliverPolicy = .lastPerSubject
        if opts.includeHistory {
            deliverPolicy = .all
        }
        if opts.updatesOnly {
            deliverPolicy = .new
        }

        let consumer = OrderedConsumer(
            ctx: ctx,
            streamName: streamName,
            filterSubject: filterSubject,
            deliverPolicy: deliverPolicy,
            idleHeartbeat: idleHeartbeat)
        self.consumer = consumer

        let (stream, continuation) = AsyncThrowingStream.makeStream(
            of: ObjectInfo?.self, throwing: Error.self)
        self.stream = stream
        self.continuation = continuation

        // Capture only values (never `self`); the pump is spawned by `start()`.
        self.client = ctx.client
        self.ignoreDeletes = opts.ignoreDeletes
        self.updatesOnly = opts.updatesOnly
    }

    public func makeAsyncIterator() -> AsyncThrowingStream<ObjectInfo?, Error>.Iterator {
        stream.makeAsyncIterator()
    }

    /// Performs the FAIL-FAST initial consumer creation, then starts the delivery pump.
    /// Callers (`watch`/`list`) await this so a missing stream, invalid filter, or
    /// permission error is THROWN here rather than the watch hanging forever waiting for
    /// initial values that never arrive. Mirrors nats.go creating the subscription
    /// synchronously in `Watch` and returning its error.
    internal func start() async throws {
        do {
            try await consumer.start()
        } catch {
            // The ordered consumer already cleaned itself up; finish this watcher's
            // stream by throwing and leave no pump running.
            continuation.finish(throwing: error)
            throw error
        }
        let task = Task { [consumer, continuation, client, ignoreDeletes, updatesOnly] in
            await ObjectStoreWatcher.pump(
                consumer: consumer,
                continuation: continuation,
                client: client,
                ignoreDeletes: ignoreDeletes,
                updatesOnly: updatesOnly)
        }
        stateLock.withLockScoped { pumpTask = task }
    }

    /// Stops the watcher: cancels the pump, tears down the ordered consumer (and its
    /// server-side ephemeral consumer) and finishes the sequence. Idempotent.
    public func stop() async {
        let task = stateLock.withLockScoped { () -> Task<Void, Never>? in
            let existing = pumpTask
            pumpTask = nil
            return existing
        }
        task?.cancel()
        await consumer.stop()
        continuation.finish()
    }

    deinit {
        // Best-effort backstop only: explicit `stop()` remains the contract for tearing
        // down the server-side consumer. If a caller drops the watcher without calling
        // `stop()`, cancel the pump and finish the stream so the unstructured `pumpTask`
        // (which strongly holds the `OrderedConsumer`) cannot run forever and leak the
        // subscription/ephemeral consumer. `deinit` cannot `await`, so the ordered
        // consumer is torn down fire-and-forget; failing that, the server reaps it after
        // its `inactiveThreshold`.
        let task = stateLock.withLockScoped { pumpTask }
        task?.cancel()
        continuation.finish()
        let consumer = self.consumer
        Task { await consumer.stop() }
    }

    // MARK: - Pump

    /// Consumes the ordered stream, decodes each meta body as an ``ObjectInfo``, applies
    /// the marker and `ignoreDeletes` logic, and feeds entries into `continuation`. Runs
    /// as a single detached task, so its accounting state stays local and race-free.
    private static func pump(
        consumer: OrderedConsumer,
        continuation: AsyncThrowingStream<ObjectInfo?, Error>.Continuation,
        client: NatsClient,
        ignoreDeletes: Bool,
        updatesOnly: Bool
    ) async {
        // The ordered consumer was already started (fail-fast) by `start()` before this
        // pump was spawned, so its `initialPending()` is already populated.
        var initDone = false
        var received: UInt64 = 0
        var initPending: UInt64 = 0

        if updatesOnly {
            // No initial values: emit the end-of-initial marker up front and never run
            // the per-message accounting below.
            continuation.yield(nil)
            initDone = true
        } else {
            // Use the stable numPending captured at the ordered consumer's first create.
            // An empty bucket fires the marker immediately.
            initPending = await initialPending(from: consumer)
            if initPending == 0 {
                continuation.yield(nil)
                initDone = true
            }
        }

        do {
            for try await message in consumer.natsMessages {
                // JS metadata is required for marker accounting; a stream delivery always
                // carries it. Without it we can neither count nor terminate, so skip.
                guard let meta = try? JetStreamMessage(message: message, client: client).metadata()
                else { continue }

                // Decode the object meta body and yield it. If the body is not decodable
                // object meta we yield nothing for it, but STILL run marker accounting
                // below so a single corrupt message can never stall the end-of-initial
                // marker (this is strictly more robust than nats.go, whose `update`
                // callback returns before consulting the metadata, object.go:1293-1300).
                if let payload = message.payload,
                    var info = try? ObjectStoreCoding.decodeInfo(payload)
                {
                    // The stored mtime is the zero time; the real one is the message
                    // timestamp (object.go:1303).
                    info.modTime = ObjectStoreCoding.modTime(fromMetadataTimestamp: meta.timestamp)
                    // Yield unless this is a delete marker filtered out by ignoreDeletes.
                    if !(ignoreDeletes && info.deleted) {
                        continuation.yield(info)
                    }
                }

                // Marker accounting runs for EVERY delivery (deletes and undecodable
                // bodies included) until the marker fires. Fire on the first of: this
                // being the last pending entry (pending == 0), or having received the
                // initial count (a backstop when numPending was stale against a growing
                // bucket).
                if !initDone {
                    received += 1
                    if received >= initPending || meta.pending == 0 {
                        initDone = true
                        continuation.yield(nil)
                    }
                }
            }
            continuation.finish()
        } catch {
            continuation.finish(throwing: error)
        }
    }

    /// Polls the ordered consumer for the `numPending` captured at its first successful
    /// create. Returns 0 if the task is cancelled before then.
    private static func initialPending(from consumer: OrderedConsumer) async -> UInt64 {
        while !Task.isCancelled {
            if let pending = await consumer.initialPending() {
                return pending
            }
            try? await Task.sleep(nanoseconds: 20_000_000)
        }
        return 0
    }
}
