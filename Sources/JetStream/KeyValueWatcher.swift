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

/// Options controlling how a ``KeyValueWatcher`` delivers entries.
///
/// The history/updates knobs are mutually exclusive in intent: if both
/// ``includeHistory`` and ``updatesOnly`` are set, ``updatesOnly`` wins.
public struct KeyValueWatchOptions: Sendable {
    /// Deliver every historical revision of each matched key before switching to
    /// the live tail, rather than only the latest revision.
    public var includeHistory: Bool = false

    /// Suppress `delete`/`purge` tombstone entries (they are still counted toward
    /// the end-of-initial-values marker, just never yielded).
    public var ignoreDeletes: Bool = false

    /// Skip all initial values and deliver only updates made after the watch
    /// starts. The end-of-initial-values marker is delivered immediately.
    public var updatesOnly: Bool = false

    /// Deliver entries with an empty value, carrying only key, revision and
    /// operation. Backed by a headers-only consumer.
    public var metaOnly: Bool = false

    /// Resume the watch from a specific revision (backing stream sequence).
    /// Overrides the history/updates deliver policy when greater than zero.
    public var resumeFromRevision: UInt64 = 0

    /// Creates watch options with the default settings (latest value per key,
    /// deletes included, all initial values delivered).
    public init() {}
}

/// A live view over a set of keys in a KeyValue bucket.
///
/// A `KeyValueWatcher` is an `AsyncSequence` of `KeyValueEntry?`. It first
/// delivers the current value of each matched key (subject to the configured
/// ``KeyValueWatchOptions``), then a single `nil` element — the
/// END-OF-INITIAL-VALUES marker — after which every element is a live update.
/// The marker mirrors nats.go's `<-nil>` sentinel and is delivered exactly
/// once. On an empty match set (or `updatesOnly`) the marker is delivered
/// immediately, before any entry.
///
/// The watcher is a SINGLE-consumer sequence backed by an
/// ``OrderedConsumer``, so it inherits the ordered consumer's no-loss / no-dup
/// recovery: if the underlying ephemeral consumer is lost the watcher
/// transparently resumes the live tail with no gap and no duplicate. When such a
/// reset happens DURING the initial snapshot (before the end-of-initial marker),
/// the ordered consumer resumes from `streamSeq + 1` via `byStartSequence`
/// (relative to the last value already yielded), so the remaining
/// last-per-subject values are still delivered exactly once and the marker fires
/// once the captured `numPending` has been accounted for — the snapshot stays
/// coherent with no missing keys and no duplicates. This matches nats.go.
///
/// Always call ``stop()`` when finished to tear down the server-side consumer.
/// The ``deinit`` is only a best-effort backstop for a dropped watcher; it does
/// not replace an explicit ``stop()``.
// `@unchecked Sendable`: every stored property is an immutable Sendable value
// except `pumpTask`, whose access is serialized by `stateLock` (mirrors
// ``PushConsumer``). A watcher is iterated and stopped from different tasks
// (e.g. a watchdog calling ``stop()``), so it must cross concurrency domains.
public final class KeyValueWatcher: AsyncSequence, @unchecked Sendable {
    public typealias Element = KeyValueEntry?

    private let consumer: OrderedConsumer
    private let stream: AsyncThrowingStream<KeyValueEntry?, Error>
    private let continuation: AsyncThrowingStream<KeyValueEntry?, Error>.Continuation

    // Immutable value copies captured for the pump (never `self`).
    private let client: NatsClient
    private let bucket: String
    private let ignoreDeletes: Bool
    private let updatesOnly: Bool

    /// The pump task, spawned by ``start()`` only after the initial consumer
    /// creation succeeds. `nil` before ``start()`` and after a failed one.
    /// Guarded by `stateLock` because ``start()``/``stop()`` may run on
    /// different tasks.
    private let stateLock = NSLock()
    private var pumpTask: Task<Void, Never>?

    /// Creates a watcher over `filterSubject` on the KV backing stream.
    ///
    /// The watcher captures only immutable value copies (the stream NAME, the
    /// bucket and the filter subject) and performs all server interaction via
    /// the ``JetStreamContext`` and the ``OrderedConsumer``. It never touches a
    /// foreground ``KeyValue`` or ``Stream`` object, so it cannot race a
    /// concurrent `Stream.info` mutation.
    ///
    /// - Parameters:
    ///   - ctx: the JetStream context.
    ///   - streamName: the name of the KV backing stream (`KV_<bucket>`).
    ///   - bucket: the bucket name, used to strip the subject prefix from keys.
    ///   - filterSubject: the `$KV.<bucket>.<keys>` subject to watch.
    ///   - opts: the watch options.
    ///   - idleHeartbeat: the ordered consumer idle heartbeat (also the
    ///     recovery-detection interval).
    internal init(
        ctx: JetStreamContext,
        streamName: String,
        bucket: String,
        filterSubject: String,
        opts: KeyValueWatchOptions,
        idleHeartbeat: TimeInterval = 5
    ) {
        // Deliver-policy mapping (later options override earlier, so updatesOnly
        // wins over includeHistory and resumeFromRevision wins over both).
        var deliverPolicy: DeliverPolicy = .lastPerSubject
        var optStartSeq: UInt64? = nil
        if opts.includeHistory {
            deliverPolicy = .all
        }
        if opts.updatesOnly {
            deliverPolicy = .new
        }
        if opts.resumeFromRevision > 0 {
            deliverPolicy = .byStartSequence
            optStartSeq = opts.resumeFromRevision
        }

        let consumer = OrderedConsumer(
            ctx: ctx,
            streamName: streamName,
            filterSubject: filterSubject,
            deliverPolicy: deliverPolicy,
            optStartSeq: optStartSeq,
            headersOnly: opts.metaOnly,
            idleHeartbeat: idleHeartbeat)
        self.consumer = consumer

        let (stream, continuation) = AsyncThrowingStream.makeStream(
            of: KeyValueEntry?.self, throwing: Error.self)
        self.stream = stream
        self.continuation = continuation

        // Capture only values (never `self`); the pump is spawned by `start()`.
        self.client = ctx.client
        self.bucket = bucket
        self.ignoreDeletes = opts.ignoreDeletes
        self.updatesOnly = opts.updatesOnly
    }

    public func makeAsyncIterator() -> AsyncThrowingStream<KeyValueEntry?, Error>.Iterator {
        stream.makeAsyncIterator()
    }

    /// Performs the FAIL-FAST initial consumer creation, then starts the delivery
    /// pump. Callers (`watch`/`watchAll`/`keys`/`history`/`purgeDeletes`) await
    /// this so a missing stream, invalid filter, or permission error is THROWN
    /// here rather than the watch hanging forever waiting for initial values that
    /// never arrive. It also establishes creation-time semantics: with
    /// `updatesOnly` (deliver policy `new`) a write made before the consumer
    /// exists would otherwise be missed. Mirrors nats.go creating the
    /// subscription synchronously in `WatchFiltered` and returning its error.
    internal func start() async throws {
        do {
            try await consumer.start()
        } catch {
            // The ordered consumer already cleaned itself up; finish this
            // watcher's stream by throwing and leave no pump running.
            continuation.finish(throwing: error)
            throw error
        }
        let task = Task {
            [consumer, continuation, client, bucket, ignoreDeletes, updatesOnly] in
            await KeyValueWatcher.pump(
                consumer: consumer,
                continuation: continuation,
                client: client,
                bucket: bucket,
                ignoreDeletes: ignoreDeletes,
                updatesOnly: updatesOnly)
        }
        stateLock.withLockScoped { pumpTask = task }
    }

    /// Stops the watcher: cancels the pump, tears down the ordered consumer (and
    /// its server-side ephemeral consumer) and finishes the sequence. Idempotent.
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
        // Best-effort backstop only: explicit `stop()` remains the contract for
        // tearing down the server-side consumer. If a caller drops the watcher
        // without calling `stop()`, cancel the pump and finish the stream so the
        // unstructured `pumpTask` (which strongly holds the `OrderedConsumer`)
        // cannot run forever and leak the subscription/ephemeral consumer.
        // `deinit` cannot `await`, so the ordered consumer is torn down
        // fire-and-forget; failing that, the server reaps it after its
        // `inactiveThreshold`.
        let task = stateLock.withLockScoped { pumpTask }
        task?.cancel()
        continuation.finish()
        let consumer = self.consumer
        Task { await consumer.stop() }
    }

    // MARK: - Pump

    /// Consumes the ordered stream, applies the marker and `ignoreDeletes` logic,
    /// and feeds entries into `continuation`. Runs as a single detached task, so
    /// its accounting state stays local and race-free.
    private static func pump(
        consumer: OrderedConsumer,
        continuation: AsyncThrowingStream<KeyValueEntry?, Error>.Continuation,
        client: NatsClient,
        bucket: String,
        ignoreDeletes: Bool,
        updatesOnly: Bool
    ) async {
        // The ordered consumer was already started (fail-fast) by `start()` before
        // this pump was spawned, so its `initialPending()` is already populated.
        var initDone = false
        var received: UInt64 = 0
        var initPending: UInt64 = 0

        if updatesOnly {
            // No initial values: emit the end-of-initial marker up front and
            // never run the per-message accounting below.
            continuation.yield(nil)
            initDone = true
        } else {
            // Use the stable numPending captured at the ordered consumer's first
            // create. An empty match set fires the marker immediately.
            initPending = await initialPending(from: consumer)
            if initPending == 0 {
                continuation.yield(nil)
                initDone = true
            }
        }

        do {
            for try await message in consumer.natsMessages {
                guard
                    let meta = try? JetStreamMessage(message: message, client: client).metadata()
                else {
                    continue
                }
                let entry = KeyValueCoding.entry(from: message, metadata: meta, bucket: bucket)

                // Yield unless this is a tombstone filtered out by ignoreDeletes.
                let isTombstone = entry.operation != .put
                if !(ignoreDeletes && isTombstone) {
                    continuation.yield(entry)
                }

                // Marker accounting runs for EVERY delivery (tombstones included)
                // until the marker fires — deliberately outside the ignoreDeletes
                // guard above. Fire on the first of: this being the last pending
                // entry (delta == 0), or having received the initial count (a
                // backstop when numPending was stale against a growing bucket).
                if !initDone {
                    received += 1
                    if received >= initPending || entry.delta == 0 {
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

    /// Polls the ordered consumer for the `numPending` captured at its first
    /// successful create. Returns 0 if the task is cancelled before then.
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
