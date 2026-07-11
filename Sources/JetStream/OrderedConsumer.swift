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

/// The advance point of an ordered consumer.
///
/// The whole no-loss / no-dup guarantee reduces to one invariant: ``streamSeq`` advances ONLY when
/// a message is accepted (and therefore yielded to the caller). It must never advance for a
/// discarded/gapped message — doing so would let the recreate's `optStartSeq` (= ``streamSeq`` + 1)
/// skip an undelivered message (silent loss); advancing it too late re-delivers (dup).
/// ``evaluate(deliverSeq:streamSeq:)`` is the single place that enforces the invariant.
internal struct OrderedConsumerCursor: Equatable {
    /// Stream sequence of the last message YIELDED to the caller — the restart point.
    internal private(set) var streamSeq: UInt64 = 0

    /// Deliver (consumer) sequence of the last accepted message. Reset to 0 on every recreate.
    internal var consumerSeq: UInt64 = 0

    internal enum Decision: Equatable {
        case accept
        case gap
    }

    /// Evaluates the next data message against the expected deliver sequence.
    ///
    /// On a contiguous message (`deliverSeq == consumerSeq + 1`) both sequences advance and the
    /// message is accepted. On any gap nothing advances and the caller must discard and reset.
    internal mutating func evaluate(deliverSeq: UInt64, streamSeq: UInt64) -> Decision {
        if deliverSeq != consumerSeq + 1 {
            return .gap
        }
        consumerSeq = deliverSeq
        self.streamSeq = streamSeq
        return .accept
    }
}

/// A production-grade ordered push consumer.
///
/// An ephemeral, memory-backed push consumer with flow control and idle heartbeats that transparently
/// recreates itself whenever delivery is interrupted (a data-sequence gap, a missed heartbeat, a
/// heartbeat sequence mismatch, a 409 `Consumer Deleted` / `Leadership Change`, or a connection
/// error) and resumes from exactly where it left off — no acknowledgements, no redeliveries, no
/// gaps, no duplicates. This mirrors nats.go's transport-agnostic reset (`ordered.go`) layered on
/// the legacy push delivery logic (`checkOrderedMsgs`, `checkForSequenceMismatch`,
/// `resetOrderedConsumer`).
///
/// ## Concurrency model
/// An `actor` owns all mutable state; exactly one long-lived pump ``Task`` drives the deliver-inbox
/// subscription (via ``PushConsumer``) and calls back into the actor to mutate state and emit
/// output. Output flows through an `AsyncThrowingStream`. This eliminates data races by type, gives
/// a single cancellation path, and reuses the repo's `nextWithTimeout` race idiom for missed
/// heartbeats.
internal actor OrderedConsumer {
    // MARK: Immutable configuration

    private let ctx: JetStreamContext
    private let streamName: String
    private let filterSubject: String?
    private let initialDeliverPolicy: DeliverPolicy
    private let initialOptStartSeq: UInt64?
    private let headersOnly: Bool
    private let inactiveThreshold: NanoTimeInterval
    private let namePrefix: String
    private let idleHeartbeatSeconds: TimeInterval

    // MARK: Output

    /// The ordered stream of delivered messages. Iterate this after calling ``start()``.
    internal nonisolated let messages: AsyncThrowingStream<NatsMessage, Error>
    private let continuation: AsyncThrowingStream<NatsMessage, Error>.Continuation

    // MARK: Mutable state (actor-isolated)

    private var cursor = OrderedConsumerCursor()
    private var serial: Int = 0
    /// The `num_pending` reported by the ConsumerInfo of the FIRST successful create, captured
    /// before any delivery. `nil` until that create completes; never updated by later recreates.
    /// The KV watcher reads it to size its end-of-initial-values accounting.
    private var initialNumPending: UInt64?
    private var resetInProgress = false
    private var startInvoked = false
    private var closed = false
    private var disconnected = false
    private var currentSub: NatsSubscription?
    private var currentName: String?
    private var pumpTask: Task<Void, Never>?
    private var connListenerId: String?

    internal init(
        ctx: JetStreamContext,
        streamName: String,
        filterSubject: String? = nil,
        deliverPolicy: DeliverPolicy = .all,
        optStartSeq: UInt64? = nil,
        headersOnly: Bool = false,
        inactiveThreshold: NanoTimeInterval = NanoTimeInterval(5 * 60),
        namePrefix: String? = nil,
        idleHeartbeat: TimeInterval = 5
    ) {
        self.ctx = ctx
        self.streamName = streamName
        self.filterSubject = filterSubject
        self.initialDeliverPolicy = deliverPolicy
        self.initialOptStartSeq = optStartSeq
        self.headersOnly = headersOnly
        self.inactiveThreshold = inactiveThreshold
        self.namePrefix = namePrefix ?? "ord\(nextNuid())"
        self.idleHeartbeatSeconds = idleHeartbeat
        let (stream, continuation) = AsyncThrowingStream.makeStream(
            of: NatsMessage.self, throwing: Error.self)
        self.messages = stream
        self.continuation = continuation
    }

    // MARK: Lifecycle

    /// Creates the FIRST consumer synchronously (failing fast) and then starts the delivery pump.
    ///
    /// The initial creation is a single attempt: if the stream/bucket does not exist, the filter
    /// subject is invalid, or permission is denied, the error is thrown out of `start()` rather than
    /// the pump wedging forever in the capped-infinite-backoff loop used for POST-success resets.
    /// This mirrors nats.go, which creates the subscription synchronously in `WatchFiltered` and
    /// returns its error. The pump only begins its read loop AFTER a successful first creation.
    ///
    /// On a first-creation failure nothing is left half-alive: the connection listener is dropped,
    /// the `messages` stream is finished by throwing, and no subscription/consumer/`Task` leaks
    /// (`recreate` tears down any subscription it created for the failed attempt).
    ///
    /// Idempotent; a no-op once started or after ``stop()``.
    internal func start() async throws {
        guard !startInvoked, !closed else {
            return
        }
        // Set synchronously before the first `await` so the actor serializes concurrent `start()`
        // calls to exactly one initial creation.
        startInvoked = true
        registerConnectionListener()
        do {
            try await recreate(maxAttempts: 1)
        } catch {
            closed = true
            if let id = connListenerId {
                ctx.client.off(id)
                connListenerId = nil
            }
            continuation.finish(throwing: error)
            throw error
        }
        pumpTask = Task { [weak self] in
            await self?.pump()
        }
    }

    /// Stops delivery: cancels the pump, finishes the stream, tears down the subscription and
    /// fire-and-forget deletes the current consumer. Idempotent.
    internal func stop() async {
        if closed {
            return
        }
        closed = true
        pumpTask?.cancel()
        pumpTask = nil
        if let id = connListenerId {
            ctx.client.off(id)
            connListenerId = nil
        }
        continuation.finish()
        if let sub = currentSub {
            try? await sub.unsubscribe()
            currentSub = nil
        }
        deleteConsumerBestEffort(currentName)
        currentName = nil
    }

    deinit {
        // Best-effort teardown for a consumer dropped without `stop()`. `deinit` cannot `await`, so
        // the subscription cannot be cleanly unsubscribed here; the server consumer is deleted
        // fire-and-forget and, failing that, reaped by `inactiveThreshold`. The primary leak fix is
        // the `closed` re-check in `recreate()`, which keeps `currentSub`/`currentName` unset once
        // `stop()`/`terminate(_:)` has run.
        pumpTask?.cancel()
        continuation.finish()
        if let name = currentName {
            let ctx = self.ctx
            let stream = streamName
            Task { try? await ctx.deleteConsumer(stream: stream, name: name) }
        }
    }

    /// The name of the currently active server-side consumer, if any. Exposed for tests.
    internal func currentConsumerName() -> String? {
        currentName
    }

    /// The `num_pending` captured from the first successful consumer creation, or `nil` if the
    /// consumer has not been created yet. Stable across recreates; used by the KV watcher to detect
    /// when all initial values have been delivered.
    internal func initialPending() -> UInt64? {
        initialNumPending
    }

    // MARK: Reset guard

    /// Single guarded entry that all reset triggers funnel through. Returns `true` for exactly one
    /// caller per generation: subsequent near-simultaneous triggers (deduped by `resetInProgress`)
    /// and stale triggers (deduped by `serial`) return `false`, so two triggers produce one recreate.
    ///
    /// This `resetInProgress`/`serial` guard is forward-looking and defensive: with the current
    /// single-pump-`Task` architecture there is exactly one reset caller, so it dedups no real
    /// concurrent race today — unlike nats.go, whose reset is driven by a separate activity-check
    /// timer (`activityCheck`) that can race the delivery path. It is kept for parity with nats.go
    /// and to stay correct if a second reset driver is ever added.
    internal func beginReset(triggeredBySerial serial: Int) -> Bool {
        guard !closed, !resetInProgress, serial == self.serial else {
            return false
        }
        resetInProgress = true
        return true
    }

    // MARK: Per-message accept-check

    internal enum DataDecision: Equatable {
        case yielded
        case ignored
        case gap
    }

    /// The accept-check (mirrors nats.go `checkOrderedMsgs`). On a contiguous message it advances the
    /// cursor and yields; on a gap it discards WITHOUT advancing `streamSeq` so the recreate resumes
    /// from `streamSeq + 1` and redelivers the missed message.
    internal func handleData(_ msg: NatsMessage) -> DataDecision {
        guard let meta = try? JetStreamMessage(message: msg, client: ctx.client).metadata() else {
            return .ignored
        }
        // Ignore stray messages from a previous consumer generation.
        if let messageSerial = OrderedConsumer.serial(fromConsumerName: meta.consumer),
            messageSerial != serial
        {
            return .ignored
        }
        switch cursor.evaluate(deliverSeq: meta.consumerSequence, streamSeq: meta.streamSequence) {
        case .accept:
            continuation.yield(msg)
            return .yielded
        case .gap:
            return .gap
        }
    }

    /// The heartbeat sequence-mismatch check (mirrors nats.go `checkForSequenceMismatch`): if the
    /// heartbeat's `Nats-Last-Consumer` (the server's last delivered deliver-seq) is ahead of our
    /// accepted deliver-seq, a data message was dropped and the consumer must be recreated.
    internal func heartbeatIndicatesGap(_ msg: NatsMessage) -> Bool {
        // Mirror nats.go `checkForSequenceMismatch`, which returns early when its control metadata
        // is empty (`ctrl == _EMPTY_`): until the (re)created consumer has established a
        // `Nats-Last-Consumer` baseline there is nothing to compare against, so an absent or empty
        // header is NOT a gap. This avoids a spurious immediate reset loop when an idle heartbeat
        // arrives before the first data message on a slow-starting producer.
        guard let raw = msg.headers?.get(.natsLastConsumer)?.description, !raw.isEmpty,
            let lastConsumerSeq = UInt64(raw)
        else {
            return false
        }
        return lastConsumerSeq != cursor.consumerSeq
    }

    // MARK: Pump

    private func pump() async {
        // The FIRST consumer was already created (fail-fast) by `start()`, which sets `currentSub`
        // before spawning this pump; the loop below reads from it directly. Only RESET recreates
        // (post-success) run inside this loop, and those keep the infinite capped backoff.
        while !closed {
            guard let sub = currentSub else {
                break
            }
            let generationSerial = serial
            let push = PushConsumer(
                client: ctx.client, subscription: sub,
                idleHeartbeatSeconds: idleHeartbeatSeconds)
            var needsReset = false

            reading: while !closed {
                let event: PushConsumer.Event
                do {
                    event = try await push.next()
                } catch {
                    // Connection / subscription error → recreate.
                    needsReset = beginReset(triggeredBySerial: generationSerial)
                    break reading
                }

                switch event {
                case .message(let msg):
                    switch handleData(msg) {
                    case .yielded, .ignored:
                        continue reading
                    case .gap:
                        needsReset = beginReset(triggeredBySerial: generationSerial)
                        break reading
                    }
                case .idleHeartbeat(let msg):
                    if heartbeatIndicatesGap(msg) {
                        needsReset = beginReset(triggeredBySerial: generationSerial)
                        break reading
                    }
                case .missedHeartbeat:
                    // Suppress while disconnected: the backoff create waits for reconnect instead
                    // of storming the server with failing creates.
                    if disconnected {
                        continue reading
                    }
                    needsReset = beginReset(triggeredBySerial: generationSerial)
                    break reading
                case .consumerDeleted, .leadershipChanged:
                    needsReset = beginReset(triggeredBySerial: generationSerial)
                    break reading
                case .closed:
                    if closed {
                        break reading
                    }
                    needsReset = beginReset(triggeredBySerial: generationSerial)
                    break reading
                }
            }

            if closed {
                break
            }
            if needsReset {
                do {
                    try await recreate()
                } catch {
                    break  // closed during backoff
                }
            } else {
                break
            }
        }

        continuation.finish()
    }

    // MARK: Recreate

    /// Number of replicas for a recreated ordered consumer (always single-replica ephemeral).
    private static let recreateReplicas = 1

    /// Extracts the serial suffix from a consumer name of the form `<prefix>_<serial>`.
    private static func serial(fromConsumerName name: String) -> Int? {
        guard let last = name.split(separator: "_").last, let value = Int(last) else {
            return nil
        }
        return value
    }

    private func deleteConsumerBestEffort(_ name: String?) {
        guard let name else {
            return
        }
        let ctx = self.ctx
        let stream = streamName
        // Fire-and-forget (the caller never awaits), but retry a bounded number of times: a single
        // delete request can time out under rapid teardown load or lose a narrow race with the
        // server finishing the create. `Task {}` does not inherit the pump's cancellation, so this
        // still runs after `stop()` cancels the pump. The inactive threshold reaps anything the
        // retries still miss.
        Task {
            for attempt in 0..<3 {
                do {
                    try await ctx.deleteConsumer(stream: stream, name: name)
                    return
                } catch {
                    if attempt < 2 {
                        try? await Task.sleep(nanoseconds: 100_000_000)
                    }
                }
            }
        }
    }

    private func buildConfig(deliverSubject: String) -> ConsumerConfig {
        var cfg = ConsumerConfig(
            name: "\(namePrefix)_\(serial)",
            deliverPolicy: .byStartSequence,
            ackPolicy: .none,
            filterSubject: filterSubject,
            headersOnly: headersOnly ? true : nil,
            inactiveThreshold: inactiveThreshold,
            replicas: OrderedConsumer.recreateReplicas,
            memoryStorage: true,
            deliverSubject: deliverSubject,
            flowControl: true,
            idleHeartbeat: NanoTimeInterval(idleHeartbeatSeconds)
        )

        if cursor.streamSeq == 0 {
            // First creation: use the original deliver policy.
            cfg.deliverPolicy = initialDeliverPolicy
            switch initialDeliverPolicy {
            case .byStartSequence:
                cfg.optStartSeq = initialOptStartSeq
            default:
                cfg.optStartSeq = nil
            }
        } else {
            // Every later recreate resumes from the next stream sequence.
            cfg.deliverPolicy = .byStartSequence
            cfg.optStartSeq = cursor.streamSeq + 1
        }
        return cfg
    }
}

extension OrderedConsumer {
    /// Async (re)create implementation. Kept in an extension so the state-machine body above stays
    /// focused; behaviour matches nats.go `resetOrderedConsumer` + `getConsumerConfig`.
    ///
    /// - Parameter maxAttempts: the number of create attempts before giving up. `nil` (the default)
    ///   retries forever with capped backoff — the correct behaviour for a POST-success reset of a
    ///   consumer that was already working. A bounded value is used for the FAIL-FAST first creation
    ///   from ``start()``: once the attempts are exhausted the underlying error is propagated (so a
    ///   missing stream / invalid filter / permission denial surfaces immediately) instead of
    ///   hanging. `1` is a single synchronous attempt, matching nats.go's `WatchFiltered`.
    fileprivate func recreate(maxAttempts: Int? = nil) async throws {
        // Tear down the current subscription.
        if let sub = currentSub {
            try? await sub.unsubscribe()
            currentSub = nil
        }
        // Fire-and-forget delete of the old consumer; do not await.
        deleteConsumerBestEffort(currentName)
        currentName = nil

        // Reset per-generation deliver-sequence tracking and advance the serial.
        cursor.consumerSeq = 0
        serial += 1

        var interval: UInt64 = 1_000_000_000  // 1s initial backoff.
        var attempt = 0
        while true {
            if closed {
                throw JetStreamError.OrderedConsumerError.closed
            }
            do {
                // Subscribe BEFORE creating the consumer so no delivery is missed in between.
                let deliver = ctx.client.newInbox()
                let cfg = buildConfig(deliverSubject: deliver)
                let sub = try await ctx.client.subscribe(subject: deliver)

                // `stop()`/`terminate(_:)` can complete while this Task is suspended at any `await`
                // in the success path — the actor releases isolation across a suspension, and
                // `stop()` also cancels the pump `Task`, which can make an in-flight create await
                // THROW after the server already created the consumer. If we committed
                // `currentSub`/`currentName` below, nothing would tear down the subscription (for
                // the actor's lifetime) or the ephemeral server consumer (until `inactiveThreshold`,
                // ~5m). Re-check `closed` after EACH `await` and bail; the `closed` branch of the
                // outer `catch` deletes any consumer created for this serial by its deterministic
                // name, covering both a create that succeeded and one that threw on cancellation.
                if closed {
                    try? await sub.unsubscribe()
                    throw JetStreamError.OrderedConsumerError.closed
                }

                let consumer: Consumer
                do {
                    consumer = try await ctx.createOrUpdateConsumer(stream: streamName, cfg: cfg)
                } catch {
                    try? await sub.unsubscribe()
                    throw error
                }

                if closed {
                    try? await sub.unsubscribe()
                    throw JetStreamError.OrderedConsumerError.closed
                }

                currentSub = sub
                currentName = consumer.info.name
                if initialNumPending == nil {
                    initialNumPending = consumer.info.numPending
                }
                resetInProgress = false
                return
            } catch {
                if closed {
                    // We are giving up for this serial. The consumer may exist server-side even if
                    // the create await threw on cancellation after the server processed it, so
                    // delete it best-effort by its deterministic `<prefix>_<serial>` name. On a
                    // retry (not closed) we deliberately keep it: the next attempt reuses the same
                    // name and `createOrUpdateConsumer` is idempotent.
                    deleteConsumerBestEffort("\(namePrefix)_\(serial)")
                    throw JetStreamError.OrderedConsumerError.closed
                }
                attempt += 1
                if let maxAttempts, attempt >= maxAttempts {
                    // Fail-fast (first-creation) path: propagate the real error instead of
                    // entering the infinite backoff reserved for post-success resets.
                    throw error
                }
                try? await Task.sleep(nanoseconds: interval)
                interval = min(interval * 2, 10_000_000_000)
            }
        }
    }

    private func registerConnectionListener() {
        let id = ctx.client.on([.connected, .disconnected, .suspended, .closed]) {
            [weak self] event in
            guard let self else {
                return
            }
            Task { await self.handleConnectionEvent(event) }
        }
        if !id.isEmpty {
            connListenerId = id
        }
    }

    private func handleConnectionEvent(_ event: NatsEvent) async {
        switch event {
        case .closed:
            // A permanently closed client never reconnects. Folding this into `disconnected`
            // (as `.disconnected`/`.suspended` do) would make the pump suppress the
            // missed-heartbeat reset forever and loop its backoff create indefinitely, so the
            // `messages` stream would never finish and the watcher would hang until the caller
            // separately called `stop()`. Terminate instead: finish the stream with a clear error
            // and stop the pump.
            await terminate(NatsError.ClientError.connectionClosed)
        case .disconnected, .suspended:
            // Transient: suppress the reset storm and wait for a reconnect.
            disconnected = true
        case .connected:
            disconnected = false
        default:
            break
        }
    }

    /// Terminates the consumer with `error`: finishes the `messages` stream by throwing, stops the
    /// pump, and best-effort tears down the subscription and server consumer. Differs from
    /// ``stop()`` only in that the stream finishes with an error rather than completing cleanly.
    private func terminate(_ error: Error) async {
        if closed {
            return
        }
        // Set `closed` and finish the stream in one synchronous step (no `await` in between) so a
        // concurrently-suspended pump can observe `closed == true` only after the error has already
        // landed on the continuation; the pump's later plain `finish()` is then a no-op, and its
        // in-flight `recreate()` throws on its next `closed` re-check.
        closed = true
        if let id = connListenerId {
            ctx.client.off(id)
            connListenerId = nil
        }
        pumpTask?.cancel()
        pumpTask = nil
        continuation.finish(throwing: error)
        if let sub = currentSub {
            try? await sub.unsubscribe()
            currentSub = nil
        }
        deleteConsumerBestEffort(currentName)
        currentName = nil
    }
}
