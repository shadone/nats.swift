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

/// Batched asynchronous JetStream publisher.
///
/// Owns ONE shared wildcard subscription (`<inbox>.*`) for all publish-ack replies plus a bounded
/// in-flight window (`maxPending`). Instead of the synchronous
/// ``JetStreamContext/publish(_:message:headers:msgTTL:)`` — which opens a fresh subscription per
/// message and awaits each ack serially — this fires publishes back to back, correlating each reply
/// by a per-message token embedded in its reply subject, and only stalls when the window is full.
///
/// The subscription and its ack pump start lazily on the first publish; construction is cheap and
/// performs no I/O.
///
/// `@unchecked Sendable`: ALL mutable state is guarded by a single `NSLock` (`lock`). This replaces an
/// earlier `actor` implementation to remove the per-message actor hop that serialized every publish
/// AND every ack. The discipline the actor provided for free is now maintained by hand:
///  - A continuation is NEVER resumed while `lock` is held: each method mutates state / collects the
///    continuations to resume into locals under the lock, releases the lock, THEN resumes them. A
///    resume under the lock could deadlock or re-enter a locked region.
///  - The backpressure "is the window full?" check and its action (reserve a slot OR park) happen in
///    the SAME locked critical section (see ``tryReserveLocked(prefix:)`` / ``reserveSlot(prefix:)``),
///    so a slot freeing between the check and the park cannot lose a wakeup.
///  - No `await` ever happens while `lock` is held (`NSLock` is not async-aware).
final class JetStreamPublishAsync: @unchecked Sendable {
    private let client: NatsClient
    private let timeout: TimeInterval
    private let maxPending: Int

    /// Nanoseconds a reservation may live before the reaper reclaims its window slot.
    private let timeoutNanos: UInt64
    /// How often the shared reaper wakes to sweep expired reservations.
    private let reaperIntervalNanos: UInt64

    /// The one lock guarding EVERY mutable field below. Held only across synchronous critical
    /// sections — never across an `await`.
    private let lock = NSLock()

    private var inboxPrefix: String?
    private var sub: NatsSubscription?
    private var pumpTask: Task<Void, Never>?
    private var reaperTask: Task<Void, Never>?
    private var startTask: Task<Void, Error>?
    private var isShutdown = false

    /// In-flight publishes awaiting an ack, keyed by the per-message token. `removeValue` on this map
    /// is the single resolve-once guard: whoever removes a token (pump, reaper, or a send failure)
    /// owns finishing it.
    private var acks: [String: PubAckBox] = [:]

    /// Per-token expiry deadline (`DispatchTime` uptime nanos). One background reaper fails any box
    /// past its deadline — reclaiming its window slot — so a lost publish frame (e.g. buffered but not
    /// flushed across a transient reconnect) can't wedge the window forever. Bounded WITHOUT a
    /// per-message timer task.
    private var deadlines: [String: UInt64] = [:]
    private var counter: UInt64 = 0

    /// Publishers parked because the window is full. Each is resumed with `nil` when a slot frees (a
    /// retry signal); the woken caller loops and re-reserves under the lock. Each freed slot wakes
    /// exactly one, and admission is always re-gated by the lock, so the window never overshoots.
    private var stallWaiters: [StallContinuation] = []

    /// Waiters parked in `complete(timeout:)`, keyed by a monotonic id so both the drain path
    /// (`finish`) and the timeout path (`timeoutComplete`) can resume a specific one exactly once.
    private var completeWaiters: [UInt64: CompleteContinuation] = [:]
    private var completeCounter: UInt64 = 0
    private var timedOutCompletes: Set<UInt64> = []

    /// A reserved window slot: the token used to correlate the ack, the box the caller awaits, and the
    /// fully-formed reply subject to publish under.
    private struct Reservation {
        let token: String
        let box: PubAckBox
        let reply: String
    }

    /// The outcome handed to a parked (backpressure-stalled) publisher when it is resumed: a slot
    /// freed (retry the reservation), or the publisher shut down (fail rather than admit a new box
    /// that nothing would resolve — the reaper and pump are gone once shut down).
    private enum ReserveOutcome {
        case reserved(Reservation)
        case retry
        case shutdown
    }

    private typealias StallContinuation = CheckedContinuation<ReserveOutcome, Never>
    private typealias CompleteContinuation = CheckedContinuation<Void, Never>

    init(client: NatsClient, timeout: TimeInterval, maxPending: Int = 4000) {
        self.client = client
        self.timeout = timeout
        self.maxPending = maxPending
        self.timeoutNanos = UInt64(timeout * 1_000_000_000)
        self.reaperIntervalNanos = UInt64(max(1.0, timeout / 2.0) * 1_000_000_000)
    }

    // MARK: - Startup

    /// Starts the shared ack subscription and pump exactly once. Concurrent first publishers all
    /// await the SAME start task, so no publish proceeds before the subscription is live (which would
    /// otherwise drop the ack). A failed start is cleared so a later publish can retry.
    private func ensureStarted() async throws {
        // Get-or-create the shared start task atomically, then await it OUTSIDE the lock.
        let task: Task<Void, Error> = lock.withLockScoped {
            if let startTask {
                return startTask
            }
            let created = Task { [weak self] in
                guard let self else { return }
                try await self.startInternal()
            }
            startTask = created
            return created
        }
        do {
            try await task.value
        } catch {
            // Clear the failed start task so a later publish can retry.
            lock.withLockScoped { startTask = nil }
            throw error
        }
    }

    private func startInternal() async throws {
        let prefix = client.newInbox()
        let sub = try await client.subscribe(subject: "\(prefix).*")
        let pump: Task<Void, Never> = Task { [weak self] in
            await self?.pump(sub: sub, prefix: prefix)
        }
        // One shared reaper (not a timer per message): periodically fail boxes past their deadline.
        let interval = reaperIntervalNanos
        let reaper: Task<Void, Never> = Task { [weak self] in
            while true {
                try? await Task.sleep(nanoseconds: interval)
                guard let self, self.reapExpired() else { return }
            }
        }
        // Publish all four fields in one critical section so no method observes a half-started state.
        // If `shutdown()` ran while we were suspended in `subscribe` above, do NOT install a
        // subscription/pump/reaper it already tore past — tear down what we just created instead, so a
        // shutdown during first-publish startup can't resurrect and leak a live ack pump.
        let installed: Bool = lock.withLockScoped {
            if isShutdown { return false }
            inboxPrefix = prefix
            self.sub = sub
            pumpTask = pump
            reaperTask = reaper
            return true
        }
        if !installed {
            pump.cancel()
            reaper.cancel()
            try? await sub.unsubscribe()
        }
    }

    /// Fails every box whose deadline has passed (reclaiming its window slot). Returns `false` once the
    /// publisher is shut down, ending the reaper loop. Tokens are collected under the lock; the actual
    /// `finish` calls run OUTSIDE it (each re-takes the lock and is resolve-once).
    private func reapExpired() -> Bool {
        let now = DispatchTime.now().uptimeNanoseconds
        let expired: [String]? = lock.withLockScoped {
            if isShutdown { return nil }
            return deadlines.filter { $0.value <= now }.map(\.key)
        }
        guard let expired else { return false }
        for token in expired {
            finish(token, .failure(JetStreamError.RequestError.timeout))
        }
        return true
    }

    // MARK: - Publish

    /// Publishes `message` and returns a ``PubAckFuture`` that resolves when the server acks (or the
    /// publish fails / times out). Stalls when `maxPending` acks are already in flight.
    func publishAsync(
        _ subject: String, message: Data, headers: NatsHeaderMap? = nil,
        msgTTL: NanoTimeInterval? = nil, keepAlive: JetStreamContext? = nil
    ) async throws -> PubAckFuture {
        try await ensureStarted()
        guard let prefix = lock.withLockScoped({ inboxPrefix }) else {
            throw NatsError.ClientError.internalError("publishAsync subscription not started")
        }

        // Same per-message TTL handling as the synchronous publish path.
        var headers = headers
        if let msgTTL, msgTTL.value > 0 {
            var withTTL = headers ?? NatsHeaderMap()
            withTTL[.natsMsgTTL] = NatsHeaderValue(msgTTL.goDurationString())
            headers = withTTL
        }

        // Backpressure: reserve a window slot (allocating token + box + deadline atomically with the
        // window check), parking while the window is full. Never lets more than `maxPending` through.
        // Throws if the publisher was shut down (rather than returning a doomed reservation).
        let reservation = try await reserveSlot(prefix: prefix)

        do {
            try await client.publish(
                message, subject: subject, reply: reservation.reply, headers: headers)
        } catch {
            // A send failure both resolves the box and is surfaced to the caller.
            finish(reservation.token, .failure(error))
            throw error
        }

        return PubAckFuture(box: reservation.box, keepAlive: keepAlive)
    }

    /// Reserves a window slot, parking the caller while the window is full.
    ///
    /// The reserve-or-park decision is made ATOMICALLY under the lock: either a slot is free (allocate
    /// and return it) or the caller is appended to `stallWaiters` in the SAME critical section — so a
    /// slot freeing between "check" and "park" cannot be lost. A parked caller is later resumed with
    /// `nil` (by `finish`/`failEverything`) and loops to re-reserve; admission is re-gated by the lock
    /// on every attempt, so the window never overshoots `maxPending`.
    private func reserveSlot(prefix: String) async throws -> Reservation {
        // Fast path: reserve immediately with NO continuation/suspension when the window has room. A
        // shut-down publisher fails here rather than admitting a box the (cancelled) reaper and pump
        // would never resolve.
        let fast: ReserveOutcome = lock.withLockScoped {
            if isShutdown { return .shutdown }
            if let reservation = tryReserveLocked(prefix: prefix) { return .reserved(reservation) }
            return .retry
        }
        switch fast {
        case .shutdown: throw NatsError.ClientError.connectionClosed
        case .reserved(let reservation): return reservation
        case .retry: break
        }
        // Slow path: window full — park, and retry each time a slot is handed to us. If `shutdown()`
        // wakes us instead (via `failEverything`), fail rather than re-park (which would hang) or
        // reserve a doomed slot.
        while true {
            let outcome: ReserveOutcome = await withCheckedContinuation {
                (cont: StallContinuation) in
                let immediate: ReserveOutcome? = lock.withLockScoped {
                    if isShutdown { return .shutdown }
                    if let reservation = tryReserveLocked(prefix: prefix) {
                        return .reserved(reservation)
                    }
                    // Window full: park this caller. `finish` resumes it with `.retry`;
                    // `failEverything` (close/shutdown) with `.shutdown`.
                    stallWaiters.append(cont)
                    return nil
                }
                if let immediate {
                    cont.resume(returning: immediate)
                }
                // else parked; resumed later → the loop acts on the outcome.
            }
            switch outcome {
            case .reserved(let reservation): return reservation
            case .shutdown: throw NatsError.ClientError.connectionClosed
            case .retry: continue
            }
        }
    }

    /// MUST hold `lock`. Allocates a token + box + deadline and returns the reservation when the window
    /// has room. Returns `nil` when the window is full (the caller parks) or the publisher is shutting
    /// down (the caller fails via ``reserveSlot(prefix:)``'s shutdown check — a box admitted after
    /// shutdown would never be resolved, the reaper and pump being gone).
    private func tryReserveLocked(prefix: String) -> Reservation? {
        guard !isShutdown, acks.count < maxPending else { return nil }
        counter += 1
        let token = String(counter)
        let box = PubAckBox()
        acks[token] = box
        // Record an expiry deadline reclaimed by the shared reaper (no per-message timer task). While
        // connected the ack normally resolves the box first; the deadline only bounds the pathological
        // case where a frame is buffered-but-not-delivered across a reconnect and no reply ever comes.
        deadlines[token] = DispatchTime.now().uptimeNanoseconds + timeoutNanos
        return Reservation(token: token, box: box, reply: "\(prefix).\(token)")
    }

    // MARK: - Ack pump

    /// Reads every ack reply off the shared subscription and resolves the matching box. On the
    /// subscription ending (unsubscribe / connection close) or throwing, all still-pending boxes are
    /// failed so no ``PubAckFuture/wait()`` hangs.
    private func pump(sub: NatsSubscription, prefix: String) async {
        let dropCount = prefix.count + 1  // "<prefix>." → token
        do {
            for try await msg in sub {
                let token = String(msg.subject.dropFirst(dropCount))
                let result = Result { try Ack.decodeAck(from: msg) }
                finish(token, result)
            }
            failEverything(NatsError.ClientError.connectionClosed)
        } catch {
            failEverything(error)
        }
    }

    // MARK: - Resolution

    /// Resolves the box for `token` exactly once (guarded by `removeValue`), clears its deadline, wakes
    /// one stalled publisher, and signals completion waiters once the window fully drains. State is
    /// mutated and the continuations to resume are collected under the lock; the box resolve and all
    /// continuation resumes happen OUTSIDE the lock.
    private func finish(_ token: String, _ result: Result<Ack, Error>) {
        let work: (PubAckBox, StallContinuation?, [CompleteContinuation])? = lock.withLockScoped {
            guard let box = acks.removeValue(forKey: token) else { return nil }
            deadlines.removeValue(forKey: token)
            // A slot just freed: wake exactly one parked publisher (it re-reserves under the lock).
            let stall = stallWaiters.popLast()
            var completers: [CompleteContinuation] = []
            if acks.isEmpty && !completeWaiters.isEmpty {
                completers = Array(completeWaiters.values)
                completeWaiters.removeAll()
            }
            return (box, stall, completers)
        }
        guard let (box, stall, completers) = work else { return }
        box.resolve(result)
        stall?.resume(returning: .retry)
        for completer in completers {
            completer.resume()
        }
    }

    /// Fails every in-flight box AND wakes every parked publisher / completion waiter — used when the
    /// pump ends (connection close) or on shutdown, so NOTHING is left suspended: not the boxes, not a
    /// backpressure-stalled `publishAsync`, not a parked `complete(timeout:)`. Idempotent. All resumes
    /// happen OUTSIDE the lock.
    private func failEverything(_ error: Error) {
        let work: ([PubAckBox], [StallContinuation], [CompleteContinuation]) = lock.withLockScoped {
            let boxes = Array(acks.values)
            acks.removeAll()
            deadlines.removeAll()
            let stalled = stallWaiters
            stallWaiters.removeAll()
            let completers = Array(completeWaiters.values)
            completeWaiters.removeAll()
            return (boxes, stalled, completers)
        }
        for box in work.0 {
            box.resolve(.failure(error))
        }
        // Wake every backpressure-parked publisher with `.shutdown` so each fails fast (its publish
        // could otherwise succeed on a still-live connection but its box would never be resolved once
        // the pump/reaper are gone). Without this wake, a publisher parked at close would hang forever.
        for cont in work.1 {
            cont.resume(returning: .shutdown)
        }
        for cont in work.2 {
            cont.resume()
        }
    }

    // MARK: - Introspection / flush

    /// Number of acks currently in flight (published but not yet acked/failed/timed out).
    ///
    /// `async` only to preserve the call-site signature the actor exposed (the body never suspends).
    func pending() async -> Int {
        lock.withLockScoped { acks.count }
    }

    /// Awaits until every in-flight publish has been acked/failed, or throws
    /// ``JetStreamError/RequestError/timeout`` if that takes longer than `timeout`.
    ///
    /// A drain (`finish` reaching an empty window) and the sleeper race, but both resume the SAME
    /// keyed continuation and each path removes the key first, so it is resumed exactly once.
    func complete(timeout: TimeInterval) async throws {
        if lock.withLockScoped({ acks.isEmpty }) { return }

        let id: UInt64 = lock.withLockScoped {
            completeCounter += 1
            return completeCounter
        }

        let sleeper = Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            self?.timeoutComplete(id: id)
        }

        await withCheckedContinuation { (cont: CompleteContinuation) in
            // Re-check the window UNDER the lock, atomically with parking: unlike the actor version,
            // the lock was released after the `acks.isEmpty` fast-path above, so `finish` could have
            // drained the window in between. Parking unconditionally here would lose that wakeup.
            let resumeNow: Bool = lock.withLockScoped {
                if acks.isEmpty {
                    return true
                }
                completeWaiters[id] = cont
                return false
            }
            if resumeNow {
                cont.resume()
            }
        }

        sleeper.cancel()

        if lock.withLockScoped({ timedOutCompletes.remove(id) != nil }) {
            throw JetStreamError.RequestError.timeout
        }
    }

    /// Timeout path for `complete(timeout:)`: resume the still-parked waiter (if any) and mark it as
    /// timed out so the awaiting call throws. Resolve-once via `removeValue`; resumes outside the lock.
    private func timeoutComplete(id: UInt64) {
        let cont: CompleteContinuation? = lock.withLockScoped {
            guard let cont = completeWaiters.removeValue(forKey: id) else { return nil }
            timedOutCompletes.insert(id)
            return cont
        }
        cont?.resume()
    }

    // MARK: - Teardown

    /// Unsubscribes, cancels the pump and reaper, fails every pending box, and wakes any parked
    /// publishers/completers so nothing hangs. Idempotent.
    func shutdown() async {
        // Flip the shutdown flag and detach the tasks/subscription in one critical section. A `nil`
        // result means we were already shut down — nothing more to do.
        let teardown: (Task<Void, Never>?, Task<Void, Never>?, NatsSubscription?)? =
            lock.withLockScoped {
                if isShutdown { return nil }
                isShutdown = true
                let pump = pumpTask
                pumpTask = nil
                let reaper = reaperTask
                reaperTask = nil
                let subToClose = sub
                sub = nil
                return (pump, reaper, subToClose)
            }
        guard let (pump, reaper, subToClose) = teardown else { return }

        pump?.cancel()
        reaper?.cancel()

        // Fails all boxes and wakes every stalled publisher + parked completer.
        failEverything(NatsError.ClientError.connectionClosed)

        if let subToClose {
            try? await subToClose.unsubscribe()
        }
    }
}

extension JetStreamContext {

    /// Publishes a message on a stream subject and returns a re-awaitable future for the server ack,
    /// WITHOUT opening a per-message subscription or blocking on the ack.
    ///
    /// All async publishes on this context share one wildcard ack subscription and a bounded in-flight
    /// window (default 4000). Fire many `publishAsync` calls back to back for throughput, awaiting the
    /// returned futures later (or calling ``publishAsyncComplete(timeout:)`` to flush). When the window
    /// is full this call applies backpressure — it suspends until an in-flight ack resolves.
    ///
    /// Ordering: acks are NOT guaranteed to resolve in publish order. Await each ``PubAckFuture`` for
    /// its own result.
    ///
    /// - Parameters:
    ///   - subject: Subject on which the message will be published.
    ///   - message: NATS message payload.
    ///   - headers: Optional set of message headers.
    ///   - msgTTL: Optional per-message time-to-live, matching
    ///     ``publish(_:message:headers:msgTTL:)`` (sent as the `Nats-TTL` header; values `<= 0`
    ///     ignored).
    /// - Returns: A ``PubAckFuture`` whose ``PubAckFuture/wait()`` yields the ``Ack`` or throws the
    ///   publish failure (CAS/wrong-last-sequence error, stream-not-found, send failure, or timeout).
    public func publishAsync(
        _ subject: String, message: Data, headers: NatsHeaderMap? = nil,
        msgTTL: NanoTimeInterval? = nil
    ) async throws -> PubAckFuture {
        // `keepAlive: self` makes the returned future retain this context, so it cannot be
        // deallocated (firing `deinit`'s `shutdown()`) while the caller still holds an outstanding
        // publish future -- which would otherwise spuriously fail an already-committed publish.
        try await asyncPublisher.publishAsync(
            subject, message: message, headers: headers, msgTTL: msgTTL, keepAlive: self)
    }

    /// Number of async publishes currently in flight (published but not yet acked/failed/timed out).
    public func publishAsyncPending() async -> Int {
        await asyncPublisher.pending()
    }

    /// Awaits until all in-flight async publishes have resolved, or throws
    /// ``JetStreamError/RequestError/timeout`` after `timeout` seconds.
    public func publishAsyncComplete(timeout: TimeInterval = 30) async throws {
        try await asyncPublisher.complete(timeout: timeout)
    }
}
