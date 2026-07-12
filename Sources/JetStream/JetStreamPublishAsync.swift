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
/// performs no I/O. Every method is serialized by the actor; the pump calls back in via
/// `await self.finish(...)`.
actor JetStreamPublishAsync {
    private let client: NatsClient
    private let timeout: TimeInterval
    private let maxPending: Int

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

    /// Publishers parked because the window is full; each freed slot wakes exactly one.
    private var stallWaiters: [CheckedContinuation<Void, Never>] = []

    /// Waiters parked in `complete(timeout:)`, keyed by a monotonic id so both the drain path
    /// (`finish`) and the timeout path (`timeoutComplete`) can resume a specific one exactly once.
    private var completeWaiters: [UInt64: CheckedContinuation<Void, Never>] = [:]
    private var completeCounter: UInt64 = 0
    private var timedOutCompletes: Set<UInt64> = []

    init(client: NatsClient, timeout: TimeInterval, maxPending: Int = 4000) {
        self.client = client
        self.timeout = timeout
        self.maxPending = maxPending
    }

    // MARK: - Startup

    /// Starts the shared ack subscription and pump exactly once. Concurrent first publishers all
    /// await the SAME start task, so no publish proceeds before the subscription is live (which would
    /// otherwise drop the ack). A failed start is cleared so a later publish can retry.
    private func ensureStarted() async throws {
        if let startTask {
            try await startTask.value
            return
        }
        let task = Task { [weak self] in
            guard let self else { return }
            try await self.startInternal()
        }
        startTask = task
        do {
            try await task.value
        } catch {
            startTask = nil
            throw error
        }
    }

    private func startInternal() async throws {
        let prefix = client.newInbox()
        let sub = try await client.subscribe(subject: "\(prefix).*")
        inboxPrefix = prefix
        self.sub = sub
        pumpTask = Task { [weak self] in
            await self?.pump(sub: sub, prefix: prefix)
        }
        // One shared reaper (not a timer per message): periodically fail boxes past their deadline.
        let interval = UInt64(max(1.0, timeout / 2.0) * 1_000_000_000)
        reaperTask = Task { [weak self] in
            while true {
                try? await Task.sleep(nanoseconds: interval)
                guard let self, await self.reapExpired() else { return }
            }
        }
    }

    /// Fails every box whose deadline has passed (reclaiming its window slot). Returns `false` once the
    /// publisher is shut down, ending the reaper loop.
    private func reapExpired() -> Bool {
        if isShutdown { return false }
        let now = DispatchTime.now().uptimeNanoseconds
        let expired = deadlines.filter { $0.value <= now }.map(\.key)
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
        msgTTL: NanoTimeInterval? = nil
    ) async throws -> PubAckFuture {
        try await ensureStarted()
        guard let prefix = inboxPrefix else {
            throw NatsError.ClientError.internalError("publishAsync subscription not started")
        }

        // Same per-message TTL handling as the synchronous publish path.
        var headers = headers
        if let msgTTL, msgTTL.value > 0 {
            var withTTL = headers ?? NatsHeaderMap()
            withTTL[.natsMsgTTL] = NatsHeaderValue(msgTTL.goDurationString())
            headers = withTTL
        }

        // Backpressure: park while the in-flight window is full. Each awaited freed slot re-checks the
        // condition, so a burst of freed slots cannot let more than `maxPending` through.
        while acks.count >= maxPending {
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                stallWaiters.append(cont)
            }
        }

        counter += 1
        let token = String(counter)
        let reply = "\(prefix).\(token)"
        let box = PubAckBox()
        acks[token] = box
        // Record an expiry deadline reclaimed by the shared reaper (no per-message timer task). While
        // connected the ack normally resolves the box first; the deadline only bounds the pathological
        // case where a frame is buffered-but-not-delivered across a reconnect and no reply ever comes.
        deadlines[token] = DispatchTime.now().uptimeNanoseconds + UInt64(timeout * 1_000_000_000)

        do {
            try await client.publish(message, subject: subject, reply: reply, headers: headers)
        } catch {
            // A send failure both resolves the box and is surfaced to the caller.
            finish(token, .failure(error))
            throw error
        }

        return PubAckFuture(box: box)
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
    /// one stalled publisher, and signals completion waiters once the window fully drains.
    private func finish(_ token: String, _ result: Result<Ack, Error>) {
        guard let box = acks.removeValue(forKey: token) else { return }
        deadlines.removeValue(forKey: token)
        box.resolve(result)

        // A slot just freed: wake exactly one parked publisher.
        stallWaiters.popLast()?.resume()

        if acks.isEmpty {
            drainCompleteWaiters()
        }
    }

    /// Fails every in-flight box AND wakes every parked publisher / completion waiter — used when the
    /// pump ends (connection close) or on shutdown, so NOTHING is left suspended: not the boxes, not a
    /// backpressure-stalled `publishAsync`, not a parked `complete(timeout:)`. Idempotent.
    private func failEverything(_ error: Error) {
        let pending = acks
        acks.removeAll()
        deadlines.removeAll()
        for (_, box) in pending {
            box.resolve(.failure(error))
        }
        // Wake every backpressure-parked publisher: the window is now empty, so each re-checks the
        // condition and proceeds (its own publish will then fail on the dead connection). Without this,
        // a publisher parked in the window when the connection closed would hang forever.
        let stalled = stallWaiters
        stallWaiters.removeAll()
        for cont in stalled {
            cont.resume()
        }
        drainCompleteWaiters()
    }

    /// Resumes all drain-parked `complete(timeout:)` waiters (the window reached empty).
    private func drainCompleteWaiters() {
        guard !completeWaiters.isEmpty else { return }
        let waiters = completeWaiters
        completeWaiters.removeAll()
        for (_, cont) in waiters {
            cont.resume()
        }
    }

    // MARK: - Introspection / flush

    /// Number of acks currently in flight (published but not yet acked/failed/timed out).
    func pending() -> Int { acks.count }

    /// Awaits until every in-flight publish has been acked/failed, or throws
    /// ``JetStreamError/RequestError/timeout`` if that takes longer than `timeout`.
    ///
    /// A drain (`finish` reaching an empty window) and the sleeper race, but both resume the SAME
    /// keyed continuation and each path removes the key first, so it is resumed exactly once.
    func complete(timeout: TimeInterval) async throws {
        if acks.isEmpty { return }

        completeCounter += 1
        let id = completeCounter

        let sleeper = Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            await self?.timeoutComplete(id: id)
        }

        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            // No `await` happened since the `acks.isEmpty` guard, so the window cannot have drained
            // underneath us; park unconditionally.
            completeWaiters[id] = cont
        }

        sleeper.cancel()

        if timedOutCompletes.remove(id) != nil {
            throw JetStreamError.RequestError.timeout
        }
    }

    /// Timeout path for `complete(timeout:)`: resume the still-parked waiter (if any) and mark it as
    /// timed out so the awaiting call throws.
    private func timeoutComplete(id: UInt64) {
        guard let cont = completeWaiters.removeValue(forKey: id) else { return }
        timedOutCompletes.insert(id)
        cont.resume()
    }

    // MARK: - Teardown

    /// Unsubscribes, cancels the pump and reaper, fails every pending box, and wakes any parked
    /// publishers/completers so nothing hangs. Idempotent.
    func shutdown() async {
        if isShutdown { return }
        isShutdown = true

        pumpTask?.cancel()
        pumpTask = nil
        reaperTask?.cancel()
        reaperTask = nil

        // Fails all boxes and wakes every stalled publisher + parked completer.
        failEverything(NatsError.ClientError.connectionClosed)

        let subToClose = sub
        sub = nil
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
        try await publishAsyncActor.publishAsync(
            subject, message: message, headers: headers, msgTTL: msgTTL)
    }

    /// Number of async publishes currently in flight (published but not yet acked/failed/timed out).
    public func publishAsyncPending() async -> Int {
        await publishAsyncActor.pending()
    }

    /// Awaits until all in-flight async publishes have resolved, or throws
    /// ``JetStreamError/RequestError/timeout`` after `timeout` seconds.
    public func publishAsyncComplete(timeout: TimeInterval = 30) async throws {
        try await publishAsyncActor.complete(timeout: timeout)
    }
}
