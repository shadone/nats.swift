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

/// A raw, single-consumer source of ``JetStreamMessage`` values.
///
/// Each consumer kind (pull batch loop, push deliver subscription, ordered engine stream) provides
/// its own ``MessageSource``. ``next()`` returns the next message or `nil` when the source ends;
/// ``teardown()`` releases the underlying subscription / server consumer.
protocol MessageSource: Sendable {
    /// Returns the next message, or `nil` when the source is exhausted / torn down.
    func next() async throws -> JetStreamMessage?

    /// Releases any underlying subscription or server-side consumer. After teardown ``next()`` is
    /// expected to drain any already-received messages and then return `nil`.
    func teardown() async
}

/// A cancellation-safe, single-consumer mailbox fed by a background pump.
///
/// A timeout resumes a pending waiter with `nil` WITHOUT consuming a message: any message that
/// arrives afterwards is buffered for the next ``receive()``. This is what lets
/// ``MessageStream/next(timeout:)`` be both bounded and lossless.
private actor MessageBuffer {
    private var items: [JetStreamMessage] = []
    private var finished = false
    private var failure: Error?
    private var waiter: CheckedContinuation<JetStreamMessage?, Error>?

    /// Buffers a message, or hands it directly to a waiting ``receive()``.
    func push(_ message: JetStreamMessage) {
        if finished {
            return
        }
        if let waiter {
            self.waiter = nil
            waiter.resume(returning: message)
        } else {
            items.append(message)
        }
    }

    /// Marks the source as finished. With `discard`, any buffered messages are dropped and a pending
    /// waiter is resolved at once (used by ``MessageStream/stop()``). Without `discard`, buffered
    /// messages remain available to subsequent ``receive()`` calls (used by drain / natural end).
    func finish(throwing error: Error?, discard: Bool) {
        if finished {
            return
        }
        finished = true
        failure = error
        if discard {
            items.removeAll()
        }
        if let waiter, items.isEmpty {
            self.waiter = nil
            if let error {
                waiter.resume(throwing: error)
            } else {
                waiter.resume(returning: nil)
            }
        }
    }

    /// Awaits the next message: a buffered one, the terminal error/`nil`, or a future delivery.
    func receive() async throws -> JetStreamMessage? {
        if !items.isEmpty {
            return items.removeFirst()
        }
        if finished {
            if let failure {
                self.failure = nil
                throw failure
            }
            return nil
        }
        return try await withCheckedThrowingContinuation { continuation in
            if let existing = waiter {
                waiter = nil
                existing.resume(returning: nil)
            }
            waiter = continuation
        }
    }

    /// Resolves a pending waiter with `nil` without changing buffer/finished state. Used to honor a
    /// read timeout or task cancellation without losing an as-yet-undelivered message.
    func wakeWaiterWithNil() {
        if let waiter {
            self.waiter = nil
            waiter.resume(returning: nil)
        }
    }
}

/// Wraps a ``MessageSource`` with a background pump and a mailbox, exposing uniform
/// stop/drain/timeout semantics for every consumer kind.
///
/// The pump reads the source in a loop and feeds the mailbox; ``next()`` / ``next(timeout:)`` read
/// the mailbox. ``stop()`` discards buffered messages and tears the source down; ``drain()`` tears
/// the source down but lets buffered messages flush first.
final class MessageStream: @unchecked Sendable {
    private let source: MessageSource
    private let buffer = MessageBuffer()
    private let stateLock = NSLock()
    private var started = false
    private var pumpTask: Task<Void, Never>?

    init(source: MessageSource) {
        self.source = source
    }

    /// Starts the background pump. Idempotent.
    func start() {
        stateLock.lock()
        defer { stateLock.unlock() }
        if started {
            return
        }
        started = true
        let buffer = self.buffer
        let source = self.source
        pumpTask = Task {
            do {
                while true {
                    if Task.isCancelled {
                        await buffer.finish(throwing: nil, discard: true)
                        return
                    }
                    guard let message = try await source.next() else {
                        await buffer.finish(throwing: nil, discard: false)
                        return
                    }
                    await buffer.push(message)
                }
            } catch {
                if Task.isCancelled {
                    await buffer.finish(throwing: nil, discard: true)
                } else {
                    await buffer.finish(throwing: error, discard: false)
                }
            }
        }
    }

    /// Reads the next message, blocking until one is available or the stream ends.
    ///
    /// Starts the pump on first use, so `next()` works whether or not delivery was started
    /// explicitly (e.g. via ``JetStreamMessagesContext``).
    func next() async throws -> JetStreamMessage? {
        start()
        let buffer = self.buffer
        return try await withTaskCancellationHandler {
            try await buffer.receive()
        } onCancel: {
            Task { await buffer.wakeWaiterWithNil() }
        }
    }

    /// Reads the next message, waiting at most `timeout` seconds. Returns `nil` on timeout or end.
    func next(timeout: TimeInterval) async throws -> JetStreamMessage? {
        if timeout <= 0 {
            return try await next()
        }
        let buffer = self.buffer
        let timer = Task {
            try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            await buffer.wakeWaiterWithNil()
        }
        defer { timer.cancel() }
        return try await next()
    }

    /// Stops delivery immediately: cancels the pump, discards buffered messages, tears the source
    /// down. Idempotent.
    func stop() async {
        pumpTask?.cancel()
        await buffer.finish(throwing: nil, discard: true)
        await source.teardown()
    }

    /// Stops delivery gracefully: tears the source down (no new messages) but lets the pump flush
    /// already-received messages into the buffer, which subsequent reads drain before the stream
    /// ends. Idempotent.
    func drain() async {
        await source.teardown()
    }

    /// Completes once the pump has finished (after stop or drain).
    func waitUntilClosed() async {
        await pumpTask?.value
    }

    deinit {
        // Best-effort backstop for a stream dropped without an explicit `stop()`/`drain()` — for
        // example a `messages()` iteration whose loop was broken and whose context and iterator were
        // then both released, leaving nothing to drive or tear the source down. `stop()`/`drain()`
        // remain the documented primary contract; this only prevents a leaked pump / subscription /
        // ephemeral consumer when the caller relies on scope exit instead. `deinit` cannot `await`,
        // so the pump is cancelled synchronously and the source is torn down fire-and-forget.
        //
        // The pump task captures only `buffer` and `source` (never `self`), so the stream can deinit
        // while the pump is still suspended in `source.next()`; cancelling it and tearing the source
        // down unblocks and ends it. Both `buffer.finish` and `source.teardown` are idempotent, so
        // racing this against a prior `stop()`/`drain()` is safe.
        pumpTask?.cancel()
        let buffer = self.buffer
        let source = self.source
        Task {
            await buffer.finish(throwing: nil, discard: true)
            await source.teardown()
        }
    }
}

/// Concrete ``ConsumeContext`` backing continuous callback delivery.
final class JetStreamConsumeContext: ConsumeContext, @unchecked Sendable {
    private let stream: MessageStream
    private let task: Task<Void, Never>
    // Optional strong reference to the consumer whose engine feeds `stream`. An ordered consumer's
    // delivery is driven by its own pump `Task { [weak self] ... }`, and its `OrderedMessageSource`
    // references it only weakly (to avoid a retain cycle). So nothing in the stream/source chain keeps
    // the consumer alive: a caller that drops the consumer handle and keeps only this context races
    // the pump's weak-self load against ARC releasing the consumer -- if the consumer wins, its
    // `deinit` deletes the server consumer and finishes the stream, silently stopping delivery.
    // Pinning it here (this context is caller-owned, so no cycle) keeps the engine alive for the
    // context's lifetime, matching push/pull consumers. Nil for consumers that need no pin.
    private let owner: (any Sendable)?

    init(
        stream: MessageStream,
        handler: @escaping MessageHandler,
        onError: ConsumeErrorHandler?,
        owner: (any Sendable)? = nil
    ) {
        self.stream = stream
        self.owner = owner
        stream.start()
        self.task = Task {
            do {
                while let message = try await stream.next() {
                    handler(message)
                }
            } catch {
                onError?(error)
            }
        }
    }

    func stop() {
        let stream = self.stream
        Task { await stream.stop() }
    }

    func drain() {
        let stream = self.stream
        Task { await stream.drain() }
    }

    func waitUntilClosed() async {
        _ = await task.value
        await stream.waitUntilClosed()
    }

    deinit {
        // Best-effort backstop for a `ConsumeContext` dropped without `stop()`/`drain()`. The consume
        // `task` strongly captures `stream`, so `stream` cannot deinit — and its own backstop cannot
        // run — while the task loops. Cancelling the task makes its in-flight `stream.next()` return
        // `nil` so the loop exits and releases `stream`; stopping the stream cancels the pump and
        // tears the source (subscription + ephemeral consumer) down. `stop()`/`drain()` remain the
        // primary contract; `stream.stop()` is idempotent, so racing this against an explicit stop is
        // safe and cannot double-tear-down or double-finish.
        task.cancel()
        let stream = self.stream
        Task { await stream.stop() }
    }
}

/// Concrete ``MessagesContext`` backing `AsyncSequence` iteration.
///
/// This type intentionally has no `deinit` of its own: each ``makeAsyncIterator()`` hands out an
/// iterator that captures `stream`, so an outstanding iterator can legitimately outlive the context.
/// Teardown is therefore owned by ``MessageStream``'s own backstop `deinit`, which fires only once
/// the context AND every iterator it produced have been released — tearing down here instead could
/// pull the source out from under a still-live iterator. `stop()`/`drain()` remain the primary
/// contract for eager teardown.
final class JetStreamMessagesContext: MessagesContext, @unchecked Sendable {
    typealias Element = JetStreamMessage

    private let stream: MessageStream
    // See `JetStreamConsumeContext.owner`. Captured into each iterator too, so an iterator that
    // outlives this context (a supported pattern) still keeps the ordered engine alive.
    private let owner: (any Sendable)?

    init(stream: MessageStream, owner: (any Sendable)? = nil) {
        self.stream = stream
        self.owner = owner
        stream.start()
    }

    func makeAsyncIterator() -> JetStreamMessageIterator {
        let stream = self.stream
        let owner = self.owner
        return JetStreamMessageIterator {
            // Retain the consumer for the iterator's lifetime (see `owner`).
            _ = owner
            return try await stream.next()
        }
    }

    func stop() {
        let stream = self.stream
        Task { await stream.stop() }
    }

    func drain() {
        let stream = self.stream
        Task { await stream.drain() }
    }
}
