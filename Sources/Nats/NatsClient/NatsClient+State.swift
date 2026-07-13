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
import NIOConcurrencyHelpers

/// Internal state machine for `NatsClient.awaitConnected(timeout:)`: guarantees the
/// checked continuation is resumed exactly once across the `.connected` event, the
/// optional timeout, the already-connected fast path, and task cancellation -- including
/// the window where cancellation fires before the continuation has been created. The
/// finishing outcome is carried in the state so it is set atomically with the resume.
private enum ConnectWait {
    case initial
    case waiting(CheckedContinuation<Void, Never>)
    case cancelledEarly(ConnectWaitResult)
    case done(ConnectWaitResult)
}

/// Why `NatsClient.awaitConnected(timeout:)` stopped waiting.
private enum ConnectWaitResult {
    case connected
    case timedOut
    case cancelled
}

extension NatsClient {

    /// The current connection state of the client.
    ///
    /// This surfaces the underlying connection lifecycle so callers can inspect the
    /// state directly instead of tracking ``NatsEventKind`` transitions (for example
    /// `.connected`/`.closed`) out-of-band via ``NatsClient/on(_:_:)-(NatsEventKind,_)``.
    ///
    /// A freshly built client that has not yet connected reports ``NatsState/pending``.
    public var state: NatsState {
        connectionHandler?.currentState ?? .closed
    }

    /// Whether the client is currently connected to a NATS server.
    ///
    /// Convenience for `state == .connected`.
    public var isConnected: Bool {
        state == .connected
    }

    /// Suspends until the client reaches the ``NatsState/connected`` state.
    ///
    /// Returns immediately if the client is already connected. This is primarily
    /// useful together with ``NatsClientOptions/retryOnfailedConnect()``, where
    /// ``NatsClient/connect()`` returns before the first successful connection is
    /// established; awaiting this method lets a caller block until the connection is
    /// actually usable without having to bridge the first `.connected` event manually.
    ///
    /// The call never throws. It waits indefinitely, but honors task cancellation: if
    /// the surrounding task is cancelled it returns early (without guaranteeing the
    /// client is connected), so check `Task.isCancelled` or call
    /// `Task.checkCancellation()` afterwards if you need to react. Use
    /// ``NatsClient/waitForConnected(timeout:)`` to bound the wait with an error.
    public func waitForConnected() async {
        _ = await awaitConnected(timeout: nil)
    }

    /// Suspends until the client reaches the ``NatsState/connected`` state, the given
    /// timeout elapses, or the surrounding task is cancelled.
    ///
    /// Returns immediately if the client is already connected. Behaves like
    /// ``NatsClient/waitForConnected()`` but throws instead of waiting forever.
    ///
    /// - Parameter timeout: the maximum time to wait, in seconds.
    ///
    /// - Throws: ``NatsError/ConnectError/timeout`` if the client does not become
    ///   connected within `timeout`, or `CancellationError` if the surrounding task is
    ///   cancelled while waiting.
    public func waitForConnected(timeout: TimeInterval) async throws {
        switch await awaitConnected(timeout: timeout) {
        case .connected:
            return
        case .timedOut:
            throw NatsError.ConnectError.timeout
        case .cancelled:
            throw CancellationError()
        }
    }

    /// Core wait implementation shared by the public overloads.
    ///
    /// Returns ``ConnectWaitResult/connected`` once the client is connected,
    /// ``ConnectWaitResult/timedOut`` if the optional `timeout` elapsed first, or
    /// ``ConnectWaitResult/cancelled`` if the surrounding task was cancelled.
    ///
    /// Missed-signal safety: the `.connected` listener is registered *before* the
    /// current state is read. The connection handler always transitions to
    /// ``NatsState/connected`` *before* firing the `.connected` event, so if we observe
    /// a non-connected state below, the event has not fired yet and the just-registered
    /// listener is guaranteed to catch it. Conversely, if we observe `.connected`, we
    /// resume via the fast path.
    ///
    /// Cancellation safety: a `ConnectWait` state machine, mutated only under a lock,
    /// resumes the continuation exactly once across the listener, the timeout task, the
    /// fast path, and the cancellation handler -- including the window where cancellation
    /// fires before the continuation is created.
    private func awaitConnected(timeout: TimeInterval?) async -> ConnectWaitResult {
        guard let connectionHandler = self.connectionHandler else {
            return .timedOut
        }

        let stateBox = NIOLockedValueBox<ConnectWait>(.initial)
        let listenerIdBox = NIOLockedValueBox<String?>(nil)
        let timeoutTaskBox = NIOLockedValueBox<Task<Void, Never>?>(nil)

        // Resume the continuation exactly once, recording why in the state so the outcome
        // is set atomically with the resume. Safe to call before the continuation exists:
        // it parks the outcome and the creation path resumes.
        let finish: @Sendable (ConnectWaitResult) -> Void = { outcome in
            let continuation: CheckedContinuation<Void, Never>? = stateBox.withLockedValue {
                state in
                switch state {
                case .initial:
                    // Cancellation raced ahead of the continuation; park the outcome.
                    state = .cancelledEarly(outcome)
                    return nil
                case .waiting(let continuation):
                    state = .done(outcome)
                    return continuation
                case .cancelledEarly, .done:
                    return nil
                }
            }
            continuation?.resume()
        }

        await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let resumeNow: Bool = stateBox.withLockedValue { state in
                    switch state {
                    case .cancelledEarly(let outcome):
                        // `finish` already ran before we had a continuation to resume.
                        state = .done(outcome)
                        return true
                    case .initial:
                        state = .waiting(continuation)
                        return false
                    case .waiting, .done:
                        return false
                    }
                }
                if resumeNow {
                    continuation.resume()
                    return
                }

                // Register the `.connected` listener BEFORE reading the state (see the
                // missed-signal note above).
                let id = connectionHandler.addListeners(for: [.connected]) { _ in
                    finish(.connected)
                }
                listenerIdBox.withLockedValue { $0 = id }

                if let timeout {
                    let task = Task {
                        try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                        finish(.timedOut)
                    }
                    timeoutTaskBox.withLockedValue { $0 = task }
                }

                // Fast path: already connected.
                if connectionHandler.currentState == .connected {
                    finish(.connected)
                }
            }
        } onCancel: {
            finish(.cancelled)
        }

        // Clean up: drop the listener and cancel any pending timeout task.
        if let id = listenerIdBox.withLockedValue({ $0 }) {
            connectionHandler.removeListener(id)
        }
        timeoutTaskBox.withLockedValue { $0 }?.cancel()

        return stateBox.withLockedValue { state in
            if case .done(let outcome) = state { return outcome }
            return .cancelled
        }
    }
}
