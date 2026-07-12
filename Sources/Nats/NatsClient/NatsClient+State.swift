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
    /// The call never throws and waits indefinitely. Use
    /// ``NatsClient/waitForConnected(timeout:)`` to bound the wait.
    public func waitForConnected() async {
        _ = await awaitConnected(timeout: nil)
    }

    /// Suspends until the client reaches the ``NatsState/connected`` state or the
    /// given timeout elapses.
    ///
    /// Returns immediately if the client is already connected. Behaves like
    /// ``NatsClient/waitForConnected()`` but throws instead of waiting forever.
    ///
    /// - Parameter timeout: the maximum time to wait, in seconds.
    ///
    /// - Throws ``NatsError/ConnectError/timeout`` if the client does not become
    ///   connected within `timeout`.
    public func waitForConnected(timeout: TimeInterval) async throws {
        let connected = await awaitConnected(timeout: timeout)
        if !connected {
            throw NatsError.ConnectError.timeout
        }
    }

    /// Core wait implementation shared by the public overloads.
    ///
    /// Returns `true` once the client is connected, or `false` if the optional
    /// `timeout` elapsed first.
    ///
    /// Missed-signal safety: the `.connected` listener is registered *before* the
    /// current state is read. The connection handler always transitions to
    /// ``NatsState/connected`` *before* firing the `.connected` event, so if we
    /// observe a non-connected state below, the event has not fired yet and the
    /// just-registered listener is guaranteed to catch it. Conversely, if we observe
    /// `.connected`, we resume via the fast path. A single-resume guard makes the two
    /// paths (and the timeout) mutually exclusive.
    private func awaitConnected(timeout: TimeInterval?) async -> Bool {
        guard let connectionHandler = self.connectionHandler else {
            return false
        }

        let resumed = NIOLockedValueBox(false)
        let listenerIdBox = NIOLockedValueBox<String?>(nil)
        let timedOut = NIOLockedValueBox(false)
        let timeoutTaskBox = NIOLockedValueBox<Task<Void, Never>?>(nil)

        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            // Resumes the continuation at most once. Returns whether this call was the
            // one that performed the resume.
            let resume: @Sendable () -> Bool = {
                let shouldResume = resumed.withLockedValue { done -> Bool in
                    if done { return false }
                    done = true
                    return true
                }
                if shouldResume {
                    continuation.resume()
                }
                return shouldResume
            }

            // Register the `.connected` listener BEFORE reading the state (see the
            // missed-signal note above).
            let id = connectionHandler.addListeners(for: [.connected]) { _ in
                _ = resume()
            }
            listenerIdBox.withLockedValue { $0 = id }

            // Optional timeout: mark `timedOut` before resuming so the awaiting side
            // observes the flag once it wakes up.
            if let timeout {
                let task = Task {
                    try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                    let shouldResume = resumed.withLockedValue { done -> Bool in
                        if done { return false }
                        done = true
                        return true
                    }
                    if shouldResume {
                        timedOut.withLockedValue { $0 = true }
                        continuation.resume()
                    }
                }
                timeoutTaskBox.withLockedValue { $0 = task }
            }

            // Fast path: already connected.
            if connectionHandler.currentState == .connected {
                _ = resume()
            }
        }

        // Clean up: drop the listener and cancel any pending timeout task.
        if let id = listenerIdBox.withLockedValue({ $0 }) {
            connectionHandler.removeListener(id)
        }
        timeoutTaskBox.withLockedValue { $0 }?.cancel()

        return !timedOut.withLockedValue { $0 }
    }
}
