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

/// A handle to the eventual acknowledgement of a message published via
/// ``JetStreamContext/publishAsync(_:message:headers:msgTTL:)``.
///
/// Unlike ``AckFuture`` (which owns a per-message subscription), a `PubAckFuture` is a thin box
/// resolved by the publisher's shared ack pump. It is **re-awaitable**: the resolved value is stored
/// permanently, so calling ``wait()`` more than once always yields the same result.
public struct PubAckFuture: Sendable {
    private let box: PubAckBox

    init(box: PubAckBox) { self.box = box }

    /// Awaits the server ack, or the failure the server/transport reported (a CAS/wrong-last-sequence
    /// error, ``JetStreamError/PublishError/streamNotFound``, a send failure, or a per-message
    /// timeout). Re-awaitable — a second call returns the already-stored result.
    public func wait() async throws -> Ack { try await box.value() }
}

/// A `Sendable` wrapper for a publish failure. Errors flowing through the async publisher (CAS
/// errors, stream-not-found, decode errors, transport errors, timeouts) are immutable value/enum
/// types or `Codable` structs and are safe to hand between isolation domains; `any Error` is not
/// statically `Sendable`, so this box carries it. Wrapping (rather than storing `Result<Ack, Error>`)
/// keeps the box's stored result and its continuation payload `Sendable`, avoiding sending-risk
/// diagnostics under Swift 6 strict concurrency.
struct PubAckFailure: Error, @unchecked Sendable {
    let underlying: any Error
}

/// A resolve-once, re-awaitable, cancellation-safe container for a single publish acknowledgement.
///
/// `@unchecked Sendable`: all mutable state is guarded by `lock`. A continuation is never resumed
/// while the lock is held — it is captured, the lock released, then resumed — so resuming can never
/// re-enter this box's own locked region.
final class PubAckBox: @unchecked Sendable {
    private let lock = NSLock()
    private var result: Result<Ack, PubAckFailure>?
    private var waiters: [CheckedContinuation<Result<Ack, PubAckFailure>, Never>] = []

    /// Resolves the box exactly once. A second (or later) resolve is a no-op, so a normal ack and a
    /// timeout racing to resolve the same box cannot double-resume. The result is stored permanently
    /// so ``value()`` stays re-awaitable. ALL parked waiters are resumed OUTSIDE the lock.
    func resolve(_ r: Result<Ack, Error>) {
        let wrapped = r.mapError { PubAckFailure(underlying: $0) }
        let parked: [CheckedContinuation<Result<Ack, PubAckFailure>, Never>] = lock.withLockScoped {
            if result != nil { return [] }  // already resolved — no-op
            result = wrapped
            let w = waiters
            waiters = []
            return w
        }
        for continuation in parked {
            continuation.resume(returning: wrapped)
        }
    }

    /// Returns the stored result if already resolved, otherwise parks until ``resolve(_:)`` is called.
    ///
    /// Supports fan-out: multiple concurrent awaiters (the same future awaited on several tasks before
    /// it resolves) all park and are all resumed exactly once by ``resolve(_:)``. After resolution any
    /// ``value()`` returns the stored result immediately. This is deliberately a plain
    /// park-until-resolved: the timeout is implemented by the publisher resolving the box (see
    /// `JetStreamPublishAsync`), NOT by racing this continuation against a `Task.sleep` (racing a
    /// `withCheckedContinuation` that loses would leak/hang it forever).
    func value() async throws -> Ack {
        let resolved: Result<Ack, PubAckFailure>
        if let stored = lock.withLockScoped({ result }) {
            resolved = stored
        } else {
            resolved = await withCheckedContinuation { continuation in
                // Re-check under the lock: `resolve` may have completed between the fast-path read
                // and here. If so, resume immediately (outside the lock); otherwise join the waiters.
                let stored: Result<Ack, PubAckFailure>? = lock.withLockScoped {
                    if let result { return result }
                    waiters.append(continuation)
                    return nil
                }
                if let stored {
                    continuation.resume(returning: stored)
                }
            }
        }
        switch resolved {
        case .success(let ack): return ack
        case .failure(let failure): throw failure.underlying
        }
    }
}
