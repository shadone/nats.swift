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

/// A thread-safe latch that times message delivery for async consume handlers.
///
/// The `@Sendable` handler calls ``record()`` for every delivered message; the harness awaits
/// ``waitElapsedNanos()`` which resumes once the target count is reached and returns the elapsed
/// time between the first and the N-th delivery.
///
/// `@unchecked Sendable`: all mutable state is guarded by `lock`.
final class DeliveryTimer: @unchecked Sendable {
    private let lock = NSLock()
    private let target: Int
    private var count = 0
    private var startNanos: UInt64 = 0
    private var endNanos: UInt64 = 0
    private var done = false
    private var cancelled = false
    private var continuation: CheckedContinuation<Void, Never>?

    init(target: Int) {
        self.target = target
    }

    /// Records one delivered message. Safe to call from any thread.
    func record() {
        let now = DispatchTime.now().uptimeNanoseconds
        lock.lock()
        if count == 0 {
            startNanos = now
        }
        count += 1
        var toResume: CheckedContinuation<Void, Never>?
        if count >= target && !done {
            done = true
            endNanos = now
            toResume = continuation
            continuation = nil
        }
        lock.unlock()
        toResume?.resume()
    }

    /// Suspends until the target count is reached (or the task is cancelled), returning the elapsed
    /// nanoseconds between the first and the N-th delivery. Returns `0` if cancelled early.
    func waitElapsedNanos() async -> UInt64 {
        await withTaskCancellationHandler {
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                lock.lock()
                if done || cancelled {
                    lock.unlock()
                    cont.resume()
                } else {
                    continuation = cont
                    lock.unlock()
                }
            }
        } onCancel: {
            lock.lock()
            cancelled = true
            let toResume = continuation
            continuation = nil
            lock.unlock()
            toResume?.resume()
        }
        return snapshotElapsed()
    }

    /// Synchronous locked read of the measured window. Kept non-async so `lock()`/`unlock()` are
    /// legal under the Swift 6 language mode (no suspension point can sit inside the critical
    /// section).
    private func snapshotElapsed() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        return done && endNanos >= startNanos ? endNanos - startNanos : 0
    }
}

/// Returns a short unique token for naming subjects/streams/buckets so reruns never collide.
func uniqueToken() -> String {
    let millis = UInt64(Date().timeIntervalSince1970 * 1000)
    let salt = UInt32.random(in: 0..<UInt32.max)
    return "\(millis)-\(salt)"
}

/// Warmup iteration count: a tenth of the run, capped at 10k, at least 1.
func warmupCount(_ count: Int) -> Int {
    max(1, min(count / 10, 10_000))
}

/// Converts a nanosecond duration to milliseconds.
func millis(fromNanos nanos: UInt64) -> Double {
    Double(nanos) / 1_000_000.0
}

/// Converts a nanosecond duration to seconds. The `max(_, 1)` is only a divide-by-zero safety net
/// for a degenerate run (e.g. `--msgs 1`); at realistic counts the elapsed nanos are never near 1,
/// so it is not a throughput claim.
func seconds(fromNanos nanos: UInt64) -> Double {
    max(Double(nanos), 1) / 1_000_000_000.0
}
