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

/// Formats the current wall-clock time as `HH:mm:ss.SSS` (UTC). Computed without
/// a shared mutable `DateFormatter`, so it is safe to call from the concurrent
/// tasks the scenarios spawn.
func timestamp() -> String {
    let now = Date().timeIntervalSince1970
    let whole = Int(now)
    let millis = Int((now - Double(whole)) * 1000)
    let secondsOfDay = whole % 86_400
    return String(
        format: "%02d:%02d:%02d.%03d",
        secondsOfDay / 3600, (secondsOfDay % 3600) / 60, secondsOfDay % 60, millis)
}

/// Prints a timestamped, tagged, observable line to stdout.
func out(_ tag: String, _ message: String) {
    print("[\(timestamp())] [\(tag)] \(message)")
}

/// A small lock-guarded box for sharing mutable state across the concurrent
/// tasks a scenario spawns (watch pumps, delivery handlers, publishers).
final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    init(_ value: Value) {
        self.value = value
    }

    func withLock<Output>(_ body: (inout Value) -> Output) -> Output {
        lock.lock()
        defer { lock.unlock() }
        return body(&value)
    }

    func get() -> Value {
        withLock { $0 }
    }
}

/// Deterministic pseudo-random bytes from a 64-bit LCG. Incompressible enough to
/// exercise real object-store chunking and SHA-256 digesting without pulling in a
/// CSPRNG.
func pseudoRandomData(_ count: Int) -> Data {
    var state: UInt64 = 0x9E37_79B9_7F4A_7C15
    var bytes = [UInt8]()
    bytes.reserveCapacity(count)
    for _ in 0..<count {
        state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
        bytes.append(UInt8(truncatingIfNeeded: state >> 33))
    }
    return Data(bytes)
}

/// Raised when a scenario waits past its deadline for a condition to hold.
struct ScenarioTimeout: Error, CustomStringConvertible {
    let seconds: Double
    var description: String { "timed out waiting for condition after \(seconds)s" }
}

/// Polls `condition` every 50 ms until it holds, or throws ``ScenarioTimeout``
/// once `deadlineSeconds` elapses.
func waitUntil(
    deadlineSeconds: Double, _ condition: @Sendable () -> Bool
) async throws {
    let deadline =
        DispatchTime.now().uptimeNanoseconds + UInt64(deadlineSeconds * 1_000_000_000)
    while !condition() {
        if DispatchTime.now().uptimeNanoseconds > deadline {
            throw ScenarioTimeout(seconds: deadlineSeconds)
        }
        try await Task.sleep(nanoseconds: 50_000_000)
    }
}

/// The optional graceful-exit duration (seconds) for the long-lived scenarios
/// (`service`, `live-consume`), read from `SCEN_DURATION`. Returns `nil` to run
/// until interrupted.
func scenarioDurationSeconds() -> Double? {
    guard let raw = ProcessInfo.processInfo.environment["SCEN_DURATION"] else {
        return nil
    }
    return Double(raw)
}

/// Sleeps for `SCEN_DURATION` seconds, or until the task is cancelled when it is
/// unset (the "leave it running" default for the long-lived scenarios).
func runUntilDurationOrCancelled() async {
    if let seconds = scenarioDurationSeconds() {
        try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
    } else {
        while !Task.isCancelled {
            try? await Task.sleep(nanoseconds: 1_000_000_000)
        }
    }
}
