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

/// Writes a line to standard error (used for progress and diagnostics so `--json` stdout stays
/// machine-readable).
func writeStderr(_ message: String) {
    FileHandle.standardError.write(Data("\(message)\n".utf8))
}

/// Raised when an awaited operation exceeds its deadline.
struct TimeoutError: Error, CustomStringConvertible {
    let seconds: Double

    var description: String { "operation timed out after \(seconds)s" }
}

/// Runs `body`, then always runs `cleanup` (both on success and on throw), rethrowing any error.
/// Used so scenarios delete streams/buckets even when the timed section fails.
func withCleanup<T>(
    _ body: () async throws -> T, cleanup: () async -> Void
) async throws -> T {
    do {
        let result = try await body()
        await cleanup()
        return result
    } catch {
        await cleanup()
        throw error
    }
}

/// Awaits `operation`, throwing ``TimeoutError`` if it does not finish within `seconds`.
/// The pending operation is cancelled when the deadline wins so no child task leaks.
func awaitWithTimeout(
    seconds: Double, _ operation: @escaping @Sendable () async -> UInt64
) async throws -> UInt64 {
    try await withThrowingTaskGroup(of: UInt64?.self) { group in
        group.addTask { await operation() }
        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            return nil
        }
        defer { group.cancelAll() }
        for try await result in group {
            if let value = result {
                return value
            }
            throw TimeoutError(seconds: seconds)
        }
        throw TimeoutError(seconds: seconds)
    }
}
