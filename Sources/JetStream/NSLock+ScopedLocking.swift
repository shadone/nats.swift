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

extension NSLock {
    /// Runs `body` while holding the lock, releasing it on scope exit.
    ///
    /// Unlike calling ``lock()`` / ``unlock()`` directly, this is safe to call from `async`
    /// contexts: the lock is acquired and released entirely within this synchronous,
    /// non-suspending call, so no `await` can occur while the lock is held. Under the Swift 6
    /// language mode, calling `lock()` / `unlock()` from an `async` function is an error precisely
    /// because a suspension point could otherwise sit between them; scoping the critical section in
    /// a synchronous closure removes that hazard.
    ///
    /// Foundation's own `NSLock.withLock(_:)` is only available on macOS 13+ / iOS 16+, and this
    /// package deploys to iOS 13, so this local shim (deliberately named to avoid shadowing the
    /// system method) provides the same guarantee on every supported platform.
    @inline(__always)
    func withLockScoped<T>(_ body: () throws -> T) rethrows -> T {
        lock()
        defer { unlock() }
        return try body()
    }
}
