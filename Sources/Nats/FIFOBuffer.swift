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

/// A first-in-first-out buffer with amortized O(1) `append` and `popFirst`.
///
/// Backed by a plain `Array` plus a `head` index that advances on each pop, so
/// dequeuing never shifts the remaining elements (unlike `Array.removeFirst()`,
/// which is O(n)). Draining a large backlog is therefore O(n) overall rather than
/// O(n^2). The consumed prefix is periodically reclaimed (when it grows to about
/// the live count) so `storage` stays bounded to roughly 2x the live element count.
struct FIFOBuffer<Element> {
    private var storage: [Element] = []
    private var head = 0

    var count: Int { storage.count - head }
    var isEmpty: Bool { head == storage.count }

    mutating func append(_ element: Element) { storage.append(element) }

    mutating func popFirst() -> Element? {
        guard head < storage.count else { return nil }
        let element = storage[head]
        head += 1
        // Reclaim the consumed prefix once it dominates: amortized O(1), storage stays bounded.
        if head >= 1024 && head * 2 >= storage.count {
            storage.removeFirst(head)
            head = 0
        }
        return element
    }
}

// Conditionally Sendable, mirroring `Array`: safe to move across isolation domains when the
// buffered elements are themselves Sendable. `State` (in `NatsSubscription`) relies on this.
extension FIFOBuffer: Sendable where Element: Sendable {}
