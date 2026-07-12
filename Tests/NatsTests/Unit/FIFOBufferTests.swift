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

import XCTest

@testable import Nats

class FIFOBufferTests: XCTestCase {
    nonisolated(unsafe) static let allTests = [
        ("testEmptyPopReturnsNil", testEmptyPopReturnsNil),
        ("testCountAndIsEmpty", testCountAndIsEmpty),
        ("testFIFOOrderPreserved", testFIFOOrderPreserved),
        ("testInterleavedAppendPop", testInterleavedAppendPop),
        ("testCountAcrossCompactionBoundary", testCountAcrossCompactionBoundary),
        ("testLargeDrainOrderAndSpeed", testLargeDrainOrderAndSpeed),
    ]

    func testEmptyPopReturnsNil() {
        var buffer = FIFOBuffer<Int>()
        XCTAssertNil(buffer.popFirst())
        XCTAssertTrue(buffer.isEmpty)
        XCTAssertEqual(buffer.count, 0)

        // Drain-to-empty then pop again still returns nil (head == storage.count).
        buffer.append(1)
        XCTAssertEqual(buffer.popFirst(), 1)
        XCTAssertNil(buffer.popFirst())
        XCTAssertNil(buffer.popFirst())
        XCTAssertTrue(buffer.isEmpty)
        XCTAssertEqual(buffer.count, 0)
    }

    func testCountAndIsEmpty() {
        var buffer = FIFOBuffer<Int>()
        XCTAssertTrue(buffer.isEmpty)

        buffer.append(10)
        buffer.append(20)
        XCTAssertFalse(buffer.isEmpty)
        XCTAssertEqual(buffer.count, 2)

        XCTAssertEqual(buffer.popFirst(), 10)
        XCTAssertEqual(buffer.count, 1)
        XCTAssertFalse(buffer.isEmpty)

        XCTAssertEqual(buffer.popFirst(), 20)
        XCTAssertEqual(buffer.count, 0)
        XCTAssertTrue(buffer.isEmpty)
    }

    func testFIFOOrderPreserved() {
        var buffer = FIFOBuffer<Int>()
        for i in 0..<50 {
            buffer.append(i)
        }
        var out: [Int] = []
        while let value = buffer.popFirst() {
            out.append(value)
        }
        XCTAssertEqual(out, Array(0..<50))
        XCTAssertTrue(buffer.isEmpty)
    }

    /// Interleaved append/pop must behave like a queue and keep `count` exact throughout.
    func testInterleavedAppendPop() {
        var buffer = FIFOBuffer<Int>()
        var reference: [Int] = []  // model queue
        var next = 0

        for step in 0..<10_000 {
            // Append two, pop one — net growth, forcing many compactions over time.
            buffer.append(next)
            reference.append(next)
            next += 1
            if step % 3 == 0 {
                buffer.append(next)
                reference.append(next)
                next += 1
            }

            let expected = reference.isEmpty ? nil : reference.removeFirst()
            XCTAssertEqual(buffer.popFirst(), expected)
            XCTAssertEqual(buffer.count, reference.count)
        }

        // Drain the remainder; order must still match the model queue exactly.
        while let value = buffer.popFirst() {
            XCTAssertEqual(value, reference.removeFirst())
        }
        XCTAssertTrue(reference.isEmpty)
        XCTAssertTrue(buffer.isEmpty)
    }

    /// `count`/`isEmpty` must stay correct as `head` crosses the 1024 compaction threshold
    /// (both just before and just after the reclaim in `popFirst`).
    func testCountAcrossCompactionBoundary() {
        var buffer = FIFOBuffer<Int>()
        let total = 4096
        for i in 0..<total {
            buffer.append(i)
        }
        XCTAssertEqual(buffer.count, total)

        for popped in 1...total {
            let value = buffer.popFirst()
            XCTAssertEqual(value, popped - 1)
            XCTAssertEqual(buffer.count, total - popped)
            XCTAssertEqual(buffer.isEmpty, popped == total)
        }
        XCTAssertNil(buffer.popFirst())
    }

    /// Append 100k then drain 100k: exact order preserved and fast enough to prove the drain
    /// is amortized O(1) per pop (a O(n^2) `removeFirst()`-per-pop drain would be far slower).
    func testLargeDrainOrderAndSpeed() {
        var buffer = FIFOBuffer<Int>()
        let count = 100_000

        let start = Date()
        for i in 0..<count {
            buffer.append(i)
        }
        XCTAssertEqual(buffer.count, count)

        var expected = 0
        while let value = buffer.popFirst() {
            XCTAssertEqual(value, expected)
            expected += 1
        }
        let elapsed = Date().timeIntervalSince(start)

        XCTAssertEqual(expected, count, "every appended element must be popped in order")
        XCTAssertTrue(buffer.isEmpty)
        XCTAssertEqual(buffer.count, 0)
        // Generous bound: amortized-O(1) drains 100k in well under a second even in debug;
        // an O(n^2) drain would take many seconds. Guards against a regression to O(n) pops.
        XCTAssertLessThan(elapsed, 5.0, "100k append+drain should be fast (amortized O(1))")
    }
}
