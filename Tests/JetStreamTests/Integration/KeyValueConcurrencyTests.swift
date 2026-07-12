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

import NatsServer
import XCTest

@testable import JetStream
@testable import Nats

/// Concurrency correctness tests for the KeyValue CAS primitives. These guard the previously-fixed
/// AckFuture CAS-swallow bug: a silently-swallowed wrong-revision update would break an optimistic
/// retry loop and lose increments.
final class KeyValueConcurrencyTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// M concurrent workers each do `increments` optimistic read-modify-write increments on a single
    /// key via `update(revision:)`. If any wrong-revision conflict were silently swallowed instead of
    /// thrown, the retry loop would break without incrementing and the final value would fall short.
    /// With correct CAS every increment lands exactly once, so the final value is `M * increments`.
    func testConcurrentOptimisticIncrementsNoLostUpdates() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        let bucket = "cnt"
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: bucket))
        _ = try await kv.put("n", "0".data(using: .utf8)!)

        let workers = 8
        let increments = 50

        // `KeyValue` is `Sendable`, so a single shared handle is captured by every worker rather
        // than each worker opening its own. This capture is exactly what Swift 6 used to reject.
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<workers {
                group.addTask {
                    for _ in 0..<increments {
                        var attempts = 0
                        while true {
                            attempts += 1
                            XCTAssertLessThan(attempts, 100_000, "runaway CAS retry")
                            let entry = try await kv.get("n")!
                            let current = Int(String(decoding: entry.value, as: UTF8.self))!
                            do {
                                _ = try await kv.update(
                                    "n", "\(current + 1)".data(using: .utf8)!,
                                    revision: entry.revision)
                                break
                            } catch {
                                // Wrong-revision conflict: another worker won the race, retry.
                            }
                        }
                    }
                }
            }
            try await group.waitForAll()
        }

        let final = try await kv.get("n")!
        XCTAssertEqual(
            Int(String(decoding: final.value, as: UTF8.self)), workers * increments,
            "no lost updates: every optimistic increment must land exactly once")
    }

    /// M concurrent workers each call `create` on the same fresh key. `create` is a CAS on
    /// revision-zero, so EXACTLY one worker must succeed and every other must throw `keyExists`.
    func testConcurrentCreateExactlyOneWinner() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        let bucket = "race"
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: bucket))

        let workers = 16
        let tally = Tally()

        // A single shared `Sendable` `KeyValue` handle is captured by every worker; the capture
        // is exactly what Swift 6 used to reject before the handle became `Sendable`.
        await withTaskGroup(of: Void.self) { group in
            for i in 0..<workers {
                group.addTask {
                    do {
                        _ = try await kv.create("k", "\(i)".data(using: .utf8)!)
                        await tally.recordSuccess()
                    } catch {
                        await tally.recordFailure()
                    }
                }
            }
            await group.waitForAll()
        }

        let (successes, failures) = await tally.counts()
        XCTAssertEqual(successes, 1, "exactly one create must win the race")
        XCTAssertEqual(failures, workers - 1, "every other create must lose")
    }

    // MARK: - Helpers

    /// Actor-guarded success/failure counter for aggregating concurrent task outcomes under Swift 6
    /// strict concurrency (no capture-and-mutate of a bare `var` across tasks).
    private actor Tally {
        private var successes = 0
        private var failures = 0

        func recordSuccess() {
            successes += 1
        }

        func recordFailure() {
            failures += 1
        }

        func counts() -> (Int, Int) {
            (successes, failures)
        }
    }

    private func connect() async throws -> NatsClient {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return client
    }
}
