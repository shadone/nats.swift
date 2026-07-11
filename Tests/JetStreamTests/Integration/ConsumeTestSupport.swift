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
import NatsServer
import XCTest

@testable import JetStream
@testable import Nats

/// Thread-safe collector for messages delivered to a `consume` handler (a `@Sendable` closure that
/// may run on any task).
final class MessageCollector: @unchecked Sendable {
    private let lock = NSLock()
    private var entries: [(payload: String, seq: UInt64)] = []

    /// Records a message by its payload string and stream sequence.
    func record(_ message: JetStreamMessage) {
        let payload = message.payload.flatMap { String(data: $0, encoding: .utf8) } ?? ""
        let seq = (try? message.metadata().streamSequence) ?? 0
        lock.lock()
        entries.append((payload, seq))
        lock.unlock()
    }

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return entries.count
    }

    var payloads: [String] {
        lock.lock()
        defer { lock.unlock() }
        return entries.map(\.payload)
    }

    var sequences: [UInt64] {
        lock.lock()
        defer { lock.unlock() }
        return entries.map(\.seq)
    }
}

/// Shared helpers for the unified consume/messages/next integration tests.
enum ConsumeTestSupport {

    /// Starts the bundled JetStream server and returns a connected client.
    static func connect(_ server: NatsServer) async throws -> NatsClient {
        let bundle = Bundle.module
        server.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: server.clientURL)!).build()
        try await client.connect()
        return client
    }

    /// Publishes `count` messages onto `subject`, each payload `"msg-<i>"` (1-based).
    static func publish(
        _ ctx: JetStreamContext, subject: String, count: Int
    ) async throws {
        for i in 1...count {
            let payload = "msg-\(i)".data(using: .utf8)!
            _ = try await ctx.publish(subject, message: payload).wait()
        }
    }

    /// Whether `stream` currently has a consumer named `name`.
    static func consumerExists(
        _ ctx: JetStreamContext, stream: String, name: String
    ) async -> Bool {
        ((try? await ctx.getConsumer(stream: stream, name: name)) ?? nil) != nil
    }

    /// Polls until `stream` no longer has a consumer named `name`, failing the test after `timeout`.
    /// Used by the teardown/leak tests: a leaked deliver-subject subscription keeps an ephemeral
    /// server consumer alive, so its disappearance is the server-observable proof teardown ran.
    static func waitUntilConsumerGone(
        _ ctx: JetStreamContext,
        stream: String,
        name: String,
        timeout: TimeInterval = 10,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await !consumerExists(ctx, stream: stream, name: name) {
                return
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        XCTFail("consumer \(name) still present after \(timeout)s", file: file, line: line)
    }

    /// Polls `condition` until it holds or `timeout` elapses (then fails the test).
    static func waitUntil(
        _ timeout: TimeInterval = 10,
        file: StaticString = #filePath,
        line: UInt = #line,
        _ condition: @Sendable () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if condition() {
                return
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTFail("condition not met within \(timeout)s", file: file, line: line)
    }
}
