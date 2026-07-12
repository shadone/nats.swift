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

/// Reconnect-resilience tests: a full server bounce (stop → restart on the same port with the
/// JetStream store preserved) forces the client through its real disconnect → reconnect path, and
/// the JetStream delivery machinery must resume with no loss or duplication. This exercises the
/// production event that matters most for long-lived (e.g. Cloud Run) clients — the ordered
/// consumer's ephemeral server consumer is lost on restart and must be transparently recreated.
final class ReconnectResilienceTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func jsConfPath() -> String {
        Bundle.module.url(forResource: "jetstream", withExtension: "conf")!.relativePath
    }

    /// A file-backed ordered consumer keeps delivering CONTIGUOUSLY across a server bounce: the client
    /// reconnects, the ordered engine detects its lost ephemeral consumer (missed heartbeat), recreates
    /// it from the last delivered stream sequence, and resumes with no gap or duplicate.
    func testOrderedConsumerResumesAcrossServerRestart() async throws {
        logger.logLevel = .critical
        let port = 4711
        natsServer.start(port: port, cfg: jsConfPath())
        let storeDir = natsServer.storeDirectory

        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(port)")!)
            .reconnectWait(0.1)
            .unlimitedReconnects()
            .build()
        let events = EventCounter()
        client.on(.disconnected) { _ in events.recordDisconnected() }
        client.on(.connected) { _ in events.recordConnected() }
        try await client.connect()
        defer { Task { try? await client.close() } }

        let ctx = JetStreamContext(client: client)
        _ = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"], storage: .file))

        let oc = OrderedConsumer(
            ctx: ctx, streamName: "test", deliverPolicy: .all, namePrefix: "ord", idleHeartbeat: 0.3
        )
        try await oc.start()

        let collector = MessageCollector()
        let reader = Task {
            for try await msg in oc.natsMessages {
                collector.record(JetStreamMessage(message: msg, client: client))
            }
        }
        defer { reader.cancel() }

        // First batch, delivered before the bounce (stream seqs 1...200).
        try await publish(ctx, count: 200)
        try await ConsumeTestSupport.waitUntil(20) { collector.count >= 200 }
        XCTAssertEqual(collector.sequences, sequences(1, through: 200), "pre-bounce delivery")

        // Bounce the server, preserving JetStream state; the client must reconnect on its own.
        natsServer.stop()
        natsServer.start(port: port, cfg: jsConfPath(), storeDir: storeDir)
        try await client.waitForConnected(timeout: 20)

        // New messages published after the reconnect (stream seqs 201...400). They persist in the
        // file-backed stream, so the recreated consumer resumes from seq 201 and drains them.
        try await publish(ctx, count: 200)
        try await ConsumeTestSupport.waitUntil(30) { collector.count >= 400 }

        reader.cancel()
        await oc.stop()

        XCTAssertEqual(
            collector.sequences, sequences(1, through: 400),
            "ordered delivery must resume contiguously across a server bounce — no gap, no duplicate"
        )
        // Prove the bounce was a real disconnect + reconnect, not a trivially-uninterrupted run.
        XCTAssertGreaterThanOrEqual(
            events.disconnectedCount, 1, "the client must observe the bounce as a disconnect")
        XCTAssertGreaterThanOrEqual(
            events.connectedCount, 2, "the client must reconnect (connected fires on reconnect too)"
        )
    }

    /// A KeyValue watch survives a server bounce: after the client reconnects, the watch (which rides
    /// the ordered engine) resumes and observes puts made after the restart.
    func testKeyValueWatchResumesAcrossServerRestart() async throws {
        logger.logLevel = .critical
        let port = 4712
        natsServer.start(port: port, cfg: jsConfPath())
        let storeDir = natsServer.storeDirectory

        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(port)")!)
            .reconnectWait(0.1)
            .unlimitedReconnects()
            .build()
        let events = EventCounter()
        client.on(.disconnected) { _ in events.recordDisconnected() }
        client.on(.connected) { _ in events.recordConnected() }
        try await client.connect()
        defer { Task { try? await client.close() } }

        let ctx = JetStreamContext(client: client)
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "rc"))
        _ = try await kv.put("a", Data("1".utf8))

        let seen = KeySet()
        let watcher = try await kv.watchAll()
        let reader = Task {
            for try await update in watcher {
                if let entry = update {
                    seen.insert(entry.key)
                }
            }
        }
        defer { reader.cancel() }

        // Initial value observed before the bounce.
        try await ConsumeTestSupport.waitUntil(20) { seen.contains("a") }

        natsServer.stop()
        natsServer.start(port: port, cfg: jsConfPath(), storeDir: storeDir)
        try await client.waitForConnected(timeout: 20)

        // A put made after the reconnect must reach the resumed watch.
        _ = try await kv.put("b", Data("2".utf8))
        try await ConsumeTestSupport.waitUntil(30) { seen.contains("b") }

        reader.cancel()
        await watcher.stop()

        XCTAssertTrue(seen.contains("a") && seen.contains("b"), "watch must resume after reconnect")
        XCTAssertGreaterThanOrEqual(
            events.disconnectedCount, 1, "the client must observe the bounce as a disconnect")
        XCTAssertGreaterThanOrEqual(
            events.connectedCount, 2, "the client must reconnect (connected fires on reconnect too)"
        )
    }

    // MARK: - Helpers

    /// Publishes `count` single-byte messages to `foo.A`, awaiting each ack.
    private func publish(_ ctx: JetStreamContext, count: Int) async throws {
        for _ in 0..<count {
            _ = try await ctx.publish("foo.A", message: Data("x".utf8)).wait()
        }
    }

    /// `[UInt64]` from `low` through `high`, for comparing against collected stream sequences.
    private func sequences(_ low: UInt64, through high: UInt64) -> [UInt64] {
        Array(low...high)
    }
}

/// Lock-guarded connect/disconnect event tallies, incremented from the client's `@Sendable` event
/// handlers, so a test can assert a real disconnect + reconnect cycle actually occurred.
private final class EventCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var connected = 0
    private var disconnected = 0

    func recordConnected() {
        lock.lock()
        connected += 1
        lock.unlock()
    }

    func recordDisconnected() {
        lock.lock()
        disconnected += 1
        lock.unlock()
    }

    var connectedCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return connected
    }

    var disconnectedCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return disconnected
    }
}

/// Lock-guarded set of observed keys for the watch test (the watch reader runs on its own task).
private final class KeySet: @unchecked Sendable {
    private let lock = NSLock()
    private var keys: Set<String> = []

    func insert(_ key: String) {
        lock.lock()
        keys.insert(key)
        lock.unlock()
    }

    func contains(_ key: String) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return keys.contains(key)
    }
}
