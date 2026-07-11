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

import JetStream
import Nats
import NatsServer
import XCTest

/// Integration tests for per-message TTL (`Nats-TTL`) and KeyValue per-key TTL
/// against a real nats-server (v2.11+ required for the feature).
final class MessageTTLTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func connectedContext() async throws -> (NatsClient, JetStreamContext) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return (client, JetStreamContext(client: client))
    }

    /// Polls `probe` until it returns true or the deadline passes. Keeps TTL
    /// tests deterministic: a short TTL plus a generous poll ceiling.
    private func pollUntil(
        timeout: TimeInterval = 8.0, interval: TimeInterval = 0.2,
        _ probe: () async throws -> Bool
    ) async throws -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if try await probe() { return true }
            try await Task.sleep(nanoseconds: UInt64(interval * 1_000_000_000))
        }
        return try await probe()
    }

    // MARK: - StreamConfig round-trip

    func testStreamAllowMsgTTLReadBack() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        var cfg = StreamConfig(name: "TTL_CFG", subjects: ["ttlcfg.>"])
        cfg.allowMsgTTL = true
        cfg.subjectDeleteMarkerTTL = NanoTimeInterval(5)

        let stream = try await ctx.createStream(cfg: cfg)
        XCTAssertEqual(stream.info.config.allowMsgTTL, true)
        XCTAssertEqual(stream.info.config.subjectDeleteMarkerTTL, NanoTimeInterval(5))

        // Re-fetch from the server to confirm the config persisted.
        let refreshed = try await stream.info()
        XCTAssertEqual(refreshed.config.allowMsgTTL, true)
        XCTAssertEqual(refreshed.config.subjectDeleteMarkerTTL, NanoTimeInterval(5))
    }

    // MARK: - Per-message TTL end to end

    /// A message published with a short TTL to an `allowMsgTTL` stream is
    /// retrievable immediately and gone after the TTL elapses.
    func testPerMessageTTLExpires() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        var cfg = StreamConfig(name: "TTL_MSG", subjects: ["ttlmsg.>"])
        cfg.allowMsgTTL = true
        let stream = try await ctx.createStream(cfg: cfg)

        let ack = try await ctx.publish(
            "ttlmsg.a", message: Data("ephemeral".utf8), msgTTL: NanoTimeInterval(1)
        ).wait()

        // Retrievable immediately.
        let present = try await stream.getMessage(sequence: ack.seq)
        XCTAssertEqual(present?.payload, Data("ephemeral".utf8))
        XCTAssertEqual(
            present?.headers?.get(.natsMsgTTL)?.description, "1s",
            "the Nats-TTL header should carry the Go-duration value")

        // Gone after the TTL elapses.
        let gone = try await pollUntil {
            try await stream.getMessage(sequence: ack.seq) == nil
        }
        XCTAssertTrue(gone, "message should be removed after its TTL expires")
    }

    /// A publish without a TTL to the same stream is unaffected.
    func testPublishWithoutTTLPersists() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        var cfg = StreamConfig(name: "TTL_MIX", subjects: ["ttlmix.>"])
        cfg.allowMsgTTL = true
        let stream = try await ctx.createStream(cfg: cfg)

        let ttlAck = try await ctx.publish(
            "ttlmix.a", message: Data("temp".utf8), msgTTL: NanoTimeInterval(1)
        ).wait()
        let permAck = try await ctx.publish("ttlmix.b", message: Data("keep".utf8)).wait()

        let ttlGone = try await pollUntil {
            try await stream.getMessage(sequence: ttlAck.seq) == nil
        }
        XCTAssertTrue(ttlGone)

        let perm = try await stream.getMessage(sequence: permAck.seq)
        XCTAssertEqual(perm?.payload, Data("keep".utf8))
        XCTAssertNil(
            perm?.headers?.get(.natsMsgTTL), "a no-TTL publish must not carry a Nats-TTL header")
    }

    // MARK: - KeyValue per-key TTL

    func testKeyValueBucketLimitMarkerTTL() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        var cfg = KeyValueConfig(bucket: "ttlbucket")
        cfg.limitMarkerTTL = NanoTimeInterval(60)
        let kv = try await ctx.createKeyValue(cfg: cfg)

        let stream = try await ctx.getStream(name: "KV_ttlbucket")
        XCTAssertEqual(stream?.info.config.allowMsgTTL, true)
        XCTAssertEqual(stream?.info.config.subjectDeleteMarkerTTL, NanoTimeInterval(60))

        let status = try await kv.status()
        XCTAssertEqual(status.limitMarkerTTL, NanoTimeInterval(60))
    }

    /// A key created with a short per-key TTL disappears after expiry while a
    /// normal key persists.
    func testKeyValuePerKeyTTLExpires() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        // A generous marker TTL keeps the expiry tombstone around across the poll
        // window, so the expired key reliably reads back as absent (not racing a
        // marker cleanup). Per-key TTL requires the bucket to enable msg TTL.
        var cfg = KeyValueConfig(bucket: "keyttl")
        cfg.limitMarkerTTL = NanoTimeInterval(60)
        let kv = try await ctx.createKeyValue(cfg: cfg)

        _ = try await kv.create("temp", Data("short-lived".utf8), ttl: NanoTimeInterval(1))
        _ = try await kv.put("perm", Data("durable".utf8))

        // Both present immediately.
        let temp = try await kv.get("temp")
        XCTAssertEqual(temp?.value, Data("short-lived".utf8))
        let permInitial = try await kv.get("perm")
        XCTAssertEqual(permInitial?.value, Data("durable".utf8))

        // The TTL'd key expires; the normal key remains.
        let expired = try await pollUntil {
            try await kv.get("temp") == nil
        }
        XCTAssertTrue(expired, "per-key TTL entry should be absent after expiry")
        let permFinal = try await kv.get("perm")
        XCTAssertEqual(
            permFinal?.value, Data("durable".utf8), "a key without a TTL must not expire")
    }

    // MARK: - Wire interop with the `nats` CLI

    /// A stream created with `allowMsgTTL` by the Swift client is reported with
    /// `allow_msg_ttl: true` by the `nats` CLI.
    func testAllowMsgTTLCLIInterop() async throws {
        try XCTSkipUnless(Self.natsCLIAvailable, "nats CLI not found on PATH")

        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }
        let url = natsServer.clientURL

        var cfg = StreamConfig(name: "TTL_CLI", subjects: ["ttlcli.>"])
        cfg.allowMsgTTL = true
        cfg.subjectDeleteMarkerTTL = NanoTimeInterval(10)
        _ = try await ctx.createStream(cfg: cfg)

        let info = try Self.runNats(["-s", url, "stream", "info", "TTL_CLI", "--json"])
        let json = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(info.utf8)) as? [String: Any])
        let streamCfg = try XCTUnwrap(json["config"] as? [String: Any])
        XCTAssertEqual(streamCfg["allow_msg_ttl"] as? Bool, true)
        XCTAssertEqual(streamCfg["subject_delete_marker_ttl"] as? Double, 10_000_000_000)
    }

    private static var natsCLIAvailable: Bool {
        (try? runNats(["--version"])) != nil
    }

    @discardableResult
    private static func runNats(_ arguments: [String]) throws -> String {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats"] + arguments
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = Pipe()
        try process.run()
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            throw NatsCLIError.nonZeroExit(Int(process.terminationStatus))
        }
        return String(data: data, encoding: .utf8) ?? ""
    }

    private enum NatsCLIError: Error {
        case nonZeroExit(Int)
    }
}
