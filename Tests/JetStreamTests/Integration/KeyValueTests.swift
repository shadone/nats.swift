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

class KeyValueTests: XCTestCase {

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

    func testCreateKeyValueAndConfig() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "config"))
        let stream = try await ctx.getStream(name: "KV_config")
        guard let cfg = stream?.info.config else {
            return XCTFail("expected backing stream KV_config")
        }

        XCTAssertEqual(cfg.name, "KV_config")
        XCTAssertEqual(cfg.subjects, ["$KV.config.>"])
        XCTAssertEqual(cfg.maxMsgsPerSubject, 1)
        XCTAssertEqual(cfg.allowDirect, true)
        XCTAssertEqual(cfg.allowRollup, true)
        XCTAssertEqual(cfg.denyDelete, true)
        XCTAssertEqual(cfg.discard, .new)
        XCTAssertEqual(kv.bucket, "config")
    }

    func testPutGetRoundtrip() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        // history > 1 so an older revision survives a later put (default history
        // is 1, which evicts prior revisions per subject).
        let kv = try await ctx.createKeyValue(
            cfg: {
                var c = KeyValueConfig(bucket: "roundtrip")
                c.history = 5
                return c
            }())
        let revision = try await kv.put("name", Data("hello".utf8))
        XCTAssertEqual(revision, 1)

        guard let entry = try await kv.get("name") else {
            return XCTFail("expected an entry")
        }
        XCTAssertEqual(entry.bucket, "roundtrip")
        XCTAssertEqual(entry.key, "name")
        XCTAssertEqual(entry.value, Data("hello".utf8))
        XCTAssertEqual(entry.revision, 1)
        XCTAssertEqual(entry.operation, .put)

        // Overwrite bumps the revision and get returns the newest value.
        let revision2 = try await kv.put("name", Data("world".utf8))
        XCTAssertEqual(revision2, 2)
        let entry2 = try await kv.get("name")
        XCTAssertEqual(entry2?.value, Data("world".utf8))
        XCTAssertEqual(entry2?.revision, 2)

        // Fetch a specific historical revision by sequence.
        let byRevision = try await kv.get("name", revision: 1)
        XCTAssertEqual(byRevision?.value, Data("hello".utf8))
        // A revision that belongs to a different key is rejected.
        _ = try await kv.put("other", Data("x".utf8))
        let crossKey = try await kv.get("name", revision: 3)
        XCTAssertNil(crossKey)
    }

    func testGetAbsentKeyReturnsNil() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "absent"))
        let missing = try await kv.get("missing")
        XCTAssertNil(missing)
    }

    func testCreateThenCreateThrowsKeyExists() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "createonce"))
        let revision = try await kv.create("k", Data("first".utf8))
        XCTAssertEqual(revision, 1)

        do {
            _ = try await kv.create("k", Data("second".utf8))
            XCTFail("expected keyExists")
        } catch JetStreamError.KeyValueError.keyExists {
            // success
        }
        // The original value is unchanged.
        let unchanged = try await kv.get("k")
        XCTAssertEqual(unchanged?.value, Data("first".utf8))
    }

    func testUpdateWithCorrectAndIncorrectRevision() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(
            cfg: {
                var c = KeyValueConfig(bucket: "cas")
                c.history = 5
                return c
            }())
        let r1 = try await kv.create("k", Data("v1".utf8))

        // Correct revision succeeds.
        let r2 = try await kv.update("k", Data("v2".utf8), revision: r1)
        XCTAssertEqual(r2, r1 + 1)
        let afterUpdate = try await kv.get("k")
        XCTAssertEqual(afterUpdate?.value, Data("v2".utf8))

        // Stale revision fails.
        do {
            _ = try await kv.update("k", Data("v3".utf8), revision: r1)
            XCTFail("expected wrongLastRevision")
        } catch JetStreamError.KeyValueError.wrongLastRevision {
            // success
        }
        let afterStale = try await kv.get("k")
        XCTAssertEqual(afterStale?.value, Data("v2".utf8))
    }

    func testDeleteThenGetReturnsNil() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "del"))
        _ = try await kv.put("k", Data("v".utf8))
        try await kv.delete("k")

        // A deleted key reads as nil.
        let deleted = try await kv.get("k")
        XCTAssertNil(deleted)

        // Create after delete is allowed (recreates over the tombstone).
        let revision = try await kv.create("k", Data("again".utf8))
        XCTAssertGreaterThan(revision, 1)
        let recreated = try await kv.get("k")
        XCTAssertEqual(recreated?.value, Data("again".utf8))
    }

    func testPurge() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(
            cfg: {
                var c = KeyValueConfig(bucket: "purge")
                c.history = 5
                return c
            }())
        _ = try await kv.put("k", Data("v1".utf8))
        _ = try await kv.put("k", Data("v2".utf8))
        try await kv.purge("k")

        // Purged key reads as nil.
        let purged = try await kv.get("k")
        XCTAssertNil(purged)
        // Purge rolls up prior history: only the purge tombstone remains for the key.
        let stream = try await ctx.getStream(name: "KV_purge")
        XCTAssertEqual(stream?.info.state.messages, 1)
    }

    func testHistoryReflectedInStatus() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(
            cfg: {
                var c = KeyValueConfig(bucket: "hist")
                c.history = 10
                return c
            }())
        _ = try await kv.put("a", Data("1".utf8))
        _ = try await kv.put("a", Data("2".utf8))
        _ = try await kv.put("b", Data("1".utf8))

        let status = try await kv.status()
        XCTAssertEqual(status.bucket, "hist")
        XCTAssertEqual(status.history, 10)
        XCTAssertEqual(status.backingStore, "JetStream")
        XCTAssertEqual(status.values, 3)  // 2 for a + 1 for b
    }

    func testOpenNonExistentBucketThrowsBucketNotFound() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        do {
            _ = try await ctx.keyValue(bucket: "nope")
            XCTFail("expected bucketNotFound")
        } catch JetStreamError.KeyValueError.bucketNotFound(let bucket) {
            XCTAssertEqual(bucket, "nope")
        }
    }

    func testCreateOrUpdateIdempotency() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let cfg = KeyValueConfig(bucket: "idem")
        let kv1 = try await ctx.createOrUpdateKeyValue(cfg: cfg)
        _ = try await kv1.put("k", Data("v".utf8))

        // Second call with identical config is a no-op and keeps the data.
        let kv2 = try await ctx.createOrUpdateKeyValue(cfg: cfg)
        let kept = try await kv2.get("k")
        XCTAssertEqual(kept?.value, Data("v".utf8))

        // Delete removes the bucket; opening it then fails.
        try await ctx.deleteKeyValue(bucket: "idem")
        do {
            _ = try await ctx.keyValue(bucket: "idem")
            XCTFail("expected bucketNotFound after delete")
        } catch JetStreamError.KeyValueError.bucketNotFound {
            // success
        }
    }

    // MARK: - Wire interop with the `nats` CLI

    /// Proves wire compatibility in both directions: a value written by the
    /// Swift client is read back by the `nats` CLI, and a bucket created and
    /// written by the `nats` CLI is read by the Swift client.
    func testNatsCLIInterop() async throws {
        try XCTSkipUnless(Self.natsCLIAvailable, "nats CLI not found on PATH")

        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }
        let url = natsServer.clientURL

        // Direction A: Swift writes, CLI reads.
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "swiftbucket"))
        _ = try await kv.put("greeting", Data("hello-from-swift".utf8))
        let cliRead = try Self.runNats(["-s", url, "kv", "get", "swiftbucket", "greeting", "--raw"])
        XCTAssertEqual(
            cliRead.trimmingCharacters(in: .whitespacesAndNewlines), "hello-from-swift")

        // Direction B: CLI creates and writes, Swift reads.
        _ = try Self.runNats(["-s", url, "kv", "add", "clibucket"])
        _ = try Self.runNats(["-s", url, "kv", "put", "clibucket", "greeting", "hello-from-cli"])
        let boundKv = try await ctx.keyValue(bucket: "clibucket")
        let entry = try await boundKv.get("greeting")
        XCTAssertEqual(entry?.value, Data("hello-from-cli".utf8))
        XCTAssertEqual(entry?.operation, .put)
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
