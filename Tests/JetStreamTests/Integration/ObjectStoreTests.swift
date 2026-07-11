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

import CryptoKit
import JetStream
import Nats
import NatsServer
import XCTest

class ObjectStoreTests: XCTestCase {

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

    func testCreateObjectStoreAndConfig() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "config"))
        let stream = try await ctx.getStream(name: "OBJ_config")
        guard let cfg = stream?.info.config else {
            return XCTFail("expected backing stream OBJ_config")
        }

        XCTAssertEqual(cfg.name, "OBJ_config")
        XCTAssertEqual(cfg.subjects, ["$O.config.C.>", "$O.config.M.>"])
        XCTAssertEqual(cfg.allowDirect, true)
        XCTAssertEqual(cfg.allowRollup, true)
        XCTAssertEqual(cfg.discard, .new)
        // The object store must NOT deny delete/purge (it purges chunks). The server
        // echoes these back as false rather than nil, so assert they are not true.
        XCTAssertNotEqual(cfg.denyDelete, true)
        XCTAssertNotEqual(cfg.denyPurge, true)
        XCTAssertEqual(obs.bucket, "config")
    }

    func testPutGetBytesRoundtrip() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "roundtrip"))
        let payload = Data("hello-object-store".utf8)
        let info = try await obs.put("greeting", data: payload)

        XCTAssertEqual(info.name, "greeting")
        XCTAssertEqual(info.bucket, "roundtrip")
        XCTAssertEqual(info.size, UInt64(payload.count))
        XCTAssertEqual(info.chunks, 1)
        XCTAssertFalse(info.nuid.isEmpty)
        XCTAssertEqual(info.digest, sha256ObjectDigest(of: payload))

        let read = try await obs.getBytes("greeting")
        XCTAssertEqual(read, payload)

        // getInfo returns the same identity with a real modTime (not the zero time).
        let fetched = try await obs.getInfo("greeting")
        XCTAssertEqual(fetched.nuid, info.nuid)
        XCTAssertEqual(fetched.size, info.size)
        XCTAssertNotEqual(fetched.modTime, "0001-01-01T00:00:00Z")
    }

    func testLargeMultiChunkObject() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "large"))

        // ~1 MiB of deterministic data, put with a 1 KiB chunk size -> many chunks.
        let size = 1024 * 1024
        let payload = Data((0..<size).map { UInt8($0 & 0xFF) })
        let meta = ObjectMeta(name: "big.bin", options: ObjectMetaOptions(maxChunkSize: 1024))
        let info = try await obs.put(meta, data: payload)

        XCTAssertEqual(info.size, UInt64(size))
        XCTAssertEqual(info.chunks, 1024)
        XCTAssertGreaterThan(info.chunks, 1)

        // Reassembled in order and digest verifies.
        let read = try await obs.getBytes("big.bin")
        XCTAssertEqual(read.count, size)
        XCTAssertEqual(read, payload)
    }

    func testExplicitZeroChunkSizeUsesDefault() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "zerochunk"))

        // An explicit maxChunkSize of 0 must fall back to the default rather than
        // hang the chunk loop, and the persisted meta must carry the resolved size.
        let payload = Data("some non-empty content".utf8)
        let meta = ObjectMeta(name: "obj", options: ObjectMetaOptions(maxChunkSize: 0))
        let info = try await obs.put(meta, data: payload)

        XCTAssertEqual(info.size, UInt64(payload.count))
        XCTAssertEqual(info.chunks, 1)  // one default-sized chunk
        XCTAssertEqual(info.options?.maxChunkSize, 128 * 1024)

        let fetched = try await obs.getInfo("obj")
        XCTAssertEqual(fetched.options?.maxChunkSize, 128 * 1024)
        let bytes = try await obs.getBytes("obj")
        XCTAssertEqual(bytes, payload)
    }

    func testZeroByteObject() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "empty"))
        let info = try await obs.put("nothing", data: Data())

        XCTAssertEqual(info.size, 0)
        XCTAssertEqual(info.chunks, 0)
        // Digest of the empty input is the well-known constant.
        XCTAssertEqual(
            info.digest, "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=")

        let read = try await obs.getBytes("nothing")
        XCTAssertTrue(read.isEmpty)
    }

    func testRePutSupersedesAndPurgesOldChunks() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "supersede"))
        let first = try await obs.put("k", data: Data("first-value".utf8))
        let second = try await obs.put("k", data: Data("second-value".utf8))

        // A new nuid is assigned on the second put.
        XCTAssertNotEqual(first.nuid, second.nuid)

        // The latest value wins.
        let read = try await obs.getBytes("k")
        XCTAssertEqual(read, Data("second-value".utf8))

        // The old object's chunks were purged.
        let stream = try await ctx.getStream(name: "OBJ_supersede")
        let oldChunkSubj = "$O.supersede.C.\(first.nuid)"
        let oldChunk = try await stream?.getMessageDirect(lastForSubject: oldChunkSubj)
        XCTAssertNil(oldChunk)
    }

    func testDeleteMarksDeletedAndPurgesChunks() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "del"))
        let info = try await obs.put("k", data: Data("value".utf8))
        try await obs.delete("k")

        // A deleted object is not found by default.
        do {
            _ = try await obs.getInfo("k")
            XCTFail("expected objectNotFound")
        } catch JetStreamError.ObjectStoreError.objectNotFound {
            // success
        }

        // With showDeleted it is visible, marked deleted with a zeroed instance.
        let deleted = try await obs.getInfo("k", showDeleted: true)
        XCTAssertTrue(deleted.deleted)
        XCTAssertEqual(deleted.size, 0)
        XCTAssertEqual(deleted.chunks, 0)

        // The chunks were purged.
        let stream = try await ctx.getStream(name: "OBJ_del")
        let chunkSubj = "$O.del.C.\(info.nuid)"
        let chunk = try await stream?.getMessageDirect(lastForSubject: chunkSubj)
        XCTAssertNil(chunk)
    }

    func testCorruptChunkFailsDigestVerification() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "corrupt"))
        let info = try await obs.put("k", data: Data("original".utf8))

        // Tamper: publish an extra chunk on the object's chunk subject so the reassembled
        // bytes no longer match the stored digest/size.
        let chunkSubj = "$O.corrupt.C.\(info.nuid)"
        _ = try await ctx.publish(chunkSubj, message: Data("tampered".utf8)).wait()

        do {
            _ = try await obs.getBytes("k")
            XCTFail("expected digestMismatch")
        } catch JetStreamError.ObjectStoreError.digestMismatch {
            // success
        }
    }

    func testOpenMissingBucketThrowsBucketNotFound() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        do {
            _ = try await ctx.objectStore(bucket: "nope")
            XCTFail("expected bucketNotFound")
        } catch JetStreamError.ObjectStoreError.bucketNotFound(let bucket) {
            XCTAssertEqual(bucket, "nope")
        }
    }

    // MARK: - Wire interop with the `nats` CLI

    /// Proves object-store wire compatibility in both directions, exercising the
    /// `OBJ_`/`$O.`/padded-name/`SHA-256=`/rollup conventions end to end.
    func testNatsCLIInterop() async throws {
        try XCTSkipUnless(Self.natsCLIAvailable, "nats CLI not found on PATH")

        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }
        let url = natsServer.clientURL

        // Direction A: Swift writes, CLI reads.
        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "swiftobj"))
        _ = try await obs.put("greeting", data: Data("hello-from-swift".utf8))

        let outPath = NSTemporaryDirectory() + "objstore-\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: outPath) }
        _ = try Self.runNats([
            "-s", url, "object", "get", "swiftobj", "greeting", "-O", outPath, "-f",
            "--no-progress",
        ])
        let cliRead = try Data(contentsOf: URL(fileURLWithPath: outPath))
        XCTAssertEqual(cliRead, Data("hello-from-swift".utf8))

        // Direction B: CLI creates and writes, Swift reads.
        _ = try Self.runNats(["-s", url, "object", "add", "cliobj"])
        let inPath = NSTemporaryDirectory() + "objstore-in-\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: inPath) }
        try Data("hello-from-cli".utf8).write(to: URL(fileURLWithPath: inPath))
        _ = try Self.runNats([
            "-s", url, "object", "put", "cliobj", inPath, "--name", "greeting", "-f",
            "--no-progress",
        ])

        let boundObs = try await ctx.objectStore(bucket: "cliobj")
        let read = try await boundObs.getBytes("greeting")
        XCTAssertEqual(read, Data("hello-from-cli".utf8))
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

/// Recomputes the object digest string `"SHA-256=<base64url>"` in the test target.
/// ``ObjectStoreCoding`` is internal to the JetStream module, so this integration target
/// (which imports JetStream non-`@testable`) cannot reach it; this mirrors the same
/// computation for assertions.
private func sha256ObjectDigest(of data: Data) -> String {
    let hash = Data(SHA256.hash(data: data))
    let base64url =
        hash.base64EncodedString()
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "/", with: "_")
    return "SHA-256=" + base64url
}
