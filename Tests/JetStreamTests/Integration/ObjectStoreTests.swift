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

#if canImport(CryptoKit)
    import CryptoKit
#else
    import Crypto
#endif

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

    private enum ReadOutcome: Equatable {
        case failedFast
        case returned
        case timedOut
    }

    /// A read of an object whose META still reports chunks, but whose chunk subject has been purged
    /// (the state a concurrent delete/overwrite leaves between `getInfo` resolving and the read
    /// subscribing), must FAIL FAST with `objectNotFound` rather than hang forever waiting for chunks
    /// that will never arrive. Regression test for the missing-chunks wedge.
    func testGetWithMissingChunksFailsFastInsteadOfHanging() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "hang"))
        let info = try await obs.put("k", data: Data("payload".utf8))

        // Purge the chunk subject directly, leaving the meta (size > 0, chunks > 0) intact.
        let streamHandle = try await ctx.getStream(name: "OBJ_hang")
        let stream = try XCTUnwrap(streamHandle)
        _ = try await stream.purge(subject: "$O.hang.C.\(info.nuid)")

        // Race the read against a timeout: a regression to the hang fails here rather than wedging
        // the whole suite.
        let outcome = try await withThrowingTaskGroup(of: ReadOutcome.self) { group in
            group.addTask {
                do {
                    _ = try await obs.getBytes("k")
                    return .returned
                } catch JetStreamError.ObjectStoreError.objectNotFound {
                    return .failedFast
                }
            }
            group.addTask {
                try await Task.sleep(nanoseconds: 8_000_000_000)
                return .timedOut
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
        XCTAssertEqual(
            outcome, .failedFast, "get must fail fast on missing chunks, not hang or return")
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

    // MARK: - updateMeta

    func testUpdateMetaChangesDescriptionAndMetadata() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "updmeta"))
        let put = try await obs.put("k", data: Data("value".utf8))

        var meta = ObjectMeta(name: "k")
        meta.description = "now described"
        meta.metadata = ["team": "core"]
        try await obs.updateMeta("k", meta: meta)

        let fetched = try await obs.getInfo("k")
        XCTAssertEqual(fetched.description, "now described")
        XCTAssertEqual(fetched.metadata?["team"], "core")
        // Identity is preserved: nuid and size are untouched by a meta update.
        XCTAssertEqual(fetched.nuid, put.nuid)
        XCTAssertEqual(fetched.size, UInt64(Data("value".utf8).count))
        XCTAssertEqual(fetched.digest, put.digest)

        // The object contents still read back correctly.
        let bytes = try await obs.getBytes("k")
        XCTAssertEqual(bytes, Data("value".utf8))
    }

    func testUpdateMetaRenameMovesTheObject() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "rename"))
        let put = try await obs.put("old", data: Data("payload".utf8))

        try await obs.updateMeta("old", meta: ObjectMeta(name: "new"))

        // The old name is gone.
        do {
            _ = try await obs.getInfo("old")
            XCTFail("expected objectNotFound for the old name")
        } catch JetStreamError.ObjectStoreError.objectNotFound {
            // success
        }

        // The new name resolves and its contents are intact.
        let fetched = try await obs.getInfo("new")
        XCTAssertEqual(fetched.name, "new")
        XCTAssertEqual(fetched.nuid, put.nuid)
        let renamedBytes = try await obs.getBytes("new")
        XCTAssertEqual(renamedBytes, Data("payload".utf8))
    }

    func testUpdateMetaRenameOntoExistingThrows() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "renameclash"))
        _ = try await obs.put("a", data: Data("va".utf8))
        _ = try await obs.put("b", data: Data("vb".utf8))

        do {
            try await obs.updateMeta("a", meta: ObjectMeta(name: "b"))
            XCTFail("expected objectAlreadyExists")
        } catch JetStreamError.ObjectStoreError.objectAlreadyExists {
            // success
        }
    }

    func testUpdateMetaOnDeletedThrows() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "updmetadel"))
        _ = try await obs.put("k", data: Data("v".utf8))
        try await obs.delete("k")

        do {
            try await obs.updateMeta("k", meta: ObjectMeta(name: "k2"))
            XCTFail("expected updateMetaDeleted")
        } catch JetStreamError.ObjectStoreError.updateMetaDeleted {
            // success
        }
    }

    // MARK: - links

    func testAddLinkAndGetInfoReturnsLink() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "links"))
        let target = try await obs.put("target", data: Data("payload".utf8))

        let link = try await obs.addLink("alias", to: target)
        XCTAssertEqual(link.name, "alias")
        XCTAssertEqual(link.options?.link?.bucket, "links")
        XCTAssertEqual(link.options?.link?.name, "target")
        XCTAssertFalse(link.nuid.isEmpty)

        // getInfo returns the link's meta with the link populated.
        let fetched = try await obs.getInfo("alias")
        XCTAssertEqual(fetched.options?.link?.bucket, "links")
        XCTAssertEqual(fetched.options?.link?.name, "target")
    }

    func testAddLinkRejectsDeletedLinkAndOverwrite() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "linkreject"))
        let target = try await obs.put("target", data: Data("payload".utf8))

        // Link to a deleted object is rejected.
        var deletedTarget = try await obs.put("gone", data: Data("x".utf8))
        try await obs.delete("gone")
        deletedTarget = try await obs.getInfo("gone", showDeleted: true)
        do {
            _ = try await obs.addLink("l1", to: deletedTarget)
            XCTFail("expected noLinkToDeleted")
        } catch JetStreamError.ObjectStoreError.noLinkToDeleted {
            // success
        }

        // Link to a link is rejected.
        let link = try await obs.addLink("alias", to: target)
        do {
            _ = try await obs.addLink("l2", to: link)
            XCTFail("expected noLinkToLink")
        } catch JetStreamError.ObjectStoreError.noLinkToLink {
            // success
        }

        // Overwriting a live non-link object with a link is rejected.
        do {
            _ = try await obs.addLink("target", to: target)
            XCTFail("expected objectAlreadyExists")
        } catch JetStreamError.ObjectStoreError.objectAlreadyExists {
            // success
        }
    }

    func testAddBucketLink() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let a = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "srcbucket"))
        let b = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "dstbucket"))

        let link = try await a.addBucketLink("dirlink", to: b)
        XCTAssertEqual(link.options?.link?.bucket, "dstbucket")
        // A whole-store link carries no object name.
        XCTAssertNil(link.options?.link?.name)

        let fetched = try await a.getInfo("dirlink")
        XCTAssertEqual(fetched.options?.link?.bucket, "dstbucket")
        XCTAssertNil(fetched.options?.link?.name)
    }

    // MARK: - seal / status

    func testSealPreventsFurtherWrites() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "sealed"))
        _ = try await obs.put("k", data: Data("v".utf8))

        try await obs.seal()

        let sealedStatus = try await obs.status()
        XCTAssertTrue(sealedStatus.sealed)

        do {
            _ = try await obs.put("k2", data: Data("v2".utf8))
            XCTFail("expected the put to fail on a sealed store")
        } catch {
            // Expected: a sealed stream rejects publishes.
        }
    }

    func testStatusReflectsConfig() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        var cfg = ObjectStoreConfig(bucket: "statusbucket")
        cfg.description = "a described bucket"
        cfg.ttl = NanoTimeInterval(60)
        let obs = try await ctx.createObjectStore(cfg: cfg)
        _ = try await obs.put("k", data: Data("some-bytes".utf8))

        let status = try await obs.status()
        XCTAssertEqual(status.bucket, "statusbucket")
        XCTAssertEqual(status.description, "a described bucket")
        XCTAssertEqual(status.ttl, NanoTimeInterval(60))
        XCTAssertFalse(status.sealed)
        XCTAssertEqual(status.backingStore, "JetStream")
        XCTAssertGreaterThan(status.size, 0)
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
