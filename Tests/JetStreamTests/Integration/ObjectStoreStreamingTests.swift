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

class ObjectStoreStreamingTests: XCTestCase {

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

    // MARK: - Streaming put

    func testStreamingPutFromAsyncSequenceRoundtrips() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "streamput"))

        // Arbitrarily-sized source pieces: some smaller than a chunk, some larger, some
        // spanning several chunks. Their concatenation is the object.
        let chunkSize = 1024
        let pieceSizes = [10, 1024, 1, 2047, 4096, 300, 5000, 0, 777]
        var expected = Data()
        var seed: UInt8 = 0
        var pieces: [Data] = []
        for size in pieceSizes {
            var piece = Data()
            for _ in 0..<size {
                piece.append(seed)
                seed = seed &+ 1
            }
            pieces.append(piece)
            expected.append(piece)
        }
        let source = AsyncStreamFromArray(pieces)

        let meta = ObjectMeta(
            name: "streamed.bin", options: ObjectMetaOptions(maxChunkSize: UInt32(chunkSize)))
        let info = try await obs.put(meta, source: source)

        // Size, digest and chunk count all match the buffered semantics.
        XCTAssertEqual(info.size, UInt64(expected.count))
        XCTAssertEqual(info.digest, sha256ObjectDigest(of: expected))
        let expectedChunks = (expected.count + chunkSize - 1) / chunkSize
        XCTAssertEqual(Int(info.chunks), expectedChunks)

        // getBytes returns the exact concatenation.
        let read = try await obs.getBytes("streamed.bin")
        XCTAssertEqual(read, expected)
    }

    func testStreamingPutEmptySource() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "streamempty"))
        let source = AsyncStreamFromArray([Data]())
        let info = try await obs.put("nothing", source: source)

        XCTAssertEqual(info.size, 0)
        XCTAssertEqual(info.chunks, 0)
        XCTAssertEqual(info.digest, sha256ObjectDigest(of: Data()))
        let read = try await obs.getBytes("nothing")
        XCTAssertTrue(read.isEmpty)
    }

    // MARK: - Round-trip equivalence with the buffered put

    func testStreamingPutMatchesDataPutDigest() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "equiv"))

        // The same bytes, one via Data put and one via streaming put with oddly-sized
        // source pieces, must produce the same digest, size and chunk count.
        let size = 100_000
        let payload = Data((0..<size).map { UInt8(($0 * 7) & 0xFF) })
        let opts = ObjectMetaOptions(maxChunkSize: 1024)

        let viaData = try await obs.put(
            ObjectMeta(name: "via-data", options: opts), data: payload)

        let pieces = splitIntoUneven(payload, sizes: [1, 5000, 1023, 1025, 33, 90000])
        let viaStream = try await obs.put(
            ObjectMeta(name: "via-stream", options: opts), source: AsyncStreamFromArray(pieces))

        XCTAssertEqual(viaStream.digest, viaData.digest)
        XCTAssertEqual(viaStream.size, viaData.size)
        XCTAssertEqual(viaStream.chunks, viaData.chunks)

        let a = try await obs.getBytes("via-data")
        let b = try await obs.getBytes("via-stream")
        XCTAssertEqual(a, b)
        XCTAssertEqual(b, payload)
    }

    // MARK: - Streaming get

    func testStreamingGetYieldsChunksInOrder() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "streamget"))

        let size = 10 * 1024 + 123
        let payload = Data((0..<size).map { UInt8(($0 * 3) & 0xFF) })
        let meta = ObjectMeta(name: "obj.bin", options: ObjectMetaOptions(maxChunkSize: 1024))
        let put = try await obs.put(meta, data: payload)

        let reader = try await obs.getStream("obj.bin")
        XCTAssertEqual(reader.info.size, put.size)
        XCTAssertEqual(reader.info.digest, put.digest)
        XCTAssertEqual(reader.info.chunks, put.chunks)

        var assembled = Data()
        var yielded = 0
        for try await chunk in reader {
            assembled.append(chunk)
            yielded += 1
        }

        XCTAssertEqual(assembled, payload)
        // The number of yielded chunks equals the object's chunk count.
        XCTAssertEqual(yielded, Int(put.chunks))
        XCTAssertEqual(UInt64(assembled.count), reader.info.size)
    }

    func testStreamingGetZeroByteObject() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "getempty"))
        _ = try await obs.put("empty", data: Data())

        let reader = try await obs.getStream("empty")
        XCTAssertEqual(reader.info.size, 0)

        var yielded = 0
        for try await _ in reader {
            yielded += 1
        }
        XCTAssertEqual(yielded, 0)
    }

    // MARK: - Large object interop through streaming

    func testStreamingLargeObjectRoundtrip() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "streambig"))

        // ~1 MiB streamed in through 64 KiB pieces, default chunk size.
        let size = 1024 * 1024
        let payload = Data((0..<size).map { UInt8($0 & 0xFF) })
        let pieces = splitIntoUneven(payload, sizes: Array(repeating: 64 * 1024, count: 16))
        let info = try await obs.put("big.bin", source: AsyncStreamFromArray(pieces))
        XCTAssertEqual(info.size, UInt64(size))

        // Read it back out through the streaming get.
        let reader = try await obs.getStream("big.bin")
        var assembled = Data()
        for try await chunk in reader {
            assembled.append(chunk)
        }
        XCTAssertEqual(assembled.count, size)
        XCTAssertEqual(assembled, payload)
    }

    // MARK: - Digest mismatch surfaced at end of stream

    func testStreamingGetDigestMismatchThrowsAtEnd() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "streamcorrupt"))
        let info = try await obs.put("k", data: Data("original".utf8))

        // Tamper: publish an extra chunk on the object's chunk subject so the reassembled
        // bytes no longer match the stored digest/size.
        let chunkSubj = "$O.streamcorrupt.C.\(info.nuid)"
        _ = try await ctx.publish(chunkSubj, message: Data("tampered".utf8)).wait()

        let reader = try await obs.getStream("k")
        do {
            for try await _ in reader {
                // Drain: chunks are yielded, the mismatch is thrown after the last one.
            }
            XCTFail("expected digestMismatch at the end of the stream")
        } catch JetStreamError.ObjectStoreError.digestMismatch {
            // success
        }
    }

    // MARK: - Early termination tears down the consumer (no leak)

    func testStreamingGetEarlyBreakTearsDownConsumer() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let bucket = "streamleak"
        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: bucket))

        // Many chunks so a single-chunk read leaves plenty pending.
        let size = 64 * 1024
        let payload = Data((0..<size).map { UInt8($0 & 0xFF) })
        let meta = ObjectMeta(name: "obj", options: ObjectMetaOptions(maxChunkSize: 1024))
        let put = try await obs.put(meta, data: payload)
        XCTAssertGreaterThan(put.chunks, 1)

        let streamName = "OBJ_\(bucket)"
        do {
            let reader = try await obs.getStream("obj")
            var count = 0
            for try await _ in reader {
                count += 1
                if count == 1 {
                    break  // Abandon iteration early.
                }
            }
            XCTAssertEqual(count, 1)
        }

        // The reader's ephemeral ordered consumer must be torn down by the cancellation
        // path (onTermination -> consumer.stop()), leaving no leaked server consumer.
        try await pollUntilNoConsumers(ctx: ctx, stream: streamName, timeout: 20)
    }

    // MARK: - CLI interop through streaming put

    func testStreamingPutReadByNatsCLI() async throws {
        try XCTSkipUnless(Self.natsCLIAvailable, "nats CLI not found on PATH")

        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }
        let url = natsServer.clientURL

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "streamcli"))

        // Stream a moderately large object in through uneven pieces.
        let size = 300_000
        let payload = Data((0..<size).map { UInt8(($0 * 5) & 0xFF) })
        let pieces = splitIntoUneven(payload, sizes: [1, 100_000, 199_999])
        _ = try await obs.put("greeting", source: AsyncStreamFromArray(pieces))

        let outPath = NSTemporaryDirectory() + "streamobj-\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: outPath) }
        _ = try Self.runNats([
            "-s", url, "object", "get", "streamcli", "greeting", "-O", outPath, "-f",
            "--no-progress",
        ])
        let cliRead = try Data(contentsOf: URL(fileURLWithPath: outPath))
        XCTAssertEqual(cliRead, payload)
    }

    // MARK: - Helpers

    /// Splits `data` into pieces of the given sizes; any remainder becomes a final piece.
    private func splitIntoUneven(_ data: Data, sizes: [Int]) -> [Data] {
        var pieces: [Data] = []
        var start = data.startIndex
        for size in sizes {
            guard start < data.endIndex else { break }
            let end = data.index(start, offsetBy: size, limitedBy: data.endIndex) ?? data.endIndex
            pieces.append(data.subdata(in: start..<end))
            start = end
        }
        if start < data.endIndex {
            pieces.append(data.subdata(in: start..<data.endIndex))
        }
        return pieces
    }

    /// Counts the consumers currently registered on `stream`.
    private func consumerCount(ctx: JetStreamContext, stream: String) async throws -> Int {
        var count = 0
        let names = await ctx.consumerNames(stream: stream)
        for try await _ in names {
            count += 1
        }
        return count
    }

    /// Polls until `stream` has zero consumers, failing after `timeout` seconds.
    private func pollUntilNoConsumers(
        ctx: JetStreamContext, stream: String, timeout: TimeInterval
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var last = -1
        while Date() < deadline {
            last = try await consumerCount(ctx: ctx, stream: stream)
            if last == 0 {
                return
            }
            try await Task.sleep(nanoseconds: 200_000_000)
        }
        XCTFail(
            "expected the streaming consumer torn down; \(last) still present after \(timeout)s")
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

    /// A source that throws mid-stream must leave no committed object and no orphaned chunks
    /// (the partial upload is purged), matching the buffered put's error path.
    func testStreamingPutSourceThrowsPurgesPartial() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "putthrow"))
        let source = ThrowingAsyncStreamFromArray(
            pieces: [Data(repeating: 1, count: 1024), Data(repeating: 2, count: 1024)],
            throwAfter: 2)
        let meta = ObjectMeta(name: "obj", options: ObjectMetaOptions(maxChunkSize: 1024))

        do {
            _ = try await obs.put(meta, source: source)
            XCTFail("expected the source error to propagate")
        } catch is StreamSourceError {
            // expected
        }

        // No meta was committed for the object.
        do {
            _ = try await obs.getInfo("obj")
            XCTFail("expected objectNotFound")
        } catch JetStreamError.ObjectStoreError.objectNotFound {}

        // The partial chunks were purged: the backing stream is empty again.
        let stream = try await ctx.getStream(name: "OBJ_putthrow")
        let info = try await stream?.info()
        XCTAssertEqual(info?.state.messages, 0, "partial upload chunks should be purged")
    }

    private enum NatsCLIError: Error {
        case nonZeroExit(Int)
    }
}

/// An `AsyncSequence` that yields some pieces then throws, to exercise the streaming put's
/// mid-stream error / partial-purge path.
struct StreamSourceError: Error {}

struct ThrowingAsyncStreamFromArray: AsyncSequence, Sendable {
    typealias Element = Data
    let pieces: [Data]
    let throwAfter: Int

    struct AsyncIterator: AsyncIteratorProtocol {
        var pieces: [Data]
        let throwAfter: Int
        var index = 0

        mutating func next() async throws -> Data? {
            if index >= throwAfter { throw StreamSourceError() }
            guard index < pieces.count else { return nil }
            defer { index += 1 }
            return pieces[index]
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(pieces: pieces, throwAfter: throwAfter)
    }
}

/// A simple `AsyncSequence` that yields the given `Data` pieces in order, used to drive the
/// streaming put from a fixed array of arbitrarily-sized pieces.
struct AsyncStreamFromArray: AsyncSequence, Sendable {
    typealias Element = Data
    let pieces: [Data]

    init(_ pieces: [Data]) {
        self.pieces = pieces
    }

    struct AsyncIterator: AsyncIteratorProtocol {
        var pieces: [Data]
        var index = 0

        mutating func next() async -> Data? {
            guard index < pieces.count else { return nil }
            defer { index += 1 }
            return pieces[index]
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(pieces: pieces)
    }
}

/// Recomputes the object digest string `"SHA-256=<base64url>"` in the test target, mirroring
/// ``ObjectStoreCoding`` (which is internal to the JetStream module).
private func sha256ObjectDigest(of data: Data) -> String {
    let hash = Data(SHA256.hash(data: data))
    let base64url =
        hash.base64EncodedString()
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "/", with: "_")
    return "SHA-256=" + base64url
}
