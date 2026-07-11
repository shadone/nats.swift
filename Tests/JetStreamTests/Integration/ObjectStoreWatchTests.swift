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

import Nats
import NatsServer
import XCTest

@testable import JetStream

final class ObjectStoreWatchTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    // MARK: - watch

    /// The initial value(s) arrive, then exactly one `nil` end-of-initial marker, then a
    /// put made after the marker is delivered as a live update. Asserts name/size/digest.
    func testWatchInitialValuesThenMarkerThenLive() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "watch"))
        let payloadA = Data("value-a".utf8)
        let a = try await obs.put("a", data: payloadA)

        let watcher = try await obs.watch()
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        guard case .entry(let initial) = try await nextElement(&it) else {
            return XCTFail("expected the initial value")
        }
        XCTAssertEqual(initial.name, "a")
        XCTAssertEqual(initial.bucket, "watch")
        XCTAssertEqual(initial.size, UInt64(payloadA.count))
        XCTAssertEqual(initial.digest, a.digest)
        XCTAssertEqual(initial.digest, ObjectStoreCoding.digest(of: payloadA))
        XCTAssertFalse(initial.deleted)

        guard case .marker = try await nextElement(&it) else {
            return XCTFail("expected the end-of-initial marker")
        }

        // A put after the marker arrives on the live tail.
        let payloadB = Data("value-b".utf8)
        let b = try await obs.put("b", data: payloadB)
        guard case .entry(let live) = try await nextElement(&it) else {
            return XCTFail("expected the live update")
        }
        XCTAssertEqual(live.name, "b")
        XCTAssertEqual(live.size, UInt64(payloadB.count))
        XCTAssertEqual(live.digest, b.digest)

        await watcher.stop()
    }

    /// watch delivers the latest info of every object before the marker.
    func testWatchAllInitialValues() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "watchall"))
        _ = try await obs.put("a", data: Data("va".utf8))
        _ = try await obs.put("b", data: Data("vb".utf8))
        _ = try await obs.put("c", data: Data("vc".utf8))

        let watcher = try await obs.watch()
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }

        let initial = try await collectUntilMarker(watcher)
        let byName = Dictionary(uniqueKeysWithValues: initial.map { ($0.name, $0) })
        XCTAssertEqual(Set(byName.keys), ["a", "b", "c"])
        XCTAssertEqual(byName["a"]?.size, UInt64(Data("va".utf8).count))
        XCTAssertEqual(byName["b"]?.digest, ObjectStoreCoding.digest(of: Data("vb".utf8)))
        XCTAssertTrue(initial.allSatisfy { !$0.deleted })

        await watcher.stop()
    }

    /// includeHistory still delivers the current object set then the marker. Object meta
    /// is rolled up per subject (`Nats-Rollup: sub`), so a re-put replaces the prior meta
    /// rather than retaining it: the "history" for an object name collapses to its latest
    /// meta, which is exactly what a correct includeHistory watch surfaces.
    func testIncludeHistoryDeliversCurrentObjects() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "objhist"))
        _ = try await obs.put("a", data: Data("first".utf8))
        _ = try await obs.put("b", data: Data("vb".utf8))
        let reput = try await obs.put("a", data: Data("second".utf8))

        var opts = ObjectStoreWatchOptions()
        opts.includeHistory = true
        let watcher = try await obs.watch(opts: opts)
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }

        let entries = try await collectUntilMarker(watcher)
        let byName = Dictionary(uniqueKeysWithValues: entries.map { ($0.name, $0) })
        XCTAssertEqual(Set(byName.keys), ["a", "b"])
        // "a" reflects the latest put (its new nuid/size), proving the rollup collapse.
        XCTAssertEqual(byName["a"]?.nuid, reput.nuid)
        XCTAssertEqual(byName["a"]?.size, UInt64(Data("second".utf8).count))

        await watcher.stop()
    }

    /// updatesOnly delivers the marker immediately with NO initial values, then only puts
    /// made after the watch started.
    func testUpdatesOnlyDeliversOnlyPostWatchPuts() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "objupdates"))
        _ = try await obs.put("pre", data: Data("before".utf8))

        var opts = ObjectStoreWatchOptions()
        opts.updatesOnly = true
        let watcher = try await obs.watch(opts: opts)
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        // The marker is the very first element: no initial values are delivered.
        guard case .marker = try await nextElement(&it) else {
            return XCTFail("updatesOnly must deliver the marker before any entry")
        }

        // Only the post-watch put is delivered.
        _ = try await obs.put("post", data: Data("after".utf8))
        guard case .entry(let live) = try await nextElement(&it) else {
            return XCTFail("expected the post-watch update")
        }
        XCTAssertEqual(live.name, "post")
        XCTAssertEqual(live.size, UInt64(Data("after".utf8).count))

        await watcher.stop()
    }

    /// ignoreDeletes suppresses deleted objects but still fires the marker, and a
    /// subsequent live object still arrives.
    func testIgnoreDeletesSuppressesDeletedObjects() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "objignoredel"))
        _ = try await obs.put("a", data: Data("va".utf8))
        try await obs.delete("a")
        _ = try await obs.put("b", data: Data("vb".utf8))

        var opts = ObjectStoreWatchOptions()
        opts.ignoreDeletes = true
        let watcher = try await obs.watch(opts: opts)
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        // The deleted object is never yielded; only live "b" arrives.
        guard case .entry(let live) = try await nextElement(&it) else {
            return XCTFail("expected the live object b")
        }
        XCTAssertEqual(live.name, "b")
        XCTAssertFalse(live.deleted)

        guard case .marker = try await nextElement(&it) else {
            return XCTFail("expected the marker after the initial snapshot")
        }

        // A live put after the marker still arrives.
        _ = try await obs.put("c", data: Data("vc".utf8))
        guard case .entry(let after) = try await nextElement(&it) else {
            return XCTFail("expected the post-marker live object c")
        }
        XCTAssertEqual(after.name, "c")

        await watcher.stop()
    }

    // MARK: - list

    /// list() returns every live object; a delete removes it, and showDeleted brings it
    /// back.
    func testListReflectsLiveAndDeletedObjects() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "objlist"))
        _ = try await obs.put("a", data: Data("va".utf8))
        _ = try await obs.put("b", data: Data("vb".utf8))
        _ = try await obs.put("c", data: Data("vc".utf8))

        let all = try await obs.list()
        XCTAssertEqual(Set(all.map { $0.name }), ["a", "b", "c"])

        try await obs.delete("b")
        let live = try await obs.list()
        XCTAssertEqual(Set(live.map { $0.name }), ["a", "c"])
        XCTAssertEqual(live.count, 2)

        let withDeleted = try await obs.list(showDeleted: true)
        XCTAssertEqual(Set(withDeleted.map { $0.name }), ["a", "b", "c"])
        XCTAssertTrue(withDeleted.contains { $0.name == "b" && $0.deleted })
    }

    /// list() on an empty bucket throws noObjectsFound.
    func testListEmptyBucketThrowsNoObjectsFound() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "objempty"))
        do {
            _ = try await obs.list()
            XCTFail("expected noObjectsFound")
        } catch JetStreamError.ObjectStoreError.noObjectsFound {
            // success
        }
    }

    // MARK: - recovery

    /// The payoff: a live watch whose underlying ephemeral consumer is deleted out from
    /// under it must RESUME the live tail with no gap and no duplicate. Proves object
    /// watch inherits the ordered consumer's hang-safety.
    func testWatchRecoversAfterConsumerDeletion() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let bucket = "objrecovery"
        let obs = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: bucket))
        _ = try await obs.put("a", data: Data("va".utf8))
        _ = try await obs.put("b", data: Data("vb".utf8))

        // Build the watcher directly with a short heartbeat so the missed-heartbeat reset
        // fires quickly after the consumer is deleted.
        let watcher = ObjectStoreWatcher(
            ctx: ctx,
            streamName: ObjectStoreCoding.streamName(forBucket: bucket),
            filterSubject: ObjectStoreCoding.allMetaSubject(forBucket: bucket),
            opts: ObjectStoreWatchOptions(),
            idleHeartbeat: 0.3)
        try await watcher.start()
        let watchdog = withWatchdog(watcher, seconds: 25)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        // Drain the initial snapshot (a, b) and the marker.
        var initialNames: [String] = []
        for _ in 0..<2 {
            guard case .entry(let entry) = try await nextElement(&it) else {
                return XCTFail("expected an initial value")
            }
            initialNames.append(entry.name)
        }
        XCTAssertEqual(Set(initialNames), ["a", "b"])
        guard case .marker = try await nextElement(&it) else {
            return XCTFail("expected the end-of-initial marker")
        }

        // Delete the watcher's ephemeral consumer out from under it.
        let streamName = ObjectStoreCoding.streamName(forBucket: bucket)
        let names = await ctx.consumerNames(stream: streamName)
        var deleted = 0
        for try await name in names {
            try await ctx.deleteConsumer(stream: streamName, name: name)
            deleted += 1
        }
        XCTAssertGreaterThan(deleted, 0, "watch must have created an ephemeral consumer")

        // Write more objects; the reset engine resumes from streamSeq + 1 (filtered to the
        // meta subjects, so the interleaved chunk messages are skipped).
        _ = try await obs.put("c", data: Data("vc".utf8))
        _ = try await obs.put("d", data: Data("vd".utf8))

        // The watcher resumes delivering the new entries, contiguous and no dup.
        var recovered: [String] = []
        for _ in 0..<2 {
            guard case .entry(let entry) = try await nextElement(&it) else {
                return XCTFail("watcher did not resume after consumer deletion")
            }
            recovered.append(entry.name)
        }
        XCTAssertEqual(recovered, ["c", "d"])

        await watcher.stop()
    }

    // MARK: - interop with the `nats` CLI

    /// A bucket created and populated by the `nats` CLI is listed and watched by the Swift
    /// client: the CLI-created objects appear in both list() and the watch snapshot.
    func testNatsCLIInteropListAndWatch() async throws {
        try XCTSkipUnless(Self.natsCLIAvailable, "nats CLI not found on PATH")

        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }
        let url = natsServer.clientURL

        _ = try Self.runNats(["-s", url, "object", "add", "cliobjlist"])
        for name in ["one", "two"] {
            let path = NSTemporaryDirectory() + "objlist-\(UUID().uuidString)"
            defer { try? FileManager.default.removeItem(atPath: path) }
            try Data("v-\(name)".utf8).write(to: URL(fileURLWithPath: path))
            _ = try Self.runNats([
                "-s", url, "object", "put", "cliobjlist", path, "--name", name, "-f",
                "--no-progress",
            ])
        }

        let obs = try await ctx.objectStore(bucket: "cliobjlist")

        let listed = try await obs.list()
        XCTAssertEqual(Set(listed.map { $0.name }), ["one", "two"])

        let watcher = try await obs.watch()
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        let watched = try await collectUntilMarker(watcher)
        XCTAssertEqual(Set(watched.map { $0.name }), ["one", "two"])
        await watcher.stop()
    }

    // MARK: - Helpers

    /// A classified watch element: an entry, the end-of-initial marker, or the end of the
    /// sequence (which a stalled read surfaces via the watchdog).
    private enum WatchElement {
        case entry(ObjectInfo)
        case marker
        case ended
    }

    /// Reads and classifies the next watch element, flattening the `ObjectInfo??` double
    /// optional (outer = end of sequence, inner = marker).
    private func nextElement(
        _ iterator: inout AsyncThrowingStream<ObjectInfo?, Error>.Iterator
    ) async throws -> WatchElement {
        guard let outer = try await iterator.next() else {
            return .ended
        }
        if let entry = outer {
            return .entry(entry)
        }
        return .marker
    }

    /// Collects the initial values delivered before the marker. Fails if the sequence ends
    /// before the marker arrives.
    private func collectUntilMarker(
        _ watcher: ObjectStoreWatcher, file: StaticString = #filePath, line: UInt = #line
    ) async throws -> [ObjectInfo] {
        var entries: [ObjectInfo] = []
        var it = watcher.makeAsyncIterator()
        while true {
            switch try await nextElement(&it) {
            case .entry(let entry):
                entries.append(entry)
            case .marker:
                return entries
            case .ended:
                XCTFail("sequence ended before the marker", file: file, line: line)
                return entries
            }
        }
    }

    /// Finishes the watcher after `seconds` so a stalled read unblocks and fails instead
    /// of hanging the whole test process.
    private func withWatchdog(
        _ watcher: ObjectStoreWatcher, seconds: TimeInterval
    ) -> Task<Void, Never> {
        Task {
            try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            await watcher.stop()
        }
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
