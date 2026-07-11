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

final class KeyValueWatchTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    // MARK: - watch

    /// The initial value(s) arrive, then exactly one `nil` end-of-initial marker,
    /// then a put made after the marker is delivered as a live update.
    func testWatchInitialValuesThenMarkerThenLive() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "watch"))
        let rev1 = try await kv.put("greeting", Data("hello".utf8))

        let watcher = try await kv.watch("greeting")
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        guard case .entry(let initial) = try await nextElement(&it) else {
            return XCTFail("expected the initial value")
        }
        XCTAssertEqual(initial.key, "greeting")
        XCTAssertEqual(initial.value, Data("hello".utf8))
        XCTAssertEqual(initial.revision, rev1)
        XCTAssertEqual(initial.operation, .put)

        guard case .marker = try await nextElement(&it) else {
            return XCTFail("expected the end-of-initial marker")
        }

        // A put after the marker arrives on the live tail.
        let rev2 = try await kv.put("greeting", Data("world".utf8))
        guard case .entry(let live) = try await nextElement(&it) else {
            return XCTFail("expected the live update")
        }
        XCTAssertEqual(live.value, Data("world".utf8))
        XCTAssertEqual(live.revision, rev2)
        XCTAssertEqual(live.operation, .put)

        await watcher.stop()
    }

    /// watchAll delivers the latest value of every key before the marker.
    func testWatchAllInitialValues() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "watchall"))
        _ = try await kv.put("a", Data("va".utf8))
        _ = try await kv.put("b", Data("vb".utf8))
        _ = try await kv.put("c", Data("vc".utf8))

        let watcher = try await kv.watchAll()
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }

        let initial = try await collectUntilMarker(watcher)
        let byKey = Dictionary(uniqueKeysWithValues: initial.map { ($0.key, $0) })
        XCTAssertEqual(Set(byKey.keys), ["a", "b", "c"])
        XCTAssertEqual(byKey["a"]?.value, Data("va".utf8))
        XCTAssertEqual(byKey["b"]?.value, Data("vb".utf8))
        XCTAssertEqual(byKey["c"]?.value, Data("vc".utf8))
        XCTAssertTrue(initial.allSatisfy { $0.operation == .put })

        await watcher.stop()
    }

    /// includeHistory delivers every retained revision of a key, oldest first.
    func testIncludeHistoryReturnsAllRevisions() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(
            cfg: {
                var c = KeyValueConfig(bucket: "hist")
                c.history = 5
                return c
            }())
        let r1 = try await kv.put("k", Data("v1".utf8))
        let r2 = try await kv.put("k", Data("v2".utf8))
        let r3 = try await kv.put("k", Data("v3".utf8))

        var opts = KeyValueWatchOptions()
        opts.includeHistory = true
        let watcher = try await kv.watch("k", opts: opts)
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }

        let entries = try await collectUntilMarker(watcher)
        XCTAssertEqual(entries.map { $0.revision }, [r1, r2, r3])
        XCTAssertEqual(
            entries.map { $0.value }, [Data("v1".utf8), Data("v2".utf8), Data("v3".utf8)])

        await watcher.stop()
    }

    /// updatesOnly delivers the marker immediately with NO initial values, then
    /// only puts made after the watch started.
    func testUpdatesOnlyDeliversOnlyPostWatchPuts() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "updates"))
        _ = try await kv.put("pre", Data("before".utf8))

        var opts = KeyValueWatchOptions()
        opts.updatesOnly = true
        let watcher = try await kv.watchAll(opts: opts)
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        // The marker is the very first element: no initial values are delivered.
        guard case .marker = try await nextElement(&it) else {
            return XCTFail("updatesOnly must deliver the marker before any entry")
        }

        // Only the post-watch put is delivered.
        let rev = try await kv.put("post", Data("after".utf8))
        guard case .entry(let live) = try await nextElement(&it) else {
            return XCTFail("expected the post-watch update")
        }
        XCTAssertEqual(live.key, "post")
        XCTAssertEqual(live.value, Data("after".utf8))
        XCTAssertEqual(live.revision, rev)

        await watcher.stop()
    }

    /// An empty bucket yields the marker immediately with no entries.
    func testEmptyBucketMarkerImmediately() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "empty"))
        let watcher = try await kv.watchAll()
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        guard case .marker = try await nextElement(&it) else {
            return XCTFail("an empty bucket must deliver the marker immediately")
        }
        await watcher.stop()
    }

    /// ignoreDeletes suppresses tombstone entries but still fires the marker, and
    /// a subsequent live key still arrives.
    func testIgnoreDeletesSuppressesTombstones() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "ignoredel"))
        _ = try await kv.put("a", Data("va".utf8))
        try await kv.delete("a")
        _ = try await kv.put("b", Data("vb".utf8))

        var opts = KeyValueWatchOptions()
        opts.ignoreDeletes = true
        let watcher = try await kv.watchAll(opts: opts)
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        // The deleted key's tombstone is never yielded; only live "b" arrives.
        guard case .entry(let live) = try await nextElement(&it) else {
            return XCTFail("expected the live key b")
        }
        XCTAssertEqual(live.key, "b")
        XCTAssertEqual(live.operation, .put)

        guard case .marker = try await nextElement(&it) else {
            return XCTFail("expected the marker after the initial snapshot")
        }

        // A live put after the marker still arrives.
        _ = try await kv.put("c", Data("vc".utf8))
        guard case .entry(let after) = try await nextElement(&it) else {
            return XCTFail("expected the post-marker live key c")
        }
        XCTAssertEqual(after.key, "c")

        await watcher.stop()
    }

    // MARK: - keys / history

    /// keys() lists live keys sorted, excluding deleted keys.
    func testKeysReflectsLiveKeysSorted() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "keys"))
        _ = try await kv.put("charlie", Data("1".utf8))
        _ = try await kv.put("alpha", Data("1".utf8))
        _ = try await kv.put("bravo", Data("1".utf8))
        try await kv.delete("bravo")

        let keys = try await kv.keys()
        XCTAssertEqual(keys, ["alpha", "charlie"])
    }

    /// keys() on an empty bucket throws noKeysFound.
    func testKeysEmptyBucketThrowsNoKeysFound() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "nokeys"))
        do {
            _ = try await kv.keys()
            XCTFail("expected noKeysFound")
        } catch JetStreamError.KeyValueError.noKeysFound {
            // success
        }
    }

    /// history() returns put/update/delete revisions in order.
    func testHistoryReturnsRevisionsInOrder() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(
            cfg: {
                var c = KeyValueConfig(bucket: "history")
                c.history = 10
                return c
            }())
        let r1 = try await kv.put("k", Data("v1".utf8))
        let r2 = try await kv.update("k", Data("v2".utf8), revision: r1)
        try await kv.delete("k")

        let history = try await kv.history("k")
        XCTAssertEqual(history.count, 3)
        XCTAssertEqual(history[0].revision, r1)
        XCTAssertEqual(history[0].operation, .put)
        XCTAssertEqual(history[1].revision, r2)
        XCTAssertEqual(history[1].operation, .put)
        XCTAssertEqual(history[2].operation, .delete)
        XCTAssertGreaterThan(history[2].revision, r2)
    }

    // MARK: - metaOnly

    /// metaOnly entries carry no value but the correct revision and operation.
    func testMetaOnlyOmitsValues() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "metaonly"))
        let rev = try await kv.put("k", Data("a-real-value".utf8))

        var opts = KeyValueWatchOptions()
        opts.metaOnly = true
        let watcher = try await kv.watch("k", opts: opts)
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }

        let entries = try await collectUntilMarker(watcher)
        XCTAssertEqual(entries.count, 1)
        XCTAssertEqual(entries[0].value, Data())
        XCTAssertEqual(entries[0].revision, rev)
        XCTAssertEqual(entries[0].operation, .put)

        await watcher.stop()
    }

    // MARK: - purgeDeletes

    /// purgeDeletes removes the messages of keys whose latest entry is a
    /// tombstone, leaving live keys untouched.
    func testPurgeDeletesRemovesTombstonedKeys() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let kv = try await ctx.createKeyValue(
            cfg: {
                var c = KeyValueConfig(bucket: "purgedel")
                c.history = 5
                return c
            }())
        _ = try await kv.put("a", Data("va".utf8))
        _ = try await kv.put("b", Data("vb".utf8))
        try await kv.delete("a")  // a's latest is now a tombstone

        // A negative age removes every marker regardless of when it was written.
        try await kv.purgeDeletes(olderThan: -1)

        // Only live key "b" remains in the backing stream.
        let stream = try await ctx.getStream(name: "KV_purgedel")
        XCTAssertEqual(stream?.info.state.messages, 1)
        let a = try await kv.get("a")
        XCTAssertNil(a)
        let b = try await kv.get("b")
        XCTAssertEqual(b?.value, Data("vb".utf8))
    }

    // MARK: - recovery

    /// The payoff: a live watch whose underlying ephemeral consumer is deleted
    /// out from under it must RESUME the live tail with no gap and no duplicate.
    /// Proves KV watch inherits the ordered consumer's hang-safety.
    func testWatchRecoversAfterConsumerDeletion() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let bucket = "recovery"
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: bucket))
        _ = try await kv.put("a", Data("va".utf8))
        _ = try await kv.put("b", Data("vb".utf8))

        // Build the watcher directly with a short heartbeat so the missed-heartbeat
        // reset fires quickly after the consumer is deleted.
        let watcher = KeyValueWatcher(
            ctx: ctx,
            streamName: KeyValueCoding.streamName(forBucket: bucket),
            bucket: bucket,
            filterSubject: KeyValueCoding.allKeysFilterSubject(forBucket: bucket),
            opts: KeyValueWatchOptions(),
            idleHeartbeat: 0.3)
        try await watcher.start()
        let watchdog = withWatchdog(watcher, seconds: 25)
        defer { watchdog.cancel() }
        var it = watcher.makeAsyncIterator()

        // Drain the initial snapshot (a, b) and the marker.
        var initialRevisions: [UInt64] = []
        for _ in 0..<2 {
            guard case .entry(let entry) = try await nextElement(&it) else {
                return XCTFail("expected an initial value")
            }
            initialRevisions.append(entry.revision)
        }
        XCTAssertEqual(initialRevisions, [1, 2])
        guard case .marker = try await nextElement(&it) else {
            return XCTFail("expected the end-of-initial marker")
        }

        // Delete the watcher's ephemeral consumer out from under it.
        let streamName = KeyValueCoding.streamName(forBucket: bucket)
        let names = await ctx.consumerNames(stream: streamName)
        var deleted = 0
        for try await name in names {
            try await ctx.deleteConsumer(stream: streamName, name: name)
            deleted += 1
        }
        XCTAssertGreaterThan(deleted, 0, "watch must have created an ephemeral consumer")

        // Write more keys; the reset engine resumes from streamSeq + 1.
        _ = try await kv.put("c", Data("vc".utf8))
        _ = try await kv.put("d", Data("vd".utf8))

        // The watcher resumes delivering the new entries, contiguous and no dup.
        var recovered: [(String, UInt64)] = []
        for _ in 0..<2 {
            guard case .entry(let entry) = try await nextElement(&it) else {
                return XCTFail("watcher did not resume after consumer deletion")
            }
            recovered.append((entry.key, entry.revision))
        }
        XCTAssertEqual(recovered.map { $0.0 }, ["c", "d"])
        XCTAssertEqual(recovered.map { $0.1 }, [3, 4])

        await watcher.stop()
    }

    /// Fail-fast: a watch against a NON-EXISTENT backing stream must throw promptly from
    /// `start()` (bounded time) rather than hang forever in the ordered consumer's create
    /// backoff. The initial consumer creation is a single synchronous attempt, so a missing
    /// stream / bucket surfaces its error immediately instead of wedging keys()/history()/watch().
    func testWatchNonexistentStreamFailsFast() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let bucket = "nope"
        let watcher = KeyValueWatcher(
            ctx: ctx,
            streamName: KeyValueCoding.streamName(forBucket: bucket),
            bucket: bucket,
            filterSubject: KeyValueCoding.allKeysFilterSubject(forBucket: bucket),
            opts: KeyValueWatchOptions())

        let start = Date()
        do {
            try await watcher.start()
            XCTFail("start() must throw against a non-existent stream")
        } catch {
            // Expected: the initial create failed fast with the underlying error.
        }
        let elapsed = Date().timeIntervalSince(start)
        XCTAssertLessThan(elapsed, 3, "start() must fail fast, not hang in the create backoff")

        await watcher.stop()
    }

    /// A reset induced DURING the initial snapshot (the ephemeral consumer is deleted before the
    /// end-of-initial marker fires) must still terminate the initial phase correctly: the marker
    /// eventually fires and the snapshot is coherent — every key present exactly once, no infinite
    /// loop. Exercises the `byStartSequence` resume-from-`lastPerSubject` path (matching nats.go).
    /// The large initial set gives the mid-snapshot delete time to land before the marker.
    func testWatchResetDuringInitialSnapshotStillCompletes() async throws {
        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }

        let bucket = "midsnap"
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: bucket))

        // Larger payloads make byte-based flow control chunk the initial delivery across several
        // round-trips, widening the window in which the mid-snapshot delete can land.
        let keyCount = 200
        let payload = Data(repeating: 0x61, count: 8 * 1024)
        for i in 0..<keyCount {
            _ = try await kv.put("k\(i)", payload)
        }

        let watcher = KeyValueWatcher(
            ctx: ctx,
            streamName: KeyValueCoding.streamName(forBucket: bucket),
            bucket: bucket,
            filterSubject: KeyValueCoding.allKeysFilterSubject(forBucket: bucket),
            opts: KeyValueWatchOptions(),
            idleHeartbeat: 0.3)
        try await watcher.start()
        let watchdog = withWatchdog(watcher, seconds: 25)
        defer { watchdog.cancel() }

        // Delete the ephemeral consumer out from under the watcher as early as possible, so the
        // reset lands while the initial snapshot is still draining.
        let streamName = KeyValueCoding.streamName(forBucket: bucket)
        let names = await ctx.consumerNames(stream: streamName)
        var deleted = 0
        for try await name in names {
            try await ctx.deleteConsumer(stream: streamName, name: name)
            deleted += 1
        }
        XCTAssertGreaterThan(deleted, 0, "watch must have created an ephemeral consumer")

        // The marker must still fire, and every key must appear exactly once across the reset.
        let initial = try await collectUntilMarker(watcher)
        XCTAssertEqual(
            Set(initial.map { $0.key }).count, keyCount, "every key must appear in the snapshot")
        XCTAssertEqual(
            initial.count, keyCount, "no key may be delivered more than once across the reset")

        await watcher.stop()
    }

    // MARK: - interop with the `nats` CLI

    /// A bucket created and populated by the `nats` CLI is watched by the Swift
    /// client: the initial values and the marker arrive as expected.
    func testNatsCLIInteropWatch() async throws {
        try XCTSkipUnless(Self.natsCLIAvailable, "nats CLI not found on PATH")

        let (client, ctx) = try await connectedContext()
        defer { Task { try? await client.close() } }
        let url = natsServer.clientURL

        _ = try Self.runNats(["-s", url, "kv", "add", "cliwatch"])
        _ = try Self.runNats(["-s", url, "kv", "put", "cliwatch", "one", "1"])
        _ = try Self.runNats(["-s", url, "kv", "put", "cliwatch", "two", "2"])

        let kv = try await ctx.keyValue(bucket: "cliwatch")
        let watcher = try await kv.watchAll()
        let watchdog = withWatchdog(watcher, seconds: 15)
        defer { watchdog.cancel() }

        let entries = try await collectUntilMarker(watcher)
        let byKey = Dictionary(uniqueKeysWithValues: entries.map { ($0.key, $0.value) })
        XCTAssertEqual(byKey["one"], Data("1".utf8))
        XCTAssertEqual(byKey["two"], Data("2".utf8))

        await watcher.stop()
    }

    // MARK: - Helpers

    /// A classified watch element: an entry, the end-of-initial marker, or the
    /// end of the sequence (which a stalled read surfaces via the watchdog).
    private enum WatchElement {
        case entry(KeyValueEntry)
        case marker
        case ended
    }

    /// Reads and classifies the next watch element, flattening the
    /// `KeyValueEntry??` double optional (outer = end of sequence, inner = marker).
    private func nextElement(
        _ iterator: inout AsyncThrowingStream<KeyValueEntry?, Error>.Iterator
    ) async throws -> WatchElement {
        guard let outer = try await iterator.next() else {
            return .ended
        }
        if let entry = outer {
            return .entry(entry)
        }
        return .marker
    }

    /// Collects the initial values delivered before the marker. Fails if the
    /// sequence ends before the marker arrives.
    private func collectUntilMarker(
        _ watcher: KeyValueWatcher, file: StaticString = #filePath, line: UInt = #line
    ) async throws -> [KeyValueEntry] {
        var entries: [KeyValueEntry] = []
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

    /// Finishes the watcher after `seconds` so a stalled read unblocks and fails
    /// instead of hanging the whole test process.
    private func withWatchdog(
        _ watcher: KeyValueWatcher, seconds: TimeInterval
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
