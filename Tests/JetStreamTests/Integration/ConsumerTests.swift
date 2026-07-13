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
import Logging
import NIOConcurrencyHelpers
import NatsServer
import XCTest

@testable import Nats

class ConsumerTests: XCTestCase {

    nonisolated(unsafe) static let allTests = [
        ("testFetchWithDefaultOptions", testFetchWithDefaultOptions),
        ("testFetchConsumerDeleted", testFetchConsumerDeleted),
        ("testFetchExpires", testFetchExpires),
        ("testFetchInvalidIdleHeartbeat", testFetchInvalidIdleHeartbeat),
        ("testAck", testAck),
        ("testNak", testNak),
        ("testNakWithDelay", testNakWithDelay),
        ("testTerm", testTerm),
        (
            "testNextTimeoutDoesNotLeakReplyInboxSubscription",
            testNextTimeoutDoesNotLeakReplyInboxSubscription
        ),
        ("testDrainDuringIdleFinishesCleanly", testDrainDuringIdleFinishesCleanly),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// Spins up a JetStream server + client + stream `test` (subjects `foo.*`) + a named pull
    /// consumer with no-ack, for the leak/drain regression tests below.
    private func makePullConsumer(
        name: String = "cons"
    ) async throws
        -> (NatsClient, JetStreamContext, Consumer)
    {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(name: name, ackPolicy: .none))
        return (client, ctx, consumer)
    }

    /// Regression: `next(timeout:)` is a `fetch(batch: 1)`, and a batch that ends exactly on its
    /// last `.ok` used to never unsubscribe (the post-switch cleanup was unreachable), leaking the
    /// reply-inbox subscription on EVERY call. Publish upfront (so the baseline excludes the publish
    /// path), poll 15 messages one at a time, and assert the active subscription count does not grow.
    func testNextTimeoutDoesNotLeakReplyInboxSubscription() async throws {
        let (client, ctx, consumer) = try await makePullConsumer()
        defer { Task { try? await client.close() } }

        let payload = "hi".data(using: .utf8)!
        for _ in 0..<15 {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }

        // Baseline AFTER publishing, so only the next() reply-inbox subscriptions are measured.
        let baseline = client.activeSubscriptionCount()
        for _ in 0..<15 {
            let msg = try await consumer.next(timeout: 5)
            XCTAssertNotNil(msg)
        }

        var count = client.activeSubscriptionCount()
        for _ in 0..<20 where count > baseline {
            try await Task.sleep(nanoseconds: 100_000_000)
            count = client.activeSubscriptionCount()
        }
        XCTAssertLessThanOrEqual(
            count, baseline,
            "next(timeout:) leaked reply-inbox subscriptions (\(count) vs baseline \(baseline))")
    }

    /// Regression: `drain()`-ing a `consume()` loop while it idles waiting for more messages must
    /// finish CLEANLY. The idle-heartbeat race conflated "subscription torn down" (drain) with
    /// "heartbeat timed out", so drain surfaced a spurious `noHeartbeatReceived` via `onError`.
    func testDrainDuringIdleFinishesCleanly() async throws {
        let (client, ctx, consumer) = try await makePullConsumer()
        defer { Task { try? await client.close() } }

        let payload = "hi".data(using: .utf8)!
        for _ in 0..<3 {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }

        let collected = NIOLockedValueBox(0)
        let errorBox = NIOLockedValueBox<Error?>(nil)
        let cc = try consumer.consume(
            { _ in collected.withLockedValue { $0 += 1 } },
            onError: { err in errorBox.withLockedValue { $0 = err } })

        // Wait for the 3 delivered, then let the loop enter its idle wait for more.
        let deadline = Date().addingTimeInterval(10)
        while collected.withLockedValue({ $0 }) < 3, Date() < deadline {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        try await Task.sleep(nanoseconds: 300_000_000)

        cc.drain()
        await cc.waitUntilClosed()

        XCTAssertNil(
            errorBox.withLockedValue { $0 },
            "drain() during idle surfaced an error: \(String(describing: errorBox.withLockedValue { $0 }))"
        )
        XCTAssertEqual(collected.withLockedValue { $0 }, 3)
    }

    func testFetchWithDefaultOptions() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...100 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 100)

        let batch = try await consumer.fetch(batch: 30)

        var i = 0
        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(msg.payload, payload)
            i += 1
        }
        XCTAssertEqual(i, 30)
    }

    func testFetchConsumerDeleted() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...10 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        let batch = try await consumer.fetch(batch: 30)

        sleep(1)
        try await stream.deleteConsumer(name: "cons")
        var i = 0
        do {
            for try await msg in batch {
                try await msg.ack()
                XCTAssertEqual(msg.payload, payload)
                i += 1
            }
        } catch JetStreamError.FetchError.consumerDeleted {
            XCTAssertEqual(i, 10)
            return
        }
        XCTFail("should get consumer deleted")
    }

    func testFetchExpires() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...10 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        let batch = try await consumer.fetch(batch: 30, expires: 1)

        var i = 0
        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(msg.payload, payload)
            i += 1
        }
        XCTAssertEqual(i, 10)
    }

    func testFetchInvalidIdleHeartbeat() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let batch = try await consumer.fetch(batch: 30, expires: 1, idleHeartbeat: 2)

        do {
            for try await _ in batch {}
        } catch JetStreamError.FetchError.badRequest {
            // success
            return
        }
        XCTFail("should get bad request")
    }

    func testFetchMissingHeartbeat() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...10 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        try await stream.deleteConsumer(name: "cons")

        let batch = try await consumer.fetch(batch: 30, idleHeartbeat: 1)

        do {
            for try await _ in batch {}
        } catch JetStreamError.FetchError.noHeartbeatReceived {
            return
        } catch JetStreamError.FetchError.noResponders {
            // This is also expected when the consumer has been deleted
            return
        }
        XCTFail("should get missing heartbeats or no responders error")
    }

    func testAck() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(name: "cons", ackWait: NanoTimeInterval(0.5)))

        // publish some messages on stream
        for i in 0..<100 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 100)

        var batch = try await consumer.fetch(batch: 10)

        var i = 0
        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(i)")
            i += 1
        }
        XCTAssertEqual(i, 10)

        // now wait 1 second and make sure the messages are not re-delivered
        sleep(1)

        batch = try await consumer.fetch(batch: 10)

        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(i)")
            i += 1
        }
        XCTAssertEqual(i, 20)
    }

    func testNak() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(name: "cons", ackWait: NanoTimeInterval(0.5)))

        // publish some messages on stream
        for i in 0..<10 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        var batch = try await consumer.fetch(batch: 1)
        var iter = batch.makeAsyncIterator()
        var msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        var meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 1)
        try await msg.ack(ackType: .nak())

        // Give the server time to process the NAK and requeue the message before fetching.
        // Without this, the fetch can race ahead and receive the next undelivered message
        // instead of the redelivered NAK'd one.
        try await Task.sleep(nanoseconds: 200_000_000)

        // now fetch the message again, it should be redelivered
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 2)
        try await msg.ack()
    }

    func testNakWithDelay() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        // publish some messages on stream
        for i in 0..<10 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        var batch = try await consumer.fetch(batch: 1)
        var iter = batch.makeAsyncIterator()
        var msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        var meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 1)
        try await msg.ack(ackType: .nak(delay: 0.5))

        // now fetch the next message immediately, it should be the next message
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(1)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 2)
        XCTAssertEqual(meta.consumerSequence, 2)
        try await msg.ack()

        // wait a second, the first message should be redelivered at this point
        sleep(1)
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 3)
    }

    func testTerm() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(name: "cons", ackWait: NanoTimeInterval(0.5)))

        // publish some messages on stream
        for i in 0..<10 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        var batch = try await consumer.fetch(batch: 1)
        var iter = batch.makeAsyncIterator()
        var msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        var meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 1)
        try await msg.ack(ackType: .term())

        // wait 1s, the first message should not be redelivered (even though we are past ack wait)
        sleep(1)
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(1)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 2)
        XCTAssertEqual(meta.consumerSequence, 2)
        try await msg.ack()
    }
}
