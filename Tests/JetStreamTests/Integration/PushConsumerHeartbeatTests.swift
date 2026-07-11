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

import NatsServer
import XCTest

@testable import JetStream
@testable import Nats

/// Correctness test for a plain push consumer with heartbeat + flow control on — exactly the
/// `PushDelivery.race()` task-group path the upcoming refactor targets. Under load it must deliver
/// every message once, with no loss and no duplicate.
final class PushConsumerHeartbeatTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// Push consumer with idle heartbeat (200ms) and flow control enabled receives all 500 messages
    /// exactly once — the heartbeat/flow-control race path must not drop or duplicate anything.
    func testHeartbeatPushConsumerNoLossUnderLoad() async throws {
        let client = try await connect()
        defer { Task { try? await client.close() } }
        let ctx = JetStreamContext(client: client)

        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        try await ConsumeTestSupport.publish(ctx, subject: "foo.A", count: 500)

        let pc = try await ctx.createPushConsumer(
            stream: "test",
            cfg: ConsumerConfig(
                ackPolicy: .none, flowControl: true, idleHeartbeat: NanoTimeInterval(0.2)))

        let collector = MessageCollector()
        let cc = try pc.consume { collector.record($0) }
        defer { cc.stop() }

        try await ConsumeTestSupport.waitUntil(20) { collector.count == 500 }

        XCTAssertEqual(
            collector.count, 500, "every published message must be delivered exactly once")
        XCTAssertEqual(
            Set(collector.payloads), Set((1...500).map { "msg-\($0)" }),
            "no loss and no duplicate under the heartbeat/flow-control path")
    }

    // MARK: - Helpers

    private func connect() async throws -> NatsClient {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return client
    }
}
