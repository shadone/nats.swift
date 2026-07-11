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

final class PushConsumerTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// Deterministic flow-control reply: hand the ``PushConsumer`` a synthetic FC control message
    /// (Status 100, Description "FlowControl Request", empty body, a reply subject) and assert it
    /// publishes an empty message to that reply subject. No real consumer or server timing involved.
    func testFlowControlReply() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        defer { Task { try? await client.close() } }

        let deliverSubject = client.newInbox()
        let deliverSub = try await client.subscribe(subject: deliverSubject)

        // The subject the FC reply must be published to.
        let replySubject = client.newInbox()
        let replySub = try await client.subscribe(subject: replySubject)

        let push = PushConsumer(
            client: client, subscription: deliverSub, idleHeartbeatSeconds: 0)

        // Synthetic FC control message. NatsMessage's memberwise init is reachable via @testable.
        let fcMessage = NatsMessage(
            payload: nil,
            subject: deliverSubject,
            replySubject: replySubject,
            length: 0,
            headers: nil,
            status: .idleHeartbeat,  // 100 == generic control code
            description: "FlowControl Request")

        let event = await push.handle(fcMessage)
        XCTAssertNil(event, "flow control must be handled inline and surface no event")

        // The pump must have published an (empty) reply to the FC reply subject.
        let received = try await receive(replySub, timeout: 2)
        let reply = try XCTUnwrap(
            received, "expected an empty flow-control reply on the reply subject")
        XCTAssertEqual(reply.subject, replySubject)
        XCTAssertTrue(
            reply.payload == nil || reply.payload?.isEmpty == true,
            "flow-control reply must have an empty payload")
    }

    /// Reads the next message on a subscription, bounded by a timeout so a broken build fails
    /// instead of hanging.
    private func receive(
        _ sub: NatsSubscription, timeout: TimeInterval
    ) async throws -> NatsMessage? {
        try await withThrowingTaskGroup(of: NatsMessage?.self) { group in
            group.addTask {
                try await sub.makeAsyncIterator().next()
            }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                return nil
            }
            defer { group.cancelAll() }
            return try await group.next() ?? nil
        }
    }
}
