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

import Foundation
import Logging
import Nats
import NatsServer
import XCTest

class ConnectionStateTests: XCTestCase {

    nonisolated(unsafe) static let allTests = [
        ("testWaitForConnected", testWaitForConnected),
        (
            "testWaitForConnectedWithRetryOnFailedConnect",
            testWaitForConnectedWithRetryOnFailedConnect
        ),
        ("testWaitForConnectedTimeout", testWaitForConnectedTimeout),
        ("testConnectionStateAccessor", testConnectionStateAccessor),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    /// `waitForConnected()` returns immediately for an already-connected client and
    /// leaves it usable for publish/subscribe.
    func testWaitForConnected() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        // Already connected: the fast path should resolve promptly.
        let start = Date()
        await client.waitForConnected()
        XCTAssertLessThan(
            Date().timeIntervalSince(start), 1.0, "already-connected fast path should be prompt")
        XCTAssertTrue(client.isConnected)

        // Client remains usable after waiting.
        let sub = try await client.subscribe(subject: "test")
        let iter = sub.makeAsyncIterator()
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let message = try await iter.next()
        XCTAssertEqual(message?.payload, "msg".data(using: .utf8)!)

        try await client.close()
    }

    /// With `retryOnfailedConnect()`, `connect()` returns before the first successful
    /// connection. Starting the server slightly later, `waitForConnected()` still
    /// resolves once the background reconnect establishes the connection.
    func testWaitForConnectedWithRetryOnFailedConnect() async throws {
        logger.logLevel = .critical
        let port = 4628
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(port)")!)
            .reconnectWait(0.1)
            .retryOnfailedConnect()
            .build()

        // Returns immediately without an established connection.
        try await client.connect()
        XCTAssertFalse(client.isConnected)

        // Start the server after the client began connecting.
        natsServer.start(port: port)

        // Resolves once the background reconnect succeeds (also exercises the timeout
        // overload's success path).
        try await client.waitForConnected(timeout: 10)
        XCTAssertTrue(client.isConnected)

        let sub = try await client.subscribe(subject: "test")
        let iter = sub.makeAsyncIterator()
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let message = try await iter.next()
        XCTAssertEqual(message?.payload, "msg".data(using: .utf8)!)

        try await client.close()
    }

    /// `waitForConnected(timeout:)` throws when the client never connects within the
    /// deadline.
    func testWaitForConnectedTimeout() async throws {
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:4629")!)
            .reconnectWait(0.1)
            .retryOnfailedConnect()
            .build()

        // No server is listening, so the client never becomes connected.
        try await client.connect()

        do {
            try await client.waitForConnected(timeout: 1)
        } catch NatsError.ConnectError.timeout {
            try? await client.close()
            return
        } catch {
            XCTFail("Expected timeout error; got: \(error)")
        }
        XCTFail("Expected timeout error")
    }

    /// The public `state`/`isConnected` accessors track the connection lifecycle:
    /// pending before connect, connected after connect, closed after close.
    func testConnectionStateAccessor() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()

        XCTAssertEqual(client.state, .pending)
        XCTAssertFalse(client.isConnected)

        try await client.connect()
        XCTAssertEqual(client.state, .connected)
        XCTAssertTrue(client.isConnected)

        try await client.close()
        XCTAssertEqual(client.state, .closed)
        XCTAssertFalse(client.isConnected)
    }
}
