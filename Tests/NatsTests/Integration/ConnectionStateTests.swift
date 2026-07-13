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
import NIOConcurrencyHelpers
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
        (
            "testWaitForConnectedRespectsCancellation",
            testWaitForConnectedRespectsCancellation
        ),
        (
            "testWaitForConnectedTimeoutThrowsOnCancellation",
            testWaitForConnectedTimeoutThrowsOnCancellation
        ),
        ("testConnectionStateAccessor", testConnectionStateAccessor),
        ("testTlsFirstRejectsPlaintextServer", testTlsFirstRejectsPlaintextServer),
        (
            "testCredentialsAndNkeyAreMutuallyExclusive",
            testCredentialsAndNkeyAreMutuallyExclusive
        ),
    ]

    /// Regression: `.withTlsFirst()` must NOT silently connect in plaintext. It now implies TLS is
    /// required, so connecting to a plain (non-TLS) server fails instead of succeeding unencrypted.
    func testTlsFirstRejectsPlaintextServer() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .withTlsFirst()
            .build()
        do {
            try await client.connect()
            try? await client.close()
            XCTFail(".withTlsFirst() must not connect in plaintext to a non-TLS server")
        } catch {
            // expected: the TLS-first handshake against a plaintext server fails.
        }
    }

    /// Regression: credentials and nkey auth are mutually exclusive -- setting both must throw a
    /// clear config error at connect, not build an ambiguous CONNECT carrying both a JWT and an nkey.
    func testCredentialsAndNkeyAreMutuallyExclusive() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .credentials("dummy-creds")
            .nkey("SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM")
            .build()
        do {
            try await client.connect()
            try? await client.close()
            XCTFail("connect must reject both credentials and nkey auth")
        } catch {
            XCTAssertTrue(error is NatsError.ConnectError, "expected a ConnectError, got \(error)")
        }
    }

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

    /// `waitForConnected()` (no timeout) honors task cancellation: a client that never
    /// connects would otherwise wait forever, but cancelling the surrounding task makes
    /// it return promptly. Guards against a regression to the old non-cancellable wait
    /// (which would hang here) by bounding the wait with a task group.
    func testWaitForConnectedRespectsCancellation() async throws {
        logger.logLevel = .critical
        // No server on this port: the client can never connect.
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:4630")!)
            .reconnectWait(0.1)
            .retryOnfailedConnect()
            .unlimitedReconnects()
            .build()
        try await client.connect()
        XCTAssertFalse(client.isConnected)

        let observedCancel = NIOLockedValueBox(false)
        let waiter = Task {
            await client.waitForConnected()
            observedCancel.withLockedValue { $0 = Task.isCancelled }
        }
        // Let the wait actually suspend, then cancel it.
        try await Task.sleep(nanoseconds: 200_000_000)
        waiter.cancel()

        // Bound the join so a regression (a hang) fails the test instead of wedging the
        // whole suite.
        let returned = await withTaskGroup(of: Bool.self) { group in
            group.addTask {
                await waiter.value
                return true
            }
            group.addTask {
                try? await Task.sleep(nanoseconds: 3_000_000_000)
                return false
            }
            let first = await group.next() ?? false
            group.cancelAll()
            return first
        }
        XCTAssertTrue(
            returned, "waitForConnected() hung on cancellation instead of returning")
        XCTAssertTrue(
            observedCancel.withLockedValue { $0 }, "the wait returned because of cancellation")
        XCTAssertFalse(client.isConnected)

        try? await client.close()
    }

    /// `waitForConnected(timeout:)` throws `CancellationError` (not the timeout error, and
    /// without waiting out the deadline) when the surrounding task is cancelled.
    func testWaitForConnectedTimeoutThrowsOnCancellation() async throws {
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:4631")!)
            .reconnectWait(0.1)
            .retryOnfailedConnect()
            .unlimitedReconnects()
            .build()
        try await client.connect()

        struct DidNotReturn: Error {}
        let waiter = Task { () -> Error? in
            do {
                // A long timeout: only cancellation (not the deadline) should end this.
                try await client.waitForConnected(timeout: 60)
                return nil
            } catch {
                return error
            }
        }
        try await Task.sleep(nanoseconds: 200_000_000)
        waiter.cancel()

        let thrown: Error? = await withTaskGroup(of: Error??.self) { group in
            group.addTask { await waiter.value }
            group.addTask {
                try? await Task.sleep(nanoseconds: 3_000_000_000)
                return DidNotReturn()
            }
            let first = await group.next() ?? nil
            group.cancelAll()
            return first ?? nil
        }
        XCTAssertFalse(
            thrown is DidNotReturn,
            "waitForConnected(timeout:) hung on cancellation instead of throwing")
        XCTAssertTrue(
            thrown is CancellationError,
            "cancelled wait must throw CancellationError; got \(String(describing: thrown))")

        try? await client.close()
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
