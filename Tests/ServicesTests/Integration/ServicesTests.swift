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
import Nats
import NatsServer
import Services
import XCTest

final class ServicesTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func connectedClient() async throws -> NatsClient {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "core", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return client
    }

    // MARK: - Helpers

    private func jsonObject(_ message: NatsMessage) throws -> [String: Any] {
        let payload = try XCTUnwrap(message.payload)
        let object = try JSONSerialization.jsonObject(with: payload)
        return try XCTUnwrap(object as? [String: Any])
    }

    /// Publishes `payload` to `subject` and collects up to `max` replies within `timeout`.
    private func collectResponses(
        _ client: NatsClient, subject: String, payload: Data = Data(), max: Int,
        timeout: TimeInterval = 2
    ) async throws -> [NatsMessage] {
        let inbox = client.newInbox()
        let subscription = try await client.subscribe(subject: inbox)
        try await client.publish(payload, subject: subject, reply: inbox)

        var messages: [NatsMessage] = []
        let iterator = subscription.makeAsyncIterator()
        let deadline = Date().addingTimeInterval(timeout)
        while messages.count < max {
            let remaining = deadline.timeIntervalSinceNow
            if remaining <= 0 { break }
            let next: NatsMessage? = try await withThrowingTaskGroup(of: NatsMessage?.self) {
                group in
                group.addTask { try await iterator.next() }
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(remaining * 1_000_000_000))
                    return nil
                }
                let result = (try await group.next()) ?? nil
                group.cancelAll()
                return result
            }
            guard let message = next else { break }
            messages.append(message)
        }
        try? await subscription.unsubscribe()
        return messages
    }

    // MARK: - Endpoints

    func testEndpointRequestEchoes() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "EchoService", version: "1.0.0"))
        try await service.addEndpoint("echo", subject: "svc.echo") { request in
            try? await request.respond(request.data)
        }
        defer { Task { await service.stop() } }

        let response = try await client.request(Data("hello".utf8), subject: "svc.echo")
        XCTAssertEqual(response.payload, Data("hello".utf8))
    }

    func testErrorResponseCarriesServiceErrorHeaders() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "EchoService", version: "1.0.0"))
        try await service.addEndpoint("echo", subject: "svc.echo") { request in
            try? await request.error(code: "400", description: "bad request")
        }
        defer { Task { await service.stop() } }

        let response = try await client.request(Data(), subject: "svc.echo")
        let headers = try XCTUnwrap(response.headers)
        XCTAssertEqual(
            headers.get(try NatsHeaderName("Nats-Service-Error"))?.description, "bad request")
        XCTAssertEqual(
            headers.get(try NatsHeaderName("Nats-Service-Error-Code"))?.description, "400")
    }

    // MARK: - Discovery / Monitoring

    func testInfoReturnsEndpointsAndQueueGroup() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "EchoService", version: "1.2.3", description: "echoes"))
        try await service.addEndpoint("echo", subject: "svc.echo") { request in
            try? await request.respond(request.data)
        }
        defer { Task { await service.stop() } }

        let response = try await client.request(Data(), subject: "$SRV.INFO.EchoService")
        let json = try jsonObject(response)

        XCTAssertEqual(json["type"] as? String, "io.nats.micro.v1.info_response")
        XCTAssertEqual(json["name"] as? String, "EchoService")
        XCTAssertEqual(json["version"] as? String, "1.2.3")
        XCTAssertEqual(json["description"] as? String, "echoes")

        let endpoints = try XCTUnwrap(json["endpoints"] as? [[String: Any]])
        XCTAssertEqual(endpoints.count, 1)
        XCTAssertEqual(endpoints[0]["name"] as? String, "echo")
        XCTAssertEqual(endpoints[0]["subject"] as? String, "svc.echo")
        XCTAssertEqual(endpoints[0]["queue_group"] as? String, "q")
    }

    func testPingReturnsPingResponse() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "EchoService", version: "1.0.0"))
        defer { Task { await service.stop() } }

        let response = try await client.request(Data(), subject: "$SRV.PING")
        let json = try jsonObject(response)
        XCTAssertEqual(json["type"] as? String, "io.nats.micro.v1.ping_response")
        XCTAssertEqual(json["name"] as? String, "EchoService")
        XCTAssertEqual(json["id"] as? String, service.id)
    }

    func testStatsCountsRequestsAndErrors() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "EchoService", version: "1.0.0"))
        try await service.addEndpoint("echo", subject: "svc.echo") { request in
            if request.data == Data("fail".utf8) {
                try? await request.error(code: "500", description: "boom")
            } else {
                try? await request.respond(request.data)
            }
        }
        defer { Task { await service.stop() } }

        let payloads = ["ok", "fail", "ok", "fail", "ok"].map { Data($0.utf8) }
        for payload in payloads {
            _ = try await client.request(payload, subject: "svc.echo")
        }
        // Stats are recorded on the actor after the handler returns; allow it to settle.
        try await Task.sleep(nanoseconds: 300_000_000)

        let response = try await client.request(Data(), subject: "$SRV.STATS.EchoService")
        let json = try jsonObject(response)
        XCTAssertEqual(json["type"] as? String, "io.nats.micro.v1.stats_response")

        let endpoints = try XCTUnwrap(json["endpoints"] as? [[String: Any]])
        let echo = try XCTUnwrap(endpoints.first)
        XCTAssertEqual(echo["num_requests"] as? Int, 5)
        XCTAssertEqual(echo["num_errors"] as? Int, 2)
        XCTAssertEqual(echo["last_error"] as? String, "500:boom")

        let processing = try XCTUnwrap(echo["processing_time"] as? Int)
        let average = try XCTUnwrap(echo["average_processing_time"] as? Int)
        XCTAssertGreaterThan(processing, 0)
        XCTAssertEqual(average, processing / 5)
    }

    func testResetZeroesStatsAndBumpsStarted() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "EchoService", version: "1.0.0"))
        try await service.addEndpoint("echo", subject: "svc.echo") { request in
            try? await request.respond(request.data)
        }
        defer { Task { await service.stop() } }

        for _ in 0..<3 {
            _ = try await client.request(Data("ok".utf8), subject: "svc.echo")
        }
        try await Task.sleep(nanoseconds: 200_000_000)

        let before = await service.stats()
        XCTAssertEqual(before.endpoints.first?.numRequests, 3)

        try await Task.sleep(nanoseconds: 5_000_000)
        await service.reset()

        let after = await service.stats()
        XCTAssertEqual(after.endpoints.first?.numRequests, 0)
        XCTAssertEqual(after.endpoints.first?.numErrors, 0)
        XCTAssertEqual(after.endpoints.first?.processingTime, 0)
        XCTAssertGreaterThan(after.started, before.started)
    }

    // MARK: - Load balancing across instances

    func testTwoInstancesLoadBalanceEndpointsButBothAnswerPing() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let config = ServiceConfig(name: "LBService", version: "1.0.0")
        let first = try await client.addService(config)
        let second = try await client.addService(config)
        for service in [first, second] {
            try await service.addEndpoint("echo", subject: "lb.echo") { request in
                try? await request.respond(request.data)
            }
        }
        defer {
            Task {
                await first.stop()
                await second.stop()
            }
        }

        let total = 20
        for _ in 0..<total {
            _ = try await client.request(Data("ping".utf8), subject: "lb.echo")
        }
        try await Task.sleep(nanoseconds: 300_000_000)

        // Endpoint requests are queue-balanced: each instance handled a share.
        let firstStats = await first.stats()
        let secondStats = await second.stats()
        let firstCount = try XCTUnwrap(firstStats.endpoints.first?.numRequests)
        let secondCount = try XCTUnwrap(secondStats.endpoints.first?.numRequests)
        XCTAssertEqual(firstCount + secondCount, total)
        XCTAssertGreaterThan(firstCount, 0)
        XCTAssertGreaterThan(secondCount, 0)

        // Control subjects are NOT queued: both instances answer PING.
        let pings = try await collectResponses(
            client, subject: "$SRV.PING.LBService", max: 2, timeout: 2)
        XCTAssertEqual(pings.count, 2)
        let ids = try Set(pings.map { try self.jsonObject($0)["id"] as? String })
        XCTAssertEqual(ids, Set([first.id, second.id].map { Optional($0) }))
    }

    // MARK: - Stop

    func testStopIsPromptIdempotentAndSilencesService() async throws {
        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "EchoService", version: "1.0.0"))
        try await service.addEndpoint("echo", subject: "svc.echo") { request in
            try? await request.respond(request.data)
        }

        let start = Date()
        await service.stop()
        XCTAssertLessThan(Date().timeIntervalSince(start), 2.0, "stop() should be prompt")
        let stopped = await service.isStopped
        XCTAssertTrue(stopped)

        // Idempotent: a second stop must not throw or hang.
        await service.stop()

        // After stop, PING and endpoint requests get no response.
        await assertNoResponse(client, subject: "$SRV.PING.EchoService")
        await assertNoResponse(client, subject: "svc.echo")
    }

    private func assertNoResponse(
        _ client: NatsClient, subject: String, file: StaticString = #filePath, line: UInt = #line
    ) async {
        do {
            _ = try await client.request(Data(), subject: subject, timeout: 1)
            XCTFail("expected no response on \(subject)", file: file, line: line)
        } catch {
            // Expected: no responders or timeout.
        }
    }

    // MARK: - nats CLI interop (the definitive wire gate)

    func testNatsCLIInterop() async throws {
        try XCTSkipUnless(Self.natsCLIAvailable, "nats CLI not available on PATH")

        let client = try await connectedClient()
        defer { Task { try? await client.close() } }

        let service = try await client.addService(
            ServiceConfig(name: "CLIService", version: "2.1.0", description: "cli interop"))
        try await service.addEndpoint("echo", subject: "cli.echo") { request in
            try? await request.respond(request.data)
        }
        defer { Task { await service.stop() } }

        let url = natsServer.clientURL

        // `nats micro info <name> --json`: the CLI parses our INFO response and re-wraps it
        // under an "info" key. Its presence proves the wire contract was understood.
        let infoOutput = try Self.runNats(["-s", url, "micro", "info", "CLIService", "--json"])
        _ = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(infoOutput.utf8)),
            "info output was not valid JSON: \(infoOutput)")
        XCTAssertTrue(infoOutput.contains("CLIService"))
        XCTAssertTrue(infoOutput.contains("io.nats.micro.v1.info_response"))

        // `nats micro stats <name> --json`
        let statsOutput = try Self.runNats(["-s", url, "micro", "stats", "CLIService", "--json"])
        _ = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(statsOutput.utf8)),
            "stats output was not valid JSON: \(statsOutput)")
        XCTAssertTrue(statsOutput.contains("io.nats.micro.v1.stats_response"))

        // `nats micro ping` (text output) must discover our service and exit 0.
        let pingOutput = try Self.runNats(["-s", url, "micro", "ping", "CLIService"])
        XCTAssertTrue(
            pingOutput.contains("CLIService"), "ping output did not mention service: \(pingOutput)")
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
