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
import XCTest

@testable import Services

final class ServiceResponsesTests: XCTestCase {

    private let encoder = JSONEncoder()

    private func jsonObject<T: Encodable>(_ value: T) throws -> [String: Any] {
        let data = try encoder.encode(value)
        let object = try JSONSerialization.jsonObject(with: data)
        return try XCTUnwrap(object as? [String: Any])
    }

    func testPingResponseEncoding() throws {
        let ping = ServicePing(
            identity: ServiceIdentity(
                name: "EchoService", id: "abc123", version: "1.2.3", metadata: [:]),
            type: ServiceSubjects.pingResponseType)
        let json = try jsonObject(ping)

        XCTAssertEqual(json["name"] as? String, "EchoService")
        XCTAssertEqual(json["id"] as? String, "abc123")
        XCTAssertEqual(json["version"] as? String, "1.2.3")
        XCTAssertEqual(json["type"] as? String, "io.nats.micro.v1.ping_response")
        // Service-level metadata is `{}` when unset (never null).
        XCTAssertEqual(try XCTUnwrap(json["metadata"] as? [String: String]), [:])
    }

    func testInfoResponseEncoding() throws {
        let info = ServiceInfo(
            identity: ServiceIdentity(
                name: "EchoService", id: "abc123", version: "1.0.0", metadata: ["k": "v"]),
            type: ServiceSubjects.infoResponseType,
            description: "",
            endpoints: [
                EndpointInfo(
                    name: "default", subject: "svc.echo", queueGroup: "q", metadata: nil)
            ])
        let data = try encoder.encode(info)
        let json = try XCTUnwrap(
            try JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertEqual(json["type"] as? String, "io.nats.micro.v1.info_response")
        XCTAssertEqual(json["description"] as? String, "")
        XCTAssertEqual(try XCTUnwrap(json["metadata"] as? [String: String]), ["k": "v"])

        let endpoints = try XCTUnwrap(json["endpoints"] as? [[String: Any]])
        XCTAssertEqual(endpoints.count, 1)
        let endpoint = endpoints[0]
        XCTAssertEqual(endpoint["name"] as? String, "default")
        XCTAssertEqual(endpoint["subject"] as? String, "svc.echo")
        XCTAssertEqual(endpoint["queue_group"] as? String, "q")
        // Endpoint metadata is JSON null when nil.
        XCTAssertTrue(endpoint["metadata"] is NSNull)
    }

    func testStatsResponseEncodingUsesIntegerNanoseconds() throws {
        let stats = ServiceStats(
            identity: ServiceIdentity(
                name: "EchoService", id: "abc123", version: "1.0.0", metadata: [:]),
            type: ServiceSubjects.statsResponseType,
            started: "2024-09-24T11:02:55.564771Z",
            endpoints: [
                EndpointStats(
                    name: "default",
                    subject: "svc.echo",
                    queueGroup: "q",
                    numRequests: 3,
                    numErrors: 1,
                    lastError: "400:bad",
                    processingTime: 1_500,
                    averageProcessingTime: 500)
            ])
        let data = try encoder.encode(stats)
        let raw = try XCTUnwrap(String(data: data, encoding: .utf8))
        let json = try XCTUnwrap(
            try JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertEqual(json["type"] as? String, "io.nats.micro.v1.stats_response")
        XCTAssertEqual(json["started"] as? String, "2024-09-24T11:02:55.564771Z")

        let endpoints = try XCTUnwrap(json["endpoints"] as? [[String: Any]])
        let endpoint = endpoints[0]
        XCTAssertEqual(endpoint["num_requests"] as? Int, 3)
        XCTAssertEqual(endpoint["num_errors"] as? Int, 1)
        XCTAssertEqual(endpoint["last_error"] as? String, "400:bad")
        // Durations must be integer nanoseconds, not a formatted string.
        XCTAssertEqual(endpoint["processing_time"] as? Int, 1_500)
        XCTAssertEqual(endpoint["average_processing_time"] as? Int, 500)
        XCTAssertTrue(raw.contains("\"processing_time\":1500"))
        XCTAssertFalse(raw.contains("\"processing_time\":\""))
        // `data` is omitted in v1.
        XCTAssertNil(endpoint["data"])
    }

    func testStartedIsRFC3339UTCWithFractionalSeconds() throws {
        let date = Date(timeIntervalSince1970: 1_727_175_775.564771)
        let formatted = ServiceTime.rfc3339(date)
        let pattern = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z$"
        let regex = try NSRegularExpression(pattern: pattern)
        let range = NSRange(formatted.startIndex..<formatted.endIndex, in: formatted)
        XCTAssertNotNil(
            regex.firstMatch(in: formatted, options: [], range: range),
            "unexpected started format: \(formatted)")
        XCTAssertTrue(formatted.hasSuffix("Z"))
    }

    func testControlSubjects() {
        let subjects = ServiceSubjects.allControl(name: "EchoService", id: "ID1")
        XCTAssertEqual(subjects.count, 9)
        let expected: Set<String> = [
            "$SRV.PING", "$SRV.PING.EchoService", "$SRV.PING.EchoService.ID1",
            "$SRV.INFO", "$SRV.INFO.EchoService", "$SRV.INFO.EchoService.ID1",
            "$SRV.STATS", "$SRV.STATS.EchoService", "$SRV.STATS.EchoService.ID1",
        ]
        XCTAssertEqual(Set(subjects), expected)
    }

    func testDefaultQueueGroup() {
        XCTAssertEqual(ServiceSubjects.defaultQueueGroup, "q")
    }

    func testGroupSubjectPrefixing() {
        XCTAssertEqual(ServiceSubjects.grouped(prefix: "numbers", subject: "add"), "numbers.add")
        XCTAssertEqual(
            ServiceSubjects.grouped(prefix: "numbers.integers", subject: "add"),
            "numbers.integers.add")
        XCTAssertEqual(ServiceSubjects.grouped(prefix: "", subject: "add"), "add")
    }

    func testValidation() {
        XCTAssertTrue(ServiceValidation.isValidName("Echo_Service-1"))
        XCTAssertFalse(ServiceValidation.isValidName("bad name"))
        XCTAssertFalse(ServiceValidation.isValidName(""))
        XCTAssertTrue(ServiceValidation.isValidVersion("1.2.3"))
        XCTAssertTrue(ServiceValidation.isValidVersion("1.0.0-beta.1"))
        XCTAssertFalse(ServiceValidation.isValidVersion("1.0"))
        XCTAssertFalse(ServiceValidation.isValidVersion("v1.0.0"))
    }
}
