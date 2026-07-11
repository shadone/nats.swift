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

@testable import Nats

final class IgnoreDiscoveredServersTests: XCTestCase {
    nonisolated(unsafe) static let allTests = [
        ("testDiscoveredServersIngestedByDefault", testDiscoveredServersIngestedByDefault),
        ("testDiscoveredServersSuppressedWhenIgnored", testDiscoveredServersSuppressedWhenIgnored),
        ("testOptionThreadsThrough", testOptionThreadsThrough),
    ]

    private let seedUrl = URL(string: "nats://127.0.0.1:4222")!

    private func makeServerInfo(connectUrls: [String]) throws -> ServerInfo {
        let urlsJson = connectUrls.map { "\"\($0)\"" }.joined(separator: ",")
        let info = """
            INFO {"server_id":"test","server_name":"test","host":"127.0.0.1","port":4222,\
            "version":"2.12.7","max_payload":1048576,"proto":1,"go":"go1.22",\
            "client_ip":"127.0.0.1","headers":true,"connect_urls":[\(urlsJson)]}
            """
        return try ServerInfo.parse(data: Data(info.utf8))
    }

    private func makeHandler(ignoreDiscoveredServers: Bool) -> ConnectionHandler {
        ConnectionHandler(
            urls: [seedUrl],
            reconnectWait: 2.0,
            maxReconnects: nil,
            retainServersOrder: false,
            ignoreDiscoveredServers: ignoreDiscoveredServers,
            pingInterval: 60.0,
            auth: nil,
            requireTls: false,
            tlsFirst: false,
            clientCertificate: nil,
            clientKey: nil,
            rootCertificate: nil,
            retryOnFailedConnect: false
        )
    }

    /// By default, servers gossiped via INFO `connect_urls` are added to the pool.
    func testDiscoveredServersIngestedByDefault() throws {
        let handler = makeHandler(ignoreDiscoveredServers: false)
        let info = try makeServerInfo(connectUrls: [
            "nats://10.0.0.1:4222", "nats://10.0.0.2:4222",
        ])

        handler.updateServersList(info: info)

        XCTAssertEqual(
            handler.serverPool.count, 3,
            "Two discovered servers should be appended to the single seed URL")
    }

    /// With `ignoreDiscoveredServers()`, `connect_urls` are suppressed and never added.
    func testDiscoveredServersSuppressedWhenIgnored() throws {
        let handler = makeHandler(ignoreDiscoveredServers: true)
        let info = try makeServerInfo(connectUrls: [
            "nats://10.0.0.1:4222", "nats://10.0.0.2:4222",
        ])

        handler.updateServersList(info: info)

        XCTAssertEqual(
            handler.serverPool, [seedUrl],
            "Discovered servers must not be added when ignoreDiscoveredServers is set")
    }

    /// The `ignoreDiscoveredServers()` builder threads the flag through to the connection handler.
    func testOptionThreadsThrough() throws {
        let client = NatsClientOptions()
            .url(seedUrl)
            .ignoreDiscoveredServers()
            .build()
        let handler = try XCTUnwrap(client.connectionHandler)
        let info = try makeServerInfo(connectUrls: ["nats://10.0.0.1:4222"])

        handler.updateServersList(info: info)

        XCTAssertEqual(
            handler.serverPool, [seedUrl],
            "Option built via NatsClientOptions must suppress discovered servers")
    }
}
