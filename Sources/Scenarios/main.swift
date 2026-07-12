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

// Quiet the client's own per-operation logging so each scenario's observable
// lines stand out (mirrors PerfBench's main).
Nats.logger.logLevel = .warning

let scenarioNames = [
    "kv-watch", "object-transfer", "work-queue", "service", "async-publish", "live-consume",
]

/// Shared connection helper: reads `NATS_URL` (default `nats://localhost:4222`),
/// builds a client that reconnects fast and forever, and connects.
func connect() async throws -> NatsClient {
    let url = ProcessInfo.processInfo.environment["NATS_URL"] ?? "nats://localhost:4222"
    let client = NatsClientOptions().url(URL(string: url)!).reconnectWait(0.25)
        .unlimitedReconnects().build()
    out("connect", "connecting to \(url) ...")
    try await client.connect()
    out("connect", "connected")
    return client
}

func printUsage() {
    print("Usage: NATS_URL=nats://localhost:4222 swift run Scenarios <name>")
    print("Scenarios:")
    for name in scenarioNames {
        print("  \(name)")
    }
}

let arguments = CommandLine.arguments
guard arguments.count >= 2 else {
    printUsage()
    exit(2)
}

let scenario = arguments[1]
var exitStatus: Int32 = 0
do {
    switch scenario {
    case "kv-watch": try await runKvWatch()
    case "object-transfer": try await runObjectTransfer()
    case "work-queue": try await runWorkQueue()
    case "service": try await runService()
    case "async-publish": try await runAsyncPublish()
    case "live-consume": try await runLiveConsume()
    default:
        out("main", "unknown scenario: \(scenario)")
        printUsage()
        exit(2)
    }
} catch {
    out("main", "\(scenario) FAILED: \(error)")
    exitStatus = 1
}
exit(exitStatus)
