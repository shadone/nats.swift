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
import JetStream
import Nats

// Quiet the client's per-operation info logging so the benchmark report stays readable. Uses
// implicit-member lookup on the `Logger.Level` type surfaced by `Nats.logger`, so no direct
// dependency on swift-log is needed.
Nats.logger.logLevel = .warning

/// Parses arguments, connects, runs the selected scenarios, and prints the report.
/// Returns a process exit status.
func perfBenchMain() async -> Int32 {
    let config: Config
    do {
        config = try parseConfig(CommandLine.arguments)
    } catch {
        writeStderr("\(error)")
        return 2
    }

    // In JSON mode, progress goes to stderr so stdout stays a clean JSON document.
    func log(_ message: String) {
        if config.json {
            writeStderr(message)
        } else {
            print(message)
        }
    }

    guard let url = URL(string: config.url) else {
        writeStderr("invalid url: \(config.url)")
        return 2
    }
    let nats = NatsClientOptions().url(url).build()
    do {
        log("connecting to \(config.url) ...")
        try await nats.connect()
    } catch {
        writeStderr("failed to connect to \(config.url): \(error)")
        return 1
    }
    let js = JetStreamContext(client: nats)

    var results = [ScenarioResult]()
    var failures = [String]()
    for name in config.scenarios {
        log("running \(name) ...")
        do {
            let result = try await runScenario(name, nats: nats, js: js, config: config)
            results.append(result)
            log("  \(name): \(String(format: "%.1f", result.elapsedMs)) ms")
        } catch {
            let message = "\(name) FAILED: \(error)"
            failures.append(message)
            writeStderr("  \(message)")
        }
    }

    try? await nats.close()

    if config.json {
        emitJSON(results)
    } else {
        print("")
        print(renderTable(results))
    }
    if !failures.isEmpty {
        writeStderr("\n\(failures.count) scenario(s) failed")
        return 1
    }
    return 0
}

let status = await perfBenchMain()
exit(status)
