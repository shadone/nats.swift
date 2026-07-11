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

/// The scenarios the harness can run, in canonical order.
let allScenarioNames: [String] = [
    "corePub",
    "corePubSub",
    "reqReply",
    "jsPublish",
    "kvPutGet",
    "objPutGet",
    "pullConsume",
    "orderedConsume",
    "pushConsume",
    "pushConsumeHB",
]

/// Parsed, validated harness configuration.
struct Config: Sendable {
    var url: String
    var scenarios: [String]
    var msgs: Int
    var size: Int
    var objSize: Int
    var reqs: Int
    var json: Bool
}

/// A parsing failure carrying a user-facing usage message.
struct UsageError: Error, CustomStringConvertible {
    let message: String

    var description: String { message }
}

/// The `--help`-style usage banner.
func usageText() -> String {
    """
    PerfBench - nats.swift performance benchmark harness

    USAGE:
        PerfBench [options]

    OPTIONS:
        --url <nats://...>   NATS server URL (default: $NATS_URL or nats://localhost:4222)
        --scenario <list>    'all' or comma-separated scenario ids (default: all)
        --msgs <int>         message count for core scenarios (default: 200000)
        --size <bytes>       payload size for message scenarios (default: 16)
        --obj-size <bytes>   object size for objPutGet (default: 33554432)
        --reqs <int>         request count for reqReply (default: 20000)
        --json               emit machine-readable JSON instead of a text table

    SCENARIOS:
        \(allScenarioNames.joined(separator: ", "))
    """
}

/// Parses command-line arguments into a ``Config``. Throws ``UsageError`` on any unknown flag,
/// missing value, or malformed number.
func parseConfig(_ arguments: [String]) throws -> Config {
    let defaultURL = ProcessInfo.processInfo.environment["NATS_URL"] ?? "nats://localhost:4222"
    var config = Config(
        url: defaultURL, scenarios: allScenarioNames, msgs: 200_000, size: 16,
        objSize: 33_554_432, reqs: 20_000, json: false)

    var index = 1  // skip the executable path
    func nextValue(for flag: String) throws -> String {
        index += 1
        guard index < arguments.count else {
            throw UsageError(message: "missing value for \(flag)\n\n\(usageText())")
        }
        return arguments[index]
    }
    func intValue(for flag: String) throws -> Int {
        let raw = try nextValue(for: flag)
        guard let value = Int(raw), value > 0 else {
            throw UsageError(
                message: "invalid positive integer for \(flag): \(raw)\n\n\(usageText())")
        }
        return value
    }

    while index < arguments.count {
        let flag = arguments[index]
        switch flag {
        case "--url":
            config.url = try nextValue(for: flag)
        case "--scenario":
            let raw = try nextValue(for: flag)
            config.scenarios = try resolveScenarios(raw)
        case "--msgs":
            config.msgs = try intValue(for: flag)
        case "--size":
            config.size = try intValue(for: flag)
        case "--obj-size":
            config.objSize = try intValue(for: flag)
        case "--reqs":
            config.reqs = try intValue(for: flag)
        case "--json":
            config.json = true
        default:
            throw UsageError(message: "unknown flag: \(flag)\n\n\(usageText())")
        }
        index += 1
    }
    return config
}

/// Resolves a `--scenario` value (`all` or a comma-separated list) into validated names.
func resolveScenarios(_ value: String) throws -> [String] {
    if value == "all" {
        return allScenarioNames
    }
    let requested = value.split(separator: ",").map(String.init)
    guard !requested.isEmpty else {
        throw UsageError(message: "no scenarios selected\n\n\(usageText())")
    }
    for name in requested where !allScenarioNames.contains(name) {
        throw UsageError(message: "unknown scenario: \(name)\n\n\(usageText())")
    }
    return requested
}
