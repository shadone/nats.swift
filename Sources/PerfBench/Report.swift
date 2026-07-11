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

/// A single named measurement, e.g. `msgs/sec` or `p99us`.
struct Metric: Codable, Sendable {
    let label: String
    let value: Double
}

/// The result of one scenario run.
struct ScenarioResult: Codable, Sendable {
    let name: String
    let count: Int
    let payloadSize: Int
    let elapsedMs: Double
    let metrics: [Metric]
}

/// Returns the value at the given percentile (0...100) from an ascending-sorted array of
/// nanosecond latencies, expressed in microseconds. Uses nearest-rank indexing.
func percentileMicros(_ sortedNanos: [UInt64], _ percentile: Double) -> Double {
    guard !sortedNanos.isEmpty else { return 0 }
    var index = Int((percentile / 100.0) * Double(sortedNanos.count))
    if index >= sortedNanos.count {
        index = sortedNanos.count - 1
    }
    if index < 0 {
        index = 0
    }
    return Double(sortedNanos[index]) / 1_000.0
}

/// Returns the arithmetic mean of nanosecond latencies, expressed in microseconds.
func meanMicros(_ nanos: [UInt64]) -> Double {
    guard !nanos.isEmpty else { return 0 }
    let total = nanos.reduce(0.0) { $0 + Double($1) }
    return total / Double(nanos.count) / 1_000.0
}

/// Formats a metric value: whole numbers for large rates, two decimals otherwise.
func formatValue(_ value: Double) -> String {
    if value >= 1000 {
        return String(format: "%.0f", value)
    }
    return String(format: "%.2f", value)
}

/// Renders the results as an aligned, human-readable text table.
func renderTable(_ results: [ScenarioResult]) -> String {
    guard !results.isEmpty else {
        return "no results"
    }
    let headers = ["SCENARIO", "COUNT", "SIZE(B)", "ELAPSED(ms)", "METRICS"]
    var rows: [[String]] = [headers]
    for result in results {
        let metricsText =
            result.metrics
            .map { "\($0.label)=\(formatValue($0.value))" }
            .joined(separator: "  ")
        rows.append([
            result.name,
            String(result.count),
            String(result.payloadSize),
            String(format: "%.2f", result.elapsedMs),
            metricsText,
        ])
    }

    var widths = [Int](repeating: 0, count: headers.count)
    for row in rows {
        for (column, cell) in row.enumerated() {
            widths[column] = max(widths[column], cell.count)
        }
    }

    var lines: [String] = []
    for (rowIndex, row) in rows.enumerated() {
        var cells: [String] = []
        for (column, cell) in row.enumerated() {
            let width = widths[column]
            let padding = String(repeating: " ", count: width - cell.count)
            // Left-align the name and metrics columns; right-align the numeric ones.
            if column == 0 || column == headers.count - 1 {
                cells.append(cell + padding)
            } else {
                cells.append(padding + cell)
            }
        }
        lines.append(cells.joined(separator: "  ").trimmingTrailingSpaces())
        if rowIndex == 0 {
            let separator = widths.map { String(repeating: "-", count: $0) }.joined(separator: "  ")
            lines.append(separator)
        }
    }
    return lines.joined(separator: "\n")
}

/// Prints the results as a pretty-printed JSON array on standard output.
func emitJSON(_ results: [ScenarioResult]) {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    do {
        let data = try encoder.encode(results)
        if let text = String(data: data, encoding: .utf8) {
            print(text)
        }
    } catch {
        writeStderr("failed to encode json: \(error)")
    }
}

extension String {
    /// Removes trailing spaces so left-aligned final columns don't leave ragged whitespace.
    fileprivate func trimmingTrailingSpaces() -> String {
        var view = self[...]
        while view.last == " " {
            view = view.dropLast()
        }
        return String(view)
    }
}
