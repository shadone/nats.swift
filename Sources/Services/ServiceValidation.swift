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

/// Validation helpers mirroring the regular expressions used by `nats.go/micro`.
enum ServiceValidation {
    /// Service and endpoint names must consist of alphanumerics, dashes and underscores.
    static let namePattern = "^[A-Za-z0-9\\-_]+$"

    /// The suggested SemVer validation regexp from https://semver.org/.
    static let semVerPattern =
        "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)"
        + "(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)"
        + "(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
        + "(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$"

    static func isValidName(_ value: String) -> Bool {
        matches(namePattern, value)
    }

    static func isValidVersion(_ value: String) -> Bool {
        matches(semVerPattern, value)
    }

    private static func matches(_ pattern: String, _ value: String) -> Bool {
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return false
        }
        let range = NSRange(value.startIndex..<value.endIndex, in: value)
        // Patterns are fully anchored (`^...$`), so a single match spans the string.
        return regex.firstMatch(in: value, options: [], range: range) != nil
    }
}
