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

/// The monitoring verbs exposed by every service.
enum Verb: String, CaseIterable, Sendable {
    case ping = "PING"
    case info = "INFO"
    case stats = "STATS"
}

/// Well-known subjects, constants and control-subject construction used by the
/// Service framework. Mirrors the wire contract of `nats.go/micro`.
enum ServiceSubjects {
    /// The root of all control subjects.
    static let apiPrefix = "$SRV"

    /// The queue group used across all endpoints unless overridden.
    static let defaultQueueGroup = "q"

    /// Response `type` strings (`io.nats.micro.v1.*`).
    static let pingResponseType = "io.nats.micro.v1.ping_response"
    static let infoResponseType = "io.nats.micro.v1.info_response"
    static let statsResponseType = "io.nats.micro.v1.stats_response"

    /// Header carrying the service error description on error responses.
    static let errorHeader = "Nats-Service-Error"

    /// Header carrying the service error code on error responses.
    static let errorCodeHeader = "Nats-Service-Error-Code"

    /// Builds a control subject for a verb.
    ///
    /// - verb only: `$SRV.<VERB>` (monitors all services)
    /// - verb + name: `$SRV.<VERB>.<name>` (monitors services with the given name)
    /// - verb + name + id: `$SRV.<VERB>.<name>.<id>` (monitors a specific instance)
    static func control(_ verb: Verb, name: String? = nil, id: String? = nil) -> String {
        if let name, let id {
            return "\(apiPrefix).\(verb.rawValue).\(name).\(id)"
        }
        if let name {
            return "\(apiPrefix).\(verb.rawValue).\(name)"
        }
        return "\(apiPrefix).\(verb.rawValue)"
    }

    /// Returns the nine control subjects (three verbs x three levels) that a service
    /// instance subscribes to for discovery and monitoring.
    static func allControl(name: String, id: String) -> [String] {
        Verb.allCases.flatMap { verb in
            [
                control(verb),
                control(verb, name: name),
                control(verb, name: name, id: id),
            ]
        }
    }

    /// Prefixes an endpoint subject with a group prefix (`"<prefix>.<subject>"`).
    static func grouped(prefix: String, subject: String) -> String {
        prefix.isEmpty ? subject : "\(prefix).\(subject)"
    }
}
