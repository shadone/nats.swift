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

/// Errors thrown by the Service (micro) framework.
public enum ServiceError: Error, CustomStringConvertible, Sendable {
    /// The service or endpoint configuration is invalid (bad name, version, etc.).
    case invalidConfig(String)

    /// A request could not be responded to because it carries no reply subject.
    case noReplySubject

    /// A required argument was missing (e.g. an empty error code or description).
    case argumentRequired(String)

    /// The operation cannot be performed because the service has been stopped.
    case stopped

    public var description: String {
        switch self {
        case .invalidConfig(let reason):
            return "nats: invalid service config: \(reason)"
        case .noReplySubject:
            return "nats: no reply subject in request"
        case .argumentRequired(let argument):
            return "nats: argument required: \(argument)"
        case .stopped:
            return "nats: service is stopped"
        }
    }
}
