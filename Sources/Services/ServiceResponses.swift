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

/// A dynamic coding key, used to flatten ``ServiceIdentity`` fields into the response
/// objects (mirroring Go's embedded-struct JSON flattening).
struct AnyCodingKey: CodingKey {
    let stringValue: String
    let intValue: Int? = nil

    init(_ stringValue: String) {
        self.stringValue = stringValue
    }

    init?(stringValue: String) {
        self.stringValue = stringValue
    }

    init?(intValue: Int) {
        return nil
    }
}

/// Fields identifying a service instance. Flattened into every monitoring response.
public struct ServiceIdentity: Encodable, Sendable {
    public let name: String
    public let id: String
    public let version: String
    public let metadata: [String: String]

    func encode(into container: inout KeyedEncodingContainer<AnyCodingKey>) throws {
        try container.encode(name, forKey: AnyCodingKey("name"))
        try container.encode(id, forKey: AnyCodingKey("id"))
        try container.encode(version, forKey: AnyCodingKey("version"))
        try container.encode(metadata, forKey: AnyCodingKey("metadata"))
    }
}

/// The response returned on the PING monitoring endpoint.
public struct ServicePing: Encodable, Sendable {
    public let identity: ServiceIdentity
    public let type: String

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: AnyCodingKey.self)
        try identity.encode(into: &container)
        try container.encode(type, forKey: AnyCodingKey("type"))
    }
}

/// Basic information about a single endpoint, returned by the INFO endpoint.
public struct EndpointInfo: Encodable, Sendable {
    public let name: String
    public let subject: String
    public let queueGroup: String
    public let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case name
        case subject
        case queueGroup = "queue_group"
        case metadata
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(subject, forKey: .subject)
        try container.encode(queueGroup, forKey: .queueGroup)
        // Endpoint metadata is encoded as JSON `null` when unset (matches nats.go).
        if let metadata {
            try container.encode(metadata, forKey: .metadata)
        } else {
            try container.encodeNil(forKey: .metadata)
        }
    }
}

/// The response returned on the INFO monitoring endpoint.
public struct ServiceInfo: Encodable, Sendable {
    public let identity: ServiceIdentity
    public let type: String
    public let description: String
    public let endpoints: [EndpointInfo]

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: AnyCodingKey.self)
        try identity.encode(into: &container)
        try container.encode(type, forKey: AnyCodingKey("type"))
        try container.encode(description, forKey: AnyCodingKey("description"))
        try container.encode(endpoints, forKey: AnyCodingKey("endpoints"))
    }
}

/// Statistics for a single endpoint, returned by the STATS endpoint.
public struct EndpointStats: Encodable, Sendable {
    public let name: String
    public let subject: String
    public let queueGroup: String
    public let numRequests: Int
    public let numErrors: Int
    public let lastError: String
    /// Total processing time, in integer nanoseconds.
    public let processingTime: Int64
    /// Average processing time, in integer nanoseconds.
    public let averageProcessingTime: Int64

    enum CodingKeys: String, CodingKey {
        case name
        case subject
        case queueGroup = "queue_group"
        case numRequests = "num_requests"
        case numErrors = "num_errors"
        case lastError = "last_error"
        case processingTime = "processing_time"
        case averageProcessingTime = "average_processing_time"
    }
}

/// The response returned on the STATS monitoring endpoint.
public struct ServiceStats: Encodable, Sendable {
    public let identity: ServiceIdentity
    public let type: String
    /// The service start time, formatted as RFC3339 UTC with fractional seconds.
    public let started: String
    public let endpoints: [EndpointStats]

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: AnyCodingKey.self)
        try identity.encode(into: &container)
        try container.encode(type, forKey: AnyCodingKey("type"))
        try container.encode(started, forKey: AnyCodingKey("started"))
        try container.encode(endpoints, forKey: AnyCodingKey("endpoints"))
    }
}

/// Formats timestamps as RFC3339 UTC with microsecond precision and a `Z` suffix,
/// e.g. `2024-09-24T11:02:55.564771Z`.
enum ServiceTime {
    // Configured once and shared: DateFormatter is thread-safe for read-only
    // formatting, so a static instance avoids rebuilding it on every response.
    private static let secondsFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        return formatter
    }()

    static func rfc3339(_ date: Date) -> String {
        let interval = date.timeIntervalSince1970
        var whole = interval.rounded(.down)
        var micros = Int(((interval - whole) * 1_000_000).rounded())
        if micros >= 1_000_000 {
            micros -= 1_000_000
            whole += 1
        }
        let base = secondsFormatter.string(from: Date(timeIntervalSince1970: whole))
        return String(format: "%@.%06dZ", base, micros)
    }
}
