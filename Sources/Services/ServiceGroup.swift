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

/// A group of endpoints sharing a common subject prefix.
///
/// Endpoints registered through a group have their subject prefixed with the group's
/// prefix. Nested groups derive a compound prefix. Created via
/// ``Service/addGroup(_:queueGroup:)``.
public struct ServiceGroup: Sendable {
    private let service: Service
    private let prefix: String
    private let queueGroup: String

    init(service: Service, prefix: String, queueGroup: String) {
        self.service = service
        self.prefix = prefix
        self.queueGroup = queueGroup
    }

    /// Registers a new endpoint on the service, prefixing its subject with the group prefix.
    ///
    /// - Parameters:
    ///   - name: the endpoint name (`^[A-Za-z0-9\-_]+$`). Also used as the (unprefixed)
    ///     subject unless `subject` is provided.
    ///   - subject: an optional subject (defaults to `name`); prefixed with the group prefix.
    ///   - queueGroup: an optional queue group override (defaults to the group's).
    ///   - metadata: optional metadata annotating the endpoint.
    ///   - handler: the request handler.
    public func addEndpoint(
        _ name: String,
        subject: String? = nil,
        queueGroup: String? = nil,
        metadata: [String: String]? = nil,
        handler: @escaping ServiceHandler
    ) async throws {
        let endpointSubject = ServiceSubjects.grouped(prefix: prefix, subject: subject ?? name)
        try await service.registerEndpoint(
            name: name,
            subject: endpointSubject,
            queueGroup: queueGroup ?? self.queueGroup,
            metadata: metadata,
            handler: handler)
    }

    /// Creates a nested group, prefixed by this group's prefix.
    ///
    /// - Parameters:
    ///   - name: the nested group name, appended to this group's prefix.
    ///   - queueGroup: an optional queue group override (defaults to the group's).
    public func addGroup(_ name: String, queueGroup: String? = nil) -> ServiceGroup {
        let nestedPrefix = ServiceSubjects.grouped(prefix: prefix, subject: name)
        return ServiceGroup(
            service: service, prefix: nestedPrefix, queueGroup: queueGroup ?? self.queueGroup)
    }
}
