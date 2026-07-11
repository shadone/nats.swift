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

/// A callback invoked when the service encounters an internal error (e.g. a failure
/// while publishing a monitoring response).
public typealias ServiceErrorHandler = @Sendable (any Error) -> Void

/// Configuration used to create a ``Service``.
///
/// A service requires a `name` and a SemVer `version`. Endpoints are registered
/// after creation using ``Service/addEndpoint(_:subject:queueGroup:metadata:handler:)``
/// or ``Service/addGroup(_:queueGroup:)``.
public struct ServiceConfig: Sendable {
    /// The name of the service. Must match `^[A-Za-z0-9\-_]+$`.
    public var name: String

    /// A SemVer compatible version string.
    public var version: String

    /// An optional human-readable description of the service.
    public var description: String?

    /// Optional metadata annotating the service.
    public var metadata: [String: String]?

    /// Optional override for the default endpoint queue group (`"q"`).
    public var queueGroup: String?

    /// Optional handler invoked on internal, NATS-related service errors.
    public var errorHandler: ServiceErrorHandler?

    /// Creates a new service configuration.
    ///
    /// - Parameters:
    ///   - name: the service name (`^[A-Za-z0-9\-_]+$`).
    ///   - version: a SemVer compatible version string.
    ///   - description: an optional description of the service.
    ///   - metadata: optional metadata annotating the service.
    ///   - queueGroup: optional override for the default endpoint queue group.
    ///   - errorHandler: optional handler invoked on internal service errors.
    public init(
        name: String,
        version: String,
        description: String? = nil,
        metadata: [String: String]? = nil,
        queueGroup: String? = nil,
        errorHandler: ServiceErrorHandler? = nil
    ) {
        self.name = name
        self.version = version
        self.description = description
        self.metadata = metadata
        self.queueGroup = queueGroup
        self.errorHandler = errorHandler
    }
}
