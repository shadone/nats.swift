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

extension NatsClient {

    /// Adds a microservice on this connection.
    ///
    /// The service enables the internal PING, INFO and STATS monitoring endpoints and is
    /// assigned a unique instance ID. Request handlers are registered separately using
    /// ``Service/addEndpoint(_:subject:queueGroup:metadata:handler:)`` and
    /// ``Service/addGroup(_:queueGroup:)``.
    ///
    /// - Parameter config: the service configuration. A name and SemVer version are required.
    /// - Returns: the started ``Service``.
    ///
    /// - Throws: ``ServiceError/invalidConfig(_:)`` if the name or version is invalid, or a
    ///   `NatsError` if the monitoring subscriptions could not be established.
    public func addService(_ config: ServiceConfig) async throws -> Service {
        try await Service.create(client: self, config: config)
    }
}
