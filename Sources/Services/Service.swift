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

import Dispatch
import Foundation
import Nats
import Nuid

/// A NATS microservice.
///
/// A `Service` aggregates request/reply endpoints under a common name and version,
/// and automatically exposes the PING, INFO and STATS monitoring endpoints on the
/// `$SRV.*` control subjects. Create one via ``Nats/NatsClient/addService(_:)``.
///
/// Endpoint subjects are load-balanced across service instances using a queue group
/// (default `"q"`), while the control subjects use plain subscriptions so that every
/// instance answers discovery requests.
public actor Service {
    /// The unique instance ID assigned to this service (a NUID).
    public nonisolated let id: String

    /// The service name.
    public nonisolated let name: String

    nonisolated let client: NatsClient
    nonisolated let version: String
    nonisolated let metadata: [String: String]
    nonisolated let serviceDescription: String
    nonisolated let defaultQueueGroup: String
    nonisolated let errorHandler: ServiceErrorHandler?

    private var endpoints: [EndpointRegistration] = []
    private var subscriptions: [NatsSubscription] = []
    private var tasks: [Task<Void, Never>] = []
    private var started: Date
    private var stopped = false

    /// The id of the connection-closed event listener, so it can be unregistered on stop
    /// (otherwise many short-lived services on one long-lived connection leak listeners).
    private var closedListenerId: String?

    private static let encoder = JSONEncoder()

    /// Per-endpoint registration and mutable statistics, owned by the actor.
    private struct EndpointRegistration {
        let name: String
        let subject: String
        let queueGroup: String
        let metadata: [String: String]?
        var numRequests = 0
        var numErrors = 0
        var lastError = ""
        var processingTimeNs: Int64 = 0
        var averageProcessingTimeNs: Int64 = 0
    }

    private init(client: NatsClient, config: ServiceConfig) {
        self.client = client
        self.id = nextNuid()
        self.name = config.name
        self.version = config.version
        self.metadata = config.metadata ?? [:]
        self.serviceDescription = config.description ?? ""
        self.defaultQueueGroup = config.queueGroup ?? ServiceSubjects.defaultQueueGroup
        self.errorHandler = config.errorHandler
        self.started = Date()
    }

    /// Validates the configuration, creates the service and starts its control-subject
    /// subscriptions.
    static func create(client: NatsClient, config: ServiceConfig) async throws -> Service {
        guard ServiceValidation.isValidName(config.name) else {
            throw ServiceError.invalidConfig(
                "service name should not be empty and should consist of alphanumerical "
                    + "characters, dashes and underscores")
        }
        guard ServiceValidation.isValidVersion(config.version) else {
            throw ServiceError.invalidConfig("version should match the SemVer format")
        }
        let service = Service(client: client, config: config)
        try await service.start()
        return service
    }

    // MARK: - Lifecycle

    private func start() async throws {
        do {
            for verb in Verb.allCases {
                let subjects = [
                    ServiceSubjects.control(verb),
                    ServiceSubjects.control(verb, name: name),
                    ServiceSubjects.control(verb, name: name, id: id),
                ]
                for subject in subjects {
                    // Control subjects use a PLAIN subscribe (no queue group) so that
                    // every service instance answers discovery and monitoring requests.
                    let subscription = try await client.subscribe(subject: subject)
                    subscriptions.append(subscription)
                    tasks.append(
                        Task { [weak self] in
                            await self?.runControlLoop(subscription: subscription, verb: verb)
                        })
                }
            }

            // If the underlying connection is closed, tear down without unsubscribing
            // (the connection is gone, so unsubscribe would throw).
            closedListenerId = client.on(.closed) { [weak self] _ in
                guard let self else { return }
                Task { await self.handleConnectionClosed() }
            }

            started = Date()
        } catch {
            // A subscribe failed partway through startup. Tear down whatever was
            // established so no subscription/task is orphaned -- `create()` throws and
            // never returns a handle the caller could `stop()`.
            await teardown(unsubscribe: true)
            throw error
        }
    }

    /// Registers an endpoint with the given resolved subject and queue group.
    func registerEndpoint(
        name: String,
        subject: String,
        queueGroup: String,
        metadata: [String: String]?,
        handler: @escaping ServiceHandler
    ) async throws {
        guard !stopped else {
            throw ServiceError.stopped
        }
        guard ServiceValidation.isValidName(name) else {
            throw ServiceError.invalidConfig("invalid endpoint name")
        }
        // Endpoint subjects use the queue group so requests load-balance across instances.
        let subscription = try await client.subscribe(subject: subject, queue: queueGroup)
        // Re-check after the await: `teardown()` may have run to completion during the
        // subscribe suspension (actors are reentrant). Registering into an
        // already-stopped service would leak a live subscription and a task that nothing
        // cancels -- and could wedge a concurrent `stop()`. Undo and report stopped.
        guard !stopped else {
            try? await subscription.unsubscribe()
            throw ServiceError.stopped
        }
        let index = endpoints.count
        endpoints.append(
            EndpointRegistration(
                name: name, subject: subject, queueGroup: queueGroup, metadata: metadata))
        subscriptions.append(subscription)
        tasks.append(
            Task { [weak self] in
                await self?.runEndpointLoop(
                    subscription: subscription, index: index, handler: handler)
            })
    }

    /// Registers a new endpoint on the service.
    ///
    /// - Parameters:
    ///   - name: the endpoint name (`^[A-Za-z0-9\-_]+$`). Also used as the subject
    ///     unless `subject` is provided.
    ///   - subject: an optional subject to register the endpoint on (defaults to `name`).
    ///   - queueGroup: an optional queue group override (defaults to the service's).
    ///   - metadata: optional metadata annotating the endpoint.
    ///   - handler: the request handler.
    public func addEndpoint(
        _ name: String,
        subject: String? = nil,
        queueGroup: String? = nil,
        metadata: [String: String]? = nil,
        handler: @escaping ServiceHandler
    ) async throws {
        try await registerEndpoint(
            name: name,
            subject: subject ?? name,
            queueGroup: queueGroup ?? defaultQueueGroup,
            metadata: metadata,
            handler: handler)
    }

    /// Creates a group, allowing endpoints to be registered under a common subject prefix.
    ///
    /// - Parameters:
    ///   - name: the group prefix.
    ///   - queueGroup: an optional queue group override inherited by the group's endpoints.
    public nonisolated func addGroup(_ name: String, queueGroup: String? = nil) -> ServiceGroup {
        ServiceGroup(
            service: self, prefix: name, queueGroup: queueGroup ?? defaultQueueGroup)
    }

    /// Drains the endpoint and monitoring subscriptions and marks the service as stopped.
    /// Idempotent.
    public func stop() async {
        await teardown(unsubscribe: true)
    }

    /// Whether ``stop()`` (or a connection close) has torn down the service.
    public var isStopped: Bool {
        stopped
    }

    private func handleConnectionClosed() async {
        await teardown(unsubscribe: false)
    }

    private func teardown(unsubscribe: Bool) async {
        guard !stopped else { return }
        stopped = true
        if let closedListenerId {
            client.off(closedListenerId)
            self.closedListenerId = nil
        }
        // Snapshot and clear the arrays up front so this teardown operates on a fixed set,
        // immune to a concurrent `registerEndpoint` that resumes from its subscribe await
        // during the `unsubscribe` suspension below. (That call also re-checks `stopped`
        // and unsubscribes itself; even if it appended, its task is not in this snapshot,
        // so the wait loop cannot block on a subscription this teardown never cancelled.)
        let tasksToStop = tasks
        let subscriptionsToStop = subscriptions
        tasks.removeAll()
        subscriptions.removeAll()

        for task in tasksToStop {
            task.cancel()
        }
        if unsubscribe {
            for subscription in subscriptionsToStop {
                try? await subscription.unsubscribe()
            }
        }
        for task in tasksToStop {
            _ = await task.value
        }
    }

    // MARK: - Monitoring

    /// Returns the service info (identity, description and endpoints).
    public func info() -> ServiceInfo {
        let endpointInfos = endpoints.map { endpoint in
            EndpointInfo(
                name: endpoint.name,
                subject: endpoint.subject,
                queueGroup: endpoint.queueGroup,
                metadata: endpoint.metadata)
        }
        return ServiceInfo(
            identity: identity(),
            type: ServiceSubjects.infoResponseType,
            description: serviceDescription,
            endpoints: endpointInfos)
    }

    /// Returns statistics for all registered endpoints.
    public func stats() -> ServiceStats {
        let endpointStats = endpoints.map { endpoint in
            EndpointStats(
                name: endpoint.name,
                subject: endpoint.subject,
                queueGroup: endpoint.queueGroup,
                numRequests: endpoint.numRequests,
                numErrors: endpoint.numErrors,
                lastError: endpoint.lastError,
                processingTime: endpoint.processingTimeNs,
                averageProcessingTime: endpoint.averageProcessingTimeNs)
        }
        return ServiceStats(
            identity: identity(),
            type: ServiceSubjects.statsResponseType,
            started: ServiceTime.rfc3339(started),
            endpoints: endpointStats)
    }

    /// Resets all statistics on all endpoints and bumps the start time.
    public func reset() {
        for index in endpoints.indices {
            endpoints[index].numRequests = 0
            endpoints[index].numErrors = 0
            endpoints[index].lastError = ""
            endpoints[index].processingTimeNs = 0
            endpoints[index].averageProcessingTimeNs = 0
        }
        started = Date()
    }

    private func identity() -> ServiceIdentity {
        ServiceIdentity(name: name, id: id, version: version, metadata: metadata)
    }

    private func pingResponse() -> ServicePing {
        ServicePing(identity: identity(), type: ServiceSubjects.pingResponseType)
    }

    /// Builds the JSON payload for a monitoring verb from the current live state.
    private func controlResponse(for verb: Verb) -> Data {
        switch verb {
        case .ping:
            return encode(pingResponse())
        case .info:
            return encode(info())
        case .stats:
            return encode(stats())
        }
    }

    private func encode<T: Encodable>(_ value: T) -> Data {
        (try? Self.encoder.encode(value)) ?? Data()
    }

    /// Records the outcome of a completed request against an endpoint's stats.
    private func recordStats(index: Int, elapsedNs: Int64, respondError: String?) {
        guard endpoints.indices.contains(index) else { return }
        endpoints[index].numRequests += 1
        endpoints[index].processingTimeNs += elapsedNs
        endpoints[index].averageProcessingTimeNs =
            endpoints[index].processingTimeNs / Int64(endpoints[index].numRequests)
        if let respondError {
            endpoints[index].numErrors += 1
            endpoints[index].lastError = respondError
        }
    }

    // MARK: - Subscription loops (run off the actor)

    private nonisolated func runControlLoop(
        subscription: NatsSubscription, verb: Verb
    ) async {
        do {
            for try await message in subscription {
                guard let reply = message.replySubject else { continue }
                let payload = await controlResponse(for: verb)
                do {
                    try await client.publish(payload, subject: reply)
                } catch {
                    errorHandler?(error)
                }
            }
        } catch {
            // Only a real, thrown subscription error reaches here — a clean unsubscribe / connection
            // close ends the `for await` by returning nil, without throwing. Surface it (e.g. a
            // mid-stream permissions violation) instead of letting the control verb silently go dark.
            errorHandler?(error)
        }
    }

    private nonisolated func runEndpointLoop(
        subscription: NatsSubscription, index: Int, handler: @escaping ServiceHandler
    ) async {
        do {
            for try await message in subscription {
                let request = ServiceRequest(message: message, client: client)
                let start = DispatchTime.now().uptimeNanoseconds
                await handler(request)
                let elapsed = DispatchTime.now().uptimeNanoseconds - start
                await recordStats(
                    index: index, elapsedNs: Int64(elapsed), respondError: request.respondError)
            }
        } catch {
            // Only a real, thrown subscription error reaches here — a clean unsubscribe / connection
            // close ends the `for await` by returning nil, without throwing. Surface it instead of
            // letting the endpoint silently stop serving with no signal.
            errorHandler?(error)
        }
    }
}
