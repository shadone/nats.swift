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
import NIOConcurrencyHelpers

import Nats

/// A request handler registered on a service endpoint.
public typealias ServiceHandler = @Sendable (ServiceRequest) async -> Void

/// A service request delivered to an endpoint handler.
///
/// Exposes the underlying message data and provides methods to respond to the
/// request. A request may only be responded to once; subsequent calls are ignored.
public final class ServiceRequest: Sendable {
    /// The subject the request was published on.
    public let subject: String

    /// The reply subject the response must be published to, if any.
    public let replySubject: String?

    /// The request headers, if any.
    public let headers: NatsHeaderMap?

    /// The request payload. Empty (never `nil`) when the request carries no data.
    public let data: Data

    private let client: NatsClient
    private let state = NIOLockedValueBox(RequestState())

    private struct RequestState: Sendable {
        var didRespond = false
        var respondError: String?
    }

    private static let errorHeader = try! NatsHeaderName(ServiceSubjects.errorHeader)
    private static let errorCodeHeader = try! NatsHeaderName(ServiceSubjects.errorCodeHeader)
    private static let encoder = JSONEncoder()

    init(message: NatsMessage, client: NatsClient) {
        self.subject = message.subject
        self.replySubject = message.replySubject
        self.headers = message.headers
        self.data = message.payload ?? Data()
        self.client = client
    }

    /// The recorded error string for stats bookkeeping (`"code:description"`), if the
    /// handler produced an error response.
    var respondError: String? {
        state.withLockedValue { $0.respondError }
    }

    /// Sends a raw response for the request.
    ///
    /// - Parameters:
    ///   - payload: the response payload.
    ///   - headers: optional response headers.
    ///
    /// - Throws: ``ServiceError/noReplySubject`` if the request has no reply subject.
    public func respond(_ payload: Data, headers: NatsHeaderMap? = nil) async throws {
        _ = try await publishReply(payload: payload, headers: headers)
    }

    /// Marshals `value` to JSON and sends it as the response.
    ///
    /// - Parameters:
    ///   - value: the value to encode and respond with.
    ///   - headers: optional response headers.
    ///
    /// - Throws: ``ServiceError/noReplySubject`` if the request has no reply subject,
    ///   or an encoding error if `value` cannot be serialized.
    public func respondJSON<T: Encodable>(_ value: T, headers: NatsHeaderMap? = nil) async throws {
        let payload = try Self.encoder.encode(value)
        _ = try await publishReply(payload: payload, headers: headers)
    }

    /// Prepares and publishes an error response from a handler.
    ///
    /// The response carries the `Nats-Service-Error` and `Nats-Service-Error-Code`
    /// headers, and records the error for the endpoint's stats.
    ///
    /// - Parameters:
    ///   - code: the error code. Must be non-empty.
    ///   - description: the error description. Must be non-empty.
    ///   - data: optional response payload.
    ///   - headers: optional additional response headers.
    ///
    /// - Throws: ``ServiceError/argumentRequired(_:)`` if `code` or `description` is
    ///   empty, or ``ServiceError/noReplySubject`` if the request has no reply subject.
    public func error(
        code: String, description: String, data: Data? = nil, headers: NatsHeaderMap? = nil
    ) async throws {
        guard !code.isEmpty else {
            throw ServiceError.argumentRequired("error code")
        }
        guard !description.isEmpty else {
            throw ServiceError.argumentRequired("description")
        }
        var responseHeaders = headers ?? NatsHeaderMap()
        responseHeaders.insert(Self.errorHeader, NatsHeaderValue(description))
        responseHeaders.insert(Self.errorCodeHeader, NatsHeaderValue(code))
        let sent = try await publishReply(payload: data ?? Data(), headers: responseHeaders)
        if sent {
            state.withLockedValue { $0.respondError = "\(code):\(description)" }
        }
    }

    /// Publishes a reply if the request has not already been responded to.
    ///
    /// - Returns: `true` if a reply was published, `false` if the request was already
    ///   responded to.
    private func publishReply(payload: Data, headers: NatsHeaderMap?) async throws -> Bool {
        guard let replySubject else {
            throw ServiceError.noReplySubject
        }
        let shouldSend = state.withLockedValue { state -> Bool in
            if state.didRespond {
                return false
            }
            state.didRespond = true
            return true
        }
        guard shouldSend else {
            return false
        }
        try await client.publish(payload, subject: replySubject, headers: headers)
        return true
    }
}
