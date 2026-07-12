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

import Combine
import Foundation
import Nats
import Nuid

/// A context which can perform jetstream scoped requests.
///
/// `@unchecked Sendable`: `client` (a `Sendable` `NatsClient`) and `prefix` are immutable `let`s;
/// the only mutable state is `timeout`, which the public ``setTimeout(_:)`` can change after
/// construction. That single field is guarded by `timeoutLock`, so every read and write is
/// serialized and sharing a context across concurrency domains is data-race-free. The lock is held
/// only across the synchronous property access (never across an `await`), matching the JetStream
/// target's existing `NSLock` synchronization idiom.
public final class JetStreamContext: @unchecked Sendable {
    internal let client: NatsClient
    private let prefix: String
    private let timeoutLock = NSLock()
    private var _timeout: TimeInterval
    private var timeout: TimeInterval {
        timeoutLock.withLockScoped { _timeout }
    }

    /// Owns the shared batched-publish machinery (one wildcard ack subscription + a bounded
    /// in-flight window). Constructed eagerly and cheaply here — no I/O happens until the first
    /// ``publishAsync(_:message:headers:msgTTL:)``, which lazily starts the subscription and pump.
    /// The reaper's expiry bound is captured from the context timeout at construction time and does
    /// not track later ``setTimeout(_:)`` changes.
    internal let asyncPublisher: JetStreamPublishAsync

    /// Creates a JetStreamContext from `NatsClient` with optional custom prefix and timeout.
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    ///  - prefix: Used to comfigure a prefix for JetStream API requests.
    ///  - timeout: Used to configure a timeout for JetStream API operations.
    public init(client: NatsClient, prefix: String = "$JS.API", timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = prefix
        self._timeout = timeout
        self.asyncPublisher = JetStreamPublishAsync(client: client, timeout: timeout)
    }

    /// Creates a JetStreamContext from `NatsClient` with custom domain and timeout.
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    ///  - domain: Used to comfigure a domain for JetStream API requests.
    ///  - timeout: Used to configure a timeout for JetStream API operations.
    public init(client: NatsClient, domain: String, timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = "$JS.\(domain).API"
        self._timeout = timeout
        self.asyncPublisher = JetStreamPublishAsync(client: client, timeout: timeout)
    }

    /// Creates a JetStreamContext from `NatsClient`
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    public init(client: NatsClient) {
        self.client = client
        self.prefix = "$JS.API"
        self._timeout = 5.0
        self.asyncPublisher = JetStreamPublishAsync(client: client, timeout: 5.0)
    }

    /// Sets a custom timeout for JetStream API requests.
    public func setTimeout(_ timeout: TimeInterval) {
        timeoutLock.withLockScoped { _timeout = timeout }
    }

    deinit {
        // Tear down the batched-publish subscription/pump best-effort. `deinit` is nonisolated, so
        // fire-and-forget the async `shutdown()`; the `Task` retains the (Sendable) publisher long
        // enough to run.
        let pub = asyncPublisher
        Task { await pub.shutdown() }
    }
}

extension JetStreamContext {

    /// Publishes a message on a stream subjec without waiting  for acknowledgment from the server that the message has been successfully delivered.
    ///
    /// - Parameters:
    ///   - subject: Subject on which the message will be published.
    ///   - message: NATS message payload.
    ///   - headers:Optional set of message headers.
    ///   - msgTTL: Optional per-message time-to-live. When set, the message is
    ///     removed from the stream after this interval. The value is sent in the
    ///     `Nats-TTL` header (formatted as a Go duration string, e.g. `"5s"`) and
    ///     is only honored when the target stream has ``StreamConfig/allowMsgTTL``
    ///     enabled. Values `<= 0` are ignored.
    ///
    /// - Returns: ``AckFuture`` allowing to await for the ack from the server.
    public func publish(
        _ subject: String, message: Data, headers: NatsHeaderMap? = nil,
        msgTTL: NanoTimeInterval? = nil
    ) async throws -> AckFuture {
        // TODO(pp): add stream header options (expected seq etc)
        var headers = headers
        if let msgTTL, msgTTL.value > 0 {
            var withTTL = headers ?? NatsHeaderMap()
            withTTL[.natsMsgTTL] = NatsHeaderValue(msgTTL.goDurationString())
            headers = withTTL
        }
        let inbox = client.newInbox()
        let sub = try await self.client.subscribe(subject: inbox)
        try await self.client.publish(message, subject: subject, reply: inbox, headers: headers)
        return AckFuture(sub: sub, timeout: self.timeout)
    }

    internal func request<T: Codable>(
        _ subject: String, message: Data? = nil
    ) async throws -> Response<T> {
        let data = message ?? Data()
        do {
            let response = try await self.client.request(
                data, subject: apiSubject(subject), timeout: self.timeout)
            let decoder = JSONDecoder()
            guard let payload = response.payload else {
                throw JetStreamError.RequestError.emptyResponsePayload
            }
            return try decoder.decode(Response<T>.self, from: payload)
        } catch let err as NatsError.RequestError {
            switch err {
            case .noResponders:
                throw JetStreamError.RequestError.noResponders
            case .timeout:
                throw JetStreamError.RequestError.timeout
            case .permissionDenied:
                throw JetStreamError.RequestError.permissionDenied(subject)
            }
        }
    }

    internal func request(_ subject: String, message: Data? = nil) async throws -> NatsMessage {
        let data = message ?? Data()
        do {
            return try await self.client.request(
                data, subject: apiSubject(subject), timeout: self.timeout)
        } catch let err as NatsError.RequestError {
            switch err {
            case .noResponders:
                throw JetStreamError.RequestError.noResponders
            case .timeout:
                throw JetStreamError.RequestError.timeout
            case .permissionDenied:
                throw JetStreamError.RequestError.permissionDenied(subject)
            }
        }
    }

    internal func apiSubject(_ subject: String) -> String {
        return "\(self.prefix).\(subject)"
    }
}

public struct JetStreamAPIResponse: Codable {
    public let type: String
    public let error: JetStreamError.APIError
}

/// Used to await for response from ``JetStreamContext/publish(_:message:headers:msgTTL:)``
public struct AckFuture {
    let sub: NatsSubscription
    let timeout: TimeInterval

    /// Waits for an ACK from JetStream server.
    ///
    /// - Returns: Acknowledgement object returned by the server.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/RequestError`` if the request timed out (client did not receive the ack in time) or
    public func wait() async throws -> Ack {
        let response = try await withThrowingTaskGroup(
            of: NatsMessage?.self,
            body: { group in
                // Capture the already-Sendable `sub`/`timeout` by value so each child
                // task closure carries an immutable Sendable copy instead of implicitly
                // capturing `self` (the non-Sendable-region `AckFuture`), which stays in
                // use by the current task. Behavior is unchanged: `sub` is a class
                // reference and `timeout` is a value.
                group.addTask { [sub] in
                    return try await sub.makeAsyncIterator().next()
                }

                // task for the timeout
                group.addTask { [timeout] in
                    try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                    return nil
                }

                for try await result in group {
                    // if the result is not empty, return it (or throw status error)
                    if let msg = result {
                        group.cancelAll()
                        return msg
                    } else {
                        group.cancelAll()
                        try await sub.unsubscribe()
                        // if result is empty, time out
                        throw JetStreamError.RequestError.timeout
                    }
                }

                // this should not be reachable
                throw NatsError.ClientError.internalError("error waiting for response")
            })
        return try Ack.decodeAck(from: response)
    }
}

public struct Ack: Codable, Sendable {
    public let stream: String
    public let seq: UInt64
    public let domain: String?
    public let duplicate: Bool

    // Custom CodingKeys to map JSON keys to Swift property names
    enum CodingKeys: String, CodingKey {
        case stream
        case seq
        case domain
        case duplicate
    }

    // Custom initializer from Decoder
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Decode `stream` and `seq` as they are required
        stream = try container.decode(String.self, forKey: .stream)
        seq = try container.decode(UInt64.self, forKey: .seq)

        // Decode `domain` as optional since it may not be present
        domain = try container.decodeIfPresent(String.self, forKey: .domain)

        // Decode `duplicate` and provide a default value of `false` if not present
        duplicate = try container.decodeIfPresent(Bool.self, forKey: .duplicate) ?? false
    }
}

extension Ack {
    /// Decodes a JetStream publish-ack ``NatsMessage`` into an ``Ack``, or throws the failure the
    /// server reported. This is the single source of truth for interpreting a publish reply and is
    /// shared by the synchronous ``AckFuture/wait()`` and the async publisher's ack pump.
    ///
    /// Interpretation order (must not change):
    /// 1. `status == noResponders` → ``JetStreamError/PublishError/streamNotFound`` (no stream is
    ///    listening on the subject).
    /// 2. A publish-ack error (e.g. wrong last sequence, err_code 10071) comes back as
    ///    `{"error":{...},"stream":..,"seq":0}`: it carries `stream`/`seq` but no `type`, so
    ///    `Response<Ack>` cannot represent it. Its success branch would mis-decode the error as an
    ///    `Ack` with `seq: 0` and swallow the failure, so the error object has to be detected
    ///    explicitly BEFORE decoding the ack. This keeps the CAS-error fix in one place.
    /// 3. Otherwise decode the ``Ack`` (empty payload → ``JetStreamError/RequestError/emptyResponsePayload``).
    static func decodeAck(from message: NatsMessage) throws -> Ack {
        if message.status == StatusCode.noResponders {
            throw JetStreamError.PublishError.streamNotFound
        }

        let decoder = JSONDecoder()
        guard let payload = message.payload else {
            throw JetStreamError.RequestError.emptyResponsePayload
        }

        if let pubAckErr = try? decoder.decode(PubAckError.self, from: payload),
            let apiErr = pubAckErr.error
        {
            if let publishErr = JetStreamError.PublishError(from: apiErr) {
                throw publishErr
            }
            throw apiErr
        }

        return try decoder.decode(Ack.self, from: payload)
    }

    private struct PubAckError: Decodable {
        let error: JetStreamError.APIError?
    }
}

/// contains info about the `JetStream` usage from the current account.
public struct AccountInfo: Codable {
    public let memory: Int64
    public let storage: Int64
    public let streams: Int64
    public let consumers: Int64
}
