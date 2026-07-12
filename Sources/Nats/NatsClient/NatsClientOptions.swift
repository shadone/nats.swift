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
import Logging
import NIO
import NIOFoundationCompat

public class NatsClientOptions {
    private var urls: [URL] = []
    private var pingInterval: TimeInterval = 60.0
    private var reconnectWait: TimeInterval = 2.0
    private var maxReconnects: Int?
    private var initialReconnect = false
    private var noRandomize = false
    private var ignoreDiscovered = false
    private var auth: Auth? = nil
    private var withTls = false
    private var tlsFirst = false
    private var rootCertificate: URL? = nil
    private var clientCertificate: URL? = nil
    private var clientKey: URL? = nil
    private var inboxPrefix: String = "_INBOX."
    private var subscriptionCapacity: UInt64 = NatsSubscription.defaultSubCapacity

    public init() {}

    /// Sets the prefix for inbox subjects used for request/reply.
    /// Defaults to "_INBOX."
    public func inboxPrefix(_ prefix: String) -> NatsClientOptions {
        if prefix.isEmpty {
            self.inboxPrefix = "_INBOX."
            return self
        }
        if prefix.last != "." {
            self.inboxPrefix = prefix + "."
            return self
        }
        self.inboxPrefix = prefix
        return self
    }

    /// A list of server urls that a client can connect to.
    public func urls(_ urls: [URL]) -> NatsClientOptions {
        self.urls = urls.map { self.applyDefaultPort(to: $0) }
        return self

    }

    /// A single url that the client can connect to.
    public func url(_ url: URL) -> NatsClientOptions {
        self.urls = [self.applyDefaultPort(to: url)]
        return self
    }

    /// The interval with which the client will send pings to NATS server.
    /// Defaults to 60s.
    public func pingInterval(_ pingInterval: TimeInterval) -> NatsClientOptions {
        self.pingInterval = pingInterval
        return self
    }

    /// Wait time between reconnect attempts.
    /// Defaults to 2s.
    public func reconnectWait(_ reconnectWait: TimeInterval) -> NatsClientOptions {
        self.reconnectWait = reconnectWait
        return self
    }

    /// Maximum number of reconnect attempts after each disconnect.
    /// Defaults to unlimited.
    ///
    /// To explicitly request unlimited reconnects, use
    /// ``NatsClientOptions/unlimitedReconnects()`` rather than relying on this
    /// method being left unset.
    public func maxReconnects(_ maxReconnects: Int) -> NatsClientOptions {
        self.maxReconnects = maxReconnects
        return self
    }

    /// Requests unlimited reconnect attempts after each disconnect.
    ///
    /// This is the default behavior when ``NatsClientOptions/maxReconnects(_:)`` is
    /// never called, but stating it explicitly makes the intent clear and lets a
    /// caller override a previously configured finite limit (last call wins). Useful
    /// for long-lived clients that must reconnect forever, e.g. services behind a
    /// single load balancer that should never give up.
    public func unlimitedReconnects() -> NatsClientOptions {
        self.maxReconnects = nil
        return self
    }

    /// Username and password used to connect to the server.
    public func usernameAndPassword(_ username: String, _ password: String) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth(user: username, password: password)
        } else {
            self.auth?.user = username
            self.auth?.password = password
        }
        return self
    }

    /// Token used for token auth to NATS server.
    public func token(_ token: String) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth(token: token)
        } else {
            self.auth?.token = token
        }
        return self
    }

    /// The location of a credentials file containing user JWT and Nkey seed.
    public func credentialsFile(_ credentials: URL) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth.fromCredentials(credentials)
        } else {
            self.auth?.credentialsPath = credentials
        }
        return self
    }

    /// The contents of a credentials file (user JWT and Nkey seed) provided inline.
    /// Use this instead of ``NatsClientOptions/credentialsFile(_:)`` when the credentials
    /// are held in memory rather than on disk.
    /// If both are set, the inline contents take precedence.
    public func credentials(_ credentials: String) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth.fromCredentialsContents(credentials)
        } else {
            self.auth?.credentialsContents = credentials
        }
        return self
    }

    /// The location of a public nkey file.
    /// This and ``NatsClientOptions/nkey(_:)`` are mutually exclusive.
    public func nkeyFile(_ nkey: URL) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth.fromNkey(nkey)
        } else {
            self.auth?.nkeyPath = nkey
        }
        return self
    }

    /// Public nkey.
    /// This and ``NatsClientOptions/nkeyFile(_:)`` are mutually exclusive.
    public func nkey(_ nkey: String) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth.fromNkey(nkey)
        } else {
            self.auth?.nkey = nkey
        }
        return self
    }

    /// Indicates whether the client requires an SSL connection.
    public func requireTls() -> NatsClientOptions {
        self.withTls = true
        return self
    }

    /// Indicates whether the client will attempt to perform a TLS handshake first, that is
    /// before receiving the INFO protocol. This requires the server to also be
    /// configured with such option, otherwise the connection will fail.
    public func withTlsFirst() -> NatsClientOptions {
        self.tlsFirst = true
        return self
    }

    /// The location of a root CAs file.
    public func rootCertificates(_ rootCertificate: URL) -> NatsClientOptions {
        self.rootCertificate = rootCertificate
        return self
    }

    /// The location of a client cert file.
    public func clientCertificate(_ clientCertificate: URL, _ clientKey: URL) -> NatsClientOptions {
        self.clientCertificate = clientCertificate
        self.clientKey = clientKey
        return self
    }

    /// Indicates whether the client will retain the order of URLs to connect to provided in ``NatsClientOptions/urls(_:)``
    /// If not set, the client will randomize the server pool.
    public func retainServersOrder() -> NatsClientOptions {
        self.noRandomize = true
        return self
    }

    /// Instructs the client to ignore server addresses gossiped via the `connect_urls`
    /// field of server INFO messages. Only the URLs explicitly provided in
    /// ``NatsClientOptions/urls(_:)`` or ``NatsClientOptions/url(_:)`` will be used for
    /// connecting and reconnecting.
    public func ignoreDiscoveredServers() -> NatsClientOptions {
        self.ignoreDiscovered = true
        return self
    }

    /// By default, ``NatsClient/connect()`` will return an error if
    /// the connection to the server cannot be established.
    ///
    /// Setting `retryOnfailedConnect()` makes the client
    /// establish the connection in the background even if the initial connect fails.
    public func retryOnfailedConnect() -> NatsClientOptions {
        self.initialReconnect = true
        return self
    }

    /// The maximum number of messages buffered per subscription before the client
    /// starts dropping inbound messages (a "slow consumer").
    ///
    /// When a subscription's buffer is full, further inbound messages are dropped and a
    /// ``NatsError/SubscriptionError/slowConsumer(subject:)`` is surfaced via the `.error`
    /// event (once per slow episode). The subscription keeps working and resumes buffering
    /// once the consumer catches up.
    ///
    /// Defaults to `512 * 1024`. Values below `1` are clamped to `1`. This is a connection-wide
    /// default applied to every subscription the client creates, including the internal JetStream
    /// deliver-inbox and request-reply subscriptions — so avoid very small values on a client that
    /// also drives JetStream.
    public func subscriptionCapacity(_ messages: Int) -> NatsClientOptions {
        self.subscriptionCapacity = UInt64(max(1, messages))
        return self
    }

    public func build() -> NatsClient {
        let connectionHandler = ConnectionHandler(
            urls: urls,
            reconnectWait: reconnectWait,
            maxReconnects: maxReconnects,
            retainServersOrder: noRandomize,
            ignoreDiscoveredServers: ignoreDiscovered,
            pingInterval: pingInterval,
            auth: auth,
            requireTls: withTls,
            tlsFirst: tlsFirst,
            clientCertificate: clientCertificate,
            clientKey: clientKey,
            rootCertificate: rootCertificate,
            retryOnFailedConnect: initialReconnect,
            subscriptionCapacity: subscriptionCapacity
        )
        return NatsClient(inboxPrefix: inboxPrefix, connectionHandler: connectionHandler)
    }

    private func applyDefaultPort(to url: URL) -> URL {
        guard url.port == nil, let scheme = url.scheme else {
            return url
        }

        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)

        switch scheme.lowercased() {
        case "nats", "tls":
            components?.port = 4222
        case "ws":
            components?.port = 80
        case "wss":
            components?.port = 443
        default:
            break
        }

        return components?.url ?? url
    }
}
