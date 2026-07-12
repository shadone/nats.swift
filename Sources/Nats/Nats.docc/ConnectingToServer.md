# Connecting to a Server

Configure, open, and observe a NATS connection.

## Overview

A ``NatsClient`` is built with the ``NatsClientOptions`` builder and then opened
with ``NatsClient/connect()``. The client keeps the connection alive across
network failures by reconnecting automatically, and reports lifecycle changes
through connection events.

### Building a client

``NatsClientOptions`` is a fluent builder. Each method returns the same options
instance, and ``NatsClientOptions/build()`` produces the ``NatsClient``.

```swift
import Nats

let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

try await client.connect()
```

For a cluster, pass several seed URLs with ``NatsClientOptions/urls(_:)``. By
default the client also learns about other servers the cluster advertises; call
``NatsClientOptions/ignoreDiscoveredServers()`` to connect only to the seeds you
configured (useful behind a single load balancer), and
``NatsClientOptions/retainServersOrder()`` to try them in order instead of
shuffled.

```swift
let client = NatsClientOptions()
    .urls([
        URL(string: "nats://a.example:4222")!,
        URL(string: "nats://b.example:4222")!,
    ])
    .ignoreDiscoveredServers()
    .build()
```

### Authenticating

The builder exposes the common credential schemes:

```swift
// Username / password
NatsClientOptions().usernameAndPassword("user", "pass")

// Token
NatsClientOptions().token("s3cr3t")

// NATS credentials from a .creds file on disk...
NatsClientOptions().credentialsFile(URL(fileURLWithPath: "app.creds"))

// ...or from an in-memory string (never touches disk)
NatsClientOptions().credentials(credsString)
```

### Waiting until connected

``NatsClient/connect()`` returns once the initial connection is established. If you
build a client that reconnects in the background and want to block until the link
is up again, use ``NatsClient/waitForConnected()`` (waits forever) or
``NatsClient/waitForConnected(timeout:)`` (throws on timeout).

```swift
try await client.connect()
await client.waitForConnected()
```

### Reconnection

By default the client retries a bounded number of times. For long-lived services
that must survive extended outages, use ``NatsClientOptions/unlimitedReconnects()``
and tune the backoff with ``NatsClientOptions/reconnectWait(_:)``.

```swift
let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .unlimitedReconnects()
    .reconnectWait(2.0)
    .build()
```

### Observing events

Register a handler with `NatsClient.on(_:_:)` to react to connection lifecycle
changes. The handler receives a ``NatsEvent``; filter on the ``NatsEventKind``
values you care about. The call returns a listener ID you can later pass to
``NatsClient/off(_:)``.

```swift
client.on([.connected, .disconnected, .closed]) { event in
    switch event {
    case .connected:
        print("connected")
    case .disconnected:
        print("disconnected — reconnecting")
    case .closed:
        print("connection closed")
    default:
        break
    }
}
```

### Closing

Call ``NatsClient/close()`` to drain and shut the connection down. You can also
``NatsClient/suspend()`` and ``NatsClient/resume()`` a connection, or force a
``NatsClient/reconnect()``.
