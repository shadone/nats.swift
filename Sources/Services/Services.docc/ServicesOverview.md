# Services Overview

Define a microservice, add endpoints and groups, and respond to requests.

## Overview

A ``Service`` turns a NATS connection into a discoverable request/reply service.
It handles the wiring — subscriptions, queue-group load balancing, and the
`$SRV` monitoring protocol — so your code is just endpoint handlers.

### Creating a service

Build a ``ServiceConfig`` (a name and a SemVer version are required) and pass it
to the `NatsClient.addService(_:)` method. The returned ``Service`` is already
running.

```swift
import Nats
import Services

let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()
try await client.connect()

let service = try await client.addService(
    ServiceConfig(name: "calc", version: "1.0.0", description: "Arithmetic"))
```

### Adding endpoints

``Service/addEndpoint(_:subject:queueGroup:metadata:handler:)`` registers a
handler. The handler receives a ``ServiceRequest``; reply with
``ServiceRequest/respond(_:headers:)`` or ``ServiceRequest/respondJSON(_:headers:)``.
By default the endpoint listens on a subject equal to its name.

```swift
try await service.addEndpoint("add") { request in
    let numbers = try JSONDecoder().decode([Int].self, from: request.data)
    try await request.respondJSON(numbers.reduce(0, +))
}
```

### Reporting errors

To return a service error (recorded in the endpoint's stats and carried in the
`Nats-Service-Error` headers), use ``ServiceRequest/error(code:description:data:headers:)``.

```swift
try await service.addEndpoint("divide") { request in
    guard let divisor = /* ... */ Int?.none, divisor != 0 else {
        try await request.error(code: "400", description: "division by zero")
        return
    }
    // ...
}
```

### Grouping endpoints

``Service/addGroup(_:queueGroup:)`` returns a ``ServiceGroup`` whose endpoints
share a subject prefix. Groups can be nested.

```swift
let v1 = service.addGroup("v1")
try await v1.addEndpoint("status") { request in
    try await request.respond(Data("ok".utf8))   // subject: v1.status
}
```

### Discovery and statistics

Every service automatically answers the `$SRV` monitoring requests. From your own
code you can read the same data directly:

- ``Service/info()`` returns a ``ServiceInfo`` (name, version, endpoints).
- ``Service/stats()`` returns a ``ServiceStats`` with per-``EndpointStats``
  request counts, errors, and processing time.
- ``Service/reset()`` clears the collected statistics.

### Stopping

Call ``Service/stop()`` to drain the endpoint and monitoring subscriptions. The
service also stops automatically if the underlying connection closes; check
``Service/isStopped`` to observe this.
