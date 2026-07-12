# ``Services``

Build NATS microservices — request/reply endpoints with built-in discovery and
statistics.

## Overview

The `Services` module implements the NATS service (micro) API. A ``Service`` is a
named, versioned unit that registers request/reply endpoints on subjects and
automatically answers the `$SRV` PING, INFO, and STATS monitoring requests used
for discovery and observability. Endpoints can be grouped under subject prefixes,
and per-endpoint statistics are tracked for you.

Create a service from a connected `NatsClient` with its `addService(_:)` method
and a ``ServiceConfig``.

```swift
import Nats
import Services

let service = try await client.addService(
    ServiceConfig(name: "calc", version: "1.0.0"))

try await service.addEndpoint("add") { request in
    let sum = // ... decode request.data, compute ...
    try await request.respond(Data("\(sum)".utf8))
}
```

See <doc:ServicesOverview> for a fuller walkthrough.

## Topics

### Essentials

- <doc:ServicesOverview>

### Defining a Service

- ``Service``
- ``ServiceConfig``
- ``ServiceGroup``

### Handling Requests

- ``ServiceRequest``
- ``ServiceHandler``
- ``ServiceErrorHandler``

### Monitoring & Discovery

- ``ServiceInfo``
- ``ServiceStats``
- ``ServiceIdentity``
- ``ServicePing``
- ``EndpointInfo``
- ``EndpointStats``

### Errors

- ``ServiceError``
