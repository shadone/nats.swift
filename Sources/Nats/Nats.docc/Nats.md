# ``Nats``

A Swift client for NATS — asynchronous, subject-based messaging for distributed
systems.

## Overview

The `Nats` module is the Core NATS client for [NATS](https://nats.io). It manages a resilient connection to a
NATS server (or cluster), publishes and subscribes on subjects, performs
request/reply, and surfaces connection lifecycle events. A ``NatsClient`` is
configured with the ``NatsClientOptions`` builder and drives everything with
Swift concurrency: subscriptions are `AsyncSequence`s of ``NatsMessage`` and every
network operation is an `async` call.

```swift
import Nats

let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

try await client.connect()

let sub = try await client.subscribe(subject: "greetings")
try await client.publish("hello".data(using: .utf8)!, subject: "greetings")

for try await message in sub {
    print(String(data: message.payload ?? Data(), encoding: .utf8) ?? "")
    break
}

try await client.close()
```

JetStream (streams, consumers, Key/Value, Object Store) and the microservice
framework build on top of this module — see the `JetStream` and `Services`
libraries.

## Topics

### Essentials

- <doc:ConnectingToServer>

### Connecting

- ``NatsClient``
- ``NatsClientOptions``
- ``NatsState``

### Publishing & Subscribing

- ``NatsSubscription``
- ``NatsMessage``
- ``StatusCode``
- ``NatsHeaderMap``
- ``NatsHeaderName``
- ``NatsHeaderValue``

### Authentication

- ``Auth``

### Events & Reconnection

- ``NatsEvent``
- ``NatsEventKind``

### Errors

- ``NatsError``
- ``NatsErrorProtocol``
