# Getting Started with JetStream

Connect, create a stream, publish, and consume with acknowledgements.

## Overview

This walkthrough takes you from a raw connection to a working
publish-and-consume loop. It assumes a NATS server with JetStream enabled
(`nats-server -js`).

### 1. Connect and make a context

A ``JetStreamContext`` is created from a connected `NatsClient`.

```swift
import Nats
import JetStream

let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()
try await client.connect()

let js = JetStreamContext(client: client)
```

If your JetStream domain is not the default, use
``JetStreamContext/init(client:domain:timeout:)`` instead.

### 2. Create a stream

A ``Stream`` captures messages published on its subjects. Configure it with
``StreamConfig`` and create it with ``JetStreamContext/createStream(cfg:)``.

```swift
let stream = try await js.createStream(
    cfg: StreamConfig(name: "ORDERS", subjects: ["orders.>"]))
```

### 3. Publish

``JetStreamContext/publish(_:message:headers:msgTTL:)`` returns an ``AckFuture``.
Await ``AckFuture/wait()`` to confirm the server stored the message.

```swift
let future = try await js.publish("orders.new", message: Data("order-1".utf8))
let ack: Ack = try await future.wait()
print("stored as sequence \(ack.sequence)")
```

To publish many messages at high throughput without awaiting each ack inline,
see <doc:AsyncPublish>.

### 4. Create a consumer

A pull ``Consumer`` reads from a stream on demand. Create one on the stream
handle with ``Stream/createConsumer(cfg:)`` (or
``JetStreamContext/createConsumer(stream:cfg:)``).

```swift
let consumer = try await stream.createConsumer(
    cfg: ConsumerConfig(durable: "worker", ackPolicy: .explicit))
```

### 5. Consume messages

All consumer types share the same delivery API. Use
``MessageConsuming/consume(_:onError:)`` for a callback-driven loop, or
``MessageConsuming/messages()`` for an `AsyncSequence`. Acknowledge each
``JetStreamMessage`` with ``JetStreamMessage/ack(ackType:)``.

```swift
let context = try consumer.consume { message in
    let body = String(data: message.payload ?? Data(), encoding: .utf8) ?? ""
    print("received: \(body)")
    Task { try? await message.ack() }
}

// ... later, stop consuming:
context.stop()
```

The iterator style reads the same:

```swift
for try await message in try consumer.messages() {
    print(message.subject)
    try await message.ack()
}
```

To pull a single message, use ``MessageConsuming/next(timeout:)``:

```swift
if let message = try await consumer.next(timeout: 5) {
    try await message.ack()
}
```

### Other consumer shapes

- ``PushConsumer`` — the server pushes messages to a delivery subject. Create
  with ``JetStreamContext/createPushConsumer(stream:cfg:)``.
- ``OrderedConsumer`` — a self-healing, single-stream consumer that guarantees
  in-order, gap-free delivery and recreates itself on resets. Create with
  ``JetStreamContext/orderedConsumer(stream:cfg:)`` and an
  ``OrderedConsumerConfig``.

Both expose the same ``MessageConsuming`` API as the pull ``Consumer``.
