# ``JetStream``

Persistence, streaming, and higher-level data structures for NATS: streams,
consumers, Key/Value, and Object Store.

## Overview

JetStream is the persistence layer of NATS. This module wraps the JetStream API
in a Swift-concurrency surface built on the `Nats` client. A
``JetStreamContext`` is the entry point: it publishes to streams (synchronously
or with a bounded async window), manages ``Stream`` and ``Consumer`` resources,
and opens ``KeyValue`` and ``ObjectStore`` handles.

```swift
import Nats
import JetStream

let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()
try await client.connect()

let js = JetStreamContext(client: client)

// Create a stream and publish to it.
let stream = try await js.createStream(
    cfg: StreamConfig(name: "ORDERS", subjects: ["orders.>"]))
let ack = try await js.publish("orders.new", message: Data("order-1".utf8))
_ = try await ack.wait()
```

Messages are consumed through the unified consume API — ``MessageConsuming/consume(_:onError:)``,
``MessageConsuming/messages()`` and ``MessageConsuming/next(timeout:)`` — which
is shared by pull ``Consumer``s, ``PushConsumer``s, and the ``OrderedConsumer``.
Delivered messages are ``JetStreamMessage``s that you acknowledge with
``JetStreamMessage/ack(ackType:)``.

## Topics

### Essentials

- <doc:GettingStarted>
- <doc:UsingKeyValue>
- <doc:UsingObjectStore>
- <doc:AsyncPublish>

### Context

- ``JetStreamContext``

### Publishing

- ``JetStreamContext/publish(_:message:headers:msgTTL:)``
- ``AckFuture``
- ``Ack``
- ``JetStreamContext/publishAsync(_:message:headers:msgTTL:)``
- ``JetStreamContext/publishAsyncComplete(timeout:)``
- ``JetStreamContext/publishAsyncPending()``
- ``PubAckFuture``

### Streams

- ``Stream``
- ``StreamConfig``
- ``StreamInfo``
- ``StreamState``
- ``StreamMessage``
- ``RetentionPolicy``
- ``DiscardPolicy``
- ``StorageType``

### Consumers

- ``Consumer``
- ``ConsumerConfig``
- ``ConsumerInfo``
- ``PushConsumer``
- ``OrderedConsumer``
- ``OrderedConsumerConfig``
- ``DeliverPolicy``
- ``AckPolicy``
- ``ReplayPolicy``

### Consuming Messages

- ``MessageConsuming``
- ``MessagesContext``
- ``ConsumeContext``
- ``MessageHandler``
- ``ConsumeErrorHandler``
- ``FetchResult``
- ``JetStreamMessage``
- ``AckKind``
- ``MessageMetadata``

### Key/Value

- ``KeyValue``
- ``KeyValueConfig``
- ``KeyValueStatus``
- ``KeyValueEntry``
- ``KeyValueOperation``
- ``KeyValueWatcher``
- ``KeyValueWatchOptions``

### Object Store

- ``ObjectStore``
- ``ObjectStoreConfig``
- ``ObjectStoreStatus``
- ``ObjectInfo``
- ``ObjectMeta``
- ``ObjectResult``
- ``ObjectStreamReader``
- ``ObjectStoreWatcher``
- ``ObjectStoreWatchOptions``
- ``ObjectLink``

### Time-To-Live

- ``NanoTimeInterval``

### Errors

- ``JetStreamError``
- ``JetStreamErrorProtocol``
