# Asynchronous Publishing

Publish to a stream at high throughput with a bounded in-flight window.

## Overview

``JetStreamContext/publish(_:message:headers:msgTTL:)`` opens a subscription and
returns an ``AckFuture`` per message — simple, but it serializes throughput if you
await each ack before sending the next message.

``JetStreamContext/publishAsync(_:message:headers:msgTTL:)`` is built for volume.
All async publishes on a context share a single ack subscription and a bounded
in-flight window (default 4000). You fire many calls back to back, collecting the
returned ``PubAckFuture`` values, then await them (or flush) later.

### Firing many publishes

```swift
import Nats
import JetStream

let js = JetStreamContext(client: client)

var futures: [PubAckFuture] = []
for i in 0..<10_000 {
    let future = try await js.publishAsync(
        "orders.new", message: Data("order-\(i)".utf8))
    futures.append(future)
}
```

### Backpressure

When the in-flight window is full, `publishAsync` suspends until an outstanding
ack resolves — so the loop above is self-throttling and will not exhaust memory,
no matter how many messages you push. Inspect the current depth with
``JetStreamContext/publishAsyncPending()``.

### Awaiting the acks

Acks are **not** guaranteed to resolve in publish order. Await each
``PubAckFuture/wait()`` for its own ``Ack`` (and to observe any per-message
failure such as a wrong-last-sequence error):

```swift
for future in futures {
    let ack = try await future.wait()
    // ack.sequence is this message's stream sequence
    _ = ack
}
```

### Flushing everything at once

When you only need to know that *all* in-flight publishes have completed, call
``JetStreamContext/publishAsyncComplete(timeout:)`` instead of awaiting each
future. It returns once every pending ack has resolved, or throws on timeout.

```swift
for i in 0..<10_000 {
    _ = try await js.publishAsync("orders.new", message: Data("order-\(i)".utf8))
}
try await js.publishAsyncComplete(timeout: 30)
```
