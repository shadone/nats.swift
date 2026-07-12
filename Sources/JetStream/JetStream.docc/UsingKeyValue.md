# Key/Value Store

Use a JetStream-backed bucket as a durable, watchable key/value store.

## Overview

A ``KeyValue`` bucket stores versioned entries keyed by string. It is backed by a
JetStream stream, so values are persisted, history is retained (configurable),
and changes can be watched in real time. Obtain a bucket from a
``JetStreamContext``.

### Creating or opening a bucket

Create a bucket with ``JetStreamContext/createKeyValue(cfg:)`` and a
``KeyValueConfig``, or open an existing one with
``JetStreamContext/keyValue(bucket:)``.

```swift
import Nats
import JetStream

let js = JetStreamContext(client: client)

var cfg = KeyValueConfig(bucket: "config")
cfg.history = 5          // keep up to 5 revisions per key
let kv = try await js.createKeyValue(cfg: cfg)
```

### Put and get

``KeyValue/put(_:_:)`` writes a value and returns its new revision.
``KeyValue/get(_:)`` returns the latest ``KeyValueEntry`` (or `nil` if absent).

```swift
let revision = try await kv.put("greeting", Data("hello".utf8))

if let entry = try await kv.get("greeting") {
    print(String(data: entry.value, encoding: .utf8) ?? "")
    print("revision: \(entry.revision)")
}
```

### Optimistic concurrency

``KeyValue/create(_:_:)`` fails if the key already exists, and
``KeyValue/update(_:_:revision:)`` fails unless the current revision matches —
giving you compare-and-set semantics.

```swift
let rev = try await kv.create("counter", Data("0".utf8))
// Only succeeds if "counter" is still at revision `rev`.
_ = try await kv.update("counter", Data("1".utf8), revision: rev)
```

### Delete and purge

``KeyValue/delete(_:lastRevision:)`` writes a delete marker (history is kept),
while ``KeyValue/purge(_:lastRevision:)`` removes the key and all of its history.

```swift
try await kv.delete("greeting")
try await kv.purge("counter")
```

### Watching for changes

``KeyValue/watchAll(opts:)`` returns a ``KeyValueWatcher`` — an `AsyncSequence`
of `KeyValueEntry?`. After the current values are replayed, the watcher emits a
single `nil` to signal "end of initial values", then streams live updates. Use
``KeyValue/watch(_:opts:)`` to watch a key or subject pattern. Tune behaviour
with ``KeyValueWatchOptions``.

```swift
let watcher = try await kv.watchAll()
for try await update in watcher {
    guard let entry = update else {
        // Initial values replayed; now watching live.
        continue
    }
    switch entry.operation {
    case .put:
        print("\(entry.key) = \(String(data: entry.value, encoding: .utf8) ?? "")")
    case .delete, .purge:
        print("\(entry.key) removed")
    }
}
```

Call ``KeyValueWatcher/stop()`` to end the watch early.

### Keys, history, and status

- ``KeyValue/keys()`` lists all live keys.
- ``KeyValue/history(_:)`` returns every retained ``KeyValueEntry`` for a key.
- ``KeyValue/status()`` returns a ``KeyValueStatus`` snapshot of the bucket.

### Per-key TTL

When the bucket allows it, ``KeyValue/create(_:_:ttl:)`` sets a time-to-live on a
single key using a ``NanoTimeInterval`` (NATS 2.11+).

```swift
_ = try await kv.create("session", Data("token".utf8), ttl: NanoTimeInterval(60))
```
