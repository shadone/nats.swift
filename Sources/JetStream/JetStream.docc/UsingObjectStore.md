# Object Store

Store and retrieve large binary objects, chunked and digest-verified, over
JetStream.

## Overview

An ``ObjectStore`` bucket holds arbitrarily large objects. Each object is split
into chunks across a backing JetStream stream and reassembled on read; a SHA-256
digest is verified on every `get`. Small objects can be handled as a single
`Data` value; large objects can be streamed so they never fully reside in memory.
Obtain a bucket from a ``JetStreamContext``.

### Creating or opening a bucket

```swift
import Nats
import JetStream

let js = JetStreamContext(client: client)

let store = try await js.createObjectStore(
    cfg: ObjectStoreConfig(bucket: "assets"))
```

Open an existing bucket with ``JetStreamContext/objectStore(bucket:)``.

### Put and get (in memory)

``ObjectStore/put(_:data:)-(String,_)`` stores raw `Data` under a name and returns an
``ObjectInfo``. ``ObjectStore/getBytes(_:showDeleted:)`` reads it back, and
``ObjectStore/get(_:showDeleted:)`` returns an ``ObjectResult`` carrying both the
bytes and the object's ``ObjectInfo``.

```swift
let info = try await store.put("logo.png", data: imageData)
print("stored \(info.size) bytes as \(info.name)")

let bytes = try await store.getBytes("logo.png")

let result = try await store.get("logo.png")
print(result.info.digest ?? "", result.data.count)
```

Fetch metadata alone with ``ObjectStore/getInfo(_:showDeleted:)``.

### Streaming large objects

For objects too large to hold in memory, stream them in and out.
``ObjectStore/put(_:source:)-(String,_)`` consumes any `AsyncSequence` of `Data` chunks, and
``ObjectStore/getStream(_:showDeleted:)`` returns an ``ObjectStreamReader`` — an
`AsyncSequence` of `Data` that verifies the running digest as it reads.

```swift
// Streaming write from an async source of Data chunks.
let info = try await store.put("backup.tar", source: chunkSequence)

// Streaming read, one chunk at a time.
let reader = try await store.getStream("backup.tar")
for try await chunk in reader {
    try fileHandle.write(contentsOf: chunk)
}
```

For finer control over a streamed write — chunk size, headers, links — pass an
``ObjectMeta`` to ``ObjectStore/put(_:source:)-(ObjectMeta,_)``:

```swift
var meta = ObjectMeta(name: "backup.tar")
meta.options = ObjectMetaOptions(maxChunkSize: 256 * 1024)
let info = try await store.put(meta, source: chunkSequence)
```

### Listing, deleting, and status

- ``ObjectStore/list(showDeleted:)`` returns the ``ObjectInfo`` for each object.
- ``ObjectStore/delete(_:)`` removes an object and purges its chunks.
- ``ObjectStore/status()`` returns an ``ObjectStoreStatus`` for the bucket.

### Watching for changes

``ObjectStore/watch(opts:)`` returns an ``ObjectStoreWatcher`` — an
`AsyncSequence` of `ObjectInfo?`. As with the Key/Value watcher, a `nil` element
marks the end of the initial set; subsequent elements are live changes. Tune it
with ``ObjectStoreWatchOptions``.

```swift
let watcher = try await store.watch()
for try await update in watcher {
    guard let info = update else { continue }  // initial set replayed
    print(info.deleted ? "deleted \(info.name)" : "updated \(info.name)")
}
```

Call ``ObjectStoreWatcher/stop()`` to end the watch.
