![NATS Swift Client](./Resources/Logo@256.png)

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fnats-io%2Fnats.swift%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/nats-io/nats.swift)
[![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fnats-io%2Fnats.swift%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/nats-io/nats.swift)




# NATS Swift Client

Welcome to the [Swift](https://www.swift.org) Client for [NATS](https://nats.io),
your gateway to asynchronous messaging in Swift applications. This client library
is designed to provide Swift developers with a seamless interface to NATS
messaging, enabling swift and efficient communication across distributed systems.

## Features

Currently, the client supports **Core NATS** with auth, TLS, lame duck mode and more.

**On the `firstclass-kv` branch** (an experiment bringing the client toward
`nats.go`/`async-nats` parity — see [FIRSTCLASS.md](./FIRSTCLASS.md)):

- **JetStream KeyValue** — buckets, get/put/create/update/delete/purge with
  optimistic concurrency, status, hang-safe `watch`/`watchAll`/`keys`/
  `history`/`purgeDeletes`, and per-key TTL (NATS 2.11+).
- **Ordered push consumer** — server-driven delivery with flow control, idle
  heartbeats, and automatic reset/recreate on gaps or disconnects (no message
  loss or duplication across a reset).
- **Push consumers** — ephemeral, durable (persist across rebind), and
  queue/deliver-group consumers that load-balance across instances.
- **Modern consumer API** — `consume`/`messages`/`next` across pull, push and
  ordered consumers.
- **JetStream ObjectStore** — chunked put/get with SHA-256 digest verification,
  streaming put/get for large objects, getInfo/delete/updateMeta/links/seal/
  status, and `watch`/`list`.
- **Per-message TTL** — the `Nats-TTL` header (NATS 2.11+), wire-identical to
  `nats.go`'s `time.Duration` formatting.
- **Service (micro) API** — an actor-based service framework with endpoints,
  auto request/reply, `$SRV` PING/INFO/STATS discovery, and per-endpoint stats
  (the `Services` module).
- **Connection ergonomics** — inline credentials (no temp file),
  `ignoreDiscoveredServers()`, `waitForConnected()`, `state`/`isConnected`,
  `unlimitedReconnects()`.

- **Cross-platform** — builds and runs on **macOS, iOS, and Linux**; the full
  test suite (308 tests) passes on macOS and Linux, with an iOS build in CI.

The whole package builds under **Swift 6 language mode**
(`swiftLanguageModes: [.v6]`) with strict concurrency enforced. Benchmarks and a
performance baseline are in [PERF.md](./PERF.md) (harness: `Sources/PerfBench`).

### JetStream KeyValue quick look

```swift
import Nats
import JetStream

let nats = NatsClientOptions().url(URL(string: "nats://localhost:4222")!).build()
try await nats.connect()

let js = JetStreamContext(client: nats)
let kv = try await js.createKeyValue(cfg: KeyValueConfig(bucket: "config"))

let rev = try await kv.put("greeting", "hello".data(using: .utf8)!)
let entry = try await kv.get("greeting")           // entry?.value == "hello"

// Watch: initial values, then a nil end-of-initial marker, then live updates.
let watcher = try await kv.watchAll()
for try await update in watcher {
    guard let entry = update else { continue }      // nil == end of initial values
    print("\(entry.key) = \(String(data: entry.value, encoding: .utf8) ?? "") @\(entry.revision)")
}
```

## Support

Join the [#swift](https://natsio.slack.com/channels/swift) channel on nats.io Slack.
We'll do our best to help quickly. You can also just drop by and say hello. We're looking forward to developing the community.

## Installation via Swift Package Manager

**Requirements:** a Swift 6.0+ toolchain, and macOS 13+, iOS 13+, or Linux. On
Linux, first install the system libsodium that the `nkeys.swift` dependency links
(`apt-get install -y libsodium-dev`).

Include this package as a dependency in your project's `Package.swift` file and add the package name to your target as shown in the following example:

```swift
// swift-tools-version:6.0

import PackageDescription

let package = Package(
    name: "YourApp",
    products: [
        .executable(name: "YourApp", targets: ["YourApp"]),
    ],
    dependencies: [
        .package(name: "Nats", url: "https://github.com/nats-io/nats.swift.git", from: "0.1")
    ],
    targets: [
        .target(name: "YourApp", dependencies: ["Nats"]),
    ]
)

```

### Xcode Package Dependencies

Open the project inspector in Xcode and select your project. It is important to select the **project** and not a target!
Click on the third tab `Package Dependencies` and add the git url `https://github.com/nats-io/nats.swift.git` by selecting the little `+`-sign at the end of the package list.

## Basic Usage

Here is a quick start example to see everything at a glance:

```swift
import Nats

// create the client
let nats = NatsClientOptions().url(URL(string: "nats://localhost:4222")!).build()

// connect to the server
try await nats.connect()

// subscribe to a subject
let subscription = try await nats.subscribe(subject: "events.>")

// publish a message
try await nats.publish("my event".data(using: .utf8)!, subject: "events.example")

// receive published messages
for try await msg in subscription {
    print("Received: \(String(data: msg.payload!, encoding: .utf8)!)")
}
 ```

### Connecting to a NATS Server

The first step is establishing a connection to a NATS server.
This example demonstrates how to connect to a NATS server using the default settings, which assume the server is
running locally on the default port (4222). You can also customize your connection by specifying additional options:

```swift
let nats = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

try await nats.connect()
```

### Publishing Messages

Once you've established a connection to a NATS server, the next step is to publish messages.
Publishing messages to a subject allows any subscribed clients to receive these messages
asynchronously. This example shows how to publish a simple text message to a specific subject.

```swift
let data = "message text".data(using: .utf8)!
try await nats.publish(data, subject: "foo.msg")
```

In more complex scenarios, you might want to include additional metadata with your messages in
the form of headers. Headers allow you to pass key-value pairs along with your message, providing
extra context or instructions for the subscriber. This example shows how to publish a
message with headers:

```swift
let data = "message text".data(using: .utf8)!

var headers = NatsHeaderMap()
headers.append(try! NatsHeaderName("X-Example"), NatsHeaderValue("example value"))

try await nats.publish(data, subject: "foo.msg.1", headers: headers)
```

### Subscribing to Subjects

After establishing a connection and publishing messages to a NATS server, the next crucial step is
subscribing to subjects. Subscriptions enable your client to listen for messages published to
specific subjects, facilitating asynchronous communication patterns. This example
will guide you through creating a subscription to a subject, allowing your application to process
incoming messages as they are received.

```swift
let subscription = try await nats.subscribe(subject: "foo.>")

for try await msg in subscription {

    if msg.subject == "foo.done" {
        break
    }

    if let payload = msg.payload {
        print("received \(msg.subject): \(String(data: payload, encoding: .utf8) ?? "")")
    }

    if let headers = msg.headers {
        if let headerValue = headers.get(try! NatsHeaderName("X-Example")) {
            print("  header: X-Example: \(headerValue.description)")
        }
    }
}
```

Notice that the subject `foo.>` uses a special wildcard syntax, allowing for subscription
to a hierarchy of subjects. For more detailed information, please refer to the [NATS documentation
on _Subject-Based Messaging_](https://docs.nats.io/nats-concepts/subjects).

### Setting Log Levels

The client logs through [swift-log](https://github.com/apple/swift-log) via a
public module-level `logger`. The default level is `.info`; set it to see more or
less verbose output (`.debug`, `.info`, `.error`, `.critical`):

```swift
import Nats

logger.logLevel = .debug
```

### Events

 You can also monitor when your app connects, disconnects, or encounters an error using events:

```swift
let nats = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

nats.on(.connected) { event in
    print("event: connected")
}
```

### AppDelegate or SceneDelegate Integration

In order to make sure the connection is managed properly in your
AppDelegate.swift or SceneDelegate.swift, integrate the NatsClient connection
management as follows:

```swift
func sceneDidBecomeActive(_ scene: UIScene) {
    Task {
        try await self.natsClient.resume()
    }
}

func sceneWillResignActive(_ scene: UIScene) {
    Task {
        try await self.natsClient.suspend()
    }
}
```

## Attribution

This library is based on excellent work in https://github.com/aus-der-Technik/SwiftyNats
