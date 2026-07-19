![NATS Swift Client](./Resources/Logo@256.png)

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![ci](https://github.com/shadone/nats.swift/actions/workflows/ci.yml/badge.svg)](https://github.com/shadone/nats.swift/actions/workflows/ci.yml)

# NATS Swift Client (fork)

**This is a fork of [`nats-io/nats.swift`](https://github.com/nats-io/nats.swift) with major additions.**
Upstream is Core NATS only. This fork closes the gaps that kept the Swift client
off par with `nats.go` and `async-nats` (Rust): the full JetStream surface,
the Service API, Swift 6, Linux, and a fix for a silent JetStream publish bug that
affects every upstream user.

**What's new on top of upstream:**

- **Full JetStream** — KeyValue and ObjectStore, ordered/push/durable consumers,
  per-message and per-key TTL, async batched publish. Upstream has none of it.
- **Fixed a silent CAS-publish bug** — upstream mis-decodes a failed `PubAck`, so
  every `expected-last-seq` / msg-id-dedup publish failure is swallowed
  library-wide. Fixed here.
- **Service (micro) API** — the `Services` module: endpoints/groups, `$SRV`
  discovery, per-endpoint stats.
- **Swift 6 language mode** — the whole package builds under `swiftLanguageModes: [.v6]`
  with strict concurrency enforced as errors.
- **Linux** — builds and passes the full suite on Linux; upstream CI is macOS/iOS only.
- **Hardened core** — slow-consumer overflow surfaces as an `.error` event (was a
  silent drop) over an amortized-O(1) buffer (was O(n²) drain), plus three
  adversarial correctness sweeps over the transport, reset engine, and parser.

**Status:** 334 tests, 0 failures, green on macOS and Linux (Swift 6.0 build floor,
6.1/6.2 tested; iOS build in CI). Reliability is treated as first-class — a release-mode
consumer-stress gate on every push, plus a nightly soak, 3-node cluster failover, and
fault-injection suite; see [TESTING.md](./TESTING.md). Full fork-vs-upstream matrix and
the commit-by-commit story: [FIRSTCLASS.md](./FIRSTCLASS.md). Not affiliated with the
NATS.io maintainers and not submitted upstream — pin it yourself if you want it.

---

Welcome to the [Swift](https://www.swift.org) Client for [NATS](https://nats.io),
your gateway to asynchronous messaging in Swift applications. This client library
is designed to provide Swift developers with a seamless interface to NATS
messaging, enabling swift and efficient communication across distributed systems.

## Features

The base client supports **Core NATS** with auth, TLS, lame duck mode and more.
On top of that, this fork adds (see [FIRSTCLASS.md](./FIRSTCLASS.md) for the full
matrix):

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
  test suite (332 tests) passes on macOS and Linux, with an iOS build in CI.

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

For anything specific to this fork — JetStream, the Service API, a bug in the
added surface — open an issue on this repo. For general NATS questions, the
[#swift](https://natsio.slack.com/channels/swift) channel on nats.io Slack is the
place; note the maintainers there don't own this fork.

## Installation via Swift Package Manager

**Requirements:** a Swift 6.0+ toolchain, and macOS 13+, iOS 13+, or Linux. On
Linux, first install the system libsodium that the `nkeys.swift` dependency links
(`apt-get install -y libsodium-dev`).

This fork has no tagged release — pin it by branch. Include it as a dependency in
your `Package.swift` and add the modules you need to your target (`Nats` for Core
NATS, `JetStream` for KV/ObjectStore/consumers, `Services` for the micro API):

```swift
// swift-tools-version:6.0

import PackageDescription

let package = Package(
    name: "YourApp",
    products: [
        .executable(name: "YourApp", targets: ["YourApp"]),
    ],
    dependencies: [
        .package(url: "https://github.com/shadone/nats.swift.git", branch: "main")
    ],
    targets: [
        .target(name: "YourApp", dependencies: [
            .product(name: "Nats", package: "nats.swift"),
            .product(name: "JetStream", package: "nats.swift"),
        ]),
    ]
)

```

### Xcode Package Dependencies

Open the project inspector in Xcode and select your project. It is important to select the **project** and not a target!
Click on the third tab `Package Dependencies` and add the git url `https://github.com/shadone/nats.swift.git` by selecting the little `+`-sign at the end of the package list; set the dependency rule to **Branch → `main`**.

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

Forked from [`nats-io/nats.swift`](https://github.com/nats-io/nats.swift) — all the
Core NATS groundwork is theirs. That client in turn builds on the excellent work in
https://github.com/aus-der-Technik/SwiftyNats.
