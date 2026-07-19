# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NATS.swift is a Swift client library for the NATS messaging system, providing asynchronous messaging capabilities for Swift applications across macOS, iOS, and Linux. The project consists of four modules:
- **Nats**: Core NATS functionality including pub/sub, request/reply, auth, TLS
- **JetStream**: JetStream streaming — KeyValue and ObjectStore stores, pull/push/ordered consumers, per-message and per-key TTL, and async batched publish
- **Services**: the NATS micro Service API (endpoints/groups, `$SRV` discovery, per-endpoint stats)
- **NatsServer**: Test server utilities for integration testing

This is a fork of upstream `nats.swift` (Core NATS only), living on `main`, that adds the full JetStream + Services surface above and builds under the Swift 6 language mode. See `FIRSTCLASS.md` for the fork-vs-upstream matrix.

## Build and Test Commands

```bash
# Build the project
swift build

# Run all tests (requires nats-server on PATH)
swift test

# Run a specific test
swift test --filter <TestClass>/<testMethod>
# Example: swift test --filter NatsTests.CoreNatsTests/testConnect

# Run tests for a specific module
swift test --filter NatsTests
swift test --filter JetStreamTests

# Build release version
swift build -c release

# Clean build artifacts
swift package clean

# Install nats-server (required for integration tests)
curl --fail https://binaries.nats.dev/nats-io/nats-server/v2@latest | PREFIX='/usr/local/bin' sh

# Linux only: nkeys.swift -> swift-sodium links the system libsodium, and the nats
# installers need curl (the swift Docker image ships neither).
# apt-get update && apt-get install -y libsodium-dev curl

# Lint (strict)
swift-format lint --configuration .swift-format -r --strict Sources Tests

# Format code
swift-format format --in-place --configuration .swift-format -r Sources Tests
```

## Architecture

### Core Components

**NatsClient** (`Sources/Nats/NatsClient/NatsClient.swift`): Main client interface providing connection management, publish/subscribe operations, and event handling. Built on Swift NIO for asynchronous networking.

**ConnectionHandler** (`Sources/Nats/NatsConnection.swift`): Internal class implementing `ChannelInboundHandler`. Handles the network connection lifecycle, reconnection logic, and protocol-level communication with NATS servers. Uses `NIOLockedValueBox` and `ManagedAtomic` for thread-safe state management.

**NatsMessage** & **NatsSubscription**: Message handling and subscription management with support for headers and wildcards. Subscriptions implement `AsyncSequence` for message iteration. `NatsSubscription.unsubscribe()` is NOT idempotent — it throws `subscriptionClosed` if already closed (and `connectionClosed` first if the connection is down); route repeated/racing teardown through a once-guard that tolerates `subscriptionClosed`.

**JetStreamContext** (`Sources/JetStream/JetStreamContext.swift`): Entry point for JetStream operations, manages streams and consumers with configurable prefixes/domains; also hosts async batched publish (`publishAsync`).

**KeyValue / ObjectStore** (`Sources/JetStream/KeyValue*.swift`, `Sources/JetStream/ObjectStore*.swift`): the two JetStream-backed stores, both wire-compatible with the NATS ecosystem (bidirectional `nats`-CLI interop tested).

**OrderedConsumer** (`Sources/JetStream/OrderedConsumer*.swift`): the reset-engine push consumer (mirrors nats.go `checkOrderedMsgs`/`resetOrderedConsumer`) that backs KV/Object watch and the public `consume`/`messages`/`next` API — one engine, not four.

**Service** (`Sources/Services/Service.swift`): actor-based micro Service — endpoints/groups, `$SRV` PING/INFO/STATS discovery, per-endpoint stats.

### Key Design Patterns

- **Async/Await**: Modern Swift concurrency throughout the API
- **AsyncSequence**: Subscriptions implement AsyncSequence for message iteration
- **Protocol-Oriented**: Extensive use of protocols for extensibility
- **Event-Driven**: Event system for connection state changes
- **NIO Integration**: `ChannelInboundHandler` for protocol parsing, `EventLoopGroup` for async I/O

### Authentication Methods

The client supports multiple authentication mechanisms configured through `NatsClientOptions`:
- Username/password
- Token authentication
- NKEY authentication (nkey file or seed)
- JWT with a credentials file or an in-memory credentials string (no temp file)
- TLS mutual authentication

### Testing Infrastructure

Integration tests use `NatsServer` helper to spawn local NATS server instances with various configurations (auth, TLS, permissions). The `nats-server` binary must be available on `PATH`. Test configurations are stored in `Tests/*/Integration/Resources/`.

## Key Dependencies

- **swift-nio**: Asynchronous event-driven network framework
- **swift-nio-ssl**: TLS support
- **swift-log**: Structured logging
- **nkeys.swift**: NKEY cryptographic operations (pulls in `swift-sodium`, pinned to 0.9.x so it builds against the stable libsodium 1.0.18 on Linux distros)
- **swift-nuid**: Unique identifier generation
- **swift-crypto**: the `Crypto` module (SHA-256), used only on Linux as the fallback for Apple's CryptoKit in ObjectStore digest verification

## Coding Style

- Swift style enforced via `swift-format` (configuration in `.swift-format`)
- Indentation: 4 spaces; line length: 100
- Types use `UpperCamelCase`, members use `lowerCamelCase`

## Development Notes

- Platforms: macOS 13.0+, iOS 13.0+, and Linux (glibc). Linux needs the system libsodium (`libsodium-dev`) for the nkeys.swift dependency.
- Requires a Swift 6.0+ toolchain; the package builds in the Swift 6 language mode (`swiftLanguageModes: [.v6]`) with strict concurrency enforced as errors.
- The LIBRARY builds on Swift 6.0, but the TEST SUITE needs 6.1+: Swift 6.0's XCTest rejects async test methods with `error: implicit capture of 'self' requires that '<TestCase>' conforms to Sendable` (6.1 relaxed it). A newer local toolchain (6.2) also silently accepts region-isolation/Sendable the CI's older Swift rejects — a passing local build is not proof CI passes; the version matrix is what catches it.
- WebSocket upgrade support available for browser-compatible connections
- Batch message processing optimized with `BatchBuffer` for performance
- CI is Linux-first (`.github/workflows/ci.yml`): a Swift `6.0`/`6.1`/`6.2` matrix (build on 6.0, full test on 6.1/6.2), a release build + a looped `PerfBench` consumer-stress gate, macOS build+test (the only place the CryptoKit path runs) + iOS, lint, and DocC. Heavier suites — soak, 3-node cluster failover, toxiproxy fault injection, ThreadSanitizer — run nightly (`.github/workflows/nightly.yml`). The full suite (334 tests) passes on macOS and Linux.
- Cross-platform note: CryptoKit / Combine / `URLSession` file reads are Apple-only; use `#if canImport(CryptoKit)` + `Crypto`, avoid Combine, and read local files with `Data(contentsOf:)` (see `Sources/JetStream/ObjectStore.swift` and `Sources/Nats/NatsConnection.swift`).

## Testing gotchas

- Before a full `swift test`, clear stale processes — a machine sleep mid-run leaves a wedged `xctest` (multi-hour etime) and orphaned servers that block later runs: `pkill -9 -f xctest; pkill -9 -f "nats-server -p -1"`, then re-run.
- Fix lint with `swift format format --in-place <files>` rather than hand-wrapping — it resolves `LineLength`/`AddLines`/`OrderedImports` automatically. (Place a regular import before any `#if canImport(...)` block; OrderedImports rejects the reverse.)
- Async tests that can hang (awaiting delivery/close/reconnect) bound the wait with a `withTaskGroup` timeout task so a regression fails fast instead of wedging the suite (see `ConnectionStateTests`, `SubscriptionLifecycleTests`).
- New XCTest cases: match the file's convention — some register in a `static let allTests` array (legacy manifest), others rely on auto-discovery.
- Swift 6 strict concurrency forbids capturing a mutable local or `weak var` in a `@Sendable`/task-group closure; wrap it in a small `@unchecked Sendable` box in test code.
- `msg.payload` is `Data?` (optional); JetStream `next(timeout:)` / pull `fetch(batch: 1)` create a reply-inbox subscription per call.
- Some lifetime/delivery races only manifest in a RELEASE build against an external server (ARC releases at last use; the in-process debug test scheduler hides them). Verify ordered/push consumer lifetime fixes by LOOPING the release harness — `swift build -c release --product PerfBench`, then a loop of `./.build/release/PerfBench --scenario orderedConsume,pushConsume,pullConsume --msgs N` — not only `swift test`.
- Scripting a release test + an executable build together: run `swift test -c release` FIRST — a prior `swift build -c release` caches a non-testable `Nats`, so `@testable import` then fails with "module was not compiled for testing".