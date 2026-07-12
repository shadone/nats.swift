// swift-tools-version:6.0

import PackageDescription

let package = Package(
    name: "nats-swift",
    platforms: [
        .macOS(.v13),
        .iOS(.v13),
    ],
    products: [
        .library(name: "Nats", targets: ["Nats"]),
        .library(name: "JetStream", targets: ["JetStream"]),
        .library(name: "Services", targets: ["Services"]),
        .library(name: "NatsServer", targets: ["NatsServer"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.68.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.2"),
        .package(url: "https://github.com/nats-io/nkeys.swift.git", from: "0.1.2"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/Jarema/swift-nuid.git", from: "0.2.0"),
        .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),
        // Provides the `Crypto` module (same API as Apple's CryptoKit) on platforms that lack
        // CryptoKit, e.g. Linux. On Apple platforms CryptoKit is used and this is not linked.
        .package(url: "https://github.com/apple/swift-crypto.git", from: "3.0.0"),
        // Transitive dependency (via nkeys.swift, which requires `from: 0.9.0`). Pinned to 0.9.x
        // because swift-sodium 0.10.0+ added Aegis (and 0.11.0 IpCrypt/ML-KEM) wrappers that
        // reference libsodium symbols absent from the stable libsodium (1.0.18) shipped by Linux
        // distros, breaking the Linux build. 0.9.x only uses symbols present in 1.0.18 and still
        // provides the Ed25519 signing nkeys needs. Harmless on Apple platforms (swift-sodium
        // vendors its own recent libsodium there).
        .package(url: "https://github.com/jedisct1/swift-sodium.git", .upToNextMinor(from: "0.9.0")),
    ],
    targets: [
        .target(
            name: "Nats",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOWebSocket", package: "swift-nio"),
                .product(name: "NKeys", package: "nkeys.swift"),
                .product(name: "Nuid", package: "swift-nuid"),
            ]),
        .target(
            name: "JetStream",
            dependencies: [
                "Nats",
                .product(name: "Logging", package: "swift-log"),
                // Only linked where CryptoKit is unavailable (Linux); the code imports CryptoKit on
                // Apple platforms and `Crypto` (swift-crypto, same SHA-256 API) elsewhere.
                .product(
                    name: "Crypto", package: "swift-crypto", condition: .when(platforms: [.linux])),
            ]),
        .target(
            name: "Services",
            dependencies: [
                "Nats"
            ]),
        .target(
            name: "NatsServer",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
            ]),

        .testTarget(
                name: "NatsTests",
                dependencies: ["Nats", "NatsServer"],
                resources: [
                .process("Integration/Resources")
                ]
        ),
        .testTarget(
                name: "JetStreamTests",
                dependencies: [
                    "Nats", "JetStream", "NatsServer",
                    // ObjectStore tests hash with SHA-256; on Linux that comes from `Crypto`
                    // (swift-crypto) since CryptoKit is Apple-only. No-op on Apple platforms.
                    .product(
                        name: "Crypto", package: "swift-crypto",
                        condition: .when(platforms: [.linux])),
                ],
                resources: [
                .process("Integration/Resources")
                ]
        ),
        .testTarget(
                name: "ServicesTests",
                dependencies: ["Nats", "Services", "NatsServer"],
                resources: [
                .process("Integration/Resources")
                ]
        ),
        .executableTarget(name: "bench", dependencies: ["Nats"]),
        .executableTarget(name: "Benchmark", dependencies: ["Nats"]),
        .executableTarget(name: "BenchmarkPubSub", dependencies: ["Nats"]),
        .executableTarget(name: "BenchmarkSub", dependencies: ["Nats"]),
        .executableTarget(name: "Example", dependencies: ["Nats"]),
        .executableTarget(name: "PerfBench", dependencies: ["Nats", "JetStream"]),
        .executableTarget(
            name: "Scenarios", dependencies: ["Nats", "JetStream", "Services"],
            exclude: [
                "README.md", "CLUSTER.md", "FAULTS.md", "cluster", "fault",
            ]),
    ],
    swiftLanguageModes: [.v6]
)
