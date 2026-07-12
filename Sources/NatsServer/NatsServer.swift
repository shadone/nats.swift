// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation
import XCTest

public class NatsServer {
    public var port: Int? { return natsServerPort }
    public var clientURL: String {
        let scheme = tlsEnabled ? "tls://" : "nats://"
        if let natsServerPort {
            return "\(scheme)localhost:\(natsServerPort)"
        } else {
            return ""
        }
    }

    public var clientWebsocketURL: String {
        let scheme = tlsEnabled ? "wss://" : "ws://"
        if let natsWebsocketPort {
            return "\(scheme)localhost:\(natsWebsocketPort)"
        } else {
            return ""
        }
    }

    /// The JetStream store directory of the most recent ``start(port:cfg:storeDir:file:line:)``.
    /// Capture it after the first start and pass it back to a later `start` (on the same port) to
    /// restart the server with its JetStream state — streams, consumers, KV/Object buckets and
    /// file-backed messages — preserved, as needed to exercise client reconnect-and-resume.
    public private(set) var storeDirectory: String?

    private var process: Process?
    private var natsServerPort: Int?
    private var natsWebsocketPort: Int?
    private var tlsEnabled = false
    private var pidFile: URL?

    public init() {}

    // TODO: When implementing JetStream, creating and deleting store dir should be handled in start/stop methods
    public func start(
        port: Int = -1, cfg: String? = nil, storeDir: String? = nil,
        file: StaticString = #file, line: UInt = #line
    ) {
        XCTAssertNil(
            self.process, "nats-server is already running on port \(port)", file: file, line: line)
        let process = Process()
        let pipe = Pipe()

        let fileManager = FileManager.default
        pidFile = fileManager.temporaryDirectory.appendingPathComponent("nats-server.pid")

        // A caller-supplied `storeDir` reuses an existing store (restart-with-persistence); otherwise
        // a fresh per-start directory keeps unrelated tests isolated, as before.
        let resolvedStoreDir =
            storeDir
            ?? FileManager.default.temporaryDirectory.appending(component: UUID().uuidString).path
        self.storeDirectory = resolvedStoreDir

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "nats-server", "-p", "\(port)", "-P", pidFile!.path, "--store_dir",
            resolvedStoreDir,
        ]
        if let cfg {
            process.arguments?.append(contentsOf: ["-c", cfg])
        }
        process.standardError = pipe
        process.standardOutput = pipe

        let outputHandle = pipe.fileHandleForReading
        let semaphore = DispatchSemaphore(value: 0)
        let maxLines = 100

        // The stdout pump runs on a background dispatch queue (the FileHandle's
        // readabilityHandler) while this method blocks on `semaphore`. Its mutable
        // state — plus the port/TLS values discovered while parsing the log — lives
        // in a Sendable, lock-guarded holder captured by the closure instead of
        // local `var`s and `self`. Delivery is serial, so behavior is unchanged; the
        // holder only makes the synchronization visible to the compiler.
        let probe = StartupProbe()

        outputHandle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            guard data.count > 0 else { return }

            let done: Bool = probe.withState { state in
                state.outputBuffer.append(data)

                guard let output = String(data: state.outputBuffer, encoding: .utf8) else {
                    return false
                }

                let lines = output.split(separator: "\n", omittingEmptySubsequences: false)
                let completedLines = lines.dropLast()

                for lineSequence in completedLines {
                    let line = String(lineSequence)
                    state.lineCount += 1

                    let errorLine = NatsServer.extracErrorMessage(from: line)

                    if let port = NatsServer.extractPort(from: line, for: "client connections") {
                        state.natsServerPort = port
                    }

                    if let port = NatsServer.extractPort(from: line, for: "websocket clients") {
                        state.natsWebsocketPort = port
                    }

                    let ready = line.contains("Server is ready")

                    if !state.tlsEnabled && NatsServer.isTLS(from: line) {
                        state.tlsEnabled = true
                    }

                    if ready || errorLine != nil || state.lineCount >= maxLines {
                        state.serverError = errorLine
                        return true
                    }
                }

                if output.hasSuffix("\n") {
                    state.outputBuffer.removeAll()
                } else {
                    if let lastLine = lines.last, let incompleteLine = lastLine.data(using: .utf8) {
                        state.outputBuffer = incompleteLine
                    }
                }

                return false
            }

            if done {
                semaphore.signal()
                outputHandle.readabilityHandler = nil
            }
        }

        XCTAssertNoThrow(
            try process.run(), "error starting nats-server on port \(port)", file: file, line: line)

        let result = semaphore.wait(timeout: .now() + .seconds(10))

        // Copy the values discovered by the pump back onto `self` on the calling
        // thread, after the semaphore has established a happens-before edge.
        let finalState = probe.withState { $0 }
        self.natsServerPort = finalState.natsServerPort
        self.natsWebsocketPort = finalState.natsWebsocketPort
        self.tlsEnabled = finalState.tlsEnabled
        let serverError = finalState.serverError

        XCTAssertFalse(
            result == .timedOut, "timeout waiting for server to be ready", file: file, line: line)
        XCTAssertNil(
            serverError, "error starting nats-server: \(serverError!)", file: file, line: line)

        self.process = process
    }

    public func stop() {
        if process == nil {
            return
        }

        self.process?.terminate()
        process?.waitUntilExit()
        process = nil
        natsServerPort = port
        tlsEnabled = false
    }

    public func sendSignal(_ signal: Signal, file: StaticString = #file, line: UInt = #line) {
        let process = Process()

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats-server", "--signal", "\(signal.rawValue)=\(self.pidFile!.path)"]

        XCTAssertNoThrow(
            try process.run(), "error setting signal", file: file, line: line)
        self.process = nil
    }

    private static func extractPort(from string: String, for phrase: String) -> Int? {
        // Listening for websocket clients on
        // Listening for client connections on
        let pattern = "Listening for \(phrase) on .*?:(\\d+)$"

        let regex = try! NSRegularExpression(pattern: pattern)
        let nsrange = NSRange(string.startIndex..<string.endIndex, in: string)

        if let match = regex.firstMatch(in: string, options: [], range: nsrange) {
            let portRange = match.range(at: 1)
            if let swiftRange = Range(portRange, in: string) {
                let portString = String(string[swiftRange])
                return Int(portString)
            }
        }

        return nil
    }

    private static func extracErrorMessage(from logLine: String) -> String? {
        if logLine.contains("nats-server: No such file or directory") {
            return "nats-server not found - make sure nats-server can be found in PATH"
        }
        guard let range = logLine.range(of: "[FTL]") else {
            return nil
        }

        let messageStartIndex = range.upperBound
        let message = logLine[messageStartIndex...]

        return String(message).trimmingCharacters(in: .whitespaces)
    }

    private static func isTLS(from logLine: String) -> Bool {
        return logLine.contains("TLS required for client connections")
            || logLine.contains("websocket clients on wss://")
    }

    deinit {
        stop()
    }

    public enum Signal: String {
        case lameDuckMode = "ldm"
        case reload = "reload"
    }
}

/// Thread-safe holder for the state mutated by the stdout `readabilityHandler`
/// pump in ``NatsServer/start(port:cfg:file:line:)``. The handler is invoked on a
/// background dispatch queue while `start()` blocks on a semaphore; every access
/// is serialized through `lock`.
///
/// Marked `@unchecked Sendable` because the compiler cannot see that `lock`
/// guards all stored state. NatsServer's own target does not depend on swift-nio,
/// so an `NSLock`-guarded box (Foundation) is used instead of `NIOLockedValueBox`.
private final class StartupProbe: @unchecked Sendable {
    struct State {
        var outputBuffer = Data()
        var lineCount = 0
        var serverError: String?
        var natsServerPort: Int?
        var natsWebsocketPort: Int?
        var tlsEnabled = false
    }

    private let lock = NSLock()
    private var state = State()

    func withState<T>(_ body: (inout State) -> T) -> T {
        lock.lock()
        defer { lock.unlock() }
        return body(&state)
    }
}
