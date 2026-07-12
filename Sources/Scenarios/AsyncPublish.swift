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
import JetStream
import Nats

/// Batched async publish: fire 20,000 `publishAsync` back to back through the
/// bounded in-flight window, drain with `publishAsyncComplete`, then confirm the
/// window emptied and the stream stored exactly N messages.
func runAsyncPublish() async throws {
    let client = try await connect()
    let js = JetStreamContext(client: client)
    let streamName = "SCEN_AP"
    let count = 20_000

    _ = try? await js.deleteStream(name: streamName)
    _ = try await js.createStream(cfg: StreamConfig(name: streamName, subjects: ["scen.ap.>"]))
    out("ap", "stream \(streamName) ready")

    let payload = Data("async-publish-benchmark-payload".utf8)
    out("ap", "firing \(count) async publishes ...")
    let start = DispatchTime.now().uptimeNanoseconds
    for _ in 0..<count {
        _ = try await js.publishAsync("scen.ap.msg", message: payload)
    }
    out("ap", "all fired; draining acks (publishAsyncComplete) ...")
    try await js.publishAsyncComplete(timeout: 60)
    let elapsedNanos = DispatchTime.now().uptimeNanoseconds - start
    let seconds = Double(elapsedNanos) / 1_000_000_000

    let pending = await js.publishAsyncPending()
    let rate = Double(count) / seconds
    out(
        "ap",
        "published \(count) in \(String(format: "%.2f", seconds))s "
            + "= \(String(format: "%.0f", rate)) msgs/sec, pending=\(pending)")

    guard let stream = try await js.getStream(name: streamName) else {
        out("ap", "DONE (FAIL: stream vanished)")
        try? await client.close()
        return
    }
    let streamMessages = try await stream.info().state.messages
    out("ap", "stream reports \(streamMessages) messages (expected \(count))")

    let ok = streamMessages == UInt64(count) && pending == 0
    _ = try? await js.deleteStream(name: streamName)
    try? await client.close()
    out("ap", ok ? "DONE (PASS)" : "DONE (FAIL)")
}
