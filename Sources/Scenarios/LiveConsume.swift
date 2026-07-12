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

/// Long-lived fault-injection harness: an ordered consumer prints each delivered
/// stream sequence while a background task publishes steady traffic. Leave it
/// running and restart / kill the server in another terminal to watch delivery
/// resume contiguously (no gap, no duplicate) across the reconnect.
func runLiveConsume() async throws {
    let client = try await connect()
    let js = JetStreamContext(client: client)
    let streamName = "SCEN_LIVE"

    _ = try? await js.deleteStream(name: streamName)
    _ = try await js.createStream(cfg: StreamConfig(name: streamName, subjects: ["scen.live.>"]))
    out("live", "stream \(streamName) ready")
    out(
        "live",
        "leave this running; in another terminal restart nats-server and watch "
            + "delivery resume contiguously")
    out("live", "  fault:  kill -9 $(pgrep -f 'nats-server -js'); nats-server -js -p 4222 &")
    out("live", "  watch:  nats stream info \(streamName)")

    // Steady traffic: one message every 500 ms.
    let publishTask = Task {
        var counter = 0
        while !Task.isCancelled {
            counter += 1
            let message = Data("tick-\(counter)".utf8)
            _ = try? await js.publish("scen.live.tick", message: message).wait()
            try? await Task.sleep(nanoseconds: 500_000_000)
        }
    }

    // Ordered consumer: contiguous, no-loss / no-dup delivery that transparently
    // recreates itself across a server restart.
    let consumer = try await js.orderedConsumer(stream: streamName, cfg: OrderedConsumerConfig())
    let lastSeq = Locked<UInt64>(0)
    let context = try consumer.consume { message in
        let text = message.payload.map { String(decoding: $0, as: UTF8.self) } ?? ""
        guard let metadata = try? message.metadata() else {
            out("live", "delivered \"\(text)\" (no metadata)")
            return
        }
        let previous = lastSeq.withLock { stored -> UInt64 in
            let old = stored
            stored = metadata.streamSequence
            return old
        }
        let gap = previous != 0 && metadata.streamSequence != previous + 1 ? "  <-- GAP" : ""
        out("live", "delivered seq=\(metadata.streamSequence) \"\(text)\"\(gap)")
    }

    // Heartbeat so an idle terminal still shows liveness.
    let heartbeatTask = Task {
        while !Task.isCancelled {
            try? await Task.sleep(nanoseconds: 3_000_000_000)
            out("live", "heartbeat: last delivered seq=\(lastSeq.get())")
        }
    }

    await runUntilDurationOrCancelled()

    context.stop()
    publishTask.cancel()
    heartbeatTask.cancel()
    try? await client.close()
    out("live", "DONE")
}
