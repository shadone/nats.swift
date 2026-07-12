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

/// KeyValue foundation + live watch: a background `watchAll()` prints the initial
/// snapshot, the `nil` end-of-initial marker, then every live mutation as the
/// foreground puts/updates/deletes/creates keys.
func runKvWatch() async throws {
    let client = try await connect()
    let js = JetStreamContext(client: client)
    let bucket = "scenarios_kv"

    _ = try? await js.deleteKeyValue(bucket: bucket)
    let kv = try await js.createKeyValue(cfg: KeyValueConfig(bucket: bucket))
    out("kv", "bucket \(bucket) ready")

    // Background watcher over every key.
    let watcher = try await kv.watchAll()
    let watchTask = Task {
        do {
            for try await entry in watcher {
                guard let entry else {
                    out("watch", "end of initial values")
                    continue
                }
                let value = String(decoding: entry.value, as: UTF8.self)
                out(
                    "watch",
                    "observed \(entry.key) = \"\(value)\" @rev\(entry.revision) "
                        + "op=\(entry.operation.rawValue)")
            }
        } catch {
            out("watch", "watch ended: \(error)")
        }
    }

    // Let the watcher emit the end-of-initial marker on the empty bucket before
    // the writes below start producing live updates.
    try await Task.sleep(nanoseconds: 300_000_000)

    let rev1 = try await kv.put("config.timeout", Data("30".utf8))
    out("kv", "PUT config.timeout rev=\(rev1)")
    let rev2 = try await kv.put("config.retries", Data("3".utf8))
    out("kv", "PUT config.retries rev=\(rev2)")
    let rev3 = try await kv.update("config.timeout", Data("45".utf8), revision: rev1)
    out("kv", "UPDATE config.timeout rev=\(rev3) (expected rev\(rev1))")
    try await kv.delete("config.retries")
    out("kv", "DELETE config.retries")
    let rev4 = try await kv.create("config.mode", Data("live".utf8))
    out("kv", "CREATE config.mode rev=\(rev4)")

    if let entry = try await kv.get("config.timeout") {
        let value = String(decoding: entry.value, as: UTF8.self)
        out("kv", "GET config.timeout = \"\(value)\" @rev\(entry.revision)")
    }
    if try await kv.get("config.retries") == nil {
        out("kv", "GET config.retries = nil (deleted)")
    }

    let status = try await kv.status()
    out(
        "kv",
        "status: bucket=\(status.bucket) values=\(status.values) "
            + "history=\(status.history) bytes=\(status.bytes)")

    out("kv", "cross-check: nats kv get \(bucket) config.timeout  |  nats kv watch \(bucket)")

    // Let the watcher print its observations of the writes above.
    try await Task.sleep(nanoseconds: 2_000_000_000)
    await watcher.stop()
    watchTask.cancel()
    try? await client.close()
    out("kv", "DONE")
}
