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

/// ObjectStore round-trip: a small object (buffered put + getBytes, exact-bytes
/// check) and an 8 MiB object (put + streamed getStream, size check). Completing
/// the stream without throwing is itself the SHA-256 digest + size verification.
func runObjectTransfer() async throws {
    let client = try await connect()
    let js = JetStreamContext(client: client)
    let bucket = "scenarios_obj"

    _ = try? await js.deleteObjectStore(bucket: bucket)
    let store = try await js.createObjectStore(cfg: ObjectStoreConfig(bucket: bucket))
    out("obj", "bucket \(bucket) ready")

    // Small object: put, get back, assert exact bytes.
    let smallBytes = Data("hello from nats.swift object store - small payload".utf8)
    let smallInfo = try await store.put("small.txt", data: smallBytes)
    out(
        "obj",
        "small.txt put size=\(smallInfo.size) chunks=\(smallInfo.chunks) "
            + "digest=\(smallInfo.digest ?? "-")")
    let smallBack = try await store.getBytes("small.txt")
    let smallOK = smallBack == smallBytes
    out("obj", "small.txt round-trip \(smallOK ? "PASS" : "FAIL") (\(smallBack.count) bytes)")

    // Large object: 8 MiB, streamed back chunk by chunk.
    let largeSize = 8 * 1024 * 1024
    let largeBytes = pseudoRandomData(largeSize)
    let largeInfo = try await store.put("large.bin", data: largeBytes)
    out(
        "obj",
        "large.bin put size=\(largeInfo.size) chunks=\(largeInfo.chunks) "
            + "digest=\(largeInfo.digest ?? "-")")

    let reader = try await store.getStream("large.bin")
    var streamedBytes = 0
    var streamedChunks = 0
    for try await chunk in reader {
        streamedBytes += chunk.count
        streamedChunks += 1
    }
    let largeOK = streamedBytes == largeSize && UInt64(streamedBytes) == largeInfo.size
    out("obj", "large.bin streamed \(streamedBytes) bytes in \(streamedChunks) chunks")
    out("obj", "large.bin size+digest \(largeOK ? "PASS" : "FAIL")")

    out("obj", "cross-check: nats object ls \(bucket)  |  nats object info \(bucket) large.bin")

    _ = try? await js.deleteObjectStore(bucket: bucket)
    try? await client.close()
    out("obj", smallOK && largeOK ? "DONE (PASS)" : "DONE (FAIL)")
}
