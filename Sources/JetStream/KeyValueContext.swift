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

/// Extension to `JetStreamContext` adding KeyValue bucket management.
extension JetStreamContext {

    /// Binds to an existing KeyValue bucket without creating it.
    ///
    /// - Parameter bucket: the name of the bucket.
    ///
    /// - Returns: a ``KeyValue`` handle to the bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidBucketName(_:)`` if the bucket
    /// >   name is not valid.
    /// > - ``JetStreamError/KeyValueError/bucketNotFound(_:)`` if the bucket does
    /// >   not exist.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func keyValue(bucket: String) async throws -> KeyValue {
        try KeyValueCoding.validateBucketName(bucket)
        let streamName = KeyValueCoding.streamName(forBucket: bucket)
        guard let stream = try await getStream(name: streamName) else {
            throw JetStreamError.KeyValueError.bucketNotFound(bucket)
        }
        return KeyValue(ctx: self, bucket: bucket, stream: stream)
    }

    /// Creates a new KeyValue bucket.
    ///
    /// - Parameter cfg: the bucket configuration.
    ///
    /// - Returns: a ``KeyValue`` handle to the created bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidBucketName(_:)`` if the bucket
    /// >   name is not valid.
    /// > - ``JetStreamError/KeyValueError/historyTooLarge`` if `history` exceeds 64.
    /// > - ``JetStreamError/StreamError/streamNameExist(_:)`` if a bucket with the
    /// >   same name but a different configuration already exists.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func createKeyValue(cfg: KeyValueConfig) async throws -> KeyValue {
        let streamCfg = try KeyValueCoding.streamConfig(from: cfg)
        let stream = try await createStream(cfg: streamCfg)
        return KeyValue(ctx: self, bucket: cfg.bucket, stream: stream)
    }

    /// Creates a KeyValue bucket, or updates its configuration if it already
    /// exists. Creating a bucket with a configuration identical to an existing
    /// one is a no-op.
    ///
    /// - Parameter cfg: the bucket configuration.
    ///
    /// - Returns: a ``KeyValue`` handle to the bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidBucketName(_:)`` if the bucket
    /// >   name is not valid.
    /// > - ``JetStreamError/KeyValueError/historyTooLarge`` if `history` exceeds 64.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func createOrUpdateKeyValue(cfg: KeyValueConfig) async throws -> KeyValue {
        let streamCfg = try KeyValueCoding.streamConfig(from: cfg)
        do {
            let stream = try await createStream(cfg: streamCfg)
            return KeyValue(ctx: self, bucket: cfg.bucket, stream: stream)
        } catch JetStreamError.StreamError.streamNameExist {
            let stream = try await updateStream(cfg: streamCfg)
            return KeyValue(ctx: self, bucket: cfg.bucket, stream: stream)
        }
    }

    /// Deletes a KeyValue bucket and all of its data.
    ///
    /// - Parameter bucket: the name of the bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/KeyValueError/invalidBucketName(_:)`` if the bucket
    /// >   name is not valid.
    /// > - ``JetStreamError/StreamError/streamNotFound(_:)`` if the bucket does
    /// >   not exist.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func deleteKeyValue(bucket: String) async throws {
        try KeyValueCoding.validateBucketName(bucket)
        try await deleteStream(name: KeyValueCoding.streamName(forBucket: bucket))
    }

    /// Lists the names of all KeyValue buckets.
    ///
    /// - Returns: a ``KeyValueNames`` async sequence over bucket names.
    public func keyValueStoreNames() async -> KeyValueNames {
        KeyValueNames(names: await streamNames(subject: "$KV.>"))
    }
}

/// An async sequence over KeyValue bucket names, stripping the `KV_` stream
/// prefix from each backing stream name.
public struct KeyValueNames: AsyncSequence {
    public typealias Element = String

    private var names: StreamNames

    internal init(names: StreamNames) {
        self.names = names
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(names: names.makeAsyncIterator())
    }

    public struct Iterator: AsyncIteratorProtocol {
        var names: StreamNames.StreamNamesIterator

        public mutating func next() async throws -> String? {
            while let name = try await names.next() {
                guard name.hasPrefix("KV_") else { continue }
                return String(name.dropFirst("KV_".count))
            }
            return nil
        }
    }
}
