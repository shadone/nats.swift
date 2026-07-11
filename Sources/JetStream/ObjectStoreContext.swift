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

/// Extension to `JetStreamContext` adding object store bucket management.
extension JetStreamContext {

    /// Binds to an existing object store bucket without creating it.
    ///
    /// - Parameter bucket: the name of the bucket.
    ///
    /// - Returns: an ``ObjectStore`` handle to the bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/invalidBucketName(_:)`` if the bucket name
    /// >   is not valid.
    /// > - ``JetStreamError/ObjectStoreError/bucketNotFound(_:)`` if the bucket does not
    /// >   exist.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func objectStore(bucket: String) async throws -> ObjectStore {
        try ObjectStoreCoding.validateBucketName(bucket)
        let streamName = ObjectStoreCoding.streamName(forBucket: bucket)
        guard let stream = try await getStream(name: streamName) else {
            throw JetStreamError.ObjectStoreError.bucketNotFound(bucket)
        }
        return ObjectStore(ctx: self, bucket: bucket, stream: stream)
    }

    /// Creates a new object store bucket.
    ///
    /// - Parameter cfg: the bucket configuration.
    ///
    /// - Returns: an ``ObjectStore`` handle to the created bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/invalidBucketName(_:)`` if the bucket name
    /// >   is not valid.
    /// > - ``JetStreamError/StreamError/streamNameExist(_:)`` if a bucket with the same
    /// >   name but a different configuration already exists.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func createObjectStore(cfg: ObjectStoreConfig) async throws -> ObjectStore {
        let streamCfg = try ObjectStoreCoding.streamConfig(from: cfg)
        let stream = try await createStream(cfg: streamCfg)
        return ObjectStore(ctx: self, bucket: cfg.bucket, stream: stream)
    }

    /// Creates an object store bucket, or updates its configuration if it already exists.
    /// Creating a bucket with a configuration identical to an existing one is a no-op.
    ///
    /// - Parameter cfg: the bucket configuration.
    ///
    /// - Returns: an ``ObjectStore`` handle to the bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/invalidBucketName(_:)`` if the bucket name
    /// >   is not valid.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func createOrUpdateObjectStore(cfg: ObjectStoreConfig) async throws -> ObjectStore {
        let streamCfg = try ObjectStoreCoding.streamConfig(from: cfg)
        do {
            let stream = try await createStream(cfg: streamCfg)
            return ObjectStore(ctx: self, bucket: cfg.bucket, stream: stream)
        } catch JetStreamError.StreamError.streamNameExist {
            let stream = try await updateStream(cfg: streamCfg)
            return ObjectStore(ctx: self, bucket: cfg.bucket, stream: stream)
        }
    }

    /// Updates the configuration of an existing object store bucket.
    ///
    /// - Parameter cfg: the bucket configuration.
    ///
    /// - Returns: an ``ObjectStore`` handle to the bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/invalidBucketName(_:)`` if the bucket name
    /// >   is not valid.
    /// > - ``JetStreamError/ObjectStoreError/bucketNotFound(_:)`` if the bucket does not
    /// >   exist.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func updateObjectStore(cfg: ObjectStoreConfig) async throws -> ObjectStore {
        let streamCfg = try ObjectStoreCoding.streamConfig(from: cfg)
        do {
            let stream = try await updateStream(cfg: streamCfg)
            return ObjectStore(ctx: self, bucket: cfg.bucket, stream: stream)
        } catch JetStreamError.StreamError.streamNotFound {
            throw JetStreamError.ObjectStoreError.bucketNotFound(cfg.bucket)
        }
    }

    /// Deletes an object store bucket and all of its data.
    ///
    /// - Parameter bucket: the name of the bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ObjectStoreError/invalidBucketName(_:)`` if the bucket name
    /// >   is not valid.
    /// > - ``JetStreamError/ObjectStoreError/bucketNotFound(_:)`` if the bucket does not
    /// >   exist.
    /// > - ``JetStreamError/RequestError`` if the request fails.
    public func deleteObjectStore(bucket: String) async throws {
        try ObjectStoreCoding.validateBucketName(bucket)
        do {
            try await deleteStream(name: ObjectStoreCoding.streamName(forBucket: bucket))
        } catch JetStreamError.StreamError.streamNotFound {
            throw JetStreamError.ObjectStoreError.bucketNotFound(bucket)
        }
    }

    /// Lists the names of all object store buckets.
    ///
    /// - Returns: an ``ObjectStoreNames`` async sequence over bucket names.
    public func objectStoreNames() async -> ObjectStoreNames {
        ObjectStoreNames(names: await streamNames(subject: "$O.>"))
    }
}

/// An async sequence over object store bucket names, stripping the `OBJ_` stream prefix
/// from each backing stream name.
public struct ObjectStoreNames: AsyncSequence {
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
                guard name.hasPrefix("OBJ_") else { continue }
                return String(name.dropFirst("OBJ_".count))
            }
            return nil
        }
    }
}
