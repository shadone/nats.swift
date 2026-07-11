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

/// `ObjectStoreConfig` describes the configuration of an object store bucket.
///
/// A bucket is backed by a JetStream stream named `OBJ_<bucket>` whose subjects are
/// `$O.<bucket>.C.>` (chunks) and `$O.<bucket>.M.>` (meta). The configuration is mapped
/// to a ``StreamConfig`` via ``JetStreamContext/createObjectStore(cfg:)``.
public struct ObjectStoreConfig {
    /// The name of the bucket. Must match `^[a-zA-Z0-9_-]+$`.
    public var bucket: String

    /// An optional human-readable description of the bucket.
    public var description: String?

    /// The maximum age of an object before it is removed from the bucket.
    /// By default, objects do not expire.
    public var ttl: NanoTimeInterval?

    /// The maximum size in bytes of the whole bucket. `-1` (the default) means unlimited;
    /// a value of `0` is treated as unlimited.
    public var maxBytes: Int64 = -1

    /// The storage backend used for the bucket (file or memory).
    public var storage: StorageType = .file

    /// The number of stream replicas in clustered JetStream.
    public var replicas: Int = 1

    /// Whether to enable S2 compression on the backing stream.
    public var compression: Bool = false

    /// A set of application-defined key-value pairs stored as stream metadata.
    public var metadata: [String: String]?

    /// Placement rules for the backing stream in a cluster.
    public var placement: Placement?

    /// Creates an `ObjectStoreConfig` for a bucket with default settings.
    ///
    /// - Parameter bucket: the name of the bucket.
    public init(bucket: String) {
        self.bucket = bucket
    }
}

/// `ObjectStoreStatus` reflects the current state of an object store bucket, derived
/// from the backing stream's info and configuration.
public struct ObjectStoreStatus {
    /// The name of the bucket.
    public let bucket: String

    /// The description supplied when creating the bucket.
    public let description: String?

    /// How long objects are kept in the bucket.
    public let ttl: NanoTimeInterval

    /// The underlying JetStream storage technology used to store data.
    public let storage: StorageType

    /// How many storage replicas are kept for the data in the bucket.
    public let replicas: Int

    /// Whether the backing stream is sealed and cannot be modified.
    public let sealed: Bool

    /// The combined size of all data in the bucket including metadata, in bytes.
    public let size: UInt64

    /// The kind of backing store. Always `"JetStream"`.
    public let backingStore: String

    /// The user-supplied metadata for the bucket.
    public let metadata: [String: String]?

    /// Whether the data is compressed on disk.
    public let isCompressed: Bool

    /// The backing stream info the status was derived from.
    internal let streamInfo: StreamInfo

    internal init(bucket: String, streamInfo: StreamInfo) {
        self.bucket = bucket
        self.description = streamInfo.config.description
        self.ttl = streamInfo.config.maxAge
        self.storage = streamInfo.config.storage
        self.replicas = streamInfo.config.replicas
        self.sealed = streamInfo.config.sealed ?? false
        self.size = streamInfo.state.bytes
        self.backingStore = "JetStream"
        self.metadata = streamInfo.config.metadata
        self.isCompressed = streamInfo.config.compression != .none
        self.streamInfo = streamInfo
    }
}
