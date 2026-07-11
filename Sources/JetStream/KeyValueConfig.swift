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

/// `KeyValueConfig` describes the configuration of a KeyValue bucket.
///
/// A bucket is backed by a JetStream stream named `KV_<bucket>` whose subjects
/// are `$KV.<bucket>.>`. The configuration is mapped to a ``StreamConfig`` via
/// ``KeyValueContext/createKeyValue(cfg:)``.
public struct KeyValueConfig {
    /// The name of the bucket. Must match `^[a-zA-Z0-9_-]+$`.
    public var bucket: String

    /// An optional human-readable description of the bucket.
    public var description: String?

    /// The maximum size of a single value in bytes. `-1` means unlimited.
    public var maxValueSize: Int32 = -1

    /// The number of historical values to keep per key. Defaults to `1`.
    /// The server caps history at 64 values per key.
    public var history: UInt8 = 1

    /// The maximum age of a value before it is removed from the bucket.
    public var ttl: NanoTimeInterval?

    /// The maximum size in bytes of the whole bucket. `-1` means unlimited.
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

    /// Republish configuration applied to the backing stream.
    public var republish: RePublish?

    /// Configures the bucket to mirror another bucket's backing stream.
    public var mirror: StreamSource?

    /// Configures the bucket to source from other buckets' backing streams.
    public var sources: [StreamSource]?

    /// Creates a `KeyValueConfig` for a bucket with default settings.
    ///
    /// - Parameter bucket: the name of the bucket.
    public init(bucket: String) {
        self.bucket = bucket
    }
}

/// `KeyValueStatus` reflects the current state of a KeyValue bucket, derived
/// from the backing stream's info and configuration.
public struct KeyValueStatus {
    /// The name of the bucket.
    public let bucket: String

    /// The number of messages currently stored in the backing stream.
    public let values: UInt64

    /// The configured number of historical values kept per key
    /// (the stream's `max_msgs_per_subject`).
    public let history: Int64

    /// The configured maximum age of a value.
    public let ttl: NanoTimeInterval

    /// The kind of backing store. Always `"JetStream"`.
    public let backingStore: String

    /// The number of bytes currently stored in the backing stream.
    public let bytes: UInt64

    /// The backing stream info the status was derived from.
    internal let streamInfo: StreamInfo

    internal init(bucket: String, streamInfo: StreamInfo) {
        self.bucket = bucket
        self.values = streamInfo.state.messages
        self.history = streamInfo.config.maxMsgsPerSubject
        self.ttl = streamInfo.config.maxAge
        self.backingStore = "JetStream"
        self.bytes = streamInfo.state.bytes
        self.streamInfo = streamInfo
    }
}
