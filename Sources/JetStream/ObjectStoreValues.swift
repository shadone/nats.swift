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

/// `ObjectLink` embeds a reference to another object or object store.
///
/// A link with an empty ``name`` points at a whole store (like a directory); a link
/// with a ``name`` points at a single object in ``bucket``.
public struct ObjectLink: Codable, Equatable, Sendable {
    /// The name of the object store the link points to.
    public var bucket: String

    /// The name of the object the link points to, or `nil`/empty for a whole-store link.
    public var name: String?

    public init(bucket: String, name: String? = nil) {
        self.bucket = bucket
        self.name = name
    }

    enum CodingKeys: String, CodingKey {
        case bucket
        case name
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(bucket, forKey: .bucket)
        // name is omitempty on the wire.
        if let name, !name.isEmpty {
            try container.encode(name, forKey: .name)
        }
    }
}

/// `ObjectMetaOptions` carries additional options for an object.
public struct ObjectMetaOptions: Codable, Equatable, Sendable {
    /// A link to another object or object store. Set only via link operations, never
    /// directly when putting an object.
    public var link: ObjectLink?

    /// The maximum size of each chunk in bytes. `nil` means the default (128 KiB).
    public var maxChunkSize: UInt32?

    public init(link: ObjectLink? = nil, maxChunkSize: UInt32? = nil) {
        self.link = link
        self.maxChunkSize = maxChunkSize
    }

    enum CodingKeys: String, CodingKey {
        case link
        case maxChunkSize = "max_chunk_size"
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        // Both fields are omitempty on the wire.
        if let link {
            try container.encode(link, forKey: .link)
        }
        if let maxChunkSize, maxChunkSize > 0 {
            try container.encode(maxChunkSize, forKey: .maxChunkSize)
        }
    }
}

/// `ObjectMeta` is the high-level, user-supplied information about an object.
///
/// It is the input to ``ObjectStore/put(_:data:)``; the object's name (``name``) is
/// required and unique within the store.
public struct ObjectMeta: Codable, Equatable, Sendable {
    /// The name of the object. Required and unique within the object store.
    public var name: String

    /// An optional human-readable description of the object.
    public var description: String?

    /// Optional user-defined headers for the object.
    public var headers: [String: [String]]?

    /// Optional user-supplied metadata for the object.
    public var metadata: [String: String]?

    /// Additional options for the object (link, chunk size).
    public var options: ObjectMetaOptions?

    public init(
        name: String,
        description: String? = nil,
        headers: [String: [String]]? = nil,
        metadata: [String: String]? = nil,
        options: ObjectMetaOptions? = nil
    ) {
        self.name = name
        self.description = description
        self.headers = headers
        self.metadata = metadata
        self.options = options
    }

    /// The configured chunk size, or `nil` for the default.
    internal var chunkSize: UInt32? { options?.maxChunkSize }

    /// The configured link, if any.
    internal var link: ObjectLink? { options?.link }

    enum CodingKeys: String, CodingKey {
        case name
        case description
        case headers
        case metadata
        case options
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        if let description, !description.isEmpty {
            try container.encode(description, forKey: .description)
        }
        if let headers, !headers.isEmpty {
            try container.encode(headers, forKey: .headers)
        }
        if let metadata, !metadata.isEmpty {
            try container.encode(metadata, forKey: .metadata)
        }
        if let options {
            try container.encode(options, forKey: .options)
        }
    }
}

/// `ObjectInfo` is ``ObjectMeta`` plus instance information about a stored object.
///
/// The ``ObjectMeta`` fields are flattened into the same JSON object as the instance
/// fields, matching nats.go's struct embedding, so the wire form interoperates with
/// every other object-store client and the `nats` CLI.
public struct ObjectInfo: Codable, Equatable, Sendable {
    /// The name of the object.
    public var name: String

    /// An optional human-readable description of the object.
    public var description: String?

    /// Optional user-defined headers for the object.
    public var headers: [String: [String]]?

    /// Optional user-supplied metadata for the object.
    public var metadata: [String: String]?

    /// Additional options for the object (link, chunk size).
    public var options: ObjectMetaOptions?

    /// The name of the object store the object belongs to.
    public var bucket: String

    /// The unique identifier assigned to the object when it was put into the store.
    public var nuid: String

    /// The size of the object in bytes (excludes metadata).
    public var size: UInt64

    /// The last modification time as an RFC3339 string.
    ///
    /// The value stored on the wire is always the zero time; the real modification
    /// time is derived on read from the meta message's server timestamp. See
    /// ``ObjectStore/getInfo(_:showDeleted:)``.
    public var modTime: String

    /// The number of chunks the object is split into.
    public var chunks: UInt32

    /// The SHA-256 digest of the object (`"SHA-256=<base64url>"`), or `nil` when absent.
    public var digest: String?

    /// Whether the object is marked as deleted.
    public var deleted: Bool

    /// Whether this object is a link to another object or store.
    internal var isLink: Bool { options?.link != nil }

    public init(
        name: String,
        bucket: String,
        nuid: String,
        size: UInt64 = 0,
        modTime: String,
        chunks: UInt32 = 0,
        digest: String? = nil,
        deleted: Bool = false,
        description: String? = nil,
        headers: [String: [String]]? = nil,
        metadata: [String: String]? = nil,
        options: ObjectMetaOptions? = nil
    ) {
        self.name = name
        self.bucket = bucket
        self.nuid = nuid
        self.size = size
        self.modTime = modTime
        self.chunks = chunks
        self.digest = digest
        self.deleted = deleted
        self.description = description
        self.headers = headers
        self.metadata = metadata
        self.options = options
    }

    /// Builds an `ObjectInfo` from an ``ObjectMeta`` and instance fields.
    internal init(
        meta: ObjectMeta,
        bucket: String,
        nuid: String,
        size: UInt64,
        chunks: UInt32,
        digest: String?,
        modTime: String,
        deleted: Bool = false
    ) {
        self.init(
            name: meta.name,
            bucket: bucket,
            nuid: nuid,
            size: size,
            modTime: modTime,
            chunks: chunks,
            digest: digest,
            deleted: deleted,
            description: meta.description,
            headers: meta.headers,
            metadata: meta.metadata,
            options: meta.options)
    }

    enum CodingKeys: String, CodingKey {
        case name
        case description
        case headers
        case metadata
        case options
        case bucket
        case nuid
        case size
        case modTime = "mtime"
        case chunks
        case digest
        case deleted
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        name = try c.decode(String.self, forKey: .name)
        description = try c.decodeIfPresent(String.self, forKey: .description)
        headers = try c.decodeIfPresent([String: [String]].self, forKey: .headers)
        metadata = try c.decodeIfPresent([String: String].self, forKey: .metadata)
        options = try c.decodeIfPresent(ObjectMetaOptions.self, forKey: .options)
        bucket = try c.decode(String.self, forKey: .bucket)
        nuid = try c.decode(String.self, forKey: .nuid)
        size = try c.decode(UInt64.self, forKey: .size)
        modTime = try c.decodeIfPresent(String.self, forKey: .modTime) ?? ObjectStoreCoding.zeroTime
        chunks = try c.decode(UInt32.self, forKey: .chunks)
        digest = try c.decodeIfPresent(String.self, forKey: .digest)
        deleted = try c.decodeIfPresent(Bool.self, forKey: .deleted) ?? false
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        // ObjectMeta fields (flattened, omitempty).
        try c.encode(name, forKey: .name)
        if let description, !description.isEmpty {
            try c.encode(description, forKey: .description)
        }
        if let headers, !headers.isEmpty {
            try c.encode(headers, forKey: .headers)
        }
        if let metadata, !metadata.isEmpty {
            try c.encode(metadata, forKey: .metadata)
        }
        if let options {
            try c.encode(options, forKey: .options)
        }
        // Instance fields.
        try c.encode(bucket, forKey: .bucket)
        try c.encode(nuid, forKey: .nuid)
        try c.encode(size, forKey: .size)
        try c.encode(modTime, forKey: .modTime)
        try c.encode(chunks, forKey: .chunks)
        if let digest, !digest.isEmpty {
            try c.encode(digest, forKey: .digest)
        }
        if deleted {
            try c.encode(deleted, forKey: .deleted)
        }
    }
}
