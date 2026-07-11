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

import CryptoKit
import Foundation
import Nats

/// Pure mapping and parsing helpers for the object-store wire conventions, factored out
/// of ``ObjectStore`` so they are unit-testable without a running server.
///
/// The wire conventions are the interop contract shared with every other NATS
/// object-store client: a bucket `<name>` is backed by stream `OBJ_<name>`; chunks live
/// on `$O.<name>.C.<object-nuid>`; each object's meta lives on
/// `$O.<name>.M.<encodeName(name)>` where `encodeName` is PADDED URL-safe base64 of the
/// UTF-8 name; and the object digest is `"SHA-256=<base64url of the sha256 bytes>"`.
enum ObjectStoreCoding {

    /// The default chunk size (128 KiB), used when the object meta does not set one.
    static let defaultChunkSize: UInt32 = 128 * 1024

    /// The digest algorithm prefix used on the wire.
    static let digestType = "SHA-256="

    /// The zero time value stored in the meta JSON on the wire (the real modification
    /// time is derived on read from the message timestamp).
    static let zeroTime = "0001-01-01T00:00:00Z"

    // MARK: - Names and subjects

    static func streamName(forBucket bucket: String) -> String {
        "OBJ_\(bucket)"
    }

    /// The stream subjects owned by an object-store bucket: chunks and meta.
    static func allSubjects(forBucket bucket: String) -> [String] {
        ["$O.\(bucket).C.>", "$O.\(bucket).M.>"]
    }

    /// The per-object chunk subject `$O.<bucket>.C.<nuid>`.
    static func chunkSubject(forBucket bucket: String, nuid: String) -> String {
        "$O.\(bucket).C.\(nuid)"
    }

    /// The per-object meta subject `$O.<bucket>.M.<encodeName(name)>`.
    static func metaSubject(forBucket bucket: String, name: String) -> String {
        "$O.\(bucket).M.\(encodeName(name))"
    }

    /// The wildcard subject covering every object's meta, `$O.<bucket>.M.>`. This is the
    /// filter an object-store watcher consumes.
    static func allMetaSubject(forBucket bucket: String) -> String {
        "$O.\(bucket).M.>"
    }

    /// Encodes an object name as PADDED URL-safe base64 of its UTF-8 bytes, matching
    /// Go's `base64.URLEncoding.EncodeToString`.
    static func encodeName(_ name: String) -> String {
        Data(name.utf8).base64URLPadded()
    }

    // MARK: - Validation

    /// Whether a bucket name is valid (`^[a-zA-Z0-9_-]+$`).
    static func isValidBucketName(_ bucket: String) -> Bool {
        guard !bucket.isEmpty else { return false }
        return bucket.range(of: "^[a-zA-Z0-9_-]+$", options: .regularExpression) != nil
    }

    static func validateBucketName(_ bucket: String) throws {
        guard isValidBucketName(bucket) else {
            throw JetStreamError.ObjectStoreError.invalidBucketName(bucket)
        }
    }

    // MARK: - Config mapping

    /// Maps an ``ObjectStoreConfig`` to the backing ``StreamConfig``, matching the layout
    /// produced by nats.go / the `nats` CLI.
    ///
    /// Key invariants (getting any of these wrong silently breaks the bucket):
    /// - `discard = new` — reject writes once limits are reached.
    /// - `allowRollup = true` — required to replace an object's meta (`Nats-Rollup: sub`).
    /// - `allowDirect = true` — enables read-after-write `DIRECT.GET` of the meta.
    /// - subjects `$O.<bucket>.C.>` and `$O.<bucket>.M.>`.
    /// - `denyDelete`/`denyPurge` are NOT set — the object store relies on `Purge` to
    ///   clean up chunks (contrast KV, which sets `denyDelete`).
    /// - `maxMsgsPerSubject` is NOT set — objects are not versioned per subject.
    static func streamConfig(from cfg: ObjectStoreConfig) throws -> StreamConfig {
        try validateBucketName(cfg.bucket)

        let replicas = cfg.replicas > 0 ? cfg.replicas : 1
        let maxBytes: Int64 = cfg.maxBytes == 0 ? -1 : cfg.maxBytes
        let compression: StoreCompression = cfg.compression ? .s2 : .none

        return StreamConfig(
            name: streamName(forBucket: cfg.bucket),
            description: cfg.description,
            subjects: allSubjects(forBucket: cfg.bucket),
            maxBytes: maxBytes,
            discard: .new,
            maxAge: cfg.ttl ?? NanoTimeInterval(0),
            storage: cfg.storage,
            replicas: replicas,
            placement: cfg.placement,
            allowRollup: true,
            compression: compression,
            allowDirect: true,
            metadata: cfg.metadata)
    }

    // MARK: - Digest

    /// Computes the object digest string `"SHA-256=<base64url>"` for the given bytes.
    static func digest(of data: Data) -> String {
        var hasher = SHA256()
        hasher.update(data: data)
        return digest(fromBytes: Data(hasher.finalize()))
    }

    /// Formats already-computed SHA-256 bytes as the object digest string.
    static func digest(fromBytes shaBytes: Data) -> String {
        digestType + shaBytes.base64URLPadded()
    }

    /// Decodes an object digest string back to its raw SHA-256 bytes.
    ///
    /// Splits on the FIRST `=` (keeping everything after it, including any base64
    /// padding), then base64url-decodes the remainder.
    static func decodeDigest(_ digest: String) throws -> Data {
        guard let separator = digest.firstIndex(of: "=") else {
            throw JetStreamError.ObjectStoreError.invalidDigestFormat
        }
        let encoded = String(digest[digest.index(after: separator)...])
        guard let bytes = Data(base64URLPadded: encoded) else {
            throw JetStreamError.ObjectStoreError.invalidDigestFormat
        }
        return bytes
    }

    // MARK: - Info coding

    /// Encodes an ``ObjectInfo`` to its meta JSON wire form.
    static func encodeInfo(_ info: ObjectInfo) throws -> Data {
        try JSONEncoder().encode(info)
    }

    /// Decodes an ``ObjectInfo`` from its meta JSON wire form.
    static func decodeInfo(_ data: Data) throws -> ObjectInfo {
        try JSONDecoder().decode(ObjectInfo.self, from: data)
    }

    // MARK: - Timestamps

    /// A shared RFC3339 formatter with fractional seconds. Immutable after configuration,
    /// so it is safe to read concurrently.
    ///
    /// `nonisolated(unsafe)` because `ISO8601DateFormatter` is not `Sendable`, yet this
    /// instance is only mutated inside the initializer closure and thereafter used
    /// read-only; Foundation's date formatters are documented to be thread-safe for
    /// formatting once configured.
    nonisolated(unsafe) private static let rfc3339Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    /// Formats the current time as an RFC3339 string, used for the caller-facing
    /// `modTime` returned from a put (the value stored on the wire is the zero time).
    static func nowTimestamp() -> String {
        rfc3339Formatter.string(from: Date())
    }

    /// Converts a JetStream ACK-metadata timestamp (Unix nanoseconds as a string) to an
    /// RFC3339 string, so a watched object's `modTime` matches the format
    /// ``ObjectStore/getInfo(_:showDeleted:)`` derives from a stored message. Falls back
    /// to the raw token when it is not an integer count of nanoseconds.
    static func modTime(fromMetadataTimestamp raw: String) -> String {
        guard let nanos = Int64(raw) else { return raw }
        let seconds = Double(nanos) / 1_000_000_000
        return rfc3339Formatter.string(from: Date(timeIntervalSince1970: seconds))
    }
}
