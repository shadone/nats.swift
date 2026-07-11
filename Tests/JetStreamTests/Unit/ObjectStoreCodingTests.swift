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

import XCTest

@testable import JetStream
@testable import Nats

final class ObjectStoreCodingTests: XCTestCase {

    // MARK: - Padded URL-safe base64

    func testBase64URLPaddedRoundtrip() {
        // A single byte needs padding: "a" -> "YQ==" (padding KEPT, unlike the
        // not-padded variant which would produce "YQ").
        XCTAssertEqual(Data("a".utf8).base64URLPadded(), "YQ==")
        XCTAssertEqual(String(data: Data(base64URLPadded: "YQ==")!, encoding: .utf8), "a")

        // URL-safe substitutions: standard base64 "+/8=" becomes "-_8=".
        XCTAssertEqual(Data([0xFB, 0xFF]).base64URLPadded(), "-_8=")
        XCTAssertEqual(Data(base64URLPadded: "-_8="), Data([0xFB, 0xFF]))

        // Round-trip a range of payloads.
        for payload in ["", "a", "ab", "abc", "abcd", "hello, world", "a.b/c+d"] {
            let data = Data(payload.utf8)
            let encoded = data.base64URLPadded()
            XCTAssertFalse(encoded.contains("+"), "expected URL-safe: \(encoded)")
            XCTAssertFalse(encoded.contains("/"), "expected URL-safe: \(encoded)")
            XCTAssertEqual(Data(base64URLPadded: encoded), data)
        }
    }

    // MARK: - Names and subjects

    func testStreamName() {
        XCTAssertEqual(ObjectStoreCoding.streamName(forBucket: "foo"), "OBJ_foo")
        XCTAssertEqual(ObjectStoreCoding.streamName(forBucket: "my-bucket"), "OBJ_my-bucket")
    }

    func testAllSubjects() {
        XCTAssertEqual(
            ObjectStoreCoding.allSubjects(forBucket: "b"), ["$O.b.C.>", "$O.b.M.>"])
    }

    func testChunkAndMetaSubjects() {
        XCTAssertEqual(
            ObjectStoreCoding.chunkSubject(forBucket: "b", nuid: "ABC123"), "$O.b.C.ABC123")
        // Meta subject uses padded URL-safe base64 of the name.
        XCTAssertEqual(ObjectStoreCoding.metaSubject(forBucket: "b", name: "a"), "$O.b.M.YQ==")
    }

    func testEncodeName() {
        XCTAssertEqual(ObjectStoreCoding.encodeName("a"), "YQ==")
        XCTAssertEqual(ObjectStoreCoding.encodeName(""), "")
        // Encoded names decode back to the original.
        for name in ["file.txt", "a/b/c.png", "with space", "unicode-\u{00e9}"] {
            let encoded = ObjectStoreCoding.encodeName(name)
            let decoded = Data(base64URLPadded: encoded).flatMap {
                String(data: $0, encoding: .utf8)
            }
            XCTAssertEqual(decoded, name)
        }
    }

    // MARK: - Digest

    func testDigestEmptyInputConstant() {
        // The well-known SHA-256 of the empty input, in padded URL-safe base64.
        XCTAssertEqual(
            ObjectStoreCoding.digest(of: Data()),
            "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=")
    }

    func testDigestRoundtrip() throws {
        for payload in ["", "hello", "world", String(repeating: "x", count: 4096)] {
            let data = Data(payload.utf8)
            let digest = ObjectStoreCoding.digest(of: data)
            XCTAssertTrue(digest.hasPrefix("SHA-256="))
            // The decoded digest bytes equal the raw SHA-256 of the input.
            let decoded = try ObjectStoreCoding.decodeDigest(digest)
            XCTAssertEqual(decoded.count, 32)
        }
    }

    func testDecodeDigestSplitsOnFirstEquals() throws {
        // The base64 value itself may carry `=` padding; decoding must keep everything
        // after the FIRST `=`.
        let digest = ObjectStoreCoding.digest(of: Data("a".utf8))
        let decoded = try ObjectStoreCoding.decodeDigest(digest)
        // Re-encoding the decoded bytes reproduces the original digest string.
        XCTAssertEqual(ObjectStoreCoding.digest(fromBytes: decoded), digest)
    }

    func testDecodeDigestInvalidFormat() {
        XCTAssertThrowsError(try ObjectStoreCoding.decodeDigest("no-separator-here")) { error in
            guard case JetStreamError.ObjectStoreError.invalidDigestFormat = error else {
                return XCTFail("expected invalidDigestFormat, got \(error)")
            }
        }
    }

    // MARK: - Bucket name validation

    func testValidBucketNames() {
        for name in ["foo", "my_bucket", "my-bucket", "abc123", "A_B-9"] {
            XCTAssertTrue(ObjectStoreCoding.isValidBucketName(name), "expected valid: \(name)")
            XCTAssertNoThrow(try ObjectStoreCoding.validateBucketName(name))
        }
    }

    func testInvalidBucketNames() {
        for name in ["", "with.dot", "with space", "with*", "with>", "with/slash"] {
            XCTAssertFalse(ObjectStoreCoding.isValidBucketName(name), "expected invalid: \(name)")
            XCTAssertThrowsError(try ObjectStoreCoding.validateBucketName(name)) { error in
                guard case JetStreamError.ObjectStoreError.invalidBucketName = error else {
                    return XCTFail("expected invalidBucketName, got \(error)")
                }
            }
        }
    }

    // MARK: - Config mapping

    func testStreamConfigDefaults() throws {
        let cfg = ObjectStoreConfig(bucket: "b")
        let stream = try ObjectStoreCoding.streamConfig(from: cfg)

        XCTAssertEqual(stream.name, "OBJ_b")
        XCTAssertEqual(stream.subjects, ["$O.b.C.>", "$O.b.M.>"])
        XCTAssertEqual(stream.discard, .new)
        XCTAssertEqual(stream.allowRollup, true)
        XCTAssertEqual(stream.allowDirect, true)
        // CRITICAL: the object store relies on Purge for chunk cleanup, so it must NOT
        // deny delete or purge (contrast KV, which sets denyDelete).
        XCTAssertNil(stream.denyDelete)
        XCTAssertNil(stream.denyPurge)
        // Objects are not versioned per subject.
        XCTAssertEqual(stream.maxMsgsPerSubject, -1)
        XCTAssertEqual(stream.maxBytes, -1)
        XCTAssertEqual(stream.storage, .file)
        XCTAssertEqual(stream.replicas, 1)
        XCTAssertEqual(stream.compression, .none)
        XCTAssertEqual(stream.maxAge, NanoTimeInterval(0))
    }

    func testStreamConfigCustomFields() throws {
        var cfg = ObjectStoreConfig(bucket: "custom")
        cfg.description = "a bucket"
        cfg.ttl = NanoTimeInterval(3600)
        cfg.maxBytes = 1_000_000
        cfg.storage = .memory
        cfg.replicas = 3
        cfg.compression = true
        cfg.metadata = ["team": "example"]

        let stream = try ObjectStoreCoding.streamConfig(from: cfg)
        XCTAssertEqual(stream.description, "a bucket")
        XCTAssertEqual(stream.maxAge, NanoTimeInterval(3600))
        XCTAssertEqual(stream.maxBytes, 1_000_000)
        XCTAssertEqual(stream.storage, .memory)
        XCTAssertEqual(stream.replicas, 3)
        XCTAssertEqual(stream.compression, .s2)
        XCTAssertEqual(stream.metadata, ["team": "example"])
    }

    func testStreamConfigZeroBytesBecomesUnlimited() throws {
        var cfg = ObjectStoreConfig(bucket: "zeros")
        cfg.maxBytes = 0
        let stream = try ObjectStoreCoding.streamConfig(from: cfg)
        XCTAssertEqual(stream.maxBytes, -1)
    }

    func testStreamConfigRejectsInvalidBucket() {
        let cfg = ObjectStoreConfig(bucket: "bad.bucket")
        XCTAssertThrowsError(try ObjectStoreCoding.streamConfig(from: cfg)) { error in
            guard case JetStreamError.ObjectStoreError.invalidBucketName = error else {
                return XCTFail("expected invalidBucketName, got \(error)")
            }
        }
    }

    // MARK: - ObjectInfo Codable

    func testObjectInfoCodableRoundtrip() throws {
        let info = ObjectInfo(
            name: "file.txt",
            bucket: "b",
            nuid: "ABC123",
            size: 42,
            modTime: ObjectStoreCoding.zeroTime,
            chunks: 3,
            digest: "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=",
            deleted: false,
            description: "hello",
            metadata: ["k": "v"])

        let data = try ObjectStoreCoding.encodeInfo(info)
        let json = String(data: data, encoding: .utf8)!
        // JSON tag names match nats.go / the CLI.
        XCTAssertTrue(json.contains("\"mtime\""))
        XCTAssertTrue(json.contains("\"nuid\""))
        XCTAssertTrue(json.contains("\"digest\""))
        // deleted is omitempty when false.
        XCTAssertFalse(json.contains("\"deleted\""))

        let decoded = try ObjectStoreCoding.decodeInfo(data)
        XCTAssertEqual(decoded, info)
    }

    func testObjectInfoDeletedEncodesWhenTrue() throws {
        let info = ObjectInfo(
            name: "gone", bucket: "b", nuid: "N", size: 0, modTime: ObjectStoreCoding.zeroTime,
            chunks: 0, digest: nil, deleted: true)
        let data = try ObjectStoreCoding.encodeInfo(info)
        let json = String(data: data, encoding: .utf8)!
        XCTAssertTrue(json.contains("\"deleted\":true"))
        // digest is omitempty when nil.
        XCTAssertFalse(json.contains("\"digest\""))
        XCTAssertEqual(try ObjectStoreCoding.decodeInfo(data), info)
    }

    func testObjectMetaOptionsChunkSizeTag() throws {
        let opts = ObjectMetaOptions(maxChunkSize: 1024)
        let data = try JSONEncoder().encode(opts)
        let json = String(data: data, encoding: .utf8)!
        XCTAssertTrue(json.contains("\"max_chunk_size\":1024"))
    }
}
