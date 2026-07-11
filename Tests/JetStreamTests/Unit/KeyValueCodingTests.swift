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

final class KeyValueCodingTests: XCTestCase {

    // MARK: - Names and subjects

    func testStreamName() {
        XCTAssertEqual(KeyValueCoding.streamName(forBucket: "foo"), "KV_foo")
        XCTAssertEqual(KeyValueCoding.streamName(forBucket: "my-bucket"), "KV_my-bucket")
    }

    func testSubject() {
        XCTAssertEqual(KeyValueCoding.subject(forBucket: "foo", key: "bar"), "$KV.foo.bar")
        XCTAssertEqual(
            KeyValueCoding.subject(forBucket: "foo", key: "a.b.c"), "$KV.foo.a.b.c")
    }

    func testAllKeysFilterSubject() {
        XCTAssertEqual(KeyValueCoding.allKeysFilterSubject(forBucket: "foo"), "$KV.foo.>")
    }

    func testKeyFromSubject() {
        XCTAssertEqual(KeyValueCoding.key(fromSubject: "$KV.foo.bar", bucket: "foo"), "bar")
        XCTAssertEqual(
            KeyValueCoding.key(fromSubject: "$KV.foo.a.b.c", bucket: "foo"), "a.b.c")
        // Wrong bucket.
        XCTAssertNil(KeyValueCoding.key(fromSubject: "$KV.other.bar", bucket: "foo"))
        // Prefix only, no key.
        XCTAssertNil(KeyValueCoding.key(fromSubject: "$KV.foo.", bucket: "foo"))
    }

    // MARK: - Bucket name validation

    func testValidBucketNames() {
        for name in ["foo", "my_bucket", "my-bucket", "abc123", "A_B-9"] {
            XCTAssertTrue(KeyValueCoding.isValidBucketName(name), "expected valid: \(name)")
            XCTAssertNoThrow(try KeyValueCoding.validateBucketName(name))
        }
    }

    func testInvalidBucketNames() {
        for name in ["", "with.dot", "with space", "with*", "with>", "with/slash", "with>wild"] {
            XCTAssertFalse(KeyValueCoding.isValidBucketName(name), "expected invalid: \(name)")
            XCTAssertThrowsError(try KeyValueCoding.validateBucketName(name)) { error in
                guard case JetStreamError.KeyValueError.invalidBucketName = error else {
                    return XCTFail("expected invalidBucketName, got \(error)")
                }
            }
        }
    }

    // MARK: - Key validation

    func testValidKeys() {
        for key in ["key", "a.b.c", "a-b_c=d/e", "123", "config.value"] {
            XCTAssertTrue(KeyValueCoding.isValidKey(key), "expected valid: \(key)")
            XCTAssertNoThrow(try KeyValueCoding.validateKey(key))
        }
    }

    func testInvalidKeys() {
        for key in ["", ".leading", "trailing.", "with space", "with*star", "with>gt", "a\tb"] {
            XCTAssertFalse(KeyValueCoding.isValidKey(key), "expected invalid: \(key)")
            XCTAssertThrowsError(try KeyValueCoding.validateKey(key)) { error in
                guard case JetStreamError.KeyValueError.invalidKey = error else {
                    return XCTFail("expected invalidKey, got \(error)")
                }
            }
        }
    }

    // MARK: - Config mapping

    func testStreamConfigDefaults() throws {
        let cfg = KeyValueConfig(bucket: "foo")
        let stream = try KeyValueCoding.streamConfig(from: cfg)

        XCTAssertEqual(stream.name, "KV_foo")
        XCTAssertEqual(stream.subjects, ["$KV.foo.>"])
        XCTAssertEqual(stream.maxMsgsPerSubject, 1)  // history default
        XCTAssertEqual(stream.maxConsumers, -1)
        XCTAssertEqual(stream.maxBytes, -1)
        XCTAssertEqual(stream.maxMsgSize, -1)
        XCTAssertEqual(stream.discard, .new)
        XCTAssertEqual(stream.storage, .file)
        XCTAssertEqual(stream.replicas, 1)
        XCTAssertEqual(stream.compression, .none)
        XCTAssertEqual(stream.allowDirect, true)
        XCTAssertEqual(stream.allowRollup, true)
        XCTAssertEqual(stream.denyDelete, true)
        XCTAssertEqual(stream.mirrorDirect, false)
        XCTAssertNil(stream.denyPurge)  // KV allows purge (rollup)
        XCTAssertEqual(stream.maxAge, NanoTimeInterval(0))
        // 2 minute default duplicate window.
        XCTAssertEqual(stream.duplicates, NanoTimeInterval(120))
    }

    func testStreamConfigCustomFields() throws {
        var cfg = KeyValueConfig(bucket: "custom")
        cfg.description = "a bucket"
        cfg.history = 10
        cfg.maxValueSize = 1024
        cfg.maxBytes = 1_000_000
        cfg.ttl = NanoTimeInterval(3600)
        cfg.storage = .memory
        cfg.replicas = 3
        cfg.compression = true
        cfg.metadata = ["team": "example"]

        let stream = try KeyValueCoding.streamConfig(from: cfg)

        XCTAssertEqual(stream.name, "KV_custom")
        XCTAssertEqual(stream.description, "a bucket")
        XCTAssertEqual(stream.maxMsgsPerSubject, 10)
        XCTAssertEqual(stream.maxMsgSize, 1024)
        XCTAssertEqual(stream.maxBytes, 1_000_000)
        XCTAssertEqual(stream.maxAge, NanoTimeInterval(3600))
        XCTAssertEqual(stream.storage, .memory)
        XCTAssertEqual(stream.replicas, 3)
        XCTAssertEqual(stream.compression, .s2)
        XCTAssertEqual(stream.metadata, ["team": "example"])
        // TTL >= 2 minutes keeps the 2 minute duplicate window.
        XCTAssertEqual(stream.duplicates, NanoTimeInterval(120))
    }

    func testStreamConfigCapsDuplicateWindowToShortTTL() throws {
        var cfg = KeyValueConfig(bucket: "shortttl")
        cfg.ttl = NanoTimeInterval(30)  // 30s < 2min
        let stream = try KeyValueCoding.streamConfig(from: cfg)
        XCTAssertEqual(stream.maxAge, NanoTimeInterval(30))
        XCTAssertEqual(stream.duplicates, NanoTimeInterval(30))
    }

    func testStreamConfigHistoryTooLarge() {
        var cfg = KeyValueConfig(bucket: "big")
        cfg.history = 65
        XCTAssertThrowsError(try KeyValueCoding.streamConfig(from: cfg)) { error in
            guard case JetStreamError.KeyValueError.historyTooLarge = error else {
                return XCTFail("expected historyTooLarge, got \(error)")
            }
        }
    }

    func testStreamConfigRejectsInvalidBucket() {
        let cfg = KeyValueConfig(bucket: "bad.bucket")
        XCTAssertThrowsError(try KeyValueCoding.streamConfig(from: cfg)) { error in
            guard case JetStreamError.KeyValueError.invalidBucketName = error else {
                return XCTFail("expected invalidBucketName, got \(error)")
            }
        }
    }

    func testStreamConfigZeroBytesAndValueSizeBecomeUnlimited() throws {
        var cfg = KeyValueConfig(bucket: "zeros")
        cfg.maxBytes = 0
        cfg.maxValueSize = 0
        let stream = try KeyValueCoding.streamConfig(from: cfg)
        XCTAssertEqual(stream.maxBytes, -1)
        XCTAssertEqual(stream.maxMsgSize, -1)
    }

    // MARK: - Operation decode

    func testOperationFromHeaders() {
        XCTAssertEqual(KeyValueCoding.operation(from: nil), .put)

        var empty = NatsHeaderMap()
        empty.insert(.natsSequence, NatsHeaderValue("5"))
        XCTAssertEqual(KeyValueCoding.operation(from: empty), .put)

        var del = NatsHeaderMap()
        del.insert(.kvOperation, NatsHeaderValue("DEL"))
        XCTAssertEqual(KeyValueCoding.operation(from: del), .delete)

        var purge = NatsHeaderMap()
        purge.insert(.kvOperation, NatsHeaderValue("PURGE"))
        XCTAssertEqual(KeyValueCoding.operation(from: purge), .purge)

        // Unknown operation values decode to put.
        var unknown = NatsHeaderMap()
        unknown.insert(.kvOperation, NatsHeaderValue("WAT"))
        XCTAssertEqual(KeyValueCoding.operation(from: unknown), .put)
    }

    // MARK: - Entry decode

    func testEntryDecodePut() {
        let message = StreamMessage(
            subject: "$KV.foo.bar", sequence: 7, payload: Data("hello".utf8),
            headers: nil, time: "2024-01-01T00:00:00Z")
        let entry = KeyValueCoding.entry(from: message, bucket: "foo", key: "bar")

        XCTAssertEqual(entry.bucket, "foo")
        XCTAssertEqual(entry.key, "bar")
        XCTAssertEqual(entry.value, Data("hello".utf8))
        XCTAssertEqual(entry.revision, 7)
        XCTAssertEqual(entry.created, "2024-01-01T00:00:00Z")
        XCTAssertEqual(entry.delta, 0)
        XCTAssertEqual(entry.operation, .put)
    }

    func testEntryDecodeTombstone() {
        var headers = NatsHeaderMap()
        headers.insert(.kvOperation, NatsHeaderValue("DEL"))
        let message = StreamMessage(
            subject: "$KV.foo.bar", sequence: 8, payload: Data(),
            headers: headers, time: "2024-01-01T00:00:01Z")
        let entry = KeyValueCoding.entry(from: message, bucket: "foo", key: "bar")

        XCTAssertEqual(entry.operation, .delete)
        XCTAssertTrue(entry.value.isEmpty)
        XCTAssertEqual(entry.revision, 8)
    }

    func testEntryDecodeKeyDerivedFromSubject() {
        // The key is derived from the message subject, not the passed-in fallback.
        let message = StreamMessage(
            subject: "$KV.foo.a.b.c", sequence: 3, payload: Data(),
            headers: nil, time: "t")
        let entry = KeyValueCoding.entry(from: message, bucket: "foo", key: "fallback")
        XCTAssertEqual(entry.key, "a.b.c")
    }

    // MARK: - Header value builder

    func testExpectedLastSubjectSequenceValue() {
        XCTAssertEqual(
            KeyValueCoding.expectedLastSubjectSequenceValue(0).description, "0")
        XCTAssertEqual(
            KeyValueCoding.expectedLastSubjectSequenceValue(42).description, "42")
    }
}
