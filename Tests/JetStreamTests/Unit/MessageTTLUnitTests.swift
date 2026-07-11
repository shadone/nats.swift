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

/// Unit tests for the per-message / per-key TTL wire mapping: the Go-duration
/// header formatting, the ``StreamConfig`` TTL fields, and the KeyValue
/// `limitMarkerTTL` → backing-stream mapping. No server required.
final class MessageTTLUnitTests: XCTestCase {

    // MARK: - Go duration formatting (Nats-TTL header value)

    /// The header value must match `time.Duration.String()` byte-for-byte so
    /// the on-wire TTL is identical to nats.go and the `nats` CLI.
    func testGoDurationString() {
        XCTAssertEqual(NanoTimeInterval(0).goDurationString(), "0s")
        XCTAssertEqual(NanoTimeInterval(1).goDurationString(), "1s")
        XCTAssertEqual(NanoTimeInterval(2).goDurationString(), "2s")
        XCTAssertEqual(NanoTimeInterval(5).goDurationString(), "5s")
        XCTAssertEqual(NanoTimeInterval(59).goDurationString(), "59s")
        XCTAssertEqual(NanoTimeInterval(60).goDurationString(), "1m0s")
        XCTAssertEqual(NanoTimeInterval(90).goDurationString(), "1m30s")
        XCTAssertEqual(NanoTimeInterval(3600).goDurationString(), "1h0m0s")
        XCTAssertEqual(NanoTimeInterval(3661).goDurationString(), "1h1m1s")
        XCTAssertEqual(NanoTimeInterval(1.5).goDurationString(), "1.5s")
        XCTAssertEqual(NanoTimeInterval(0.25).goDurationString(), "250ms")
        XCTAssertEqual(NanoTimeInterval(0.001).goDurationString(), "1ms")
        // Sub-second unit branches (ns, µs) and genuine fractional trimming --
        // interop-critical formatting that would otherwise regress silently.
        XCTAssertEqual(NanoTimeInterval(0.000000001).goDurationString(), "1ns")
        XCTAssertEqual(NanoTimeInterval(0.000001).goDurationString(), "1µs")
        XCTAssertEqual(NanoTimeInterval(0.0000015).goDurationString(), "1.5µs")
        XCTAssertEqual(NanoTimeInterval(0.0012345).goDurationString(), "1.2345ms")
        // Negative durations round-trip through the static helper.
        XCTAssertEqual(NanoTimeInterval.goDurationString(nanoseconds: -1_500_000_000), "-1.5s")
    }

    // MARK: - StreamConfig TTL fields

    func testStreamConfigEncodesTTLFields() throws {
        var cfg = StreamConfig(name: "ttl", subjects: ["ttl.>"])
        cfg.allowMsgTTL = true
        cfg.subjectDeleteMarkerTTL = NanoTimeInterval(1)

        let data = try JSONEncoder().encode(cfg)
        let json = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertEqual(json["allow_msg_ttl"] as? Bool, true)
        // Durations encode as integer nanoseconds, like max_age / duplicate_window.
        XCTAssertEqual(json["subject_delete_marker_ttl"] as? Double, 1_000_000_000)
    }

    func testStreamConfigOmitsTTLFieldsWhenNil() throws {
        let cfg = StreamConfig(name: "plain", subjects: ["plain.>"])
        let data = try JSONEncoder().encode(cfg)
        let json = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertNil(json["allow_msg_ttl"])
        XCTAssertNil(json["subject_delete_marker_ttl"])
    }

    func testStreamConfigTTLRoundTrip() throws {
        // Every non-optional field is set to a non-default value so the custom,
        // omit-at-default encoder emits them all and the synthesized decoder can
        // reconstruct the value (StreamConfig has no custom `init(from:)`).
        var cfg = StreamConfig(
            name: "ttl", subjects: ["ttl.>"], retention: .interest, maxConsumers: 5, maxMsgs: 10,
            maxBytes: 100, discard: .new, maxAge: NanoTimeInterval(300), maxMsgsPerSubject: 5,
            maxMsgSize: 50, storage: .memory, replicas: 3, compression: .s2, allowDirect: true,
            mirrorDirect: true)
        cfg.allowMsgTTL = true
        cfg.subjectDeleteMarkerTTL = NanoTimeInterval(30)

        let data = try JSONEncoder().encode(cfg)
        let decoded = try JSONDecoder().decode(StreamConfig.self, from: data)

        XCTAssertEqual(decoded.allowMsgTTL, true)
        XCTAssertEqual(decoded.subjectDeleteMarkerTTL, NanoTimeInterval(30))
        XCTAssertEqual(decoded, cfg)
    }

    // MARK: - KeyValue limitMarkerTTL mapping

    /// A bucket-level `limitMarkerTTL` must enable `allowMsgTTL` and set
    /// `subjectDeleteMarkerTTL` on the backing stream (matching nats.go).
    func testKeyValueLimitMarkerTTLMapsToStreamConfig() throws {
        var cfg = KeyValueConfig(bucket: "ttlkv")
        cfg.limitMarkerTTL = NanoTimeInterval(2)

        let stream = try KeyValueCoding.streamConfig(from: cfg)
        XCTAssertEqual(stream.allowMsgTTL, true)
        XCTAssertEqual(stream.subjectDeleteMarkerTTL, NanoTimeInterval(2))
    }

    func testKeyValueWithoutLimitMarkerTTLLeavesStreamTTLUnset() throws {
        let cfg = KeyValueConfig(bucket: "plainkv")
        let stream = try KeyValueCoding.streamConfig(from: cfg)
        XCTAssertNil(stream.allowMsgTTL)
        XCTAssertNil(stream.subjectDeleteMarkerTTL)
    }
}
