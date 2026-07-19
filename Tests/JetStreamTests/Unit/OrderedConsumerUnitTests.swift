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

final class OrderedConsumerUnitTests: XCTestCase {

    // MARK: - parseAckFields (hot-path metadata extraction)

    /// The fast-path extractor must agree with the full `MessageMetadata` parser on the three fields
    /// the accept-check consults (stream seq, deliver/consumer seq, consumer-name serial), for both
    /// the v1 (7-token) and v2 (9-token) `$JS.ACK.*` layouts.
    func testParseAckFieldsMatchesMetadata() throws {
        let cases: [(subject: String, serial: Int?)] = [
            // v1: stream.consumer.delivered.streamSeq.consumerSeq.timestamp.pending
            ("$JS.ACK.MYSTREAM.cons_9.1.42.7.1700000000000000000.3", 9),
            // v2: domain.account.stream.consumer.delivered.streamSeq.consumerSeq.timestamp.pending
            ("$JS.ACK.dom.ACCTHASH.MYSTREAM.cons_5.1.42.7.1700000000000000000.3", 5),
            // Consumer name with no `_<n>` suffix → serial nil, sequences still parse.
            ("$JS.ACK.MYSTREAM.plainname.1.42.7.1700000000000000000.3", nil),
        ]
        for (subject, expectedSerial) in cases {
            let parsed = try XCTUnwrap(
                OrderedConsumer.parseAckFields(subject), "should parse \(subject)")
            let tokens = subject.dropFirst("$JS.ACK.".count).split(separator: ".")
            let meta = try MessageMetadata(tokens: Array(tokens))
            XCTAssertEqual(parsed.streamSeq, meta.streamSequence, "streamSeq for \(subject)")
            XCTAssertEqual(parsed.deliverSeq, meta.consumerSequence, "deliverSeq for \(subject)")
            XCTAssertEqual(parsed.serial, expectedSerial, "serial for \(subject)")
        }
    }

    /// Any subject that isn't a well-formed ack must return `nil` (the old `try? …metadata()` "ignore
    /// on parse failure" behavior).
    func testParseAckFieldsRejectsMalformed() {
        let bad = [
            "not.a.js.ack.subject",  // wrong prefix
            "$JS.ACK.too.few.tokens",  // 3 tokens
            "$JS.ACK.a.b.c.d.e.f.g.h",  // 8 tokens (neither v1=7 nor v2>=9)
            "$JS.ACK.MYSTREAM.cons.1.NOTANUM.7.ts.3",  // non-numeric stream seq (v1)
            "$JS.ACK.MYSTREAM.cons_1.BADTOK.42.7.ts.3",  // non-numeric `delivered`, valid seqs (v1)
            "$JS.ACK.MYSTREAM.cons_1.1.42.7.ts.BADTOK",  // non-numeric `pending`, valid seqs (v1)
        ]
        for subject in bad {
            XCTAssertNil(OrderedConsumer.parseAckFields(subject), "must reject \(subject)")
        }
        XCTAssertNil(OrderedConsumer.parseAckFields(nil), "nil subject must return nil")
    }

    // MARK: - ConsumerConfig push-field JSON round-trip

    func testPushFieldsEncodeWhenSet() throws {
        let cfg = ConsumerConfig(
            name: "c",
            deliverPolicy: .byStartSequence,
            optStartSeq: 5,
            ackPolicy: .none,
            deliverSubject: "d.inbox",
            deliverGroup: "grp",
            flowControl: true,
            idleHeartbeat: NanoTimeInterval(5))

        let data = try JSONEncoder().encode(cfg)
        let json = try XCTUnwrap(
            JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertEqual(json["deliver_subject"] as? String, "d.inbox")
        XCTAssertEqual(json["deliver_group"] as? String, "grp")
        XCTAssertEqual(json["flow_control"] as? Bool, true)
        // idle_heartbeat encodes as integer nanoseconds, like ack_wait.
        XCTAssertEqual(json["idle_heartbeat"] as? Double, 5_000_000_000)

        let decoded = try JSONDecoder().decode(ConsumerConfig.self, from: data)
        XCTAssertEqual(decoded.deliverSubject, "d.inbox")
        XCTAssertEqual(decoded.deliverGroup, "grp")
        XCTAssertEqual(decoded.flowControl, true)
        XCTAssertEqual(decoded.idleHeartbeat, NanoTimeInterval(5))
    }

    func testPushFieldsOmittedWhenNil() throws {
        let cfg = ConsumerConfig(name: "c")
        let data = try JSONEncoder().encode(cfg)
        let json = try XCTUnwrap(
            JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertNil(json["deliver_subject"])
        XCTAssertNil(json["deliver_group"])
        XCTAssertNil(json["flow_control"])
        XCTAssertNil(json["idle_heartbeat"])
    }

    // MARK: - Consumer-sequence gap (the streamSeq-advance invariant)

    func testCursorAcceptsContiguousAndGapsNonContiguous() {
        var cursor = OrderedConsumerCursor()

        XCTAssertEqual(cursor.evaluate(deliverSeq: 1, streamSeq: 10), .accept)
        XCTAssertEqual(cursor.streamSeq, 10)
        XCTAssertEqual(cursor.consumerSeq, 1)

        // Deliver seq jumps 1 -> 3 (expected 2): a gap. The message is discarded and NOTHING
        // advances, so a recreate resumes from streamSeq + 1 and redelivers the missed message.
        XCTAssertEqual(cursor.evaluate(deliverSeq: 3, streamSeq: 30), .gap)
        XCTAssertEqual(cursor.streamSeq, 10, "streamSeq must NOT advance on a gap")
        XCTAssertEqual(cursor.consumerSeq, 1, "consumerSeq must NOT advance on a gap")

        // The recreated consumer restarts deliver seqs from 1; the redelivered message is accepted.
        XCTAssertEqual(cursor.evaluate(deliverSeq: 2, streamSeq: 20), .accept)
        XCTAssertEqual(cursor.streamSeq, 20)
        XCTAssertEqual(cursor.consumerSeq, 2)
    }

    func testSeqGapFromReplySubjects() throws {
        // Two synthetic data messages whose $JS.ACK reply subjects encode NON-contiguous consumer
        // seqs (dseq 1 then dseq 3). MessageMetadata parsing feeds the accept-check.
        let first = try metadata(streamSeq: 10, deliverSeq: 1)
        let second = try metadata(streamSeq: 30, deliverSeq: 3)

        var cursor = OrderedConsumerCursor()
        XCTAssertEqual(
            cursor.evaluate(
                deliverSeq: first.consumerSequence, streamSeq: first.streamSequence),
            .accept)
        XCTAssertEqual(
            cursor.evaluate(
                deliverSeq: second.consumerSequence, streamSeq: second.streamSequence),
            .gap, "the second message must be discarded (gap), not accepted")
        XCTAssertEqual(cursor.streamSeq, 10, "streamSeq stays at the last accepted message")
    }

    /// Property/model test of the whole no-loss / no-dup guarantee: drive the cursor + the recreate
    /// rule (resume from `streamSeq + 1`, deliver-seq reset to 0) through many RANDOM schedules of
    /// dropped deliveries (deliver-seq gaps) and spontaneous resets (missed heartbeats), and assert
    /// the yielded stream sequences are always EXACTLY `1...N` — contiguous, no gap, no duplicate.
    /// A seeded PRNG makes any failure reproducible by its seed.
    func testCursorNoLossNoDupUnderRandomGapsAndResets() {
        for seed in UInt64(0)..<400 {
            var rng = SeededXorshift(seed: seed)
            let total = 5 + rng.next() % 40  // 5...44 messages in the stream
            var cursor = OrderedConsumerCursor()
            var yielded: [UInt64] = []
            var guardCount = 0

            // Each outer iteration models one consumer generation created by `recreate()`.
            while cursor.streamSeq < total {
                cursor.consumerSeq = 0  // recreate resets deliver-seq tracking
                var deliverSeq: UInt64 = 0
                var streamSeq = cursor.streamSeq + 1  // recreate resumes from the next stream seq

                generation: while streamSeq <= total {
                    deliverSeq += 1  // server assigns a deliver seq to every message it sends

                    // Spontaneous reset (missed heartbeat / consumer deleted) with no gap: the outer
                    // loop recreates from `streamSeq + 1`; nothing yielded is lost because the cursor
                    // only advanced for accepted messages.
                    if rng.next() % 12 == 0 {
                        break generation
                    }
                    // Drop this delivery before the client sees it: the next RECEIVED message's
                    // deliver seq will skip, which the accept-check must catch as a gap.
                    if rng.next() % 6 == 0 {
                        streamSeq += 1
                        continue generation
                    }

                    switch cursor.evaluate(deliverSeq: deliverSeq, streamSeq: streamSeq) {
                    case .accept:
                        yielded.append(streamSeq)
                        streamSeq += 1
                    case .gap:
                        break generation  // discard + recreate from streamSeq + 1
                    }

                    guardCount += 1
                    XCTAssertLessThan(guardCount, 1_000_000, "seed \(seed): did not converge")
                }
            }

            XCTAssertEqual(
                yielded, Array(1...total),
                "seed \(seed): yielded stream sequences must be exactly 1...\(total) (no loss/dup)")
        }
    }

    /// Builds ``MessageMetadata`` from a `$JS.ACK` reply subject (v1 token layout).
    private func metadata(streamSeq: UInt64, deliverSeq: UInt64) throws -> MessageMetadata {
        // $JS.ACK.<stream>.<consumer>.<delivered>.<streamSeq>.<consumerSeq>.<ts>.<pending>
        let reply = "$JS.ACK.TEST.ord_1.1.\(streamSeq).\(deliverSeq).1700000000000000000.0"
        let prefix = "$JS.ACK."
        let tokens = reply.dropFirst(prefix.count).split(separator: ".")
        return try MessageMetadata(tokens: Array(tokens))
    }

    // MARK: - Reset re-entrancy

    func testResetReentrancyGuardProducesOneRecreate() async {
        // beginReset never touches the connection, so a bare (unconnected) client is enough.
        let client = NatsClient()
        let ctx = JetStreamContext(client: client)
        let oc = OrderedConsumer(ctx: ctx, streamName: "TEST", namePrefix: "ord")

        // Two near-simultaneous triggers at the same serial: exactly one wins.
        let first = await oc.beginReset(triggeredBySerial: 0)
        let second = await oc.beginReset(triggeredBySerial: 0)

        XCTAssertTrue(first, "the first trigger must own the recreate")
        XCTAssertFalse(second, "a concurrent trigger must be deduped, not spawn a second recreate")

        await oc.stop()
    }

    func testResetIgnoresStaleSerial() async {
        let client = NatsClient()
        let ctx = JetStreamContext(client: client)
        let oc = OrderedConsumer(ctx: ctx, streamName: "TEST", namePrefix: "ord")

        // A trigger carrying a serial from an old generation is ignored.
        let stale = await oc.beginReset(triggeredBySerial: 99)
        XCTAssertFalse(stale)

        await oc.stop()
    }

    // MARK: - Heartbeat gap-check before the first post-recreate message

    /// A heartbeat that carries no (or an empty) `Nats-Last-Consumer` header — e.g. one that arrives
    /// before the first data message on a slow-starting producer — must NOT be treated as a gap.
    /// Mirrors nats.go `checkForSequenceMismatch` returning early on empty control metadata, and
    /// avoids a spurious immediate reset loop. A heartbeat whose header is genuinely ahead of the
    /// accepted deliver-seq must still be detected, so the mismatch check is not blunted.
    func testHeartbeatWithoutLastConsumerHeaderIsNotAGap() async {
        let client = NatsClient()
        let ctx = JetStreamContext(client: client)
        let oc = OrderedConsumer(ctx: ctx, streamName: "TEST", namePrefix: "ord")

        // No Nats-Last-Consumer header (only the unrelated Nats-Last-Stream, as a fresh consumer
        // would send before delivering anything) -> no gap.
        var noConsumerHeader = NatsHeaderMap()
        noConsumerHeader.insert(.natsLastStream, NatsHeaderValue("0"))
        let missingHeaderGap = await oc.heartbeatIndicatesGap(heartbeat(headers: noConsumerHeader))
        XCTAssertFalse(
            missingHeaderGap, "a heartbeat without Nats-Last-Consumer must not trigger a reset")

        // Present but empty header value -> no gap.
        var emptyHeader = NatsHeaderMap()
        emptyHeader.insert(.natsLastConsumer, NatsHeaderValue(""))
        let emptyHeaderGap = await oc.heartbeatIndicatesGap(heartbeat(headers: emptyHeader))
        XCTAssertFalse(
            emptyHeaderGap, "a heartbeat with an empty Nats-Last-Consumer must not trigger a reset")

        // Header ahead of the accepted deliver-seq (0 on a fresh cursor) -> gap, reset needed.
        var aheadHeader = NatsHeaderMap()
        aheadHeader.insert(.natsLastConsumer, NatsHeaderValue("3"))
        let aheadHeaderGap = await oc.heartbeatIndicatesGap(heartbeat(headers: aheadHeader))
        XCTAssertTrue(
            aheadHeaderGap, "a heartbeat ahead of the accepted deliver-seq must trigger a reset")

        await oc.stop()
    }

    /// Builds a status-100 idle-heartbeat ``NatsMessage`` carrying `headers`.
    private func heartbeat(headers: NatsHeaderMap) -> NatsMessage {
        NatsMessage(
            payload: nil, subject: "inbox", replySubject: nil, length: 0,
            headers: headers, status: .idleHeartbeat, description: "Idle Heartbeat")
    }
}

/// A tiny seeded xorshift64* PRNG so the property test above is deterministic and a failure
/// reproduces from its seed. Not for cryptographic use.
private struct SeededXorshift {
    private var state: UInt64

    init(seed: UInt64) {
        // Avoid the all-zero state (xorshift's fixed point); any non-zero seed mixing works.
        state = seed &* 0x9E37_79B9_7F4A_7C15 | 1
    }

    mutating func next() -> UInt64 {
        state ^= state >> 12
        state ^= state << 25
        state ^= state >> 27
        return state &* 0x2545_F491_4F6C_DD1D
    }
}
