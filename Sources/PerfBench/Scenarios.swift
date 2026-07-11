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
import JetStream
import Nats

/// JetStream round-trip / delivery scenarios cap at 50k for realistic runtimes.
func jetStreamCount(_ config: Config) -> Int {
    min(config.msgs, 50_000)
}

/// Deadline for a single delivery cycle, so a lost message never hangs the harness.
private let deliveryTimeoutSeconds = 120.0

/// Dispatches a scenario by name.
func runScenario(
    _ name: String, nats: NatsClient, js: JetStreamContext, config: Config
) async throws -> ScenarioResult {
    switch name {
    case "corePub": return try await runCorePub(nats: nats, config: config)
    case "corePubSub": return try await runCorePubSub(nats: nats, config: config)
    case "reqReply": return try await runReqReply(nats: nats, config: config)
    case "jsPublish": return try await runJsPublish(nats: nats, js: js, config: config)
    case "kvPutGet": return try await runKvPutGet(js: js, config: config)
    case "objPutGet": return try await runObjPutGet(js: js, config: config)
    case "orderedConsume": return try await runOrderedConsume(nats: nats, js: js, config: config)
    case "pushConsume": return try await runPushConsume(nats: nats, js: js, config: config)
    case "pushConsumeHB":
        return try await runPushConsumeHeartbeat(nats: nats, js: js, config: config)
    default:
        throw UsageError(message: "unknown scenario: \(name)")
    }
}

// MARK: - Core publish

func runCorePub(nats: NatsClient, config: Config) async throws -> ScenarioResult {
    let subject = "perf.pub.\(uniqueToken())"
    let payload = Data(count: config.size)

    for _ in 0..<warmupCount(config.msgs) {
        try await nats.publish(payload, subject: subject)
    }
    try await nats.flush()

    let start = DispatchTime.now().uptimeNanoseconds
    for _ in 0..<config.msgs {
        try await nats.publish(payload, subject: subject)
    }
    // The trailing flush (one RTT) is intentionally inside the timed window: it confirms every
    // message reached the server, so the rate reflects delivered throughput, not just enqueue speed.
    // Kept consistent across all publish scenarios.
    try await nats.flush()
    let elapsed = DispatchTime.now().uptimeNanoseconds - start

    let secs = seconds(fromNanos: elapsed)
    let mb = Double(config.msgs * config.size) / 1_000_000.0
    return ScenarioResult(
        name: "corePub", count: config.msgs, payloadSize: config.size,
        elapsedMs: millis(fromNanos: elapsed),
        metrics: [
            Metric(label: "msgs/sec", value: Double(config.msgs) / secs),
            Metric(label: "MB/sec", value: mb / secs),
        ])
}

// MARK: - Core publish / subscribe

func runCorePubSub(nats: NatsClient, config: Config) async throws -> ScenarioResult {
    let payload = Data(count: config.size)

    // Warmup: a full publish + receive cycle on a throwaway subject.
    _ = try await pubSubCycle(
        nats: nats, subject: "perf.warm.\(uniqueToken())",
        count: warmupCount(config.msgs), payload: payload)

    let elapsed = try await pubSubCycle(
        nats: nats, subject: "perf.pubsub.\(uniqueToken())",
        count: config.msgs, payload: payload)

    let secs = seconds(fromNanos: elapsed)
    return ScenarioResult(
        name: "corePubSub", count: config.msgs, payloadSize: config.size,
        elapsedMs: millis(fromNanos: elapsed),
        metrics: [Metric(label: "msgs/sec", value: Double(config.msgs) / secs)])
}

/// Subscribes, then concurrently publishes and receives `count` messages. Returns the elapsed
/// nanoseconds of the timed window.
private func pubSubCycle(
    nats: NatsClient, subject: String, count: Int, payload: Data
) async throws -> UInt64 {
    let sub = try await nats.subscribe(subject: subject)
    try await nats.flush()
    let start = DispatchTime.now().uptimeNanoseconds
    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
            var received = 0
            for try await _ in sub {
                received += 1
                if received >= count {
                    break
                }
            }
        }
        group.addTask {
            for _ in 0..<count {
                try await nats.publish(payload, subject: subject)
            }
            try await nats.flush()
        }
        try await group.waitForAll()
    }
    let elapsed = DispatchTime.now().uptimeNanoseconds - start
    try? await sub.unsubscribe()
    return elapsed
}

// MARK: - Request / reply

func runReqReply(nats: NatsClient, config: Config) async throws -> ScenarioResult {
    let subject = "perf.req.\(uniqueToken())"
    let payload = Data(count: config.size)

    let sub = try await nats.subscribe(subject: subject)
    try await nats.flush()
    let responder = Task {
        for try await msg in sub {
            if let reply = msg.replySubject {
                try await nats.publish(Data(), subject: reply)
            }
        }
    }

    for _ in 0..<warmupCount(config.reqs) {
        _ = try await nats.request(payload, subject: subject)
    }

    var latencies = [UInt64]()
    latencies.reserveCapacity(config.reqs)
    let start = DispatchTime.now().uptimeNanoseconds
    for _ in 0..<config.reqs {
        let callStart = DispatchTime.now().uptimeNanoseconds
        _ = try await nats.request(payload, subject: subject)
        latencies.append(DispatchTime.now().uptimeNanoseconds - callStart)
    }
    let elapsed = DispatchTime.now().uptimeNanoseconds - start

    responder.cancel()
    try? await sub.unsubscribe()

    latencies.sort()
    return ScenarioResult(
        name: "reqReply", count: config.reqs, payloadSize: config.size,
        elapsedMs: millis(fromNanos: elapsed),
        metrics: [
            Metric(label: "p50us", value: percentileMicros(latencies, 50)),
            Metric(label: "p90us", value: percentileMicros(latencies, 90)),
            Metric(label: "p99us", value: percentileMicros(latencies, 99)),
            Metric(label: "maxus", value: percentileMicros(latencies, 100)),
            Metric(label: "meanus", value: meanMicros(latencies)),
        ])
}

// MARK: - JetStream publish

func runJsPublish(
    nats: NatsClient, js: JetStreamContext, config: Config
) async throws
    -> ScenarioResult
{
    let count = jetStreamCount(config)
    let token = uniqueToken()
    let stream = "perf_js_\(token)"
    let subject = "perf.js.\(token)"
    let payload = Data(count: config.size)

    _ = try await js.createStream(cfg: StreamConfig(name: stream, subjects: [subject]))
    return try await withCleanup {
        for _ in 0..<warmupCount(count) {
            _ = try await js.publish(subject, message: payload).wait()
        }
        let start = DispatchTime.now().uptimeNanoseconds
        for _ in 0..<count {
            _ = try await js.publish(subject, message: payload).wait()
        }
        let elapsed = DispatchTime.now().uptimeNanoseconds - start
        let secs = seconds(fromNanos: elapsed)
        return ScenarioResult(
            name: "jsPublish", count: count, payloadSize: config.size,
            elapsedMs: millis(fromNanos: elapsed),
            metrics: [Metric(label: "msgs/sec", value: Double(count) / secs)])
    } cleanup: {
        try? await js.deleteStream(name: stream)
    }
}

// MARK: - Key/Value put + get

func runKvPutGet(js: JetStreamContext, config: Config) async throws -> ScenarioResult {
    let count = jetStreamCount(config)
    let bucket = "perf-kv-\(uniqueToken())"
    let value = Data(count: config.size)
    let kv = try await js.createKeyValue(cfg: KeyValueConfig(bucket: bucket))
    return try await withCleanup {
        for i in 0..<warmupCount(count) {
            _ = try await kv.put("k\(i % 1000)", value)
        }

        let putStart = DispatchTime.now().uptimeNanoseconds
        for i in 0..<count {
            _ = try await kv.put("k\(i % 1000)", value)
        }
        let putElapsed = DispatchTime.now().uptimeNanoseconds - putStart

        let getStart = DispatchTime.now().uptimeNanoseconds
        for i in 0..<count {
            _ = try await kv.get("k\(i % 1000)")
        }
        let getElapsed = DispatchTime.now().uptimeNanoseconds - getStart

        return ScenarioResult(
            name: "kvPutGet", count: count, payloadSize: config.size,
            elapsedMs: millis(fromNanos: putElapsed + getElapsed),
            metrics: [
                Metric(label: "put ops/sec", value: Double(count) / seconds(fromNanos: putElapsed)),
                Metric(label: "get ops/sec", value: Double(count) / seconds(fromNanos: getElapsed)),
            ])
    } cleanup: {
        try? await js.deleteKeyValue(bucket: bucket)
    }
}

// MARK: - Object store put + get

func runObjPutGet(js: JetStreamContext, config: Config) async throws -> ScenarioResult {
    let bucket = "perf-obj-\(uniqueToken())"
    let size = config.objSize
    let data = Data(count: size)
    let store = try await js.createObjectStore(cfg: ObjectStoreConfig(bucket: bucket))
    return try await withCleanup {
        // Warmup: one full put + get round-trip on a throwaway object.
        _ = try await store.put("warm", data: data)
        _ = try await store.get("warm")

        let putStart = DispatchTime.now().uptimeNanoseconds
        _ = try await store.put("obj", data: data)
        let putElapsed = DispatchTime.now().uptimeNanoseconds - putStart

        let getStart = DispatchTime.now().uptimeNanoseconds
        let result = try await store.get("obj")
        let getElapsed = DispatchTime.now().uptimeNanoseconds - getStart
        // Force the reassembled bytes to be realized and sanity-check the round-trip.
        precondition(result.data.count == size, "object round-trip size mismatch")

        let mb = Double(size) / 1_000_000.0
        return ScenarioResult(
            name: "objPutGet", count: 1, payloadSize: size,
            elapsedMs: millis(fromNanos: putElapsed + getElapsed),
            metrics: [
                Metric(label: "put MB/sec", value: mb / seconds(fromNanos: putElapsed)),
                Metric(label: "get MB/sec", value: mb / seconds(fromNanos: getElapsed)),
            ])
    } cleanup: {
        try? await js.deleteObjectStore(bucket: bucket)
    }
}

// MARK: - Ordered consume

func runOrderedConsume(
    nats: NatsClient, js: JetStreamContext, config: Config
) async throws
    -> ScenarioResult
{
    let count = jetStreamCount(config)
    let start: @Sendable (JetStreamContext, String, DeliveryTimer) async throws -> ConsumeContext =
        {
            context, stream, timer in
            let consumer = try await context.orderedConsumer(
                stream: stream, cfg: OrderedConsumerConfig())
            return try consumer.consume { _ in timer.record() }
        }

    _ = try await deliverCycle(
        nats: nats, js: js, count: warmupCount(count), size: config.size, kind: "ord",
        startConsumer: start)
    let elapsed = try await deliverCycle(
        nats: nats, js: js, count: count, size: config.size, kind: "ord", startConsumer: start)

    let secs = seconds(fromNanos: elapsed)
    return ScenarioResult(
        name: "orderedConsume", count: count, payloadSize: config.size,
        elapsedMs: millis(fromNanos: elapsed),
        metrics: [Metric(label: "msgs/sec", value: Double(count) / secs)])
}

// MARK: - Push consume

/// Plain ephemeral ack-none push consumer, NO idle heartbeat. With `idleHeartbeat` unset,
/// `PushDelivery.race()` takes its unbounded `await iterator.next()` fast path (no per-message task
/// group), so this measures raw push delivery throughput.
func runPushConsume(
    nats: NatsClient, js: JetStreamContext, config: Config
) async throws -> ScenarioResult {
    try await runPushConsumeVariant(
        name: "pushConsume", kind: "push", nats: nats, js: js, config: config,
        makeConfig: { ConsumerConfig(ackPolicy: .none) })
}

/// Push consumer with flow control + a 5s idle heartbeat — the SAME delivery config the ordered
/// consumer forces (see ``OrderedConsumerConfig``). This isolates the cost of the heartbeat-driven
/// per-message `withThrowingTaskGroup` in `PushDelivery.race()` from the ordered-specific bookkeeping
/// (metadata parse + cursor tracking), so the `orderedConsume` vs `pushConsume` gap can be decomposed
/// rather than read as a pure "ordered vs push" comparison.
func runPushConsumeHeartbeat(
    nats: NatsClient, js: JetStreamContext, config: Config
) async throws -> ScenarioResult {
    try await runPushConsumeVariant(
        name: "pushConsumeHB", kind: "pushhb", nats: nats, js: js, config: config,
        makeConfig: {
            ConsumerConfig(
                ackPolicy: .none, flowControl: true, idleHeartbeat: NanoTimeInterval(5))
        })
}

/// Shared driver for the push-consume variants: warmup cycle, timed cycle, msgs/sec result.
private func runPushConsumeVariant(
    name: String, kind: String, nats: NatsClient, js: JetStreamContext, config: Config,
    makeConfig: @escaping @Sendable () -> ConsumerConfig
) async throws -> ScenarioResult {
    let count = jetStreamCount(config)
    let start: @Sendable (JetStreamContext, String, DeliveryTimer) async throws -> ConsumeContext =
        {
            context, stream, timer in
            let consumer = try await context.createPushConsumer(stream: stream, cfg: makeConfig())
            return try consumer.consume { _ in timer.record() }
        }

    _ = try await deliverCycle(
        nats: nats, js: js, count: warmupCount(count), size: config.size, kind: kind,
        startConsumer: start)
    let elapsed = try await deliverCycle(
        nats: nats, js: js, count: count, size: config.size, kind: kind, startConsumer: start)

    let secs = seconds(fromNanos: elapsed)
    return ScenarioResult(
        name: name, count: count, payloadSize: config.size,
        elapsedMs: millis(fromNanos: elapsed),
        metrics: [Metric(label: "msgs/sec", value: Double(count) / secs)])
}

/// Creates a stream, publishes `count` messages, then starts a consumer and times delivery from the
/// first to the N-th message. Deletes the stream afterwards. Returns the delivery elapsed nanos.
private func deliverCycle(
    nats: NatsClient, js: JetStreamContext, count: Int, size: Int, kind: String,
    startConsumer:
        @Sendable (JetStreamContext, String, DeliveryTimer) async throws -> ConsumeContext
) async throws -> UInt64 {
    let token = uniqueToken()
    let stream = "perf_\(kind)_\(token)"
    let subject = "perf.\(kind).\(token)"
    let payload = Data(count: size)

    _ = try await js.createStream(cfg: StreamConfig(name: stream, subjects: [subject]))
    return try await withCleanup {
        for _ in 0..<count {
            try await nats.publish(payload, subject: subject)
        }
        try await nats.flush()

        let timer = DeliveryTimer(target: count)
        let context = try await startConsumer(js, stream, timer)
        // Stop the consumer even if the delivery times out, so its subscription/ephemeral consumer
        // teardown is driven before the stream is deleted (rather than racing `deinit`).
        defer { context.stop() }
        return try await awaitWithTimeout(seconds: deliveryTimeoutSeconds) { [timer] in
            await timer.waitElapsedNanos()
        }
    } cleanup: {
        try? await js.deleteStream(name: stream)
    }
}
