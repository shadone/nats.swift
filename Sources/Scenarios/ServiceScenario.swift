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
import Nats
import Services

private struct AddRequest: Decodable {
    let a: Int
    let b: Int
}

private struct AddResponse: Encodable {
    let sum: Int
}

/// Micro Service: registers a `calc` service with an `add` endpoint that adds two
/// integers from a JSON request, then keeps running so the user can call it and
/// inspect discovery with the `nats` CLI.
func runService() async throws {
    let client = try await connect()
    let service = try await client.addService(
        ServiceConfig(name: "calc", version: "1.0.0", description: "adds two integers"))
    // Register on the explicit subject `calc.add`. Without a subject the endpoint
    // would listen on its bare name (`add`), since the service name is not a
    // subject prefix in this client (matching nats.go).
    try await service.addEndpoint("add", subject: "calc.add") { request in
        do {
            let addRequest = try JSONDecoder().decode(AddRequest.self, from: request.data)
            let sum = addRequest.a + addRequest.b
            out("service", "add(\(addRequest.a), \(addRequest.b)) -> \(sum)")
            try await request.respondJSON(AddResponse(sum: sum))
        } catch {
            let body = String(decoding: request.data, as: UTF8.self)
            out("service", "bad request \"\(body)\": \(error)")
            try? await request.error(
                code: "400", description: "expected JSON body {\"a\":<int>,\"b\":<int>}")
        }
    }

    out("service", "service 'calc' v1.0.0 running with endpoint 'add' (subject calc.add)")
    out("service", "call it:   nats req calc.add '{\"a\":2,\"b\":3}'")
    out("service", "discover:  nats micro ping  |  nats micro info calc  |  nats micro stats calc")
    out("service", "leave running; Ctrl-C to stop (or set SCEN_DURATION=<seconds> for a timed run)")

    await runUntilDurationOrCancelled()

    await service.stop()
    try? await client.close()
    out("service", "DONE")
}
