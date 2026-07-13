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

extension Data {
    private static let cr = UInt8(ascii: "\r")
    private static let lf = UInt8(ascii: "\n")
    private static let crlf = Data([cr, lf])
    private static let currentNum = 0
    private static let errored = false
    internal static let versionLinePrefix = "NATS/1.0"

    func removePrefix(_ prefix: Data) -> Data {
        guard self.starts(with: prefix) else { return self }
        return self.dropFirst(prefix.count)
    }

    func split(
        separator: Data, maxSplits: Int = .max, omittingEmptySubsequences: Bool = true
    )
        -> [Data]
    {
        var chunks: [Data] = []
        var start = startIndex
        var end = startIndex
        var splitsCount = 0

        while end < count {
            if splitsCount >= maxSplits {
                break
            }
            if self[start..<end].elementsEqual(separator) {
                if !omittingEmptySubsequences || start != end {
                    chunks.append(self[start..<end])
                }
                start = index(end, offsetBy: separator.count)
                end = start
                splitsCount += 1
                continue
            }
            end = index(after: end)
        }

        if start <= endIndex {
            if !omittingEmptySubsequences || start != endIndex {
                chunks.append(self[start..<endIndex])
            }
        }

        return chunks
    }

    func getMessageType() -> NatsOperation? {
        guard self.count > 2 else { return nil }
        for operation in NatsOperation.allOperations() {
            if self.starts(with: operation.rawBytes) {
                return operation
            }
        }
        return nil
    }

    func starts(with bytes: [UInt8]) -> Bool {
        guard self.count >= bytes.count else { return false }
        return self.prefix(bytes.count).elementsEqual(bytes)
    }

    internal mutating func prepend(_ other: Data) {
        self = other + self
    }

    internal func parseOutMessages() throws -> (ops: [ServerOp], remainder: Data?) {
        var serverOps = [ServerOp]()
        var startIndex = self.startIndex
        var remainder: Data?

        while startIndex < self.endIndex {
            var nextLineStartIndex: Int
            var lineData: Data
            if let range = self[startIndex...].range(of: Data.crlf) {
                let lineEndIndex = range.lowerBound
                nextLineStartIndex =
                    self.index(range.upperBound, offsetBy: 0, limitedBy: self.endIndex)
                    ?? self.endIndex
                lineData = self[startIndex..<lineEndIndex]
            } else {
                remainder = self[startIndex..<self.endIndex]
                break
            }
            if lineData.count == 0 {
                startIndex = nextLineStartIndex
                continue
            }

            let serverOp = try ServerOp.parse(from: lineData)

            // if it's a message, get the full payload and add to returned data
            if case .message(var msg) = serverOp {
                if msg.length == 0 {
                    serverOps.append(serverOp)
                } else {
                    // Validate the wire-provided payload length before deriving slice bounds. A
                    // negative length (a syntactically valid integer on the wire) would form an
                    // inverted slice, and an enormous length would overflow Int in the index
                    // arithmetic below -- either turns a protocol anomaly (a misbehaving proxy or an
                    // attacker on a non-TLS link) into a process crash. Compare against the remaining
                    // bytes without adding to the wire value (which could overflow).
                    guard msg.length > 0 else {
                        throw NatsError.ProtocolError.parserFailure(
                            "invalid MSG length: \(msg.length)")
                    }
                    // include crlf in the expected payload length; if the full payload+crlf is not
                    // buffered yet, return the remainder and wait for more data.
                    let remaining = self.endIndex - nextLineStartIndex
                    if msg.length > remaining - Data.crlf.count {
                        remainder = self[startIndex..<self.endIndex]
                        break
                    }
                    var payload = Data()
                    let payloadStartIndex = nextLineStartIndex
                    let payloadEndIndex = nextLineStartIndex + msg.length
                    payload.append(self[payloadStartIndex..<payloadEndIndex])
                    msg.payload = payload
                    startIndex =
                        self.index(
                            payloadEndIndex, offsetBy: Data.crlf.count, limitedBy: self.endIndex)
                        ?? self.endIndex
                    serverOps.append(.message(msg))
                    continue
                }
                //TODO(jrm): Add HMSG handling here too.
            } else if case .hMessage(var msg) = serverOp {
                if msg.length == 0 {
                    serverOps.append(serverOp)
                } else {
                    // Validate the wire-provided header length before deriving any slice bounds:
                    // hdr_len must be within (0, total_len]. A malformed hdr_len (0, or greater than
                    // total_len) would otherwise form an out-of-range slice, or an empty header block
                    // the header parser traps on -- turning a protocol anomaly (a misbehaving proxy or
                    // an attacker on a non-TLS link) into a process crash.
                    guard msg.headersLength > 0, msg.headersLength <= msg.length else {
                        throw NatsError.ProtocolError.parserFailure(
                            "invalid HMSG lengths: hdr_len=\(msg.headersLength) "
                                + "total_len=\(msg.length)")
                    }
                    // The guard above bounds hdr_len by total_len, but total_len itself is
                    // wire-controlled and could be enormous; comparing it against the remaining
                    // bytes (without adding to it, which could overflow Int) both rejects an
                    // oversized frame and preserves the "message split across reads -> return
                    // remainder" behavior. hdr_len <= total_len, so every index below is in range.
                    let remaining = self.endIndex - nextLineStartIndex
                    if msg.length > remaining - Data.crlf.count {
                        remainder = self[startIndex..<self.endIndex]
                        break
                    }
                    let headersStartIndex = nextLineStartIndex
                    let headersEndIndex = nextLineStartIndex + msg.headersLength
                    let payloadStartIndex = headersEndIndex
                    let payloadEndIndex = nextLineStartIndex + msg.length

                    var payload: Data?
                    if msg.length > msg.headersLength {
                        payload = Data()
                    }

                    // The header block must be valid UTF-8. Silently substituting an empty header
                    // map on a decode failure would drop control headers (e.g. KV-Operation),
                    // making a delete/purge marker decode as a plain put -- fail the frame instead.
                    let headersData = self[headersStartIndex..<headersEndIndex]
                    guard let headersString = String(data: headersData, encoding: .utf8) else {
                        throw NatsError.ProtocolError.parserFailure(
                            "invalid HMSG header block: not valid UTF-8")
                    }
                    let headers = try NatsHeaderMap(from: headersString)
                    msg.status = headers.status
                    msg.description = headers.description
                    msg.headers = headers

                    if var payload = payload {
                        payload.append(self[payloadStartIndex..<payloadEndIndex])
                        msg.payload = payload
                    }

                    startIndex =
                        self.index(
                            payloadEndIndex, offsetBy: Data.crlf.count, limitedBy: self.endIndex)
                        ?? self.endIndex
                    serverOps.append(.hMessage(msg))
                    continue
                }

            } else {
                // otherwise, just add this server op to the result
                serverOps.append(serverOp)
            }
            startIndex = nextLineStartIndex

        }

        return (serverOps, remainder)
    }
}
