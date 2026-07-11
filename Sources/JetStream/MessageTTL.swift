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

extension NanoTimeInterval {

    /// Formats this interval the way nats.go writes the per-message TTL header,
    /// so the on-wire `Nats-TTL` value is byte-for-byte identical across clients.
    ///
    /// nats.go sets `Nats-TTL` to `time.Duration.String()` (e.g. `"1s"`,
    /// `"1.5s"`, `"1m30s"`); this reproduces that exact formatting.
    internal func goDurationString() -> String {
        // Round to the nearest whole nanosecond, matching the integer-nanosecond
        // resolution of a Go `time.Duration`.
        let nanos = Int64((value * 1_000_000_000).rounded())
        return NanoTimeInterval.goDurationString(nanoseconds: nanos)
    }

    /// Port of Go's `time.Duration.String()` (`src/time/format.go`). Reproduces
    /// its output character-for-character so TTL header values match nats.go.
    internal static func goDurationString(nanoseconds: Int64) -> String {
        if nanoseconds == 0 { return "0s" }

        // 2540400h10m10.000000000s is the widest value; 32 bytes is ample.
        var buf = [UInt8](repeating: 0, count: 32)
        var w = buf.count
        let neg = nanoseconds < 0
        // magnitude() avoids overflow on Int64.min.
        var u = nanoseconds.magnitude

        let second: UInt64 = 1_000_000_000
        if u < second {
            // Sub-second: use a smaller unit (ns / µs / ms) and a fraction.
            let prec: Int
            w -= 1
            buf[w] = UInt8(ascii: "s")
            w -= 1
            if u < 1_000 {
                prec = 0
                buf[w] = UInt8(ascii: "n")
            } else if u < 1_000_000 {
                prec = 3
                // U+00B5 'µ' micro sign, encoded as UTF-8 0xC2 0xB5.
                w -= 1
                buf[w] = 0xC2
                buf[w + 1] = 0xB5
            } else {
                prec = 6
                buf[w] = UInt8(ascii: "m")
            }
            (w, u) = fmtFrac(&buf, upTo: w, value: u, prec: prec)
            w = fmtInt(&buf, upTo: w, value: u)
        } else {
            w -= 1
            buf[w] = UInt8(ascii: "s")
            (w, u) = fmtFrac(&buf, upTo: w, value: u, prec: 9)

            // u is now integer seconds.
            w = fmtInt(&buf, upTo: w, value: u % 60)
            u /= 60

            // u is now integer minutes.
            if u > 0 {
                w -= 1
                buf[w] = UInt8(ascii: "m")
                w = fmtInt(&buf, upTo: w, value: u % 60)
                u /= 60

                // u is now integer hours. Stop here: days vary in length.
                if u > 0 {
                    w -= 1
                    buf[w] = UInt8(ascii: "h")
                    w = fmtInt(&buf, upTo: w, value: u)
                }
            }
        }

        if neg {
            w -= 1
            buf[w] = UInt8(ascii: "-")
        }
        return String(decoding: buf[w...], as: UTF8.self)
    }

    /// Formats the fraction `value / 10**prec` into the tail of `buf` (ending at
    /// `w`), omitting trailing zeros and the decimal point when the fraction is
    /// zero. Returns the new write index and `value / 10**prec`.
    private static func fmtFrac(
        _ buf: inout [UInt8], upTo w0: Int, value v0: UInt64, prec: Int
    ) -> (Int, UInt64) {
        var w = w0
        var v = v0
        var print = false
        for _ in 0..<prec {
            let digit = v % 10
            print = print || digit != 0
            if print {
                w -= 1
                buf[w] = UInt8(digit) + UInt8(ascii: "0")
            }
            v /= 10
        }
        if print {
            w -= 1
            buf[w] = UInt8(ascii: ".")
        }
        return (w, v)
    }

    /// Formats the integer `value` into the tail of `buf` (ending at `w`) and
    /// returns the new write index.
    private static func fmtInt(_ buf: inout [UInt8], upTo w0: Int, value v0: UInt64) -> Int {
        var w = w0
        var v = v0
        if v == 0 {
            w -= 1
            buf[w] = UInt8(ascii: "0")
        } else {
            while v > 0 {
                w -= 1
                buf[w] = UInt8(v % 10) + UInt8(ascii: "0")
                v /= 10
            }
        }
        return w
    }
}
