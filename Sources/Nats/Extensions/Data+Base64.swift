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
    /// Swift does not provide a way to encode data to base64 without padding in URL safe way.
    func base64EncodedURLSafeNotPadded() -> String {
        return self.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .trimmingCharacters(in: CharacterSet(charactersIn: "="))
    }

    /// Encodes the data as PADDED URL-safe base64, matching Go's `base64.URLEncoding`.
    ///
    /// Unlike ``base64EncodedURLSafeNotPadded()`` this KEEPS the `=` padding, which is
    /// the encoding the JetStream object store uses for both object names
    /// (`$O.<bucket>.M.<encodeName(name)>`) and SHA-256 digest values. Stripping the
    /// padding — as the not-padded variant does — would break interop with nats.go, the
    /// `nats` CLI, and every other object-store client.
    public func base64URLPadded() -> String {
        return self.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
    }

    /// Decodes a PADDED URL-safe base64 string, matching Go's `base64.URLEncoding`.
    ///
    /// Reverses the URL-safe substitutions (`-` -> `+`, `_` -> `/`) and re-pads the
    /// string to a multiple of four before decoding, so it also accepts input whose
    /// `=` padding was stripped.
    public init?(base64URLPadded string: String) {
        var s =
            string
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        let remainder = s.count % 4
        if remainder != 0 {
            s += String(repeating: "=", count: 4 - remainder)
        }
        self.init(base64Encoded: s)
    }
}
