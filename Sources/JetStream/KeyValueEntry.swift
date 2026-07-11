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

/// `KeyValueOperation` describes the kind of a KeyValue entry, encoded on the
/// wire in the `KV-Operation` header. An entry with no such header is a `put`.
public enum KeyValueOperation: String, Sendable, Equatable {
    /// The entry sets a value for the key.
    case put = "PUT"

    /// The entry marks the key as deleted (a tombstone).
    case delete = "DEL"

    /// The entry purges the key, removing all prior history (a rollup tombstone).
    case purge = "PURGE"
}

/// `KeyValueEntry` is a single value read from a KeyValue bucket.
public struct KeyValueEntry: Sendable, Equatable {
    /// The bucket the entry belongs to.
    public let bucket: String

    /// The key of the entry.
    public let key: String

    /// The value bytes. Empty for `delete`/`purge` tombstones.
    public let value: Data

    /// The revision of the entry (the backing stream sequence).
    public let revision: UInt64

    /// The server timestamp when the entry was stored, as an RFC3339 string.
    public let created: String

    /// The distance of this entry from the latest revision of the key.
    /// `0` for the latest entry.
    public let delta: UInt64

    /// The operation the entry represents.
    public let operation: KeyValueOperation
}
