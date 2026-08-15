//! `KeyValueStore` — the durable byte-keyed-by-string primitive.
//!
//! Every cluster backend (single-node memory/file, redis, nats, …)
//! provides this. Capabilities that need durable cross-replica state
//! (sessions, tasks, pipelines, subscriptions) consume it via
//! `Arc<dyn KeyValueStore>`.

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::ClusterError;

/// A stored value with optional TTL metadata.
///
/// Backends own the byte payload. `expires_at` is None when the
/// entry has no TTL set; some backends (memory, file) compute it
/// from the per-key expiration supplied at put time.
#[derive(Debug, Clone)]
pub struct Entry {
    pub bytes: Bytes,
    pub expires_at: Option<std::time::SystemTime>,
}

/// Durable namespaced key/value store.
///
/// Keys are operator-bounded. The backend may impose a maximum key
/// length and/or value size; exceeding it returns
/// [`ClusterError::Precondition`].
#[async_trait]
pub trait KeyValueStore: Send + Sync + std::fmt::Debug {
    /// Fetch the value for `key`. Returns `Ok(None)` when the key
    /// does not exist.
    async fn get(&self, key: &str) -> Result<Option<Entry>, ClusterError>;

    /// Store `value` under `key`. Optional `ttl` caps the entry's
    /// lifetime; when None, the entry persists until explicit
    /// `delete` (subject to backend-specific retention defaults).
    async fn put(&self, key: &str, value: Bytes, ttl: Option<Duration>)
    -> Result<(), ClusterError>;

    /// Atomically store `value` under `key` **iff the key is absent**
    /// (an *expired* entry counts as absent). Returns `Ok(true)` when
    /// this call created the entry, `Ok(false)` when a live entry was
    /// already present (another writer won).
    ///
    /// This is the cross-replica **single-winner claim** primitive — the
    /// building block for exactly-once idempotency reservations and
    /// race-free resource claims. Unlike a `get`-then-`put`, no two
    /// concurrent callers (on any number of replicas) can both observe
    /// `true`. Implementations MUST be atomic against the backing store
    /// (memory: compare-and-insert under one lock; redis: `SET NX`;
    /// nats JetStream KV: `create`; file: `O_EXCL` create). There is
    /// deliberately **no default impl** — a non-atomic get+put would
    /// silently defeat the contract, so every backend must provide a
    /// genuinely atomic implementation (or document why it can't, as
    /// the plugin-`Store` adapter does).
    async fn put_if_absent(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<bool, ClusterError>;

    /// Delete `key`. Returns `Ok(false)` when the key did not
    /// exist (idempotent), `Ok(true)` when deletion happened.
    async fn delete(&self, key: &str) -> Result<bool, ClusterError>;

    /// List all `(key, value)` pairs under `prefix`. The order is
    /// implementation-defined and may not be lexicographic;
    /// callers that need a specific order must sort post-hoc.
    ///
    /// `limit` caps the number of entries returned. Backends may
    /// return fewer than `limit` entries even when more exist
    /// (paging is intentionally not exposed at the trait surface;
    /// callers needing pagination use a backend-aware adapter).
    async fn list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, ClusterError>;

    /// Update only the TTL of an existing key, leaving the value
    /// unchanged. Returns `Ok(false)` when the key does not exist.
    /// Useful for session keep-alive / lease renewal of values
    /// the caller does not want to re-encode.
    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError>;
}

// ---------------------------------------------------------------------------
// FFI wire shapes
// ---------------------------------------------------------------------------
//
// Cluster coordinators ship as cdylibs; the `KeyValueStore` trait is async
// but each coordinator vtable slot is a sync `extern "C" fn(handle, args_json)
// -> RString` (the coordinator blocks on its own runtime internally, exactly
// like the `publish` slot). These DTOs are the JSON shapes the host marshals
// across that boundary. They are deliberately serde-stable: a `Duration`
// becomes whole milliseconds, an `Entry.expires_at` becomes a Unix-epoch
// millisecond stamp — both wire-portable without leaking `SystemTime` /
// `Duration` representations.

/// Wire form of an [`Entry`]. `expires_at_unix_ms` is the absolute expiry as
/// milliseconds since the Unix epoch (None == no TTL).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvEntryWire {
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<u64>,
}

impl KvEntryWire {
    /// Encode an [`Entry`] for the wire. An `expires_at` before the epoch
    /// (impossible in practice) clamps to 0.
    #[must_use]
    pub fn from_entry(entry: &Entry) -> Self {
        let expires_at_unix_ms = entry.expires_at.and_then(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        });
        Self {
            bytes: entry.bytes.to_vec(),
            expires_at_unix_ms,
        }
    }

    /// Decode back into an [`Entry`].
    #[must_use]
    pub fn into_entry(self) -> Entry {
        let expires_at = self
            .expires_at_unix_ms
            .map(|ms| std::time::UNIX_EPOCH + Duration::from_millis(ms));
        Entry {
            bytes: Bytes::from(self.bytes),
            expires_at,
        }
    }
}

/// Args for the `kv_get` / `kv_delete` slots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvKeyArgs {
    pub key: String,
}

/// Args for the `kv_put` / `kv_put_if_absent` slots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvPutArgs {
    pub key: String,
    pub value: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

/// Args for the `kv_list_prefix` slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvListPrefixArgs {
    pub prefix: String,
    pub limit: u64,
}

/// One `(key, entry)` pair in a `kv_list_prefix` reply.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvListEntryWire {
    pub key: String,
    pub entry: KvEntryWire,
}

/// Args for the `kv_expire` slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvExpireArgs {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn entry_wire_round_trip_preserves_bytes_and_ttl() {
        let at = std::time::UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let entry = Entry {
            bytes: Bytes::from_static(b"hello"),
            expires_at: Some(at),
        };
        let wire = KvEntryWire::from_entry(&entry);
        assert_eq!(wire.expires_at_unix_ms, Some(1_700_000_000_123));
        let json = serde_json::to_string(&wire).unwrap();
        let back: KvEntryWire = serde_json::from_str(&json).unwrap();
        let decoded = back.into_entry();
        assert_eq!(decoded.bytes.as_ref(), b"hello");
        assert_eq!(decoded.expires_at, Some(at));
    }

    #[test]
    fn entry_wire_no_ttl_round_trips_as_none() {
        let entry = Entry {
            bytes: Bytes::from_static(b"x"),
            expires_at: None,
        };
        let wire = KvEntryWire::from_entry(&entry);
        assert_eq!(wire.expires_at_unix_ms, None);
        let json = serde_json::to_string(&wire).unwrap();
        assert!(!json.contains("expires_at_unix_ms"));
        let back: KvEntryWire = serde_json::from_str(&json).unwrap();
        assert_eq!(back.into_entry().expires_at, None);
    }
}
