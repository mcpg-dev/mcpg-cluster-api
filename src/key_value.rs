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
pub use mcpg_plugin_protocol::wire_bytes::WireBytes;

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

/// One page of a [`KeyValueStore::list_page`] pass.
#[derive(Debug, Clone, Default)]
pub struct KvPage {
    pub entries: Vec<(String, Entry)>,
    /// The cursor of the next page; `None` when the pass is complete.
    pub next: Option<String>,
}

/// Page size [`KeyValueStore::list_prefix`] asks [`KeyValueStore::list_page`] for.
pub const LIST_PAGE_SIZE: usize = 512;

/// Every live `(key, value)` pair under `prefix`: one complete pass of
/// [`KeyValueStore::list_page`], each key once, in key order.
pub async fn list_all(
    store: &(impl KeyValueStore + ?Sized),
    prefix: &str,
) -> Result<Vec<(String, Entry)>, ClusterError> {
    let mut found = std::collections::BTreeMap::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = store
            .list_page(prefix, cursor.as_deref(), LIST_PAGE_SIZE)
            .await?;
        // A key written during the pass can come twice; the later copy is
        // the newer value.
        found.extend(page.entries);
        match page.next {
            Some(next) if cursor.as_deref() != Some(next.as_str()) => cursor = Some(next),
            _ => break,
        }
    }
    Ok(found.into_iter().collect())
}

impl KvPage {
    /// The page of a store that can enumerate its live entries in any
    /// order: the keys under `prefix` after `cursor` in byte order, at most
    /// `limit` of them; the cursor is the last key of the page.
    #[must_use]
    pub fn from_unordered(
        entries: impl IntoIterator<Item = (String, Entry)>,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Self {
        let limit = limit.max(1);
        let mut picked: Vec<(String, Entry)> = entries
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix) && cursor.is_none_or(|c| key.as_str() > c))
            .collect();
        picked.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let more = picked.len() > limit;
        picked.truncate(limit);
        let next = more
            .then(|| picked.last().map(|(key, _)| key.clone()))
            .flatten();
        Self {
            entries: picked,
            next,
        }
    }
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

    /// List the live `(key, value)` pairs under `prefix`, one page at a
    /// time: `cursor` is `None` for the first page and the previous page's
    /// [`KvPage::next`] after it; the pass is complete when `next` is
    /// `None`.
    ///
    /// A page holds at most `limit` entries (a `limit` of 0 counts as 1) and
    /// may hold fewer, also none, while `next` is still `Some`. A complete
    /// pass returns at least once every key that is live for the whole
    /// pass; a key written or removed during the pass may or may not
    /// appear, and a key may appear twice. The order is
    /// implementation-defined. The cursor is opaque and valid only for the
    /// same `prefix` on the same store.
    async fn list_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, ClusterError>;

    /// Up to `limit` live `(key, value)` pairs under `prefix`: the pages of
    /// [`Self::list_page`] in order, cut at `limit`. A caller that must see
    /// every key pages with [`Self::list_page`] instead.
    async fn list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, ClusterError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        while out.len() < limit {
            let want = (limit - out.len()).min(LIST_PAGE_SIZE);
            let page = self.list_page(prefix, cursor.as_deref(), want).await?;
            let room = limit - out.len();
            out.extend(page.entries.into_iter().take(room));
            match page.next {
                // A backend that hands back the cursor it got would loop.
                Some(next) if cursor.as_deref() != Some(next.as_str()) => cursor = Some(next),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Update only the TTL of an existing key, leaving the value
    /// unchanged. Returns `Ok(false)` when the key does not exist (an
    /// *expired* entry counts as absent).
    /// Useful for session keep-alive / lease renewal of values
    /// the caller does not want to re-encode.
    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError>;

    /// Atomically add `delta` to the integer counter stored under `key`
    /// and return the post-increment value. A missing (or expired) key
    /// starts at 0, so the first call returns `delta`. `delta` may be
    /// negative.
    ///
    /// This is the cross-replica **atomic counter** primitive — the
    /// building block for rate-limit token accounting and monotonic
    /// per-session sequences. Unlike a `get`-then-`put`, no two
    /// concurrent callers (on any number of replicas) can observe the
    /// same post-increment value or lose an update. Implementations
    /// MUST be atomic against the backing store (memory:
    /// add-under-shard-lock; redis: `INCRBY` in a Lua script; nats
    /// JetStream KV: revision CAS loop; file: RMW under one
    /// in-process lock — that backend is single-process by contract).
    /// There is deliberately **no default impl** — a non-atomic
    /// get+put would silently defeat the contract, so every backend
    /// must provide a genuinely atomic implementation (or document why
    /// it can't, as the plugin-`Store` adapter does).
    ///
    /// The counter is stored as its ASCII base-10 representation, so
    /// `get` on a counter key returns e.g. `b"42"` on every backend
    /// and `incr` on a key holding a non-integer value returns
    /// [`ClusterError::Precondition`] (as does i64 overflow).
    ///
    /// `ttl` semantics differ from `put`: `Some` re-applies the TTL to
    /// the key on **every** call (sliding expiry — an idle counter
    /// vanishes, an active one persists); `None` leaves any existing
    /// expiry unchanged (a fresh key created by a `None` call has no
    /// TTL).
    async fn incr(&self, key: &str, delta: i64, ttl: Option<Duration>)
    -> Result<i64, ClusterError>;

    /// Atomically raise the counter under `key` to at least `floor`, add
    /// `delta`, and return the result: `max(current, floor) + delta`, where a
    /// missing (or expired) key counts as 0. Same atomicity, storage form and
    /// `ttl` semantics as [`Self::incr`].
    ///
    /// A rate bucket needs the raise and the add as one step: two replicas
    /// that each raise an idle counter to the clock and then add would both
    /// count the idle time. A backend that cannot do it atomically answers
    /// [`ClusterError::Unsupported`] — the default — and the caller falls
    /// back to what [`Self::incr`] can do.
    async fn incr_floor(
        &self,
        key: &str,
        delta: i64,
        floor: i64,
        ttl: Option<Duration>,
    ) -> Result<i64, ClusterError> {
        let _ = (key, delta, floor, ttl);
        Err(ClusterError::Unsupported {
            reason: "incr_floor is not supported by this store".into(),
        })
    }
}

/// `max(current, floor) + delta`, the value [`KeyValueStore::incr_floor`]
/// stores; overflow is a [`ClusterError::Precondition`].
pub fn floor_add(current: i64, floor: i64, delta: i64) -> Result<i64, ClusterError> {
    current
        .max(floor)
        .checked_add(delta)
        .ok_or_else(counter_overflow)
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

/// The wire feature a coordinator lists under `wire_features` in the manifest
/// JSON its `manifest` slot returns when it reads a key/value value written as
/// base64, and answers values as base64 to a call that sets `base64`. The host
/// writes base64 only to a coordinator that lists it; both sides read either
/// form, so a host and a coordinator of different releases work together.
pub const KV_VALUE_BASE64: &str = "kv-value-base64";

/// The wire feature a coordinator lists under `wire_features` when its
/// `kv_incr` slot honours [`KvIncrArgs::floor`] ([`KeyValueStore::incr_floor`]).
/// The host sends `floor` only to a coordinator that lists it: one that does
/// not would ignore the field and add without the raise.
pub const KV_INCR_FLOOR: &str = "kv-incr-floor";

/// Wire form of an [`Entry`]. `expires_at_unix_ms` is the absolute expiry as
/// milliseconds since the Unix epoch (None == no TTL).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvEntryWire {
    pub bytes: WireBytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<u64>,
}

impl KvEntryWire {
    /// Encode an [`Entry`] for the wire, its value as base64 or as an array.
    /// An `expires_at` before the epoch (impossible in practice) clamps to 0.
    #[must_use]
    pub fn from_entry(entry: &Entry, base64: bool) -> Self {
        let expires_at_unix_ms = entry.expires_at.and_then(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        });
        Self {
            bytes: WireBytes::new(entry.bytes.to_vec(), base64),
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
            bytes: Bytes::from(self.bytes.bytes),
            expires_at,
        }
    }
}

/// Args for the `kv_get` / `kv_delete` slots.
///
/// `store`, on every KV slot's args, names a store of its own the caller
/// wants (see `ClusterBackend::key_value_store_named`); absent, the
/// coordinator's shared one. A coordinator that keeps one store ignores it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvKeyArgs {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
    /// Answer the value as base64 ([`KV_VALUE_BASE64`]). A coordinator that
    /// predates it answers an array, which reads too.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub base64: bool,
}

/// Args for the `kv_put` / `kv_put_if_absent` slots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvPutArgs {
    pub key: String,
    pub value: WireBytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
}

/// Args for the `kv_list_prefix` slot.
///
/// Without `page`, the reply is the bare `Vec<KvListEntryWire>` of the first
/// `limit` entries. With `page`, it is a [`KvPageWire`] that starts after
/// `cursor`. A coordinator that predates `page` ignores both fields and
/// answers with the bare list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvListPrefixArgs {
    pub prefix: String,
    pub limit: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub page: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
    /// Answer the values as base64 ([`KV_VALUE_BASE64`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub base64: bool,
}

/// One `(key, entry)` pair in a `kv_list_prefix` reply.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvListEntryWire {
    pub key: String,
    pub entry: KvEntryWire,
}

/// A `kv_list_prefix` reply as the host reads it: a page from a coordinator
/// that pages, or the bare list from one that predates `page` (one page, and
/// the last).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum KvListReplyWire {
    Page(KvPageWire),
    List(Vec<KvListEntryWire>),
}

impl KvListReplyWire {
    /// Decode into a [`KvPage`]; a bare list ends the pass.
    #[must_use]
    pub fn into_page(self) -> KvPage {
        match self {
            Self::Page(page) => page.into_page(),
            Self::List(list) => KvPageWire {
                entries: list,
                next: None,
            }
            .into_page(),
        }
    }

    /// Whether the coordinator answered with a page.
    #[must_use]
    pub fn is_page(&self) -> bool {
        matches!(self, Self::Page(_))
    }
}

/// Wire form of a [`KvPage`], the paged `kv_list_prefix` reply.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvPageWire {
    pub entries: Vec<KvListEntryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

impl KvPageWire {
    /// Encode a [`KvPage`] for the wire, its values as base64 or as arrays.
    #[must_use]
    pub fn from_page(page: &KvPage, base64: bool) -> Self {
        Self {
            entries: page
                .entries
                .iter()
                .map(|(key, entry)| KvListEntryWire {
                    key: key.clone(),
                    entry: KvEntryWire::from_entry(entry, base64),
                })
                .collect(),
            next: page.next.clone(),
        }
    }

    /// Decode back into a [`KvPage`].
    #[must_use]
    pub fn into_page(self) -> KvPage {
        KvPage {
            entries: self
                .entries
                .into_iter()
                .map(|e| (e.key, e.entry.into_entry()))
                .collect(),
            next: self.next,
        }
    }
}

/// Args for the `kv_expire` slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvExpireArgs {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
}

/// Args for the `kv_incr` slot. The reply is the post-increment value
/// (`Result<i64, ClusterError>` via the result envelope).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvIncrArgs {
    pub key: String,
    pub delta: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
    /// Raise the counter to at least this value before adding `delta`
    /// ([`KeyValueStore::incr_floor`]); only for a coordinator that lists
    /// [`KV_INCR_FLOOR`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor: Option<i64>,
}

/// Decode a stored counter value for [`KeyValueStore::incr`]. Strict
/// ASCII base-10 (optional leading `-`), matching what redis `INCRBY`
/// accepts, so every backend agrees on which values are counters.
pub fn parse_counter(bytes: &[u8]) -> Result<i64, ClusterError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| ClusterError::Precondition {
            reason: "incr target holds a non-integer value".into(),
        })
}

/// The error [`KeyValueStore::incr`] returns when the addition would
/// overflow i64 (mirrors redis' `INCRBY` overflow error).
#[must_use]
pub fn counter_overflow() -> ClusterError {
    ClusterError::Precondition {
        reason: "incr overflows the 64-bit counter range".into(),
    }
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
        for base64 in [false, true] {
            let wire = KvEntryWire::from_entry(&entry, base64);
            assert_eq!(wire.expires_at_unix_ms, Some(1_700_000_000_123));
            let json = serde_json::to_string(&wire).unwrap();
            assert_eq!(json.contains("\"aGVsbG8=\""), base64, "{json}");
            let back: KvEntryWire = serde_json::from_str(&json).unwrap();
            let decoded = back.into_entry();
            assert_eq!(decoded.bytes.as_ref(), b"hello");
            assert_eq!(decoded.expires_at, Some(at));
        }
    }

    /// A value reads in either form, and args from a host that predates
    /// `base64` read as before.
    #[test]
    fn values_read_in_either_form() {
        let old: KvPutArgs = serde_json::from_str(r#"{"key":"k","value":[118]}"#).unwrap();
        assert_eq!(old.value, WireBytes::new(b"v".to_vec(), false));
        let new: KvPutArgs = serde_json::from_str(r#"{"key":"k","value":"dg=="}"#).unwrap();
        assert_eq!(new.value, WireBytes::new(b"v".to_vec(), true));
        let get: KvKeyArgs = serde_json::from_str(r#"{"key":"k"}"#).unwrap();
        assert!(!get.base64);
        let get: KvKeyArgs = serde_json::from_str(r#"{"key":"k","base64":true}"#).unwrap();
        assert!(get.base64);
    }

    #[test]
    fn entry_wire_no_ttl_round_trips_as_none() {
        let entry = Entry {
            bytes: Bytes::from_static(b"x"),
            expires_at: None,
        };
        let wire = KvEntryWire::from_entry(&entry, false);
        assert_eq!(wire.expires_at_unix_ms, None);
        let json = serde_json::to_string(&wire).unwrap();
        assert!(!json.contains("expires_at_unix_ms"));
        let back: KvEntryWire = serde_json::from_str(&json).unwrap();
        assert_eq!(back.into_entry().expires_at, None);
    }

    #[test]
    fn a_paged_reply_carries_its_cursor() {
        let page = KvPage {
            entries: vec![(
                "a:1".to_owned(),
                Entry {
                    bytes: Bytes::from_static(b"v"),
                    expires_at: None,
                },
            )],
            next: Some("c1".to_owned()),
        };
        let json = serde_json::to_string(&KvPageWire::from_page(&page, true)).unwrap();
        let reply: KvListReplyWire = serde_json::from_str(&json).unwrap();
        assert!(reply.is_page());
        let back = reply.into_page();
        assert_eq!(back.next.as_deref(), Some("c1"));
        assert_eq!(back.entries[0].0, "a:1");
        assert_eq!(back.entries[0].1.bytes.as_ref(), b"v");
    }

    #[test]
    fn a_bare_list_reply_is_the_last_page() {
        let json = r#"[{"key":"a:1","entry":{"bytes":[118]}}]"#;
        let reply: KvListReplyWire = serde_json::from_str(json).unwrap();
        assert!(!reply.is_page());
        let page = reply.into_page();
        assert_eq!(page.next, None);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].1.bytes.as_ref(), b"v");
    }

    #[test]
    fn list_args_without_page_stay_the_old_shape() {
        let args = KvListPrefixArgs {
            prefix: "a:".into(),
            limit: 10,
            page: false,
            cursor: None,
            store: None,
            base64: false,
        };
        assert_eq!(
            serde_json::to_string(&args).unwrap(),
            r#"{"prefix":"a:","limit":10}"#
        );
        let old: KvListPrefixArgs = serde_json::from_str(r#"{"prefix":"a:","limit":3}"#).unwrap();
        assert!(!old.page);
        assert_eq!(old.cursor, None);
    }
}
