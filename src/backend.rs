//! `cluster_backend` — peer discovery, leader election for
//! singleton roles, fenced distributed locks, and notification
//! routing — bundled as a single trait surface alongside the four
//! orthogonal primitives ([`crate::KeyValueStore`], [`crate::PubSub`],
//! [`crate::Lease`], [`crate::Watch`]).
//!
//! Most cluster backends (Raft libraries, NATS JetStream, Consul,
//! etcd, redis with Lua scripts) provide all of these with a shared
//! quorum boundary, so they live behind a single entity kind to
//! avoid mismatched quorum across primitives.
//!
//! # Composition
//!
//! Singleton. Exactly one `cluster_backend` plugin is active
//! in a gateway. Operators pick it via the top-level
//! `cluster: { kind: <kind>, ... }` block; the gateway resolves the
//! kind to the corresponding `dev.mcpg.cluster.<kind>` plugin.
//!
//! # Fencing tokens
//!
//! Lease handles carry a strictly-monotonic `fencing_token` (per
//! lock key, per coordinator lifetime). Consumers use it to reject
//! stale writes: classic "previous holder lost lease, new holder
//! has a higher token, a write from the old holder with a lower
//! token is rejected". Single-node coordinators still monotonically
//! bump their token across acquisitions so consumer code that
//! defensively uses tokens behaves identically between single-node
//! and multi-node modes.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::ClusterError;
use crate::{KeyValueStore, Lease, PubSub, Watch};
use mcpg_plugin_protocol::manifest::PluginManifest;

/// Health classification of a peer as observed by the local node.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PeerHealth {
    Healthy,
    Degraded,
    Unreachable,
}

impl PeerHealth {
    /// Bounded metrics label.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unreachable => "unreachable",
        }
    }
}

impl std::fmt::Display for PeerHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// Information about the local node. Returned from `node_info`.
/// `started_at` is an RFC3339 string (keeps the cluster-api crate
/// dep-free of chrono / time).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeInfo {
    pub node_id: String,
    pub address: String,
    pub version: String,
    pub started_at: String,
    /// Roles this node currently holds leadership for.
    pub roles: Vec<String>,
}

/// Information about a peer as observed locally. `last_seen` is
/// RFC3339.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterPeer {
    pub node_id: String,
    pub address: String,
    pub last_seen: String,
    pub health: PeerHealth,
    pub roles: Vec<String>,
}

/// Peer-lifecycle event emitted on the `watch_peers` stream.
/// `Left(node_id)` and `HealthChanged(node_id, health)` use the
/// node id rather than the full peer so an informer can consume
/// the event without having to hold the last-known snapshot of
/// every peer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PeerEvent {
    Joined { peer: ClusterPeer },
    Left { node_id: String },
    HealthChanged { node_id: String, health: PeerHealth },
}

/// A message received on a coordinator-level subscription. Adds
/// publisher provenance (`from_node`) on top of the primitive
/// [`crate::pub_sub::Message`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishedMessage {
    pub topic: String,
    pub routing_key: Option<String>,
    pub payload: Bytes,
    /// Publisher node id — best-effort and NOT an authenticated signal: on
    /// NATS it is a self-asserted, forgeable header; on redis/etcd/consul the
    /// wire envelope carries no sender, so it is filled from the subscriber's
    /// own node id. Use for diagnostics only, never for an authorization
    /// decision.
    pub from_node: String,
}

/// Stream of peer-lifecycle events. Returned by
/// [`ClusterBackend::watch_peers`].
pub type BoxPeerEventStream = Pin<Box<dyn futures_core::Stream<Item = PeerEvent> + Send + 'static>>;

/// Stream of published messages. Returned by
/// [`ClusterBackend::subscribe`].
pub type BoxPublishedMessageStream =
    Pin<Box<dyn futures_core::Stream<Item = PublishedMessage> + Send + 'static>>;

/// A handle to an acquired lease (leadership role or distributed
/// lock). Drop does NOT release — callers MUST call `release`
/// explicitly so a failure to release is visible rather than
/// silent. Drop-without-release leaves the lease to expire
/// naturally by TTL; that's the fallback but not the intended
/// path.
///
/// Returned as a boxed trait object so different backends can
/// ship wildly different handle internals (Raft log index, NATS
/// KV revision, Consul session id, etcd lease id, ...) without
/// leaking those details across the ABI.
///
/// Disambiguation: this trait is the *active* coordinator-managed
/// lease, complete with `renew` / `release` methods. The crate also
/// exposes a [`crate::LeaseHandle`] **struct** returned by the
/// primitive [`crate::Lease`] trait — that's an opaque snapshot of
/// lease metadata (name, holder, fence, expiry); the primitive
/// trait's CAS-style `renew` / `release` methods take it as an
/// argument rather than as `&self`.
#[async_trait::async_trait]
pub trait ActiveLease: Send + Sync {
    /// Strictly-monotonic fencing token. Per lock key / role,
    /// per coordinator lifetime. Consumers embed this in writes
    /// to backends that honour fencing (most modern distributed
    /// stores) so a lease that silently expired can't resurrect
    /// as a stale writer.
    fn fencing_token(&self) -> u64;

    /// Wall-clock expiry of the current lease grant. RFC3339
    /// string. Caller SHOULD renew before this time or release
    /// explicitly.
    fn expires_at(&self) -> String;

    /// Extend the lease by the same TTL it was acquired with.
    /// Returns [`ClusterError::LeaseExpired`] if the coordinator
    /// has already reassigned the lease to another node.
    async fn renew(&self) -> Result<(), ClusterError>;

    /// Release the lease, making the role / lock available to
    /// the next acquirer immediately (rather than on TTL).
    /// Idempotent — a double-release is a no-op.
    async fn release(&self) -> Result<(), ClusterError>;
}

/// Boxed [`ActiveLease`] — the coordinator-level handle returned
/// by `acquire_leadership` / `acquire_lock`.
pub type BoxActiveLease = Box<dyn ActiveLease>;

/// The cluster-coordinator entity trait. Spec §9.13.
///
/// Implementors expose four orthogonal primitives via the
/// `key_value_store` / `pub_sub` / `lease` / `watch` accessors plus
/// the coordinator-level surface (peer discovery, leader election,
/// distributed locks, broadcast publish/subscribe). Any subset of the
/// primitive accessors may return `None`.
///
/// Which gateway *slots* a coordinator can back is declared separately
/// via [`cluster_provides`](ClusterBackend::cluster_provides) (the
/// `cache` / `kv` / `bus` role vocabulary) — the gateway cross-checks
/// that role set against the plugin manifest's `provides` field (and,
/// for built-in kinds, the wiring fallback table) at boot and
/// fails-closed on drift.
#[async_trait::async_trait]
pub trait ClusterBackend: Send + Sync {
    fn manifest(&self) -> &PluginManifest;

    /// Set of slot roles this coordinator provides natively.
    ///
    /// The gateway calls this at boot to populate the role-set
    /// that `resolve_kind(SlotClass::*, KindRef { kind: "cluster", ... })`
    /// consults — refusing `kind: cluster` for any slot whose role
    /// isn't in the returned set, with a precise error pointing
    /// at the missing capability.
    ///
    /// Recognised role strings — `"cache"` / `"kv"` / `"bus"`
    /// (`mcpg_plugin_protocol::descriptor::CLUSTER_PROVIDES_ROLES`).
    /// This MUST agree with the coordinator's manifest `provides`
    /// field — the gateway cross-checks the two (plus, for built-in
    /// kinds, the wiring fallback table) at boot and fails-closed on
    /// drift.
    ///
    /// Default impl derives the role-set from `self.manifest().provides`
    /// so the manifest is the single authored source of truth.
    /// First-party coordinators populate `provides` in their manifest
    /// (mirrored in `plugin.yaml`); the default surfaces exactly that
    /// set here with no second hand-maintained list. A coordinator that
    /// genuinely computes its roles dynamically MAY override, but then
    /// it owns keeping the override and the manifest in sync (the boot
    /// cross-check enforces it).
    fn cluster_provides(&self) -> std::collections::BTreeSet<String> {
        self.manifest().provides.iter().cloned().collect()
    }

    /// Optional [`KeyValueStore`] primitive. Cluster plugins return
    /// `Some` when they provide durable namespaced KV (redis, nats
    /// JetStream, single-node memory/file). `None` for backends that
    /// don't ship a KV (consul / etcd in v0.1 — operators wire a
    /// per-capability `store:` override).
    fn key_value_store(&self) -> Option<Arc<dyn KeyValueStore>> {
        None
    }

    /// Optional [`PubSub`] primitive. `Some` for backends with
    /// transient topic-based fire-and-forget messaging (redis pub/sub,
    /// nats core, single-node broadcast); `None` otherwise.
    fn pub_sub(&self) -> Option<Arc<dyn PubSub>> {
        None
    }

    /// Optional [`Lease`] primitive. `Some` for backends with native
    /// or constructible split-brain-safe leases (redis SETNX + Lua,
    /// nats JetStream KV CAS, etcd lease, consul session, single-node
    /// always-acquire). Most cluster backends implement this.
    fn lease(&self) -> Option<Arc<dyn Lease>> {
        None
    }

    /// Optional [`Watch`] primitive. `Some` for backends with native
    /// change-notification feeds (etcd watch, nats KV watch, consul
    /// blocking queries, single-node broadcast); `None` otherwise.
    fn watch(&self) -> Option<Arc<dyn Watch>> {
        None
    }

    /// Information about this node. Used for admin surfaces +
    /// audit provenance (so audit events carry "which node
    /// emitted this" even in multi-node deployments).
    async fn node_info(&self) -> ClusterNodeInfo;

    /// Current list of peers as observed locally. Cheap snapshot
    /// — callers refresh on their own cadence.
    async fn list_peers(&self) -> Vec<ClusterPeer>;

    /// Stream of peer-lifecycle events. Subscribers get every
    /// Joined / Left / HealthChanged event the coordinator
    /// observes from the moment the stream is created.
    async fn watch_peers(&self) -> BoxPeerEventStream;

    /// Acquire leadership for the named role. Returns a lease
    /// handle whose `renew` keeps the lease alive + whose
    /// `release` relinquishes it. If another node holds the role,
    /// waits until that node's lease expires then takes over.
    /// Backends with synchronous leader election (Raft) return
    /// immediately when they already know who's leader.
    async fn acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError>;

    /// Non-blocking variant of [`Self::acquire_leadership`]. Returns
    /// `Ok(Some(lease))` on immediate success, `Ok(None)` when
    /// another node currently holds the role, `Err(...)` on
    /// backend failure.
    ///
    /// Use this from a tight refresh / poll loop where blocking
    /// waiting for leadership rotation would defeat the loop's
    /// purpose. The default impl falls back to `acquire_leadership`
    /// and is therefore blocking — backends that have a native
    /// non-blocking acquire (Consul `?cas=`, etcd lease+txn,
    /// JetStream KV CAS, redis SETNX) override it for true
    /// try-semantics. Backends that don't override pay no
    /// correctness penalty; they just pay the same wait cost as
    /// the blocking variant.
    async fn try_acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        self.acquire_leadership(role, lease_ttl).await.map(Some)
    }

    /// Distributed fenced lock. Non-reentrant: caller MUST NOT
    /// double-acquire the same key from the same node. Fencing
    /// token on the returned handle is strictly monotonic per
    /// key, per coordinator lifetime.
    async fn acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError>;

    /// Non-blocking variant of [`Self::acquire_lock`]. Same semantics
    /// as [`Self::try_acquire_leadership`] (`Ok(Some)` / `Ok(None)` /
    /// `Err`).
    ///
    /// Default impl is the blocking acquire wrapped in `Some`.
    /// Bundle-reload pre-tick hooks consume this exclusively —
    /// see `mcpg-bundle-reload`'s `PreTickHook` for the canonical
    /// "skip my tick if a peer holds the lock" pattern.
    async fn try_acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        self.acquire_lock(key, lease_ttl).await.map(Some)
    }

    /// Publish a notification. If `routing_key` is Some, the
    /// coordinator delivers only to peers subscribed with that
    /// routing_key.
    async fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Bytes,
    ) -> Result<(), ClusterError>;

    /// Subscribe to a topic. If `group` is Some, messages are
    /// load-balanced across every subscriber in the group (queue
    /// semantics). If `group` is None, every subscriber receives
    /// every message (broadcast).
    async fn subscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        routing_key: Option<&str>,
    ) -> Result<BoxPublishedMessageStream, ClusterError>;

    /// Called on gateway shutdown. Default is a no-op.
    async fn shutdown(&self) {}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_health_label_bounded() {
        assert_eq!(PeerHealth::Healthy.label(), "healthy");
        assert_eq!(PeerHealth::Degraded.label(), "degraded");
        assert_eq!(PeerHealth::Unreachable.label(), "unreachable");
    }

    #[test]
    fn peer_event_json_roundtrip() {
        let ev = PeerEvent::Joined {
            peer: ClusterPeer {
                node_id: "n1".into(),
                address: "10.0.0.1:7777".into(),
                last_seen: "2026-04-23T00:00:00Z".into(),
                health: PeerHealth::Healthy,
                roles: vec!["task-sweeper".into()],
            },
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: PeerEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);

        let ev = PeerEvent::Left {
            node_id: "n2".into(),
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: PeerEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);

        let ev = PeerEvent::HealthChanged {
            node_id: "n3".into(),
            health: PeerHealth::Degraded,
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: PeerEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn cluster_node_info_roundtrip() {
        let info = ClusterNodeInfo {
            node_id: "n1".into(),
            address: "10.0.0.1:7777".into(),
            version: "1.0.0".into(),
            started_at: "2026-04-23T00:00:00Z".into(),
            roles: vec!["task-sweeper".into()],
        };
        let s = serde_json::to_string(&info).unwrap();
        let back: ClusterNodeInfo = serde_json::from_str(&s).unwrap();
        assert_eq!(info, back);
    }

    #[test]
    fn published_message_roundtrip_preserves_bytes() {
        let m = PublishedMessage {
            topic: "t1".into(),
            routing_key: Some("rk".into()),
            payload: Bytes::from_static(b"hello"),
            from_node: "n1".into(),
        };
        let s = serde_json::to_string(&m).unwrap();
        let back: PublishedMessage = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }
}
