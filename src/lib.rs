//! Trait surface for the MCPG cluster backbone.
//!
//! Every cluster backend (single-node memory/file, redis, nats, …)
//! exposes a subset of four orthogonal primitives — backends advertise
//! which they support, capabilities declare which they depend on, and
//! the gateway wires `Arc<dyn ...>` per capability.
//!
//! ## Primitives
//!
//! - [`KeyValueStore`] — durable namespaced key/value (get / put /
//!   delete / list_prefix / expire). Every cluster backend implements
//!   this; it's the canonical state surface.
//! - [`PubSub`] — transient topic-based fire-and-forget messaging.
//!   At-most-once, no replay. Used for cancellation / delivery buses.
//! - [`Lease`] — split-brain-safe distributed locks with monotonic
//!   fence tokens. Used for leader election and atomic ownership of
//!   shared resources (pipeline-claim, janitor leadership).
//! - [`Watch`] — durable change-notification feed over a key prefix.
//!   Ordered, replayable within retention. Used for cache
//!   invalidation, config-reload, peer-membership change feeds.
//!
//! ## Why orthogonal
//!
//! Each backend implements the subset it naturally supports.
//! ZooKeeper-style ephemeral nodes don't ride a KV; etcd's KV +
//! leases are tightly fused; Redis's leases are KV-CAS scripts.
//! The trait surface doesn't dictate the implementation shape —
//! it just says "here is what 'a lease' looks like to a caller."
//!
//! ## Backend support matrix
//!
//! | Backend       | KeyValueStore | PubSub | Lease | Watch |
//! |---------------|---------------|--------|-------|-------|
//! | single-node   | ✓             | ✓      | ✓     | ✓     |
//! | redis         | ✓             | ✓      | ✓     | ✓     |
//! | nats (JS)     | ✓             | ✓      | ✓     | ✓     |
//! | consul        | (deferred)    | (deferred — Events) | ✓ | ✓ |
//! | etcd          | (deferred)    | (deferred — Watch) | ✓ | ✓ |
//!
//! ## Operator config
//!
//! ```yaml
//! cluster:
//!   kind: redis
//!   url: ${env.REDIS_URL}
//!   key_prefix: mcpg:cluster:
//!
//! # Optional per-capability overrides:
//! mcp:
//!   configurations:
//!     sessions:
//!       store: { kind: file, dir: /var/lib/mcpg/sessions }
//! ```

pub mod backend;
pub mod error;
pub mod key_value;
pub mod lease;
pub mod pub_sub;
#[cfg(feature = "test-suite")]
pub mod test_suite;
pub mod watch;

pub use backend::{
    ActiveLease, BoxActiveLease, BoxPeerEventStream, BoxPublishedMessageStream, ClusterBackend,
    ClusterNodeInfo, ClusterPeer, PeerEvent, PeerHealth, PublishedMessage,
};
pub use error::ClusterError;
pub use key_value::{
    Entry, KeyValueStore, KvEntryWire, KvExpireArgs, KvKeyArgs, KvListEntryWire, KvListPrefixArgs,
    KvPutArgs,
};
pub use lease::{FenceToken, Lease, LeaseHandle};
pub use pub_sub::{Message, PubSub, Subscription};
pub use watch::{Watch, WatchEvent, WatchEventKind, WatchStream};
