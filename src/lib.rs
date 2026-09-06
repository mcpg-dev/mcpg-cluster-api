//! Trait surface for the MCPG cluster backbone.
//!
//! Every cluster backend (single-node memory/file, redis, nats, …)
//! exposes a subset of two orthogonal primitives — backends advertise
//! which they support, capabilities declare which they depend on, and
//! the gateway wires `Arc<dyn ...>` per capability.
//!
//! ## Primitives
//!
//! - [`KeyValueStore`] — durable namespaced key/value (get / put /
//!   delete / list_prefix / expire / incr). Every cluster backend
//!   implements this; it's the canonical state surface. `incr` is
//!   the atomic cross-replica counter (rate-limit accounting,
//!   monotonic sequences); `put_if_absent` is the single-winner
//!   claim.
//! - [`PubSub`] — transient topic-based fire-and-forget messaging.
//!   At-most-once, no replay. Used for cancellation / delivery buses.
//!
//! Distributed mutual exclusion is NOT a primitive: leadership and
//! fenced locks live on the coordinator surface itself
//! ([`ClusterBackend::acquire_leadership`] /
//! [`ClusterBackend::acquire_lock`], returning [`ActiveLease`]
//! handles with fencing tokens).
//!
//! ## Backend support matrix
//!
//! | Backend       | KeyValueStore | PubSub |
//! |---------------|---------------|--------|
//! | single-node   | ✓             | ✓      |
//! | redis         | ✓             | ✓      |
//! | nats (JS)     | ✓             | ✓      |
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
pub mod pub_sub;
#[cfg(feature = "test-suite")]
pub mod test_suite;

pub use backend::{
    ActiveLease, BoxActiveLease, BoxPeerEventStream, BoxPublishedMessageStream, ClusterBackend,
    ClusterNodeInfo, ClusterPeer, PeerEvent, PeerHealth, PublishedMessage,
};
pub use error::ClusterError;
pub use key_value::{
    Entry, KeyValueStore, KvEntryWire, KvExpireArgs, KvIncrArgs, KvKeyArgs, KvListEntryWire,
    KvListPrefixArgs, KvPutArgs, counter_overflow, parse_counter,
};
pub use pub_sub::{Message, PubSub, Subscription};
