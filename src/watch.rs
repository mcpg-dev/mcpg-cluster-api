//! `Watch` — durable key-prefix change-notification feed.
//!
//! Differs from [`crate::pub_sub::PubSub`] by being **ordered and
//! replayable within retention**: a subscriber that connects after
//! a write still sees the latest state and the change event when
//! the backend supports it (etcd, NATS KV, Redis keyspace events).

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::error::ClusterError;

/// Type of change observed on a watched key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEventKind {
    /// A new key was created.
    Created,
    /// An existing key's value was updated.
    Updated,
    /// The key was deleted (explicitly or via TTL).
    Deleted,
}

/// One change notification on a watched prefix.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub key: String,
    pub kind: WatchEventKind,
    /// The new value for `Created` / `Updated`, or the last-known
    /// value for `Deleted` if the backend can supply it. Backends
    /// that can't deliver a payload on delete return None.
    pub value: Option<Bytes>,
}

/// Type alias for the boxed stream returned by `watch_prefix`.
pub type WatchStream = BoxStream<'static, Result<WatchEvent, ClusterError>>;

/// Durable change-notification feed over a key prefix.
///
/// Implemented by backends with native change-notification:
/// - etcd: native Watch
/// - NATS JetStream: KV watch streams
/// - Redis: keyspace notifications via PSUBSCRIBE on `__keyspace@<db>__:<prefix>*`
/// - Consul: long-poll on the catalog index
/// - Single-node: in-process broadcast over the in-memory KV
#[async_trait]
pub trait Watch: Send + Sync + std::fmt::Debug {
    /// Subscribe to changes on every key under `prefix`. The
    /// returned stream yields events as they arrive; closing the
    /// stream cancels the subscription. The stream may emit a
    /// terminal `Err(ClusterError::BackendUnavailable)` on connection failure;
    /// callers may resubscribe.
    async fn watch_prefix(&self, prefix: &str) -> Result<WatchStream, ClusterError>;
}
