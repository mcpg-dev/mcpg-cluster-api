//! `PubSub` — the transient topic-based fire-and-forget primitive.
//!
//! Used by cancellation and delivery buses for cluster-wide
//! signalling. Distinct from [`crate::watch::Watch`]: pub/sub is
//! at-most-once with no replay; watch is durable + replayable
//! within retention.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::error::ClusterError;

/// One message delivered on a topic subscription.
#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub payload: Bytes,
}

/// Type alias for the boxed stream returned by `subscribe`.
pub type Subscription = BoxStream<'static, Result<Message, ClusterError>>;

/// Topic-based pub/sub primitive.
///
/// Implemented by backends with native pub/sub:
/// - Single-node: in-process tokio::broadcast
/// - Redis: PUBLISH / PSUBSCRIBE
/// - NATS: Core NATS publish + subscribe (queue-group aware)
/// - etcd / Consul: emulated via Watch / Events (best-effort gossip)
///
/// Delivery is at-most-once and best-effort. Topic patterns may
/// include `*` (single-token wildcard) and `>` (multi-token, NATS
/// convention; backends translate as needed).
#[async_trait]
pub trait PubSub: Send + Sync + std::fmt::Debug {
    /// Publish `payload` on `topic`. Fire-and-forget — returns Ok
    /// once the broker / channel has accepted the message; does
    /// not wait for any subscriber to receive it.
    async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), ClusterError>;

    /// Subscribe to `pattern` (literal topic or wildcard). The
    /// returned stream yields messages until dropped. Closing the
    /// stream cancels the subscription.
    ///
    /// `queue_group` (when supported) load-balances messages
    /// across subscribers in the same group; backends without
    /// queue-group support ignore the parameter and broadcast.
    async fn subscribe(
        &self,
        pattern: &str,
        queue_group: Option<&str>,
    ) -> Result<Subscription, ClusterError>;
}
