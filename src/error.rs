//! Unified error type for cluster operations (primitives + coordinator).

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Failure modes returned by every cluster surface in this crate —
/// the four primitives ([`crate::KeyValueStore`], [`crate::PubSub`],
/// [`crate::Lease`], [`crate::Watch`]) plus the higher-level
/// [`crate::ClusterBackend`] (peers, leadership, distributed
/// locks, broadcast publish).
///
/// One unified enum keeps the trait surfaces ergonomic — callers
/// pattern-match on the variant they care about and bubble the rest
/// up. Backends translate native errors into one of these variants;
/// the variant tells the caller whether to retry, surface, or give
/// up.
#[derive(Debug, Clone, Error, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClusterError {
    /// Key not found. Returned by `get` / `delete` when the key
    /// does not exist; not returned for missing values inside lists.
    #[error("cluster: key not found: {key}")]
    NotFound { key: String },

    /// Compare-and-swap conflict. Used by [`crate::Lease`] when a
    /// renew/release sees a different holder or fence token than
    /// expected.
    #[error("cluster: cas conflict on `{key}`: {reason}")]
    CasConflict { key: String, reason: String },

    /// Backend rejected the request because of a precondition
    /// (TTL expired, lease lost, value too large, …).
    #[error("cluster: precondition failed: {reason}")]
    Precondition { reason: String },

    /// Transport / network / protocol failure. The backend (Raft,
    /// JetStream, Consul, etcd, redis, …) is unreachable or down.
    /// Generally retryable at the caller's discretion.
    #[error("cluster: backend unavailable: {reason}")]
    BackendUnavailable { reason: String },

    /// The backend cannot satisfy the requested operation
    /// (e.g. a single-node primitive backed by an impl that doesn't
    /// support cluster-shared state).
    #[error("cluster: unsupported operation: {reason}")]
    Unsupported { reason: String },

    /// Caller asked for a leader-only operation but doesn't hold
    /// the leadership lease for the role.
    #[error("cluster: not leader for role '{role}'")]
    NotLeader { role: String },

    /// Lease expired between acquire and use; caller MUST re-
    /// acquire. Returned from `renew` when the coordinator has
    /// already handed the lease to another node.
    #[error("cluster: lease expired")]
    LeaseExpired,

    /// Lock key / topic / role is malformed (wrong shape, empty,
    /// contains reserved characters).
    #[error("cluster: invalid reference: {message}")]
    InvalidReference { message: String },

    /// Operation timed out. Lease TTL exceeded without acquiring,
    /// pub/sub delivery stalled past the backend's deadline, etc.
    #[error("cluster: operation timed out")]
    Timeout,

    /// Backend is draining or shutting down.
    #[error("cluster: backend shutting down")]
    Shutdown,

    /// Internal invariant violated; bug rather than environmental.
    /// Catch-all for backend-specific failures that don't fit the
    /// other variants — operators drill in by reading `reason` in
    /// logs.
    #[error("cluster: internal: {reason}")]
    Internal { reason: String },
}

impl ClusterError {
    /// Bounded metrics label.
    #[must_use]
    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => "not_found",
            Self::CasConflict { .. } => "cas_conflict",
            Self::Precondition { .. } => "precondition",
            Self::BackendUnavailable { .. } => "backend_unavailable",
            Self::Unsupported { .. } => "unsupported",
            Self::NotLeader { .. } => "not_leader",
            Self::LeaseExpired => "lease_expired",
            Self::InvalidReference { .. } => "invalid_reference",
            Self::Timeout => "timeout",
            Self::Shutdown => "shutdown",
            Self::Internal { .. } => "internal",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_label_bounded() {
        assert_eq!(
            ClusterError::NotFound { key: "k".into() }.kind_label(),
            "not_found"
        );
        assert_eq!(
            ClusterError::BackendUnavailable {
                reason: "EIO".into()
            }
            .kind_label(),
            "backend_unavailable"
        );
        assert_eq!(
            ClusterError::NotLeader {
                role: "replay-compactor".into()
            }
            .kind_label(),
            "not_leader"
        );
        assert_eq!(ClusterError::LeaseExpired.kind_label(), "lease_expired");
        assert_eq!(ClusterError::Timeout.kind_label(), "timeout");
        assert_eq!(ClusterError::Shutdown.kind_label(), "shutdown");
    }

    #[test]
    fn display_includes_detail() {
        let e = ClusterError::BackendUnavailable {
            reason: "etcd connection refused".into(),
        };
        assert!(e.to_string().contains("etcd connection refused"));

        let e = ClusterError::NotLeader {
            role: "task-sweeper".into(),
        };
        assert!(e.to_string().contains("task-sweeper"));
    }

    #[test]
    fn json_roundtrip() {
        let e = ClusterError::NotLeader { role: "foo".into() };
        let s = serde_json::to_string(&e).unwrap();
        let back: ClusterError = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }
}
