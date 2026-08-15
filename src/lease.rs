//! `Lease` — split-brain-safe distributed mutual exclusion.
//!
//! A lease is a time-bounded distributed lock with a monotonically
//! increasing fence token. Holders include the token in fencing
//! checks against shared resources so a slow holder can't write
//! past a new acquirer.

use async_trait::async_trait;
use std::time::Duration;

use crate::error::ClusterError;

/// A monotonically-increasing token minted on every successful
/// lease acquisition. Holders include this token in fencing checks
/// against shared resources so a slow holder can't write past a
/// new acquirer (the canonical "fencing token" pattern).
///
/// Backends MUST guarantee strict monotonicity: the token returned
/// by every successful `try_acquire` for the same `name` must be
/// strictly greater than every previously-issued token for that
/// name, even across crashes / restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FenceToken(pub u64);

/// Handle to an active lease. Holders renew before expiry and
/// release explicitly when done. Dropping a `LeaseHandle` without
/// releasing is permitted — the backend's TTL cleans up — but
/// release is preferred for prompt re-acquisition.
#[derive(Debug, Clone)]
pub struct LeaseHandle {
    /// The lease name (cluster-wide unique).
    pub name: String,
    /// Identity of the current holder (operator-supplied; usually
    /// node id + uuid). Backends use this to authorize renew /
    /// release, rejecting requests from a different holder.
    pub holder: String,
    /// Monotonic fence token. See [`FenceToken`].
    pub fence: FenceToken,
    /// Wall-clock instant after which the lease auto-expires
    /// unless renewed.
    pub expires_at: std::time::SystemTime,
}

/// Coordinated leases — split-brain-safe distributed mutual exclusion.
///
/// Implemented by backends with a real coordination primitive:
/// - Redis: SETNX + Lua scripts; fence via `INCR <name>:fence`
/// - NATS JetStream: KV CAS; fence = revision number
/// - etcd: native lease primitive; fence = `mod_revision`
/// - Consul: Sessions + KV CAS; fence = `LockIndex`
///
/// Single-node backends provide a trivial always-acquire
/// implementation (in-process counter for fence tokens). Capability
/// config validates that the cluster backend wired to a
/// lease-requiring slot actually provides `Lease`.
#[async_trait]
pub trait Lease: Send + Sync + std::fmt::Debug {
    /// Try to acquire the lease for `name` on behalf of `holder`,
    /// for `ttl`. Returns:
    /// - `Ok(Some(handle))` — the lease was acquired (or was already
    ///   held by `holder` and got refreshed).
    /// - `Ok(None)` — another holder owns the lease and it has not
    ///   expired.
    /// - `Err(_)` — backend failure.
    async fn try_acquire(
        &self,
        name: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseHandle>, ClusterError>;

    /// Renew an active lease, extending its expiry by `ttl`. The
    /// caller's `holder` MUST match the current owner; mismatches
    /// return [`ClusterError::CasConflict`].
    async fn renew(&self, lease: &LeaseHandle, ttl: Duration) -> Result<LeaseHandle, ClusterError>;

    /// Release an active lease. Idempotent against double-release
    /// (returns `Ok(())` even when the lease has already expired
    /// or been re-acquired). `holder` mismatch returns
    /// [`ClusterError::CasConflict`].
    async fn release(&self, lease: &LeaseHandle) -> Result<(), ClusterError>;

    /// Inspect the current holder of `name` without acquiring.
    /// Returns `Ok(None)` when no holder exists (or the lease
    /// has expired).
    async fn current_holder(&self, name: &str) -> Result<Option<LeaseHandle>, ClusterError>;
}
