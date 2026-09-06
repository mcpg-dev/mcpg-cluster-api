//! Shared contract test suite for [`KeyValueStore`] implementations.
//!
//! Every backend (the built-in memory/file stores, redis, nats —
//! directly and through the coordinator-equivalence fixtures)
//! runs this battery to prove it satisfies the `KeyValueStore`
//! contract. New impls add a tiny test that calls into
//! [`run_kv_contract`] with a factory that produces a fresh, isolated
//! store view per call (a fresh instance, or a unique key-namespace
//! wrapper over a shared backend).
//!
//! Enable via the `test-suite` Cargo feature (dev-only):
//!
//! ```ignore
//! [dev-dependencies]
//! mcpg-cluster-api = { path = "../../../cluster-api", features = ["test-suite"] }
//!
//! #[tokio::test]
//! async fn kv_contract() {
//!     mcpg_cluster_api::test_suite::run_kv_contract(|| async {
//!         std::sync::Arc::new(MyKeyValueStore::new())
//!     }).await;
//! }
//! ```
//!
//! Backends whose TTL granularity is coarser than milliseconds (nats'
//! inline-TTL envelope and the file store clamp to whole seconds ≥ 1)
//! run the same battery through
//! [`run_kv_contract_with`] with a scaled [`KvContractTiming`] instead
//! of skipping the TTL cases.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::key_value::KeyValueStore;

/// Timing knobs for the TTL cases, so second-granularity backends run
/// the same assertions on a scaled clock instead of silently skipping
/// them.
#[derive(Debug, Clone, Copy)]
pub struct KvContractTiming {
    /// The short TTL the suite writes. MUST be at least the backend's
    /// TTL granularity (1 s for nats / file; the millisecond
    /// default suits memory / redis).
    pub short_ttl: Duration,
    /// Extra slack added on top of `short_ttl` before asserting an
    /// entry has expired. Covers granularity truncation + scheduler
    /// jitter; keep it ≥ one granularity unit.
    pub expiry_slack: Duration,
}

impl Default for KvContractTiming {
    fn default() -> Self {
        Self {
            short_ttl: Duration::from_millis(50),
            expiry_slack: Duration::from_millis(120),
        }
    }
}

impl KvContractTiming {
    /// Timing for backends whose TTLs round to whole seconds (nats,
    /// file).
    #[must_use]
    pub fn seconds_granularity() -> Self {
        Self {
            short_ttl: Duration::from_secs(1),
            expiry_slack: Duration::from_millis(1500),
        }
    }
}

/// Suspend the current thread. The suite is deliberately dep-free (no
/// async runtime), so waits block the thread instead of yielding; every
/// caller drives the battery from a test-owned runtime where nothing
/// else needs the thread while the suite waits.
async fn pause(d: Duration) {
    std::thread::sleep(d);
}

/// Run the `KeyValueStore` contract battery against `make_state` with
/// default (millisecond-granularity) timing. Panics on the first
/// contract violation; backends that pass the entire suite satisfy the
/// trait.
pub async fn run_kv_contract<F, Fut>(make_state: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    run_kv_contract_with(KvContractTiming::default(), make_state).await;
}

/// [`run_kv_contract`] with explicit timing — for backends whose TTL
/// granularity is coarser than the default.
pub async fn run_kv_contract_with<F, Fut>(timing: KvContractTiming, make_state: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    get_missing_returns_none(&make_state).await;
    put_then_get_roundtrips(&make_state).await;
    delete_idempotent(&make_state).await;
    list_prefix_filters_correctly(&make_state).await;
    expire_updates_ttl(&make_state).await;
    ttl_purges_keys(timing, &make_state).await;
    overwrite_replaces_value_and_ttl(timing, &make_state).await;
    put_if_absent_is_single_winner(&make_state).await;
    incr_missing_key_starts_at_zero(&make_state).await;
    incr_accumulates_and_roundtrips_via_get(&make_state).await;
    incr_no_lost_updates_under_interleaved_callers(&make_state).await;
    incr_rejects_non_integer_values(&make_state).await;
    incr_ttl_slides_and_purges(timing, &make_state).await;
}

async fn put_if_absent_is_single_winner<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    // First claim on an absent key wins.
    let first = s
        .put_if_absent("claim", Bytes::from_static(b"a"), None)
        .await
        .unwrap();
    assert!(first, "put_if_absent on an absent key must return true");
    // Second claim on the live key loses and must NOT overwrite.
    let second = s
        .put_if_absent("claim", Bytes::from_static(b"b"), None)
        .await
        .unwrap();
    assert!(!second, "put_if_absent on a present key must return false");
    let v = s.get("claim").await.unwrap().expect("claim key exists");
    assert_eq!(
        &v.bytes[..],
        b"a",
        "a losing put_if_absent must not overwrite the winner's value"
    );
    // After delete, the slot is claimable again.
    assert!(s.delete("claim").await.unwrap());
    let third = s
        .put_if_absent("claim", Bytes::from_static(b"c"), None)
        .await
        .unwrap();
    assert!(third, "put_if_absent after delete must return true");
}

async fn get_missing_returns_none<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    let v = s.get("nope").await.unwrap();
    assert!(v.is_none(), "get on missing key must return None");
}

async fn put_then_get_roundtrips<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put("k", Bytes::from_static(b"v"), None).await.unwrap();
    let v = s.get("k").await.unwrap().expect("just-put key must exist");
    assert_eq!(&v.bytes[..], b"v");
}

async fn delete_idempotent<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put("k", Bytes::from_static(b"v"), None).await.unwrap();
    let first = s.delete("k").await.unwrap();
    assert!(first, "first delete returns true");
    let second = s.delete("k").await.unwrap();
    assert!(!second, "double-delete is idempotent (returns false)");
    assert!(
        s.get("k").await.unwrap().is_none(),
        "deleted key must be gone"
    );
}

async fn list_prefix_filters_correctly<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put("a:1", Bytes::from_static(b"a1"), None).await.unwrap();
    s.put("a:2", Bytes::from_static(b"a2"), None).await.unwrap();
    s.put("b:1", Bytes::from_static(b"b1"), None).await.unwrap();
    let entries = s.list_prefix("a:", 100).await.unwrap();
    assert_eq!(entries.len(), 2, "prefix `a:` must yield exactly 2 entries");
    let keys: std::collections::BTreeSet<_> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains("a:1"));
    assert!(keys.contains("a:2"));
    assert!(!keys.contains("b:1"));
}

async fn expire_updates_ttl<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put("k", Bytes::from_static(b"v"), None).await.unwrap();
    let updated = s.expire("k", Some(Duration::from_secs(60))).await.unwrap();
    assert!(updated, "expire on existing key returns true");
    let missing = s
        .expire("nope", Some(Duration::from_secs(60)))
        .await
        .unwrap();
    assert!(!missing, "expire on missing key returns false");
}

async fn ttl_purges_keys<F, Fut>(timing: KvContractTiming, make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put("k", Bytes::from_static(b"v"), Some(timing.short_ttl))
        .await
        .unwrap();
    pause(timing.short_ttl + timing.expiry_slack).await;
    let v = s.get("k").await.unwrap();
    assert!(v.is_none(), "key must be purged after TTL elapses");
}

async fn overwrite_replaces_value_and_ttl<F, Fut>(timing: KvContractTiming, make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put("k", Bytes::from_static(b"old"), Some(timing.short_ttl))
        .await
        .unwrap();
    s.put("k", Bytes::from_static(b"new"), None).await.unwrap();
    pause(timing.short_ttl + timing.expiry_slack).await;
    // Overwrite cleared the TTL; key must still exist.
    let v = s.get("k").await.unwrap().expect("overwrite must clear TTL");
    assert_eq!(&v.bytes[..], b"new");
}

async fn incr_missing_key_starts_at_zero<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    let v = s.incr("ctr", 5, None).await.unwrap();
    assert_eq!(v, 5, "incr on a missing key must start from 0");
    // After delete, the counter restarts at 0.
    assert!(s.delete("ctr").await.unwrap());
    let v = s.incr("ctr", 3, None).await.unwrap();
    assert_eq!(v, 3, "incr after delete must start from 0 again");
}

async fn incr_accumulates_and_roundtrips_via_get<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    assert_eq!(s.incr("ctr", 10, None).await.unwrap(), 10);
    assert_eq!(s.incr("ctr", -3, None).await.unwrap(), 7, "negative delta");
    assert_eq!(s.incr("ctr", 0, None).await.unwrap(), 7, "zero delta reads");
    // The counter is stored as ASCII base-10 — `get` sees the digits.
    let raw = s.get("ctr").await.unwrap().expect("counter key exists");
    assert_eq!(
        &raw.bytes[..],
        b"7",
        "counter must round-trip through get as ASCII base-10"
    );
}

/// No lost updates with many increments in flight at once. The callers
/// here interleave on one task (the suite carries no runtime to spawn
/// real threads with); backends with an OS-thread race window pair this
/// with their own spawn-based test next to the impl.
async fn incr_no_lost_updates_under_interleaved_callers<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    use futures::stream::{FuturesUnordered, StreamExt};
    let s = make_state().await;
    const CALLERS: usize = 8;
    const PER_CALLER: usize = 5;
    let mut in_flight: FuturesUnordered<_> = (0..CALLERS * PER_CALLER)
        .map(|_| {
            let s = Arc::clone(&s);
            async move { s.incr("ctr", 1, None).await.unwrap() }
        })
        .collect();
    let mut seen = std::collections::BTreeSet::new();
    while let Some(v) = in_flight.next().await {
        assert!(
            seen.insert(v),
            "two callers observed the same post-increment value {v}"
        );
    }
    let total = s.incr("ctr", 0, None).await.unwrap();
    assert_eq!(
        total,
        (CALLERS * PER_CALLER) as i64,
        "increments must not be lost under concurrent callers"
    );
}

async fn incr_rejects_non_integer_values<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put("blob", Bytes::from_static(b"not-a-number"), None)
        .await
        .unwrap();
    let r = s.incr("blob", 1, None).await;
    assert!(
        r.is_err(),
        "incr on a non-integer value must error, got {r:?}"
    );
    // The bad value is left intact — a failed incr must not clobber it.
    let raw = s.get("blob").await.unwrap().expect("value survives");
    assert_eq!(&raw.bytes[..], b"not-a-number");
}

async fn incr_ttl_slides_and_purges<F, Fut>(timing: KvContractTiming, make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    // A TTL'd counter left idle purges; the next incr restarts at 0.
    let ttl = timing.short_ttl;
    assert_eq!(s.incr("idle", 7, Some(ttl)).await.unwrap(), 7);
    pause(ttl + timing.expiry_slack).await;
    assert_eq!(
        s.incr("idle", 2, Some(ttl)).await.unwrap(),
        2,
        "an expired counter must restart from 0"
    );

    // The TTL is sliding: each incr re-arms it, so a refreshed counter
    // survives past the FIRST call's expiry. The waits leave a full
    // granularity unit of slack on both sides, because a
    // whole-second backend (nats / file) may truncate a TTL's
    // effective lifetime by up to one unit:
    //   t0        incr, ttl = 3u   (worst-case lifetime 2u..3u)
    //   t0+1.5u   incr, ttl = 3u   (before the first arm can lapse;
    //                               worst-case new expiry t0+3.5u)
    //   t0+3.2u   get → Some       (past the first arm's nominal 3u,
    //                               before the refreshed arm's 3.5u)
    let unit = timing.short_ttl;
    let live_ttl = unit * 3;
    assert_eq!(s.incr("live", 1, Some(live_ttl)).await.unwrap(), 1);
    pause(unit + unit / 2).await;
    assert_eq!(
        s.incr("live", 1, Some(live_ttl)).await.unwrap(),
        2,
        "refresh before expiry must see the accumulated count"
    );
    pause(unit + unit / 2 + unit / 5).await;
    let v = s.get("live").await.unwrap();
    assert!(
        v.is_some(),
        "a refreshed counter must outlive the first call's TTL arm"
    );
    // And once left idle, the refreshed arm purges it too.
    pause(live_ttl + timing.expiry_slack).await;
    assert!(
        s.get("live").await.unwrap().is_none(),
        "an idle counter must purge after its sliding TTL"
    );
}
