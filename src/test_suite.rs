//! Shared contract test suite for [`KeyValueStore`] implementations.
//!
//! Every backend (single-node, redis, nats) runs this battery to
//! prove it satisfies the `KeyValueStore` contract. New impls add a
//! tiny integration test that calls into [`run_kv_contract`] with a
//! factory that produces a fresh, isolated store instance.
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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::key_value::KeyValueStore;

/// Run the `KeyValueStore` contract battery against `make_state`. Panics
/// on the first contract violation; backends that pass the entire
/// suite satisfy the trait.
pub async fn run_kv_contract<F, Fut>(make_state: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    get_missing_returns_none(&make_state).await;
    put_then_get_roundtrips(&make_state).await;
    delete_idempotent(&make_state).await;
    list_prefix_filters_correctly(&make_state).await;
    expire_updates_ttl(&make_state).await;
    ttl_purges_keys(&make_state).await;
    overwrite_replaces_value_and_ttl(&make_state).await;
    put_if_absent_is_single_winner(&make_state).await;
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

async fn ttl_purges_keys<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put(
        "k",
        Bytes::from_static(b"v"),
        Some(Duration::from_millis(50)),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let v = s.get("k").await.unwrap();
    assert!(v.is_none(), "key must be purged after TTL elapses");
}

async fn overwrite_replaces_value_and_ttl<F, Fut>(make_state: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn KeyValueStore>>,
{
    let s = make_state().await;
    s.put(
        "k",
        Bytes::from_static(b"old"),
        Some(Duration::from_millis(50)),
    )
    .await
    .unwrap();
    s.put("k", Bytes::from_static(b"new"), None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    // Overwrite cleared the TTL; key must still exist.
    let v = s.get("k").await.unwrap().expect("overwrite must clear TTL");
    assert_eq!(&v.bytes[..], b"new");
}
