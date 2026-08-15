# mcpg-cluster-api

> The trait surface for the MCPG cluster backbone: four orthogonal coordination primitives plus the backend that bundles them.

This crate defines the contracts a cluster backend implements so a fleet of MCPG
gateways can share state, coordinate, and elect leaders without any capability in
the gateway knowing which backing store is in play. The four primitives are
deliberately orthogonal, because real systems fuse them differently — etcd's
leases ride its KV, Redis's leases are compare-and-set scripts, ZooKeeper-style
ephemeral nodes ride neither — so each backend implements the subset it can
honour and the gateway wires an `Arc<dyn …>` per capability. This crate is
contracts only: no backend implementation lives here, and nothing in it opens a
connection.

## What's here
- `KeyValueStore` — durable namespaced key/value: `get`, `put`, `put_if_absent`,
  `delete`, `list_prefix`, `expire`, returning `Entry { bytes, expires_at }`.
  `put_if_absent` is the cross-replica single-winner claim primitive behind
  idempotency reservations and race-free resource claims, and it deliberately
  has no default implementation so that no backend can satisfy it with a
  non-atomic get-then-put.
- `PubSub` — `publish` and `subscribe(pattern, queue_group)` over a
  `Message { topic, payload }` stream. At-most-once with no replay; the
  cancellation and delivery buses use it.
- `Lease` — split-brain-safe distributed locks: `try_acquire`, `renew`,
  `release`, `current_holder`, handing back a `LeaseHandle` carrying a strictly
  monotonic `FenceToken`. Backends must keep that token increasing for a given
  name across crashes and restarts, so a slow holder cannot write past a new
  acquirer.
- `Watch` — `watch_prefix` returning an ordered, replayable-within-retention
  `WatchStream` of `WatchEvent { key, kind, value }` with
  `WatchEventKind::{Created, Updated, Deleted}`. The durable counterpart to
  `PubSub`, used for cache invalidation, config reload and membership feeds.
- `ClusterBackend` — the higher-level trait a cluster plugin implements. It
  hands out whichever primitives it has (`key_value_store`, `pub_sub`, `lease`,
  `watch`, each returning `None` by default), declares which gateway slot roles
  it can back via `cluster_provides` (`cache`, `kv`, `bus`, derived from the
  plugin manifest), exposes peer discovery (`node_info`, `list_peers`,
  `watch_peers` over `ClusterNodeInfo`, `ClusterPeer`, `PeerEvent`,
  `PeerHealth`), leadership (`acquire_leadership`,
  `try_acquire_leadership`, yielding a `BoxActiveLease` whose `ActiveLease`
  exposes `fencing_token`, `renew` and `release`), locking (`acquire_lock`,
  `try_acquire_lock`), message fan-out (`publish`, `subscribe`), and `shutdown`.
- `ClusterError` — the shared failure vocabulary: `NotFound`, `CasConflict`,
  `Precondition`, `BackendUnavailable`, `Unsupported`, `NotLeader`,
  `LeaseExpired`, `InvalidReference`, `Timeout`, `Shutdown` and `Internal`,
  each with a stable `kind_label()`.
- `test_suite::run_kv_contract` (behind the non-default `test-suite` feature) —
  a shared `KeyValueStore` conformance battery a backend points at a factory for
  its own store: single-winner `put_if_absent`, idempotent delete, prefix
  filtering, TTL purge and overwrite semantics.

## Used by
- The cluster backend plugins under `libs/plugins/cluster/` — `redis`, `nats`,
  `consul` and `etcd` — plus their shared equivalence-test suites. The gateway's
  built-in `single_node` mode needs no plugin.
- `apps/gateway`, where sessions, tasks, pipelines, subscriptions and the
  cancellation and delivery buses consume the primitives directly.
- `libs/plugin-host` and `libs/plugin-sdk`, which carry the primitives across the
  plugin boundary, and the policy and identity plugins that take a lease to
  coordinate bundle refresh.

## Build / test
```bash
cargo build -p mcpg-cluster-api
cargo test  -p mcpg-cluster-api --features test-suite
```

## Licence
Apache-2.0.

## See also
- [Clustering and high availability](https://mcpg.dev/docs/self-hosting/clustering) — how an operator selects a backend with `cluster.kind`.
- [Plugins and the plugin protocol](https://mcpg.dev/docs/plugins/plugins-and-protocol)
- `libs/plugins/cluster/redis` — a complete implementation to read alongside the traits.
