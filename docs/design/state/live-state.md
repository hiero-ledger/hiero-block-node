# Live Hashgraph State — Design Doc

> Status: **Beta / experimental** — not yet listed in chart deployments. Plugins
> outside the chart manifest are considered beta. See
> `docs/block-node/architecture/architecture-overview.md`.

## 1. Purpose & Goals

Maintain a local, queryable copy of the Hashgraph network state inside the Block
Node so that other plugins and external clients can answer state queries
(`getBinaryKV`, `getBinarySingleton`, `getBinaryQueue`) without depending on a
Consensus Node.

Initial scope (this epic):

1. A single new plugin, `state-management-hashgraph`, owning state load, apply,
   snapshot, and binary state query.
2. Persistence of state-on-disk snapshots under
   `${stateSnapshotRecentPath}/<blockNumber>` matching the consensus-node
   `data/state/` spec (see
   `hiero-consensus-node/platform-sdk/swirlds-state-api/docs/state-snapshot-spec.md`).
3. New protobuf surface: `StateMetadata`, `BinaryStateQuery`,
   `BinaryStateQueryResponse`, and three `getBinary*` RPCs on `StateService`.
   `StateMetadata` is returned on every `BinaryStateQueryResponse` so a client
   can see which block the latest state corresponds to.

Explicitly out of scope:

- Historical state queries (only the current immutable snapshot is queryable).
- Remote archival / S3 of snapshots.
- Block stream **filtering** — separate epic, tracked under
  `agent/ref/state-and-filtering/state-and-filtering-plan.md`.

## 2. Architectural overview

```
┌──────────────────────────┐    handleVerification    ┌──────────────────────┐
│ stream-verifier plugin   │ ───────────────────────▶ │ StateManagementPlugin      │
└──────────────────────────┘                          │  (this epic)         │
                                                      │                      │
                                                      │  ┌────────────────┐  │
                                                      │  │ ScheduledExec  │  │
                                                      │  │ stateChanges   │  │
                                                      │  │ (every 2s)     │  │
                                                      │  └──────┬─────────┘  │
                                                      │         │            │
                                                      │  ┌──────▼─────────┐  │
                                                      │  │ Virtual map    │  │
                                                      │  │  state lifecycle│ │
                                                      │  │  manager (CN)  │  │
                                                      │  └──────┬─────────┘  │
                                                      │         │            │
                                                      │  ┌──────▼─────────┐  │
                                                      │  │ ScheduledExec  │  │
                                                      │  │ snapshot       │  │
                                                      │  │ (every 15m)    │  │
                                                      │  └────────────────┘  │
                                                      └──────────┬───────────┘
                                                                 │ getBinaryKV / getBinarySingleton / getBinaryQueue
                                                                 ▼
                                              ┌──────────────────────────────┐
                                              │ gRPC clients & other plugins  │
                                              └──────────────────────────────┘
```

State is owned by `com.swirlds.state.merkle.VirtualMapStateLifecycleManager`
(confirmed live class name; the plan's "VirtualMapStateLifyCycleManager" is a
typo — see `fyi-to-plan.md`). The plugin holds a single instance and treats it
as the source of truth for the mutable and latest-immutable copies.

## 3. Module layout

```
block-node/state-management-hashgraph/
├── build.gradle.kts                     # org.hiero.gradle.module.library
├── src/main/java/
│   └── org/hiero/block/node/state/management/
│       ├── StateManagementPlugin.java    # BlockNodePlugin + BlockNotificationHandler + StateService gRPC + lifecycle/snapshot
│       ├── StateManagementConfig.java    # @ConfigData("state.management")
│       ├── StateChangeApplier.java       # parses StateChanges, calls BinaryState.update*
│       ├── StateMetadataStore.java       # JSON load/save of stateMetadata.json
│       ├── StateSource.java              # read-target selector (IMMUTABLE / MUTABLE / HISTORICAL)
│       └── module-info.java
└── src/test/java/...
```

There is no separate access-service or snapshot-manager class: the plugin
itself implements the `StateService` gRPC surface and owns the snapshot
save/load/prune logic (recent-dir retention only — no tar/historic archival).

Module-info (the state libraries are required non-transitively — the plugin
consumes them internally and does not re-export them):

```
provides org.hiero.block.node.spi.BlockNodePlugin with StateManagementPlugin;
requires transitive org.hiero.block.node.spi;
requires transitive org.hiero.block.protobuf.pbj;
requires com.swirlds.state.api;
requires com.swirlds.state.impl;
requires com.swirlds.virtualmap;
requires org.hiero.metrics;
```

The new swirlds dependencies must be added to
`hiero-dependency-versions/build.gradle.kts` (`swirlds-state-api`,
`swirlds-state-impl`, `swirlds-virtualmap`) at the same `hederaVersion` already
used for `swirlds-config-*`.

## 4. Configuration (`@ConfigData("state.management")`)

|              Property               |                        Default                        |                        Notes                         |
|-------------------------------------|-------------------------------------------------------|------------------------------------------------------|
| `stateMetadataPath`                 | `/opt/hiero/block-node/data/state/stateMetadata.json` | Path to the metadata file.                           |
| `stateSnapshotRecentPath`           | `/opt/hiero/block-node/data/state/snapshot/recent`    | Directory for live snapshot dirs (`<blockNumber>/`). |
| `snapshotInterval`                  | `15m`                                                 | Min 1m.                                              |
| `stateChangesApplyInterval`         | `2s`                                                  | Min 100ms.                                           |
| `historicCatchUpBatchSize`          | `64`                                                  | Blocks pulled per catch-up loop iteration.           |
| `stateSnapshotRecentRetentionCount` | `3`                                                   | Recent snapshot dirs kept on disk (oldest pruned).   |

## 5. Data model

### 5.1 `StateMetadata` (protobuf — `shared_message_types.proto`)

```proto
message StateMetadata {
  uint64 block_number     = 1;
  uint64 round_number     = 2;
  bytes  state_root_hash  = 3;   // VirtualMap.getHash() bytes
  uint64 state_size       = 4;   // VirtualMap.getMetadata().getSize()
}
```

Persisted on disk as JSON at `stateMetadataPath` (lightweight, human-readable —
the snapshot dir itself carries the binary state). Serialised form is
PBJ-driven so the wire format on the new `BinaryStateQuery` flow is the same as
what is held in memory.

### 5.2 Binary state query

```proto
message BinaryStateQuery {
  // Modeled on BlockRequest.block_specifier in block_access_service.proto.
  // Exactly one of these MUST be set. Block 0 (genesis) is a valid block_number.
  oneof block_specifier {
    uint64 block_number    = 1;
    bool   retrieve_latest = 5;
  }
  uint64 state_id    = 2;
  bytes  key_bytes   = 3;        // KV key (mutually exclusive with queue_index)
  uint64 queue_index = 4;        // Queue index (0-based, optional)
}

message BinaryStateQueryResponse {
  enum Code {
    UNKNOWN          = 0;
    SUCCESS          = 1;
    INVALID_REQUEST  = 2;
    NOT_FOUND        = 3;
    NOT_READY        = 4;       // state still loading / catching up
  }
  Code              status          = 1;
  StateMetadata     state_metadata  = 2;
  bytes             kv_bytes        = 3;
  bytes             singleton_bytes = 4;
  repeated bytes    queue_bytes     = 5;
}
```

RPC service updates on `state_service.proto`:

```proto
service StateService {
  rpc getBinaryKV(BinaryStateQuery)        returns (BinaryStateQueryResponse);
  rpc getBinarySingleton(BinaryStateQuery) returns (BinaryStateQueryResponse);
  rpc getBinaryQueue(BinaryStateQuery)     returns (BinaryStateQueryResponse);
  // stateSnapshot RPC removed (was a placeholder, marked TEMPORARY in proto)
}
```

### 5.3 No state-update notification

An earlier revision pushed a `StateUpdateNotification` (VERIFIED / SNAPSHOT) over
the messaging SPI and surfaced a `StateMetadata` pointer on
`ServerStatusDetailResponse`. That was removed for the spike: the plugin does not
push notifications and `ServerStatusDetailResponse` carries no state field
(field 5 reserved). Consumers read the latest `StateMetadata` directly off the
`BinaryStateQueryResponse`. If plugins later need a discoverable in-process read
API, that is STORY-20 (the `BinaryStateReader` SPI), not a push notification.

## 6. Plugin lifecycle

### 6.1 `init(BlockNodeContext, ServiceBuilder)`

- Capture context.
- Construct `VirtualMapStateLifecycleManager(metrics, time, configuration, fileSystemManager)`.
  - `metrics` is a `NoOpMetrics` from `org.hiero.consensus.metrics.noop` (the
    plugin's own metrics live in `hiero-metrics`; the swirlds bookkeeping is
    not surfaced).
  - `fileSystemManager` is `new FileSystemManager(Path.of(stateSnapshotRecentPath))`.
    Anchoring the swirlds temp scratchpad next to the snapshots keeps disk
    layout predictable.
  - The constructor eagerly creates a genesis state which is replaced later
    if a snapshot is loaded.
- Register self as the `StateServiceInterface` gRPC service via
  `serviceBuilder.registerGrpcService(this)` — the plugin **is** the service.

### 6.2 `start()`

1. Load `stateMetadata.json` if it exists; otherwise treat as genesis.
2. If a recent snapshot dir at `${recentPath}/<blockNumber>` exists, call
   `lifecycleManager.loadSnapshot(...)` to replace the eager genesis state
   with the on-disk state.
3. Register self as `BlockNotificationHandler` via
   `context.blockMessaging().registerBlockNotificationHandler(this, true, "StateManagementPlugin")`.
   `cpuIntensive=true` because state apply is CPU-bound parsing work.
4. Kick off three single-thread `ScheduledExecutorService`s:
   - `stateChangesExecutor.scheduleWithFixedDelay(this::applyPending, 0, applyInterval, ms)` — drains pending blocks in order.
   - `snapshotExecutor.scheduleWithFixedDelay(this::saveSnapshot, snapshotInterval, snapshotInterval, ms)`.
   - `catchUpExecutor.execute(this::catchUpFromHistoricalBlocks)` — see §6.4.
5. **`ready` is NOT yet set.** The catch-up task flips it to `true` when the
   plugin is caught up to the latest historical block; query traffic returns
   `NOT_READY` until then.

### 6.3 `handleVerification(VerificationNotification)`

- Only proceed on `success=true` with a non-null block payload.
- Park the block in the `ConcurrentSkipListMap<Long, BlockUnparsed> pendingBlocks`
  keyed by block number.
- The actual state mutation runs on the scheduled apply executor, not on the
  messaging thread, to keep the messaging dispatcher unblocked.

### 6.4 `catchUpFromHistoricalBlocks()` (one-shot on `start()`)

Closes the gap between `metadata.blockNumber()` and the latest block the
Block-Node has on hand without waiting for verification notifications.

1. Read `context.historicalBlockProvider()`. If null or empty → set
   `ready=true` and return.
2. Compute the catch-up window:
   - genesis → `from = availableBlocks().min()`
   - otherwise → `from = metadata.blockNumber() + 1`
   - `to = availableBlocks().max()`
3. Walk `from..to` in batches of `historicCatchUpBatchSize` (default 64).
   For each block in the batch:
   - Skip if already in `pendingBlocks` (a verification beat us to it).
   - Otherwise `context.historicalBlockProvider().block(n).blockUnparsed()`
     and `pendingBlocks.put(n, block)`.
4. After each batch, call `applyPending()` synchronously to drain.
5. Stop early if the plugin is `degraded` (a hash mismatch occurred) or
   `stopping`.
6. Set `ready=true` in `finally` — even on partial catch-up, queries can
   start serving against whatever did apply.

### 6.5 `applyPending()` (scheduled, runs every `stateChangesApplyIntervalMillis`)

```
while (!pendingBlocks.isEmpty() && !stopping) {
  long next = atGenesis ? pendingBlocks.firstKey()
                        : metadata.blockNumber() + 1;
  BlockUnparsed b = pendingBlocks.remove(next);
  if (b == null) break;             // gap → wait
  applyOne(b);
}
```

`applyBlockStateChanges(block N)` — **lag-1 commit**: a block is applied into
the live mutable, but only exposed to readers once the *next* block's footer
attests its root hash. Readers therefore never see unattested state.

1. Pull `BlockFooter.startOfBlockStateRootHash` from N (cheap reverse scan).
2. If block header is unparseable → log, set `degraded=true`, return.
3. **Validate** N's start hash against `lastAppliedHash` (the hash of the last
   applied state, post-(N-1)):
   - genesis (`lastAppliedBlock < 0`): accept iff the footer hash is empty/all-zeros.
   - otherwise: must equal `lastAppliedHash`.
   - On mismatch: increment `hashMismatchTotal`, set `degraded=true`, log at ERROR,
     return without exposing anything. Further applies short-circuit on `degraded`.
4. A match means N's footer attests post-(N-1). **Commit/expose** it (unless it is
   already exposed, e.g. just after a snapshot reload): set `attestedImmutable` to
   `getLatestImmutableState()` — reserving its reference so the `copyMutableState()`
   in step 6 does not release it — and record its `StateMetadata`.
5. Apply N's `state_changes` via `StateChangeApplier.applyBlock(mutable, block)`.
6. `lifecycleManager.copyMutableState()` seals post-N; record `lastAppliedBlock=N`,
   `lastAppliedRound`, `lastAppliedHash=hash(post-N)`. Post-N stays staged
   (un-exposed) until block N+1 attests it.

Reference counting: `attestedImmutable` is held across applies by reserving its
root (`copyMutableState()` releases the lifecycle manager's own reference to the
superseded version); the previously-held attested state is released when a new one
is adopted. Queries read `attestedImmutable`; a node that has applied only genesis
has nothing attested and reports NOT_READY. Snapshots capture `attestedImmutable`.

### 6.6 `saveSnapshot()` (scheduled, runs every `snapshotIntervalMillis`)

1. If `metadata.blockNumber == lastSnapshottedBlock` → nothing new, skip.
2. `lifecycleManager.createSnapshot(latestImmutable, recentPath/<blockNumber>)`
   writes the canonical consensus-node `data/state/` layout into the directory.
3. `pruneOldRecentSnapshots(blockNumber)`: delete recent snapshot dirs beyond
   `stateSnapshotRecentRetentionCount`, oldest first; the just-written
   `<blockNumber>` dir is always kept. Snapshots live only under `recentPath`
   (each dir hard-links into the live MerkleDb, so they are cheap). Long-term
   archival is out of scope for this plugin — a future archiving plugin owns
   compaction / off-box transfer / random-read indexes.
4. Atomically rewrite `stateMetadata.json`.

### 6.7 `stop()`

- Set `stopping=true`, `ready=false`.
- Unregister from `blockMessaging`.
- `shutdownNow()` all three executors (apply, snapshot, catch-up) with
  short await.
- One final `saveSnapshot()` if `metadata.blockNumber > lastSnapshottedBlock`.

## 7. State change application

`StateChangeApplier` walks block items via PBJ codecs (no manual wire-byte
parsing) and dispatches each `StateChange` oneof variant to a `BinaryState`
write call. The wire form of the change carrier is preserved as the canonical
storage shape, so clients querying the live state get back identical bytes
they could parse themselves with the same PBJ codecs.

|         Wire variant         |                BinaryState call                |                Stored bytes                |
|------------------------------|------------------------------------------------|--------------------------------------------|
| `SingletonUpdateChange`      | `updateSingleton(stateId, bytes)`              | `SingletonUpdateChange.PROTOBUF.toBytes`   |
| `MapUpdateChange`            | `updateKv(stateId, keyBytes, valueBytes)`      | `MapChangeKey` / `MapChangeValue` carriers |
| `MapDeleteChange`            | `removeKv(stateId, keyBytes)`                  | `MapChangeKey` carrier                     |
| `QueuePushChange`            | `pushQueue(stateId, bytes)`                    | `QueuePushChange.PROTOBUF.toBytes`         |
| `QueuePopChange`             | `popQueue(stateId)`                            | —                                          |
| `STATE_ADD` / `STATE_REMOVE` | (skipped — schema events, out of scope for v1) | —                                          |

A malformed `state_changes` item aborts the apply with
`IllegalStateException`; the plugin treats that as a hard failure for the
block (does not advance metadata). An open question — whether to centralise
this applier into `swirlds-state-impl` so every consumer doesn't reinvent —
is captured in STORY-12's Foundation-feedback section.

## 8. Query path

`StateManagementPlugin` implements `StateServiceInterface` directly and is
registered as the gRPC service in `init`. Each RPC:

1. Returns `NOT_READY` if catch-up hasn't completed.
2. Returns `INVALID_REQUEST` if:
   - `getBinaryKV` is called without `key_bytes`, or with `queue_index`;
   - `getBinarySingleton` is called with `key_bytes` or `queue_index`;
   - `getBinaryQueue` is called with `key_bytes`;
   - `block_number` is non-zero and not equal to the latest applied.
3. Selects the read state: `getLatestImmutableState()` if present, else
   `getMutableState()` as a fallback so the first read before any
   `copyMutableState` returns sensible data.
4. Delegates to `getKv` / `getSingleton` / `getQueueAsList` / `peekQueue`.
5. `getBinaryQueue` defensively maps any `RuntimeException` from
   `getQueueState`/`getQueueAsList`/`peekQueue` (which NPE on unknown
   state IDs in `VirtualMapStateImpl`) to `NOT_FOUND`.
6. Wraps the result in `BinaryStateQueryResponse` with the current
   `StateMetadata` always populated.

## 9. Metrics

Current implementation tracks `hashMismatchTotal` as an in-process
`AtomicLong` (test-visible via `StateManagementPlugin.hashMismatchTotal()`). Full
metric-registry integration — gauges and histograms below — is not yet
wired and is parked as a follow-up:

|                 Metric                 |   Type    |
|----------------------------------------|-----------|
| `applied_block_number`                 | Gauge     |
| `apply_latency_ms`                     | Histogram |
| `snapshot_latency_ms`                  | Histogram |
| `pending_blocks`                       | Gauge     |
| `catch_up_blocks_total`                | Counter   |
| `hash_mismatch_total`                  | Counter   |
| `query_total{kind=kv|singleton|queue}` | Counter   |

## 10. Failure modes

|               Failure                |                                                                   Behaviour                                                                   |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| Snapshot dir unreadable at startup   | Log at WARNING, continue with the eagerly-created genesis state; plugin is not failed.                                                        |
| `startOfBlockStateRootHash` mismatch | Increment `hashMismatchTotal`, set `degraded=true`, log at ERROR. Apply loop short-circuits thereafter; further blocks ignored until restart. |
| Malformed `state_changes` item       | `IllegalStateException` from the applier; plugin refuses to advance metadata for that block.                                                  |
| Verification gap                     | Block parks in `pendingBlocks` until predecessor arrives (via notification or catch-up); apply loop never advances past a gap.                |
| Historical block missing locally     | Catch-up skips it; the same block can arrive via notification later.                                                                          |
| Snapshot write fails (I/O)           | Log at WARNING; recent dir not deleted; next snapshot cycle retries.                                                                          |
| Missing-queue query                  | gRPC handler defensively maps NPE from `getQueueState`/`getQueueAsList`/`peekQueue` to `NOT_FOUND`.                                           |

## 11. Acceptance tests

Acceptance scenarios live in
`block-node/state-management-hashgraph/src/test/java/.../StateManagementAcceptanceTest.java`
and exercise the real `VirtualMapStateLifecycleManager`:

1. **scenario1** — genesis startup applies the first verified block.
2. **scenario2** — `metadata.block=n`, verification of `n+1` with the right
   footer hash advances metadata.
3. **scenario3** — gap: block `n+2` parks until `n+1` fills the gap; both
   then apply in order.
4. **scenario4** — chained blocks `0..3` apply in order; under lag-1 the exposed
   block is `2` (the tip stays staged until confirmed).
5. **scenario5** — `saveSnapshot` writes `stateMetadata.json` and the
   `recent/<block>/` directory tree; a fresh plugin pointed at the same
   dirs restores from disk.
6. **scenario6** — footer hash mismatch refuses apply, increments
   `hashMismatchTotal`, sets `degraded=true`.
7. **recentSnapshotRetentionKeepsConfiguredCount** — with retention 3,
   applying/snapshotting blocks 1..4 keeps `recent/{2,3,4}` and deletes
   `recent/1`; no `historic/` directory is created.

Other test classes:

- `StateManagementTest` (in-tree fixture for the applier's BinaryState calls)
- `StateChangeApplierTest` — singleton / KV / queue dispatch via PBJ codecs
- `StateManagementAccessTest` — gRPC query rules (invalid shapes, NOT_FOUND, success)
- `StateManagementCatchUpTest` — `catchUpFromHistoricalBlocks` with an in-tree
  `HistoricalBlockFacility` fixture
- `StateManagementPluginLifecycleTest` — start → apply → snapshot → stop →
  restart round-trip

E2E (`tools-and-tests/suites/.../StateManagementE2ETests.java`) boots the full
`BlockNodeApp` and exercises `StateService` over real gRPC via the
generated `StateServiceClient`. Two tests: `getBinarySingleton` round-trip
and `getBinaryKV` / `getBinaryQueue` structured responses.

## 12. Open questions

Q-1. Ordering vs. throughput of apply loop. We rely on the
`ConcurrentSkipListMap` + 2s scheduler to serialise apply. If apply takes
longer than the interval, the next tick is harmless (it sees an empty queue or
the same set). The scheduled-rate pacing is sufficient for v1; if throughput
becomes a bottleneck the apply loop becomes event-driven on
`handleVerification` rather than scheduled.

Q-2. `roundNumber` extraction. Pulled directly from the `RoundHeader` block
item — every block carries at least one. The applier tracks the last seen
`RoundHeader.roundNumber()` during the block walk and writes it into
`StateMetadata` after apply completes. No timestamp approximation.

Q-3. Single plugin for live + historical state? See `fyi-to-plan.md` —
historical state is **explicitly out of scope** for this epic. The plan's
question is parked.

Q-4. Snapshot consensus-round files (`<round>/data/state/`). The CN spec
expects per-round directories. We follow the same `data/state/` layout but
nest under `<blockNumber>/` because BN doesn't track rounds the way CN does.
Documented as a known divergence in `expansion.md`.

## 13. References

- `agent/ref/state-and-filtering/state-and-filtering-plan.md` — source plan.
- `hiero-consensus-node/platform-sdk/swirlds-state-api/README.md` — current API.
- `hiero-consensus-node/platform-sdk/swirlds-state-api/docs/state-snapshot-spec.md` — disk layout.
- `hiero-consensus-node/hedera-state-validator/src/main/java/com/hedera/statevalidation/blockstream/BlockStreamRecoveryWorkflow.java` — canonical `BinaryStateChangeApplier`.
- `agent/proposals/state-and-filtering/live-state/fyi-to-plan.md` — corrections to the input plan.
- `agent/proposals/state-and-filtering/live-state/expansion.md` — deferred items and recommendations.
