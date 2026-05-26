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

1. A single new plugin, `state-hashgraph-live`, owning state load, apply,
   snapshot, and binary state query.
2. Persistence of state-on-disk snapshots under
   `${stateSnapshotRecentPath}/<blockNumber>` matching the consensus-node
   `data/state/` spec (see
   `hiero-consensus-node/platform-sdk/swirlds-state-api/docs/state-snapshot-spec.md`).
3. New protobuf surface: `StateMetadata`, `BinaryStateQuery`,
   `BinaryStateQueryResponse`, and three `getBinary*` RPCs on `StateService`.
4. New SPI hook: `StateUpdateNotification` and
   `BlockNotificationHandler.handleStateUpdate`.
5. `ServerStatusDetailResponse.hash_state_metadata` exposes the latest applied
   state pointer.

Explicitly out of scope:

- Historical state queries (only the current immutable snapshot is queryable).
- Remote archival / S3 of snapshots.
- Block stream **filtering** — separate epic, tracked under
  `agent/ref/state-and-filtering/state-and-filtering-plan.md`.

## 2. Architectural overview

```
┌──────────────────────────┐    handleVerification    ┌──────────────────────┐
│ stream-verifier plugin   │ ───────────────────────▶ │ LiveStatePlugin      │
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
                                                                 │ sendStateUpdate
                                                                 ▼
                                              ┌──────────────────────────────┐
                                              │ server-status & other plugins│
                                              └──────────────────────────────┘
```

State is owned by `com.swirlds.state.merkle.VirtualMapStateLifecycleManager`
(confirmed live class name; the plan's "VirtualMapStateLifyCycleManager" is a
typo — see `fyi-to-plan.md`). The plugin holds a single instance and treats it
as the source of truth for the mutable and latest-immutable copies.

## 3. Module layout

```
block-node/state-hashgraph-live/
├── build.gradle.kts                     # org.hiero.gradle.module.library
├── src/main/java/
│   └── org/hiero/block/node/state/live/
│       ├── LiveStatePlugin.java         # BlockNodePlugin + BlockNotificationHandler + gRPC service
│       ├── LiveStateConfig.java         # @ConfigData("state.live")
│       ├── LiveStateAccessService.java  # implements StateService gRPC
│       ├── StateChangeApplier.java      # parses StateChanges, calls BinaryState.update*
│       ├── StateMetadataStore.java      # JSON load/save of stateMetadata.json
│       ├── SnapshotManager.java         # createSnapshot/loadSnapshot/tar+atomic move
│       └── module-info.java
└── src/test/java/...
```

Module-info, mirroring `stream-publisher`:

```
provides org.hiero.block.node.spi.BlockNodePlugin with LiveStatePlugin;
requires transitive com.swirlds.state.api;
requires transitive com.swirlds.state.impl;
requires transitive com.swirlds.virtualmap;
requires transitive org.hiero.block.node.spi;
requires transitive org.hiero.block.protobuf.pbj;
```

The new swirlds dependencies must be added to
`hiero-dependency-versions/build.gradle.kts` (`swirlds-state-api`,
`swirlds-state-impl`, `swirlds-virtualmap`) at the same `hederaVersion` already
used for `swirlds-config-*`.

## 4. Configuration (`@ConfigData("state.live")`)

| Property                     | Default                                            | Notes                                                  |
|------------------------------|----------------------------------------------------|--------------------------------------------------------|
| `stateMetadataPath`          | `/opt/hiero/block-node/data/state/stateMetadata.json` | Path to the metadata file.                          |
| `stateSnapshotRecentPath`    | `/opt/hiero/block-node/data/state/snapshot/recent` | Directory for live snapshot dirs (`<blockNumber>/`).   |
| `stateSnapshotHistoricPath`  | `/opt/hiero/block-node/data/state/snapshot/historic` | Directory of tarred archival snapshots.              |
| `snapshotInterval`           | `15m`                                              | Min 1m.                                                |
| `stateChangesApplyInterval`  | `2s`                                               | Min 100ms.                                             |
| `historicCatchUpBatchSize`   | `64`                                               | Blocks pulled per catch-up loop iteration.             |

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
  uint64 block_number = 1;        // 0 = latest; otherwise must equal latest
  uint64 state_id     = 2;
  bytes  key_bytes    = 3;        // KV key (mutually exclusive with queue_index)
  uint64 queue_index  = 4;        // Queue index (0-based, optional)
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

### 5.3 SPI: `StateUpdateNotification`

```java
public record StateUpdateNotification(
    StateUpdateType type,        // VERIFIED or SNAPSHOT
    long blockNumber,
    long roundNumber,
    Bytes stateRootHash,
    long stateSize) {}
```

Added alongside `VerificationNotification` in
`org.hiero.block.node.spi.blockmessaging`. New default-empty hook on
`BlockNotificationHandler`:

```java
default void handleStateUpdate(StateUpdateNotification n) {}
```

New send method on `BlockMessagingFacility`:

```java
void sendStateUpdate(StateUpdateNotification notification);
```

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
   `context.blockMessaging().registerBlockNotificationHandler(this, true, "LiveStatePlugin")`.
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

`applyOne(block)`:

1. Walk block items via `StateChangeApplier.inspectBlock(block)` (no
   mutation) to extract `blockNumber`, the last `RoundHeader.roundNumber`
   seen, and the `BlockFooter.startOfBlockStateRootHash`.
2. If block header is unparseable → log, return without applying.
3. **Validate** `BlockFooter.startOfBlockStateRootHash` against the
   current mutable state hash:
   - genesis (`metadata.blockNumber==0` and `metadata.stateRootHash` is
     empty): accept iff the footer hash is empty/all-zeros.
   - otherwise: footer hash must equal `mutable.getRoot().getHash()`.
   - On mismatch: increment `hashMismatchTotal`, set `degraded=true`,
     log at ERROR, return. All further `applyOne` calls short-circuit on
     `degraded`.
4. Apply the block's `state_changes` items via
   `StateChangeApplier.applyBlock(mutable, block)`.
5. `lifecycleManager.copyMutableState()` promotes the mutated state to
   `latestImmutable`.
6. Build a fresh `StateMetadata` from the immutable's hash + size and the
   block number / round number from step 1. Update the `volatile` reference.
7. Emit `StateUpdateNotification(VERIFIED, blockNumber, roundNumber, …)`.

### 6.6 `saveSnapshot()` (scheduled, runs every `snapshotIntervalMillis`)

1. If `metadata.blockNumber == lastSnapshottedBlock` → nothing new, skip.
2. `lifecycleManager.createSnapshot(latestImmutable, recentPath/<blockNumber>)`
   writes the canonical consensus-node `data/state/` layout into the directory.
3. `pruneRecentExcept(blockNumber)`:
   - for every other directory under `recentPath`, call `SnapshotArchiver`
     to tar it into `historicPath/<otherBlock>.tar` (UStar, atomic via
     `.tar.tmp` + move), then delete the recent dir;
   - enforce `historicArchiveRetentionCount`: if non-zero, list `*.tar`
     under `historicPath`, sort by block number, delete the oldest until
     the count is at or under the threshold (mirrors
     `BlockFileHistoricPlugin.cleanup`).
4. Atomically rewrite `stateMetadata.json`.
5. Emit `StateUpdateNotification(SNAPSHOT, ...)`.

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

| Wire variant                  | BinaryState call                                 | Stored bytes                          |
|-------------------------------|--------------------------------------------------|---------------------------------------|
| `SingletonUpdateChange`       | `updateSingleton(stateId, bytes)`                | `SingletonUpdateChange.PROTOBUF.toBytes` |
| `MapUpdateChange`             | `updateKv(stateId, keyBytes, valueBytes)`        | `MapChangeKey` / `MapChangeValue` carriers |
| `MapDeleteChange`             | `removeKv(stateId, keyBytes)`                    | `MapChangeKey` carrier                |
| `QueuePushChange`             | `pushQueue(stateId, bytes)`                      | `QueuePushChange.PROTOBUF.toBytes`    |
| `QueuePopChange`              | `popQueue(stateId)`                              | —                                     |
| `STATE_ADD` / `STATE_REMOVE`  | (skipped — schema events, out of scope for v1)   | —                                     |

A malformed `state_changes` item aborts the apply with
`IllegalStateException`; the plugin treats that as a hard failure for the
block (does not advance metadata). An open question — whether to centralise
this applier into `swirlds-state-impl` so every consumer doesn't reinvent —
is captured in STORY-12's Foundation-feedback section.

## 8. Query path

`LiveStatePlugin` implements `StateServiceInterface` directly and is
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
`AtomicLong` (test-visible via `LiveStatePlugin.hashMismatchTotal()`). Full
metric-registry integration — gauges and histograms below — is not yet
wired and is parked as a follow-up:

| Metric                              | Type      |
|-------------------------------------|-----------|
| `applied_block_number`              | Gauge     |
| `apply_latency_ms`                  | Histogram |
| `snapshot_latency_ms`               | Histogram |
| `pending_blocks`                    | Gauge     |
| `catch_up_blocks_total`             | Counter   |
| `hash_mismatch_total`               | Counter   |
| `query_total{kind=kv|singleton|queue}` | Counter |

## 10. Failure modes

| Failure                              | Behaviour                                                                                              |
|--------------------------------------|--------------------------------------------------------------------------------------------------------|
| Snapshot dir unreadable at startup   | Log at WARNING, continue with the eagerly-created genesis state; plugin is not failed.                |
| `startOfBlockStateRootHash` mismatch | Increment `hashMismatchTotal`, set `degraded=true`, log at ERROR. Apply loop short-circuits thereafter; further blocks ignored until restart. |
| Malformed `state_changes` item        | `IllegalStateException` from the applier; plugin refuses to advance metadata for that block.          |
| Verification gap                     | Block parks in `pendingBlocks` until predecessor arrives (via notification or catch-up); apply loop never advances past a gap. |
| Historical block missing locally     | Catch-up skips it; the same block can arrive via notification later.                                  |
| Snapshot write fails (I/O)           | Log at WARNING; recent dir not deleted; next snapshot cycle retries.                                  |
| Historic archive fails               | Log at WARNING; the recent dir is kept so the next snapshot retries the archive.                      |
| Missing-queue query                  | gRPC handler defensively maps NPE from `getQueueState`/`getQueueAsList`/`peekQueue` to `NOT_FOUND`.    |

## 11. Acceptance tests

Acceptance scenarios live in
`block-node/state-hashgraph-live/src/test/java/.../LiveStateAcceptanceTest.java`
and exercise the real `VirtualMapStateLifecycleManager`:

1. **scenario1** — genesis startup applies the first verified block.
2. **scenario2** — `metadata.block=n`, verification of `n+1` with the right
   footer hash advances metadata.
3. **scenario3** — gap: block `n+2` parks until `n+1` fills the gap; both
   then apply in order.
4. **scenario4** — three chained blocks (`0,1,2`) emit three VERIFIED
   notifications.
5. **scenario5** — `saveSnapshot` writes `stateMetadata.json` and the
   `recent/<block>/` directory tree; a fresh plugin pointed at the same
   dirs restores from disk.
6. **scenario6** — footer hash mismatch refuses apply, increments
   `hashMismatchTotal`, sets `degraded=true`.
7. **scenario7** — older recent snapshot is tarred to
   `historic/<block>.tar` before the recent dir is deleted.
8. **scenario8** — `historicArchiveRetentionCount=2` caps the historic
   store at two archives, deleting the oldest.

Other test classes:

- `LiveStateTest` (in-tree fixture for the applier's BinaryState calls)
- `StateChangeApplierTest` — singleton / KV / queue dispatch via PBJ codecs
- `LiveStateAccessTest` — gRPC query rules (invalid shapes, NOT_FOUND, success)
- `LiveStateCatchUpTest` — `catchUpFromHistoricalBlocks` with an in-tree
  `HistoricalBlockFacility` fixture
- `LiveStatePluginLifecycleTest` — start → apply → snapshot → stop →
  restart round-trip
- `SnapshotArchiverTest` — UStar round-trip of the tar emitter

E2E (`tools-and-tests/suites/.../LiveStateE2ETests.java`) boots the full
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
