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
- Construct `VirtualMapStateLifecycleManager(metrics, time, configuration)`.
  The construction itself **eagerly creates a genesis state** — this is OK and
  is replaced later if a snapshot is loaded.
- Register `LiveStateAccessService` (gRPC) with the service builder.

### 6.2 `start()`

1. Load `stateMetadata.json` if it exists; otherwise treat as genesis.
2. If metadata indicates a recent snapshot dir at
   `${recentPath}/<blockNumber>` exists, call `lifecycleManager.loadSnapshot(p)`.
3. Register self as `BlockNotificationHandler` via
   `context.blockMessaging().registerBlockNotificationHandler(this, true, "LiveState")`.
   `cpuIntensive=true` because state apply is CPU-bound parsing work.
4. Kick off two single-thread `ScheduledExecutorService`s:
   - `stateChangesExecutor.scheduleAtFixedRate(this::applyPending, ...)`
   - `snapshotExecutor.scheduleAtFixedRate(this::saveSnapshot, ...)`
5. Mark plugin ready — `LiveStateAccessService` responds `NOT_READY` until this
   point.

### 6.3 `handleVerification(VerificationNotification)`

- Only proceed on `success=true`. Failures are logged and ignored.
- Parse the `BlockHeader` from `notification.block()`. If
  `header.blockNumber() != metadata.blockNumber() + 1` and metadata is not at
  genesis, schedule a `catchUpFromHistoricalBlocks()` that pulls the missing
  blocks from `historicalBlockProvider` and feeds them to `applyBlockStateChanges`.
- Otherwise insert into the `ConcurrentSkipListMap<Long, BlockUnparsed>
  pendingBlocks` keyed by block number.

The actual state mutation runs **on the scheduled executor**, not on the
messaging thread, to keep the messaging dispatcher unblocked.

### 6.4 `applyPending()` (scheduled, runs every 2s)

```
while (!pendingBlocks.isEmpty()) {
  long next = metadata.blockNumber() + 1;
  BlockUnparsed b = pendingBlocks.remove(next);
  if (b == null) break;             // gap → wait for catch-up

  applyBlockStateChanges(b, next);
}
```

`applyBlockStateChanges`:

1. Validate `block.blockHeader.previousStateRootHash` matches the current
   mutable state hash (after the previous block was applied). If mismatched at
   block n+1, refuse and abort — the live state is no longer correct.
2. Iterate `block.blockItems()`; for every `state_changes` item, decode and
   apply via `StateChangeApplier.apply(binaryState, stateChangesBytes)` (see
   §7).
3. After all changes, `state.computeHash()` and update `StateMetadata` in-memory:
   `blockNumber = header.blockNumber(); rootHash = state.getHash(); size = state.getMetadata().getSize(); roundNumber = roundHeader.roundNumber()` (taken from the last `RoundHeader` block item seen during the block walk).
4. `lifecycleManager.copyMutableState()` — promote current mutable to
   latest-immutable so queries see the just-applied data.
5. `context.blockMessaging().sendStateUpdate(new StateUpdateNotification(VERIFIED, ...))`.

### 6.5 `saveSnapshot()` (scheduled, runs every 15m)

1. If `metadata.blockNumber == lastSnapshottedBlock` → nothing new, skip.
2. `lifecycleManager.createSnapshot(latestImmutable, recentPath/<blockNumber>)`.
3. Tar the recent snapshot to `historicPath/<blockNumber>.tar` (temp file +
   atomic `mv`).
4. Delete any `recentPath/<other>` directories — only the most-recent is kept
   warm; older recents are durable in `historic` as tars.
5. Atomically rewrite `stateMetadata.json`.
6. Emit `StateUpdateNotification(SNAPSHOT, ...)`.

### 6.6 `stop()`

- Unregister handler, `shutdownNow()` both executors with short await.
- One final `saveSnapshot()` if `metadata.blockNumber > lastSnapshottedBlock`.

## 7. State change application

The plan called for a hand-rolled `StateChangeParser`. **Recon shows the
canonical implementation is `BinaryStateChangeApplier` in
`hedera-state-validator/src/main/java/com/hedera/statevalidation/blockstream/BlockStreamRecoveryWorkflow.java`**
(lines 190–266). We follow that pattern verbatim: walk the
`state_changes.proto` wire bytes, dispatch by tag, and call into `BinaryState`
write methods:

| Wire tag                     | Action                                          |
|------------------------------|-------------------------------------------------|
| `state_change.singleton_update` | `binaryState.updateSingleton(stateId, value)` |
| `state_change.map_update`       | `binaryState.updateKv(stateId, key, value)`   |
| `state_change.map_delete`       | `binaryState.removeKv(stateId, key)`          |
| `state_change.queue_push`       | `binaryState.pushQueue(stateId, value)`       |
| `state_change.queue_pop`        | `binaryState.popQueue(stateId)`               |

Open issue forwarded to Foundation (see `fyi-to-plan.md`): this parser belongs
in `swirlds-state-impl` so every consumer doesn't reinvent the wheel. Until
that ships, BN carries its own copy.

## 8. Query path

`LiveStateAccessService` is a thin shim that:

1. Rejects requests with `state_metadata_block_number == 0` mismatching latest
   metadata (current scope: latest only).
2. Asks the lifecycle manager for the latest immutable state.
3. Delegates to `getKv` / `getSingleton` / `getQueueAsList` / `peekQueue` on
   `BinaryState`.
4. Wraps the result in `BinaryStateQueryResponse` with the current
   `StateMetadata`.

## 9. Metrics (under `blocknode.state.live`)

| Metric                              | Type    |
|-------------------------------------|---------|
| `applied_block_number`              | Gauge   |
| `apply_latency_ms`                  | Histogram |
| `snapshot_latency_ms`               | Histogram |
| `pending_blocks`                    | Gauge   |
| `catch_up_blocks_total`             | Counter |
| `hash_mismatch_total`               | Counter |
| `query_total{kind=kv|singleton|queue}` | Counter |

## 10. Failure modes

| Failure                          | Behaviour                                                      |
|----------------------------------|----------------------------------------------------------------|
| Snapshot dir corrupt at startup  | Fail-fast plugin start; `Severe` log; healthFacility = DEGRADED. |
| `prev_state_root_hash` mismatch  | Stop apply loop, increment `hash_mismatch_total`, refuse further verifications. Plugin reports degraded. |
| Apply throws                     | Log, increment counter, requeue block, exponential backoff up to 30s. |
| Schedule snapshot directory full | Skip historic tar, log, retry next interval.                   |
| Verification gap > catch-up      | Pause apply, kick `catchUpFromHistoricalBlocks` (pull from `historicalBlockProvider`). |

## 11. Acceptance tests (mirroring plan §state-hashgraph-live)

1. Genesis startup → `stateMetadata.json` written with block 0; block-0 stream
   apply succeeds.
2. Genesis startup, block-N (N>0) without prior metadata → plugin refuses
   apply and remains at genesis until catch-up reaches N-1.
3. Non-genesis startup with metadata.block=n, verification of n+1 with a
   matching `prev_state_root_hash` → applies, immutable promoted, emits
   `StateUpdateNotification(VERIFIED, n+1, ...)`.
4. Non-genesis, verification of n+1 with mismatched hash → not applied,
   `hash_mismatch_total++`, plugin marks itself degraded.
5. Sequential n+1 then n+2 chained correctly → both apply; final
   `StateUpdateNotification` carries n+2 metadata.

E2E suite (`tools-and-tests/suites`) mirrors
`BlockNodeCloudStorageTests`: spin up `BlockNodeApp`, publish three chained
blocks, poll for the snapshot directory to materialise on disk and for the
`getBinarySingleton` RPC to start returning `SUCCESS`.

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
