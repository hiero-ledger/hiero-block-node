# Live Hashgraph State — Design Doc

> Status: **Beta / experimental** — not yet listed in chart deployments.

## 1. Purpose & Goals

Maintain a local, queryable copy of the Hashgraph network state inside the Block
Node so that other plugins and external clients can answer state queries
(`getBinaryKV`, `getBinarySingleton`, `getBinaryQueue`) without depending on a
Consensus Node.

Initial scope (#196):

1. A single new plugin, `state-management-hashgraph`, owning state load, apply,
   snapshot, and binary state query.
2. Persistence of state-on-disk snapshots under
   `${snapshotRecentPath}/<blockNumber>` matching the consensus-node
   `data/state/` spec (see
   `hiero-consensus-node/platform-sdk/swirlds-state-api/docs/state-snapshot-spec.md`).
3. New protobuf surface: `StateMetadata`, `BinaryStateQuery`,
   `BinaryStateQueryResponse`, and three `getBinary*` RPCs on `StateService`.
   `StateMetadata` is returned on every `BinaryStateQueryResponse` so a client
   can see which block the latest state corresponds to.

Explicitly out of scope:

- Historical state queries (only the current immutable snapshot is queryable).
- Remote archival / S3 of snapshots.
- Hiero entity get APIs e.g. getStateById.

## 2. Architectural overview

```
┌──────────────────────────┐    handleVerification    ┌────────────────────────┐
│ stream-verifier plugin   │ ───────────────────────▶ │ StateManagementPlugin  │
└──────────────────────────┘                          │                        │
                                                       │  ┌──────────────────┐  │
                                                       │  │ apply worker     │  │
                                                       │  │ (blocks on a     │  │
                                                       │  │ queue; applies   │  │
                                                       │  │ on arrival)      │  │
                                                       │  └────────┬─────────┘  │
                                                       │           │            │
                                                       │  ┌────────▼─────────┐  │
                                                       │  │ Virtual map      │  │
                                                       │  │  state lifecycle │  │
                                                       │  │  manager (CN)    │  │
                                                       │  └────────┬─────────┘  │
                                                       │           │            │
                                                       │  ┌────────▼─────────┐  │
                                                       │  │ ScheduledExec    │  │
                                                       │  │ snapshot         │  │
                                                       │  │ (every 15m)      │  │
                                                       │  └──────────────────┘  │
                                                       └────────────┬───────────┘
                                                                    │ getBinaryKV / getBinarySingleton / getBinaryQueue
                                                                    ▼
                                                 ┌──────────────────────────────┐
                                                 │ gRPC clients & other plugins  │
                                                 └──────────────────────────────┘
```

State is owned by `com.swirlds.state.merkle.VirtualMapStateLifecycleManager`.
The plugin holds a single instance and treats it as the source of truth for
the mutable and latest-immutable copies.

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

|            Property            |                        Default                        |                                         Notes                                          |
|--------------------------------|-------------------------------------------------------|----------------------------------------------------------------------------------------|
| `metadataPath`                 | `/opt/hiero/block-node/data/state/stateMetadata.json` | Path to the metadata file.                                                             |
| `snapshotRecentPath`           | `/opt/hiero/block-node/data/state/snapshot/recent`    | Directory for live snapshot dirs (`<blockNumber>/`).                                   |
| `snapshotIntervalMillis`       | `900000` (15m)                                        | Periodic snapshot cadence.                                                             |
| `historicCatchUpBatchSize`     | `64`                                                  | Blocks pulled per catch-up loop iteration.                                             |
| `snapshotRecentRetentionCount` | `3`                                                   | Recent snapshot dirs kept on disk (oldest pruned).                                     |
| `port`                         | `null` (share `server.port`)                          | Dedicated port for the `StateService` gRPC API; `null` shares the default server port. |

There is no apply-interval setting: state changes are applied the moment a verified block
arrives (see §6.5), not on a timer.

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

Persisted on disk as JSON at `metadataPath` (lightweight, human-readable —
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
    bool   retrieve_latest = 2;
  }
  uint64 state_id    = 3;
  bytes  key_bytes   = 4;        // KV key (mutually exclusive with queue_index)
  uint64 queue_index = 5;        // Queue index (0-based, optional)
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

`kv_bytes`/`singleton_bytes`/`queue_bytes` are mutually exclusive (exactly one populated per
RPC method) and were considered for a `oneof`, but `queue_bytes` is `repeated` and protobuf
does not allow `repeated` fields inside a `oneof` — it would need a wrapper message
(`QueueValues { repeated bytes values = 1; }`). That wrapping cost more devex than the
`oneof` saved, so left as plain fields; revisit if this becomes a pain point in practice.

RPC service updates on `state_service.proto`:

```proto
service StateService {
  rpc getBinaryKV(BinaryStateQuery)        returns (BinaryStateQueryResponse);
  rpc getBinarySingleton(BinaryStateQuery) returns (BinaryStateQueryResponse);
  rpc getBinaryQueue(BinaryStateQuery)     returns (BinaryStateQueryResponse);
}
```

## 6. Plugin lifecycle

### 6.1 `init(BlockNodeContext, ServiceBuilder)`

- Capture context.
- Construct `VirtualMapStateLifecycleManager(metrics, time, configuration, fileSystemManager)`.
  - `fileSystemManager` is `new FileSystemManager(Path.of(snapshotRecentPath))`.
    Anchoring the swirlds temp scratchpad next to the snapshots keeps disk
    layout predictable.
  - The constructor eagerly creates a genesis state which is replaced later
    if a snapshot is loaded.
- Register self as the `StateServiceInterface` gRPC service via
  `serviceBuilder.registerGrpcService(this, config.port())` — the plugin **is** the
  service. A `null` `port` shares the default `server.port`; a configured port opens the
  `StateService` API on its own port.

### 6.2 `start()`

1. Load `stateMetadata.json` if it exists; otherwise treat as genesis.
2. If a recent snapshot dir at `${recentPath}/<blockNumber>` exists, call
   `lifecycleManager.loadSnapshot(...)` to replace the eager genesis state
   with the on-disk state.
3. Register self as `BlockNotificationHandler` via
   `context.blockMessaging().registerBlockNotificationHandler(this, true, "StateManagementPlugin")`.
   `cpuIntensive=true` because state apply is CPU-bound parsing work.
4. Start the periodic snapshot executor
   (`snapshotExecutor.scheduleWithFixedDelay(this::saveSnapshot, …)`) and a single
   dedicated **apply-worker thread** (`StateManagement-apply`). The worker first runs
   one-shot historical catch-up (§6.4), then blocks on a `BlockingQueue` and drains
   `pendingBlocks` the instant a verified block arrives (§6.5). Catch-up and live apply
   share the one worker thread, so `applyPending()` is only ever driven from one place.

### 6.3 `handleVerification(VerificationNotification)`

- Only proceed on `success=true` with a non-null block payload.
- Park the block in the `ConcurrentSkipListMap<Long, BlockUnparsed> pendingBlocks`
  keyed by block number (the ordered gap buffer — a plain queue cannot hold gaps).
- `offer` the block number to the apply worker's `BlockingQueue` to wake it immediately.
  The actual state mutation runs on the apply worker, not on the messaging thread, to keep
  the messaging dispatcher unblocked; `applyPending()` is guarded by a `ReentrantLock` so
  the worker and direct (test) calls never mutate state concurrently. In production the
  worker thread is the only caller (catch-up and live apply are sequential on it, never
  concurrent), so the lock adds no production safety; it exists so tests can call
  `applyPending()` directly without racing a running worker.

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
5. Stop early if the plugin is `applyHalted` (a hash mismatch occurred) or
   `stopping`.
6. Set `ready=true` in `finally` — even on partial catch-up, queries can
   start serving against whatever did apply.

### 6.5 `applyPending()` (event-driven: runs when the apply worker is woken by a new block)

The apply worker blocks on a `BlockingQueue` and calls `applyPending()` the moment a block
arrives (no timer). `applyPending()` takes the `applyLock` and drains contiguous blocks:

```
while (!pendingBlocks.isEmpty() && !stopping && !applyHalted) {
  long next = atGenesis ? pendingBlocks.firstKey()
                        : max(lastAppliedBlock, metadata.blockNumber()) + 1;
  BlockUnparsed b = pendingBlocks.get(next);
  if (b == null) return;            // gap → wait for the missing predecessor
  if (!applyBlockStateChanges(b)) return;   // failure → leave queued, stop draining
  pendingBlocks.remove(next);
}
```

#### Three concurrent state versions

The apply pipeline runs three versions of the state concurrently:

- **(3) MUTABLE** — `lifecycleManager.getMutableState()`: receives the current block's
  `state_changes` (library-managed).
- **(2) HASHING** — `hashingImmutable`: sealed by `copyMutableState()`, awaiting hash +
  attestation by the next block's footer (library-managed, held by the plugin).
- **(1) ATTESTED** — `attestedImmutable`: the network-confirmed, query-serving copy (a
  plugin-local cache variable; the only state the query API reads).

A state is hashed **exactly once**, never while it is still mutable (3). `VirtualMap.getHash()`
caches its result, so the total hashing work is the same whether it runs eagerly or lazily —
the only real choice is *when* to pay the cost. Hashing starts in the background right after a
state is sealed into (2), overlapping with whatever the apply pipeline does next; the block
that needs the hash to validate against joins that in-flight computation instead of blocking
on a synchronous hash. (An earlier version of this design deferred hashing until promotion,
reasoning it would "avoid unnecessary work" for a never-confirmed stream tip — that reasoning
doesn't hold, since the hash is computed exactly once either way; deferring only serialized the
cost onto the apply path instead of overlapping it.)

`applyBlockStateChanges(block N)` — **lag-1 commit**: a block is applied into the live
mutable, but only exposed once the *next* block's footer attests its root hash.

1. Pull `BlockFooter.startOfBlockStateRootHash` from N (cheap reverse scan). Missing →
   `applyHalted=true`, return.
2. **Join state 2's hash** (`hashingImmutable`, post-(N-1)), started in the background when it
   was sealed, and **validate** N's start hash against it. State 2 is seeded at boot — with the
   loaded snapshot if one exists, otherwise with the sealed *empty* `VirtualMap` — so this check
   has no genesis special case: block 0's footer carries the real hash of empty state (which
   differs per network/config, e.g. a seeded genesis bootstrap vs. a truly empty map), and this
   step computes that hash the same way it computes every other block's, rather than assuming
   it's empty or all-zeros.
   - On mismatch: increment `hashMismatchTotal`, set `applyHalted=true`, log at ERROR, return
     without exposing anything. Further applies short-circuit on `applyHalted`.
     `hashMismatchTotal` is a counter rather than a boolean because it's cumulative over the
     node's lifetime: the hash-mismatch recovery protocol's bounded local retry can increment
     it more than once per incident before `applyHalted` is finally set, and a restarted node
     can hit an independent, later mismatch — `applyHalted` alone can't distinguish "never
     happened" from "happened repeatedly".
3. A match means N's footer attests post-(N-1). **Promote** state 2 → state 1
   (`attestedImmutable`) unless it is already exposed (e.g. just after a snapshot reload),
   reserving its reference, and record its `StateMetadata` with the just-computed hash.
4. Apply N's `state_changes` via `StateChangeApplier.applyBlock(mutable, block)`.
5. `lifecycleManager.copyMutableState()` fast-copies/seals post-N as the new state 2
   (`hashingImmutable`); record `lastAppliedBlock=N`, `lastAppliedRound`. Post-N is left
   **un-hashed and un-exposed** until block N+1 attests it.

Reference counting: `attestedImmutable` is held across applies by reserving its root
(`copyMutableState()` releases the lifecycle manager's own reference to the superseded
version); the previously-held attested state is released when a new one is adopted. Queries
read `attestedImmutable`; a node that has applied only genesis has nothing attested and
reports NOT_READY. Snapshots capture `attestedImmutable`.

### 6.6 `saveSnapshot()` (scheduled, runs every `snapshotIntervalMillis`)

The schedule is the trigger — the executor invokes `saveSnapshot()` on a fixed delay
regardless of whether anything changed. Step 1 below is a skip check, not the trigger: it
decides whether that scheduled invocation has anything new to write.

1. If `metadata.blockNumber <= lastSnapshottedBlock` → nothing new, skip.
2. `lifecycleManager.createSnapshot(latestImmutable, recentPath/<blockNumber>)`
   writes the canonical consensus-node `data/state/` layout into the directory.
3. `pruneOldRecentSnapshots(blockNumber)`: delete recent snapshot dirs beyond
   `snapshotRecentRetentionCount`, oldest first; the just-written
   `<blockNumber>` dir is always kept. Snapshots live only under `recentPath`
   (each dir hard-links into the live MerkleDb, so they are cheap). Long-term
   archival is out of scope for this plugin — a future archiving plugin owns
   compaction / off-box transfer / random-read indexes.
4. Atomically rewrite `stateMetadata.json`.

### 6.7 `stop()`

- Set `stopping=true`, `ready=false`.
- Unregister from `blockMessaging`.
- Interrupt + join the apply worker (waking it out of the `BlockingQueue`) and shut down
  the snapshot executor with a short await.
- One final `saveSnapshot()` if `metadata.blockNumber > lastSnapshottedBlock`.

## 7. State change application

`BinaryState` (`VirtualMapStateImpl`) tags whatever bytes it's given with its own
`(stateId << 3 | WIRE_TYPE_DELIMITED)` wrapper — it expects the **raw inner entity payload**,
not an already-tagged `state_changes` oneof wrapper message. So `StateChangeApplier` walks each
`state_changes` item at the protobuf wire level (mirroring consensus-node's reference
`BlockStreamRecoveryWorkflow.BinaryStateChangeApplier`) and extracts just the first delimited
field — the oneof-selected payload — rather than storing the whole wrapper. PBJ's generated
oneof accessors only hand back a decoded Java object for whichever variant is set, not that
variant's raw bytes, so doing this extraction from an already-parsed `StateChange` would need a
hand-written switch over every oneof variant that silently goes stale as HAPI adds new ones;
walking the wire format directly needs no such switch.

|         Wire variant         |                BinaryState call                |                  Stored bytes                   |
|------------------------------|------------------------------------------------|-------------------------------------------------|
| `SingletonUpdateChange`      | `updateSingleton(stateId, bytes)`              | the oneof-selected value's own encoded bytes    |
| `MapUpdateChange`            | `updateKv(stateId, keyBytes, valueBytes)`      | `MapChangeKey`/`MapChangeValue`'s oneof payload |
| `MapDeleteChange`            | `removeKv(stateId, keyBytes)`                  | `MapChangeKey`'s oneof payload                  |
| `QueuePushChange`            | `pushQueue(stateId, bytes)`                    | the oneof-selected element's own encoded bytes  |
| `QueuePopChange`             | `popQueue(stateId)`                            | —                                               |
| `STATE_ADD` / `STATE_REMOVE` | (skipped — schema events, out of scope for v1) | —                                               |

Token-relationship keys (`stateId == 9`) need no special-casing: this plugin's pinned
`state_changes.proto` carries that key as `AccountTokenAssociation` (field 25), introduced
upstream specifically so its field order/types match state's `EntityIDPair` exactly — unlike
the older `TokenAssociation` encoding (now reserved field 2) that consensus-node's own reference
applier still remaps for compatibility with older captured chains.

Because stored bytes are now the oneof payload rather than the wrapper carrier, a caller can no
longer recover the concrete entity type from the bytes alone (the wrapper's field number, which
disclosed the type, isn't stored) — `getBinary*` callers need that mapping from elsewhere, e.g.
a `stateId → type` table. This is an open API-surface question for the not-yet-implemented
`StateService` gRPC query path (§8); it doesn't affect this plugin's own apply-and-store logic.

A malformed `state_changes` item aborts the apply with
`IllegalStateException`; the plugin treats that as a hard failure for the
block (does not advance metadata). An open question for upstream feedback:
whether to centralize this applier into `swirlds-state-impl` so every
consumer doesn't reinvent it.

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

Registered with the block node's `MetricRegistry` in `init()`, under the `blocknode:` prefix:

|           Metric            |      Type       |                              Description                              |
|-----------------------------|-----------------|-----------------------------------------------------------------------|
| `state_applied_block`       | ObservableGauge | Latest committed (reader-visible) block number applied to live state  |
| `state_size`                | ObservableGauge | Node count of the latest network-attested state                       |
| `state_pending_blocks`      | ObservableGauge | Blocks buffered awaiting apply (bounded by `pendingBlocksMax`)        |
| `state_ready`               | ObservableGauge | `1` once catch-up has completed without halting, else `0`             |
| `state_apply_halted`        | ObservableGauge | `1` when block apply has halted (e.g. footer hash mismatch), else `0` |
| `state_apply_latency_ms`    | ObservableGauge | Wall-clock duration of the most recent block apply, in milliseconds   |
| `state_hash_mismatch_total` | LongCounter     | Cumulative footer hash mismatches observed                            |

`snapshot_latency_ms`/`catch_up_blocks_total`/`query_total{kind=...}` from an earlier draft of
this table are not implemented — snapshot creation and the query path don't exist yet in this
plugin (see Roadmap); add them alongside those features.

## 10. Failure modes

|                 Failure                  |                                                                                                                        Behaviour                                                                                                                         |
|------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Snapshot dir unreadable at startup (I/O) | Log at WARNING, continue with the eagerly-created genesis state; plugin is not failed.                                                                                                                                                                   |
| Snapshot root hash mismatch at startup   | On-disk snapshot's hash doesn't match persisted metadata's `stateRootHash` — treated the same as a block-apply hash mismatch below: halt rather than serve a stale/corrupt snapshot.                                                                     |
| `startOfBlockStateRootHash` mismatch     | Increment `hashMismatchTotal`, set `applyHalted=true`, log at ERROR. Apply loop short-circuits thereafter; further blocks ignored until restart.                                                                                                         |
| Malformed `state_changes` item           | `IllegalStateException` from the applier; plugin refuses to advance metadata for that block.                                                                                                                                                             |
| Verification gap                         | Block parks in `pendingBlocks` until the predecessor arrives (via notification or catch-up); apply loop retries from `historicalBlockProvider` on a delay. `pendingBlocks` is capped (`pendingBlocksMax`) so a long-lived gap can't grow it unboundedly. |
| Historical block missing locally         | Catch-up skips it; the same block can arrive via notification later.                                                                                                                                                                                     |
| Snapshot write fails (I/O)               | Log at WARNING; recent dir not deleted; next snapshot cycle retries.                                                                                                                                                                                     |
| Missing-queue query                      | gRPC handler defensively maps NPE from `getQueueState`/`getQueueAsList`/`peekQueue` to `NOT_FOUND`.                                                                                                                                                      |

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
   `hashMismatchTotal`, sets `applyHalted=true`.
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

## 12. Design decisions

**Apply loop is event-driven, not polled.** `handleVerification` stages into
the `ConcurrentSkipListMap` (ordering + gap handling) and wakes a single
apply-worker thread via a `BlockingQueue`; the worker drains under
`applyLock`. State changes are applied the moment a block arrives.

**`roundNumber` is sourced from `RoundHeader`.** Pulled directly from the
`RoundHeader` block item — every block carries at least one. The applier
tracks the last seen `RoundHeader.roundNumber()` during the block walk and
writes it into `StateMetadata` after apply completes. No timestamp
approximation.

**Historical state is explicitly out of scope** for this plugin — only the
current applied state is queryable.

**Snapshot layout nests under block number, not consensus round.** The CN
spec expects per-round directories (`<round>/data/state/`); this plugin
follows the same `data/state/` layout but nests under `<blockNumber>/`
since the Block Node doesn't track rounds the way the Consensus Node does.

## 13. References

- `hiero-consensus-node/platform-sdk/swirlds-state-api/README.md` — current API.
- `hiero-consensus-node/platform-sdk/swirlds-state-api/docs/state-snapshot-spec.md` — disk layout.
- `hiero-consensus-node/hedera-state-validator/src/main/java/com/hedera/statevalidation/blockstream/BlockStreamRecoveryWorkflow.java` — canonical `BinaryStateChangeApplier`.
