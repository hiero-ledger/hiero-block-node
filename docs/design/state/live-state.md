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
                                                     │ gRPC clients & other plugins │
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
  uint64 queue_index = 5;        // 1-indexed; 0 or unset = full queue
}

message BinaryStateQueryResponse {
  enum Code {
    UNKNOWN          = 0;
    SUCCESS          = 1;
    INVALID_REQUEST  = 2;
    NOT_FOUND        = 3;       // requested key/singleton/queue index absent from the attested state
    NOT_READY        = 4;       // state still loading / catching up
    NOT_AVAILABLE    = 5;       // block_number isn't the attested block (no historical retention)
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

`NOT_AVAILABLE` mirrors `BlockAccessService.getBlock()`'s `NOT_AVAILABLE` for a block outside
the range this node currently holds. This plugin has no historical retention (§2/§7): the
*only* state it ever holds is the currently attested one, so any other requested `block_number`
is `NOT_AVAILABLE`, never `NOT_FOUND` (`NOT_FOUND` is reserved for "the attested state doesn't
have this key/singleton/queue index"). Whether apply has halted is an internal detail, not a
query concern: `retrieve_latest` or a `block_number` matching the attested block still answers
`SUCCESS` from that (now-frozen) state even while halted, since that state is still valid — only
requests for a block the node hasn't (or will never) reach are affected.

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
  `serviceBuilder.registerGrpcService(config.port(), this)` — the plugin **is** the
  service. A `null` `port` shares the default `server.port`; a configured port opens the
  `StateService` API on its own port.

### 6.2 `start()`

1. Load `stateMetadata.json`. If it doesn't exist, there's no persisted operator intent to
   honor — treat as genesis.
2. If it does exist and names block N: if a recent snapshot dir at `${recentPath}/N` exists and
   its hash matches the metadata, call `lifecycleManager.loadSnapshot(...)` to replace the eager
   genesis state with the on-disk state. If the snapshot dir is missing, unreadable, or its hash
   doesn't match the metadata, **halt** (`applyHalted=true`) rather than silently falling back to
   genesis — persisted metadata is the operator's recorded intent, and discarding it quietly
   would only surface later as a confusing hash mismatch on some unrelated block. The plugin
   still starts successfully (no exception, no shutdown request); it just never becomes `ready`
   and never applies or serves anything until a fixed snapshot is in place and the node restarts.
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
6. In `finally`, set `ready=true` **only if not `applyHalted`** — ready means catch-up finished
   *without* halting, not merely "catch-up attempt finished." This lets a monitoring consumer
   tell "healthy and caught up" apart from "caught up once, then halted" from `ready` alone,
   without also having to check `apply_halted`. A halt leaves the node `NOT_READY` permanently
   (until a restart) and logs a warning.

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
- **(1) ATTESTED** — `attested` (an `AttestedSnapshot(state, metadata)` record behind a single
  volatile field): the network-confirmed, query-serving copy and its `StateMetadata`, published
  together — the only state the query API reads (§8).

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
   - **Known gap, not yet handled:** a fresh node (no persisted metadata) whose *first* real
     block is N > 0, not 0 — e.g. a deployment pointed at a mid-chain `historicalBlockProvider`
     with no snapshot ever supplied — has no real seed state to validate against either; N's
     footer carries the hash of real pre-N state the node never had, which the empty-state
     seed can never match. This is a missing-seed-state condition, not a genuine hash mismatch,
     but is not currently distinguished from one (see §10). Needs a design decision before
     implementing; out of scope for this slice.
   - On mismatch: increment `hashMismatchTotal`, set `applyHalted=true`, log at ERROR, return
     without exposing anything. Further applies short-circuit on `applyHalted`.
     `hashMismatchTotal` is a counter rather than a boolean because it's cumulative over the
     node's lifetime: the hash-mismatch recovery protocol's bounded local retry can increment
     it more than once per incident before `applyHalted` is finally set, and a restarted node
     can hit an independent, later mismatch — `applyHalted` alone can't distinguish "never
     happened" from "happened repeatedly".
3. A match means N's footer attests post-(N-1). **Promote** state 2 → state 1 (`attested`)
   unless it is already exposed (e.g. just after a snapshot reload): publish the state and its
   just-built `StateMetadata` together as one `AttestedSnapshot`, in a single volatile write, so
   a reader can never observe a new state paired with the previous block's metadata or vice
   versa.
4. Apply N's `state_changes` via `StateChangeApplier.applyBlock(mutable, block)`.
5. `lifecycleManager.copyMutableState()` fast-copies/seals post-N as the new state 2
   (`hashingImmutable`); record `lastAppliedBlock=N`, `lastAppliedRound`. Post-N is left
   **un-hashed and un-exposed** until block N+1 attests it.

Reference counting: `attested.state()` is held across applies by reserving its root
(`copyMutableState()` releases the lifecycle manager's own reference to the superseded
version); the previously-held attested state is released when a new one is adopted. A node that
has applied only genesis has nothing attested (`attested.state() == null`) and reports
NOT_READY. Snapshots capture `attested.state()`.

This reservation is sufficient for the apply thread's own bookkeeping, but gives a *query*
reader no safety by itself: nothing today takes a reservation before reading `attested.state()`
from another thread, so a reader could race the next promotion's release. Not reachable yet —
no query path exists in this plugin slice — but see §8 for the contract the query-API PR must
add before reading `attested.state()` from any thread but the apply worker.

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

Token-relationship keys (`stateId == 9`) need no special-casing for the current wire format:
this plugin's pinned `state_changes.proto` carries that key as `AccountTokenAssociation`
(field 25), introduced upstream specifically so its field order/types match state's
`EntityIDPair` exactly — unlike the older `TokenAssociation` encoding (now reserved field 2)
that consensus-node's own reference applier still remaps for compatibility with older captured
chains.

This plugin does not remap field 2: `reserved` in protobuf is a compile-time guard against
accidental reuse, not a runtime wire-format check, so wire bytes still carrying the old field-2
encoding (a replayed pre-migration chain segment, or any producer still on the old encoding)
would otherwise pass through `StateChangeApplier` unrecognized as the legacy shape, writing
wrong state with no immediate error — surfacing only as a generic hash mismatch on some later
block, indistinguishable from any other cause. Instead, the applier detects field 2 on
`stateId == 9` specifically and halts immediately with a distinct "unsupported legacy
token-association encoding" error, rather than the generic hash-mismatch halt. This keeps the
applier's "no per-variant switch" simplicity for every other wire variant while still failing
fast and legibly for the one encoding this plugin deliberately does not support.

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

1. Reads `attested` (the `AttestedSnapshot` — state and metadata published together, §6.5).
   Returns `NOT_READY` if `attested.state() == null` (nothing has been attested yet — true
   genesis, no block fully confirmed). This is the *only* state this RPC ever reads: never the
   live mutable or the sealed-but-unattested `hashingImmutable`, which the apply worker is
   actively writing/sealing concurrently with no synchronization a query thread could safely
   join.
2. Returns `INVALID_REQUEST` if:
   - `getBinaryKV` is called without `key_bytes`, or with `queue_index`;
   - `getBinarySingleton` is called with `key_bytes` or `queue_index`;
   - `getBinaryQueue` is called with `key_bytes`.
3. Returns `NOT_AVAILABLE` if an explicit `block_number` was set and it doesn't match
   `attested.metadata().blockNumber()`. This holds regardless of `applyHalted`: a halted node's
   attested block is simply frozen, so requests for that exact block (or `retrieve_latest`)
   still succeed — only a `block_number` beyond it is affected, and halting makes that
   permanently true rather than transiently true. Block 0 is a valid, non-special `block_number`.
4. Delegates to `getKv` / `getSingleton` / `getQueueAsList` / `peekQueue` on `attested.state()`.
5. `getBinaryQueue` defensively maps any `RuntimeException` from
   `getQueueState`/`getQueueAsList`/`peekQueue` (which NPE on unknown
   state IDs in `VirtualMapStateImpl`) to `NOT_FOUND`.
6. Wraps the result in `BinaryStateQueryResponse` with `attested.metadata()` always populated —
   state and metadata come from the same `AttestedSnapshot` read, so a response can never pair
   one block's data with another block's metadata.

Reader reservation on `attested.state()`'s `VirtualMap` root is **not yet designed**: today
nothing reads it except the apply thread itself, so `setAttested`'s reserve/release pair is
race-free. Once this query path is implemented, a reader must take its own reservation
(`tryReserve()`, retrying if it loses the race) before use and release it after, or risk reading
a root the apply thread has since released on the next promotion. Define that contract when
building this RPC, not retrofit it after.

## 9. Metrics

Registered with the block node's `MetricRegistry` in `init()`, under the `blocknode:` prefix:

|           Metric            |      Type       |                                                                                         Description                                                                                         |
|-----------------------------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `state_applied_block`       | ObservableGauge | Latest committed (reader-visible) block number applied to live state                                                                                                                        |
| `state_size`                | ObservableGauge | Node count of the latest network-attested state                                                                                                                                             |
| `state_pending_blocks`      | ObservableGauge | Blocks buffered awaiting apply (bounded by `pendingBlocksMax`)                                                                                                                              |
| `state_ready`               | ObservableGauge | `1` once catch-up has completed without halting, else `0`                                                                                                                                   |
| `state_apply_halted`        | ObservableGauge | `1` when block apply has halted (e.g. footer hash mismatch), else `0`                                                                                                                       |
| `state_apply_latency_ms`    | ObservableGauge | Wall-clock duration of the most recent block apply, in milliseconds                                                                                                                         |
| `state_hash_mismatch_total` | LongCounter     | Cumulative footer hash mismatches observed                                                                                                                                                  |
| `state_block_lag`           | ObservableGauge | Blocks between the latest available block and the attested block; grows during a stall — alert on it independent of `state_apply_halted`, since a stall can also be a slow/stuck apply path |

`snapshot_latency_ms`/`catch_up_blocks_total`/`query_total{kind=...}` from an earlier draft of
this table are not implemented — snapshot creation and the query path don't exist yet in this
plugin (see Roadmap); add them alongside those features.

## 10. Failure modes

|                                          Failure                                           |                                                                                                                        Behaviour                                                                                                                         |
|--------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| No persisted metadata at startup                                                           | Genesis, cleanly — there's no operator intent to honor, so this is the normal first-boot path, not a failure.                                                                                                                                            |
| Persisted metadata names a block whose snapshot is missing, unreadable, or hash-mismatched | Halt (`applyHalted=true`), log at ERROR. Starting from genesis instead would silently discard the operator's recorded intent and surface only later as a confusing hash mismatch on an unrelated block. Plugin still starts; never becomes `ready`.      |
| `startOfBlockStateRootHash` mismatch                                                       | Increment `hashMismatchTotal`, set `applyHalted=true`, log at ERROR. Apply loop short-circuits thereafter; further blocks ignored until restart.                                                                                                         |
| First block after a fresh (no-metadata) start is N > 0, not 0                              | **Not yet handled** — no real seed state exists to validate N's footer against; currently indistinguishable from a genuine hash mismatch. See §6.5. Needs a design decision.                                                                             |
| Malformed `state_changes` item                                                             | `IllegalStateException` from the applier; plugin refuses to advance metadata for that block.                                                                                                                                                             |
| Legacy field-2 token-association encoding on a `stateId == 9` change                       | Halt with a distinct "unsupported legacy token-association encoding" error, not a generic hash mismatch — this plugin does not remap field 2 the way consensus-node's reference applier does. See §7.                                                   |
| Verification gap                                                                           | Block parks in `pendingBlocks` until the predecessor arrives (via notification or catch-up); apply loop retries from `historicalBlockProvider` on a delay. `pendingBlocks` is capped (`pendingBlocksMax`) so a long-lived gap can't grow it unboundedly. |
| Historical block missing locally                                                           | Catch-up skips it; the same block can arrive via notification later.                                                                                                                                                                                     |
| Snapshot write fails (I/O)                                                                 | Log at WARNING; recent dir not deleted; next snapshot cycle retries.                                                                                                                                                                                     |
| Missing-queue query                                                                        | gRPC handler defensively maps NPE from `getQueueState`/`getQueueAsList`/`peekQueue` to `NOT_FOUND`.                                                                                                                                                      |

**Recovery from a halt** is restart-from-snapshot, but *which* restart recovers depends on
whether the cause is transient or deterministic. A snapshot-hash-mismatch or missing-snapshot
halt is transient from the plugin's perspective and recovers once the underlying disk state is
fixed, but does **not** self-heal on restart alone — the same bad snapshot directory gets
reloaded and halts again immediately. For that cause, the operator must remove or replace the
snapshot directory before restarting. A deterministic cause — an apply bug, or an unhandled
wire encoding such as the field-2 case above — replays the exact same bytes for the exact same
block on every restart, so it does **not** self-heal either: recovery needs a fixed build (or a
snapshot taken past the bad block), not just a restart. Only a genuinely transient fault (e.g. a
footer hash mismatch caused by a passing I/O glitch rather than an actual code or encoding bug)
recovers from a plain restart. There is no distinct query-time signal for "halted" versus "healthy" today;
`state_apply_halted` and `state_ready` are the only observable signals (see §9). Internally the
halted/not-halted distinction doesn't change query behavior at all (§8) — a halted node still
answers queries for its (frozen) attested block with `SUCCESS`, since that state remains valid;
only requests for a block beyond it are affected, identically to a node that just hasn't caught
up yet.

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
