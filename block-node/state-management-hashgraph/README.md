# `state-management-hashgraph` plugin

> **Status:** *Beta.* Not yet shipped in the default chart manifest. The
> plugin jar is included only in opt-in deployments.

Live Hashgraph state on the Block Node. The plugin subscribes to verified
block-stream notifications, applies `state_changes` items to an in-memory
state store, periodically snapshots that store to disk, and exposes three
gRPC methods for clients that want to read network state without running a
Consensus Node.

## How it works

The state is owned by a `com.swirlds.state.merkle.VirtualMapStateLifecycleManager`
backed by a real `VirtualMapState` — the same lifecycle types the consensus
node uses. The plugin layers scheduling, hash validation, catch-up, snapshot
management, gRPC queries, and an SPI notification on top.

1. **Subscribe.** On `start()` the plugin registers as a
   `BlockNotificationHandler` and receives every `VerificationNotification`
   with `success == true`.
2. **Queue.** Each verified block is parked in a `ConcurrentSkipListMap` keyed
   by block number.
3. **Catch up.** A one-shot `catchUpFromHistoricalBlocks` task runs after
   `start()`. It compares `metadata.blockNumber` to
   `context.historicalBlockProvider().availableBlocks().max()` and pulls
   missing blocks via `block(n)` in batches of `historicCatchUpBatchSize`,
   enqueueing each into the pending map. The plugin reports `NOT_READY` on
   gRPC queries until catch-up completes.
4. **Apply.** A scheduled executor drains the pending map in strict
   block-number order. For each block:
   - `inspectBlock` walks items without mutating to grab `blockNumber`,
     `roundNumber` (last `RoundHeader` seen), and
     `BlockFooter.startOfBlockStateRootHash`.
   - The footer hash is compared to the current mutable state hash. On
     mismatch the plugin sets `degraded=true`, increments `hashMismatchTotal`,
     and refuses further applies.
   - On match, `StateChangeApplier` walks the items and translates every
     `state_changes` mutation into a call on the mutable `BinaryState`:
     | wire variant | BinaryState call |
     |---|---|
     | `SingletonUpdateChange` | `updateSingleton(stateId, bytes)` |
     | `MapUpdateChange`       | `updateKv(stateId, keyBytes, valueBytes)` |
     | `MapDeleteChange`       | `removeKv(stateId, keyBytes)` |
     | `QueuePushChange`       | `pushQueue(stateId, bytes)` |
     | `QueuePopChange`        | `popQueue(stateId)` |
   - `lifecycleManager.copyMutableState()` promotes the just-mutated state
     to the latest immutable copy.
5. **Track metadata.** After each apply the plugin builds a fresh
   `StateMetadata` (block number, round number, immutable root hash,
   leaf-count size) and emits a `StateUpdateNotification(VERIFIED, …)`.
6. **Snapshot.** A second scheduled executor calls `saveSnapshot()` every
   `state.management.snapshotIntervalMillis`. The snapshot calls
   `lifecycleManager.createSnapshot(latestImmutable, recent/<blockNumber>)`
   which writes the canonical consensus-node `data/state/` directory layout
   (see `swirlds-state-api/docs/state-snapshot-spec.md`); archives every
   older `<recent>/<other>` directory to `<historic>/<other>.tar` via
   `SnapshotArchiver` (hand-rolled UStar, mirrors block-base
   `TaredBlockIterator`); enforces `historicArchiveRetentionCount` by
   deleting the oldest tars when over threshold (mirrors
   `BlockFileHistoricPlugin.cleanup`); and rewrites `stateMetadata.json`
   atomically. A `StateUpdateNotification(SNAPSHOT, …)` follows.

## Storage encoding

All values written to `BinaryState` are PBJ-encoded carriers from
`com.hedera.hapi.block.stream.output`:

- Singletons store the bytes of `SingletonUpdateChange`.
- KV entries use `MapChangeKey` bytes as the key and `MapChangeValue`
  bytes as the value.
- Queues store the bytes of each `QueuePushChange`.

This keeps the apply path codec-free and gives clients a single, consistent
shape to send back through the gRPC reads.

## Configuration

Bound under `@ConfigData("state.management")` in `StateManagementConfig`:

| Property                          | Default                                                | Notes                                                                 |
|-----------------------------------|--------------------------------------------------------|-----------------------------------------------------------------------|
| `stateMetadataPath`               | `/opt/hiero/block-node/data/state/stateMetadata.json`  | JSON file with the latest metadata                                    |
| `stateSnapshotRecentPath`         | `/opt/hiero/block-node/data/state/snapshot/recent`     | Holds the most recent snapshot directory                              |
| `stateSnapshotHistoricPath`       | `/opt/hiero/block-node/data/state/snapshot/historic`   | Holds tar archives of older snapshots                                 |
| `snapshotIntervalMillis`          | `900000` (15 min)                                      | Rate of `saveSnapshot()`                                              |
| `stateChangesApplyIntervalMillis` | `2000` (2 s)                                           | Rate of the apply loop                                                |
| `historicCatchUpBatchSize`        | `64`                                                   | Blocks fetched per batch during start-up catch-up                     |
| `historicArchiveRetentionCount`   | `0`                                                    | Max historic tar archives to keep (`0` = unbounded)                   |

There is no `enabled` flag. Block-Node plugins are active whenever their jar
is on the classpath; opt-in lives in the deployment manifest.

## gRPC API

The plugin implements `org.hiero.block.api.StateServiceInterface`:

```
rpc getBinaryKV(BinaryStateQuery) returns (BinaryStateQueryResponse);
rpc getBinarySingleton(BinaryStateQuery) returns (BinaryStateQueryResponse);
rpc getBinaryQueue(BinaryStateQuery) returns (BinaryStateQueryResponse);
```

Request rules (enforced in code):

- `state_id` is required.
- `key_bytes` is required for `getBinaryKV`, forbidden for the other two.
- `queue_index` may be set for `getBinaryQueue` (`0` returns the whole queue).
- `block_number` is either `0` (latest) or must equal the current applied
  block; older block numbers are answered with `INVALID_REQUEST`.
- `NOT_READY` is returned until the start-up catch-up completes.

Every response carries the current `StateMetadata` so the client can decide
whether to retry against a different Block-Node or wait.

## Failure modes

- **State unreadable at startup.** A corrupt snapshot directory is logged at
  `WARNING` and the plugin continues with the eagerly-created genesis state;
  it does not refuse to start.
- **Hash mismatch.** When a block's `BlockFooter.startOfBlockStateRootHash`
  doesn't equal the current live root hash, the plugin increments
  `hashMismatchTotal`, sets `degraded=true`, logs at `ERROR`, and the apply
  loop short-circuits on `degraded` for every subsequent block.
- **Malformed `state_changes` item.** Raises `IllegalStateException` from the
  applier; the plugin logs and does **not** advance metadata.
- **Concurrent gap.** A block arriving out of order parks in the pending
  map until its predecessor lands. The apply loop never advances past a
  gap.
- **Missing queue / KV.** Queries map any underlying NPE from
  `VirtualMapStateImpl` (which throws on unknown state IDs) to `NOT_FOUND`
  rather than propagating as a gRPC `INTERNAL`.

## Limitations (v1)

- Latest applied state only; no historical state queries.
- Merkle proof RPCs (`getKvPath`, `getMerkleProof`) are not yet exposed
  through the plugin even though the backing `BinaryState` supports them.
- Hash-mismatch recovery requires operator intervention; the plugin stays
  degraded until restart and there is no automatic rewind/replay.
- Catch-up is sequential and synchronous within batches; very large catch-up
  windows block `ready=true` proportionally.
- Plugin metrics (`apply_latency_ms`, `pending_blocks`, etc.) are not yet
  wired through the metric registry; `hashMismatchTotal` is exposed only
  as an in-process counter.
- The plugin contributes swirlds-library config records via
  `configDataTypes()` — see the TODO above
  `StateManagementPlugin.configDataTypes()` and STORY-16.

## Roadmap

See the design doc, expansion notes, and ticket set under
`agent/proposals/live-state/tickets/` for the open follow-ups:

- **STORY-12** — trim swirlds dependency tree, surface upstream feedback.
- **STORY-16** — stop the plugin owning swirlds library config records.
- Hash-mismatch recovery protocol.
- Merkle proof RPC exposure.
- Metric registry integration.
- Strict genesis-block-0 gating (current implementation is permissive).
